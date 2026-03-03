"""
Normalização de dados: balancete, PDF misto, clean_data, categorize_transactions.
Legado: o pipeline principal de extração é 100% LLM; clean_data/categorize_transactions
permanecem para scripts de diagnóstico e categorize_transactions é usado sobre o DataFrame já normalizado pela LLM.
"""
import re
import pandas as pd
from datetime import datetime
from typing import Optional

from . import text_utils
from .text_utils import (
    _parse_valor_monetario,
    _RE_VALOR_BR,
    _RE_VALOR_BR_COM_SINAL,
    _BALANCETE_CONTA,
    _BALANCETE_CREDITO,
    _BALANCETE_DEBITO,
    extract_condominio_name,
    extract_period_from_text,
)



def _try_normalize_balancete(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Detecta formato de balancete (Conta, Créditos, Débitos) nas primeiras linhas
    e converte para o padrão (data, descricao, tipo, valor).
    Retorna None se não reconhecer o formato.
    """
    if df is None or df.empty or len(df) < 2:
        return None
    ncols = len(df.columns)
    # Procurar linha de cabeçalho nas primeiras linhas
    for row_idx in range(min(10, len(df))):
        row = df.iloc[row_idx]
        col_conta = None
        col_credito = None
        col_debito = None
        for col_idx in range(ncols):
            try:
                cell = row.iloc[col_idx]
            except IndexError:
                continue
            cell_str = (str(cell).strip().lower() if pd.notna(cell) else "") or ""
            if not cell_str or len(cell_str) > 80:
                continue
            if any(k in cell_str for k in text_utils._BALANCETE_CONTA) and "condomínio" not in cell_str:
                col_conta = col_idx
            if any(k in cell_str for k in text_utils._BALANCETE_CREDITO):
                col_credito = col_idx
            if any(k in cell_str for k in text_utils._BALANCETE_DEBITO):
                col_debito = col_idx
        if col_conta is not None and (col_credito is not None or col_debito is not None):
            if col_credito is None:
                col_credito = col_debito  # usar mesma coluna
            if col_debito is None:
                col_debito = col_credito
            rows_out = []
            for r in range(row_idx + 1, len(df)):
                try:
                    conta = df.iloc[r, col_conta]
                    if pd.isna(conta) or str(conta).strip() == "":
                        continue
                    descricao = str(conta).strip()
                    # Se descricao é só "0", usar texto de outra coluna (ex.: detalhe "CONTR. PREVID. INSS...")
                    if descricao in ("0", "0.0"):
                        for c in range(ncols):
                            if c in (col_conta, col_credito, col_debito):
                                continue
                            cell = df.iloc[r, c]
                            if pd.notna(cell) and str(cell).strip() and len(str(cell).strip()) > 5:
                                descricao = str(cell).strip()[:500]
                                break
                    credito = text_utils._parse_valor_monetario(df.iloc[r, col_credito] if col_credito is not None else 0)
                    debito = text_utils._parse_valor_monetario(df.iloc[r, col_debito] if col_debito is not None else 0)
                except (IndexError, KeyError):
                    continue
                if credito and credito > 0:
                    rows_out.append({"data": datetime.now(), "descricao": descricao, "tipo": "receita", "valor": credito})
                if debito and debito > 0:
                    rows_out.append({"data": datetime.now(), "descricao": descricao, "tipo": "despesa", "valor": debito})
            if rows_out:
                out = pd.DataFrame(rows_out)
                print("[OK] Formato balancete (Conta/Creditos/Debitos) detectado e convertido para o padrao.")
                return out
    return None


def _infer_tipo_linha(line_lower: str, secao_atual: str) -> str:
    """Infere receita/despesa pela linha e pela seção do balancete (RECEBIMENTOS vs DESPESAS)."""
    if secao_atual == "despesa":
        return "despesa"
    if secao_atual == "receita":
        return "receita"
    if any(x in line_lower for x in ("recebimento", "receita", "saldo inicial", "total dos recebimentos", "rendimento", "aplicação", "taxa condominial", "cotas")):
        return "receita"
    if any(x in line_lower for x in ("despesa", "salário", "salario", "fgts", "inss", "irrf", "tributo", "luz", "água", "agua", "portaria", "manutenção", "seguro", "taxa ", "tarifa", "subtotal", "holerite", "contracheque", "proventos", "descontos", "folha", "gps", "grf", "esocial", "e-social")):
        return "despesa"
    return "despesa"


def _try_normalize_pdf_mixed_text(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Quando o PDF extrai uma única coluna com texto misto (ex.: balancete com RECEBIMENTOS e DESPESAS),
    extrai descrição e valor para o padrão (data, descricao, tipo, valor).
    Suporta valores negativos e usa contexto de seção (RECEBIMENTOS vs DESPESAS) para classificar tipo.
    Retorna None se não encontrar valores monetários no texto.
    """
    if df is None or df.empty:
        return None
    rows_out = []
    for col in df.columns:
        for idx in range(len(df)):
            cell = df.iloc[idx, df.columns.get_loc(col)]
            if pd.isna(cell):
                continue
            text = str(cell).strip()
            if not text or len(text) < 4:
                continue
            # Rastrear seção atual (receita vs despesa) para classificar linhas
            secao_atual = ""
            lines_all = text.replace("\\n", "\n").split("\n")
            for line in lines_all:
                line = line.strip()
                if not line:
                    continue
                line_lower = line.lower()
                # Atualizar seção ao encontrar títulos (sempre; assim linhas com valor na mesma linha são classificadas)
                if "recebimentos" in line_lower or "ordinária" in line_lower or "ordinaria" in line_lower:
                    secao_atual = "receita"
                if "despesas" in line_lower or "despesa " in line_lower:
                    secao_atual = "despesa"
                # Pular apenas linhas que são só título (sem valor), para não criar linha com descrição = título
                if (
                    ("recebimentos" in line_lower or "despesas" in line_lower or "despesa " in line_lower or "ordinária" in line_lower or "ordinaria" in line_lower)
                    and text_utils._RE_VALOR_BR.search(line) is None
                ):
                    continue
                # Ignorar linhas que são só cabeçalho (sem valor)
                if any(
                    x in line_lower
                    for x in (
                        "histórico valor total",
                        "historicovalortotal",
                        "condomínio:",
                        "total da conta",
                        "100,00%",
                        "página ",
                        "emitido em ",
                    )
                ) and text_utils._RE_VALOR_BR.search(line) is None:
                    continue
                # Procurar TODOS os valores na linha (PDF pode vir com vários itens na mesma linha)
                prev_end = 0
                for m in text_utils._RE_VALOR_BR_COM_SINAL.finditer(line):
                    valor_str = m.group(0)
                    is_neg = valor_str.startswith("-")
                    valor = text_utils._parse_valor_monetario(valor_str.lstrip("-"))
                    if is_neg:
                        valor = -abs(valor)
                    # Evitar capturar apenas percentuais (ex.: "46,22%" sozinho)
                    segmento = line[prev_end : m.start()]
                    if "%" in segmento and abs(valor) < 1000 and not any(c.isalpha() for c in segmento[-50:]):
                        prev_end = m.end()
                        continue
                    descricao = line[prev_end : m.start()].strip()
                    descricao = re.sub(r"\s*-?\d{1,3}(?:\.\d{3})*,\d{2}\s*%?\s*$", "", descricao).strip()
                    if not descricao or len(descricao) < 2:
                        descricao = "Item extraído do demonstrativo"
                    tipo = _infer_tipo_linha(line_lower, secao_atual)
                    rows_out.append(
                        {"data": datetime.now(), "descricao": descricao[:500], "tipo": tipo, "valor": valor}
                    )
                    prev_end = m.end()
    if rows_out:
        out = pd.DataFrame(rows_out)
        print("[OK] Formato PDF com texto misto (descricao + valor) detectado e convertido para o padrao.")
        return out
    return None


def clean_data(df: pd.DataFrame, metadata: Optional[dict] = None) -> pd.DataFrame:
    """
    Limpa e padroniza os dados financeiros (legado).
    O pipeline principal usa extração via LLM; use esta função apenas para diagnóstico ou testes.
    Se metadata for passado e o DataFrame tiver coluna 'texto_extraido' (PDF bruto),
    extrai o nome do condomínio ANTES de perder o texto e grava em metadata['condominio_name'].
    """
    try:
        # Fazer uma cópia para não modificar o DataFrame original
        df = df.copy()
        
        # Detectar DataFrames especiais (PDF escaneado, texto não estruturado, etc.)
        df_columns_lower = [col.lower() for col in df.columns]
        # Extrair nome do condomínio do texto bruto (PDF) antes de clean_data descartar a linha "Condomínio: NOME"
        if metadata is not None and "texto_extraido" in df_columns_lower:
            nome = text_utils.extract_condominio_name(df)
            if nome:
                metadata["condominio_name"] = nome
        
        # Verificar se é DataFrame de PDF escaneado ou texto não estruturado
        is_special_df = (
            'tipo_documento' in df_columns_lower or
            'pdf_escaneado' in df_columns_lower or
            'pdf_sem_dados_extraidos' in df_columns_lower or
            ('texto_extraido' in df_columns_lower and 'data' not in df_columns_lower and 'valor' not in df_columns_lower)
        )
        # Quando temos texto_extraido (PDF balancete), tentar extrair valores antes de tratar como especial
        if is_special_df and 'texto_extraido' in df_columns_lower:
            if metadata is not None:
                first_text = str(df.iloc[0, df.columns.get_loc('texto_extraido')]) if len(df) > 0 else ""
                period = text_utils.extract_period_from_text(first_text)
                if period.get("period_start"):
                    metadata["period_start"] = period["period_start"]
                if period.get("period_end"):
                    metadata["period_end"] = period["period_end"]
                if "_ocr_used" in df.columns:
                    metadata["ocr_used"] = bool(df["_ocr_used"].iloc[0])
                if "_ocr_text_len" in df.columns:
                    try:
                        metadata["ocr_text_len"] = int(df["_ocr_text_len"].iloc[0])
                    except Exception:
                        pass
            df_pdf = _try_normalize_pdf_mixed_text(df)
            if df_pdf is not None and not df_pdf.empty:
                df = df_pdf
                is_special_df = False
        if is_special_df:
            # DataFrame especial - criar colunas padrão para permitir processamento
            print("[AVISO] DataFrame especial detectado (PDF escaneado ou texto nao estruturado). Criando estrutura padrao.")
            
            # Criar DataFrame com estrutura padrão mas vazio (ou com dados mínimos)
            num_rows = len(df)
            if num_rows == 0:
                num_rows = 1  # Garantir pelo menos uma linha
            
            # Extrair informações do DataFrame original
            descricao_base = 'Documento processado mas sem dados estruturados'
            if 'mensagem' in df.columns and len(df) > 0:
                descricao_base = str(df['mensagem'].iloc[0])
            elif 'texto_extraido' in df.columns and len(df) > 0:
                texto = str(df['texto_extraido'].iloc[0])
                # Limitar tamanho da descrição
                descricao_base = texto[:200] if len(texto) > 200 else texto
            
            # Criar DataFrame com colunas essenciais preenchidas com valores padrão
            df_standard = pd.DataFrame({
                'data': [datetime.now()] * num_rows,
                'descricao': [descricao_base] * num_rows,
                'tipo': ['despesa'] * num_rows,  # Padrão: despesa
                'valor': [0.0] * num_rows  # Valor zero para documentos sem dados financeiros
            })
            
            # Adicionar informações originais como metadados nas colunas de descrição
            if 'tipo_documento' in df.columns and len(df) > 0:
                tipo_doc = str(df['tipo_documento'].iloc[0])
                df_standard['descricao'] = df_standard['descricao'].astype(str) + f' [Tipo: {tipo_doc}]'
            
            if 'total_paginas' in df.columns and len(df) > 0:
                total_pag = int(df['total_paginas'].iloc[0]) if pd.notna(df['total_paginas'].iloc[0]) else 0
                df_standard['descricao'] = df_standard['descricao'].astype(str) + f' [Páginas: {total_pag}]'
            
            print(f"[OK] DataFrame especial convertido: {len(df_standard)} linha(s) com estrutura padrao")
            df = df_standard
        else:
            # Tentar normalizar formato balancete (Conta, Créditos, Débitos) só quando ainda não temos 'valor' preenchido
            # (evita reaplicar em DataFrame já combinado ou já normalizado)
            cols_lower = [str(c).lower() for c in df.columns]
            has_valor_col = "valor" in cols_lower
            valor_sum = 0.0
            if has_valor_col:
                try:
                    v = pd.to_numeric(df["valor"], errors="coerce").fillna(0).astype(float)
                    valor_sum = float(v.sum())
                except Exception:
                    pass
            if not has_valor_col or valor_sum == 0:
                # Se já temos colunas tipo Conta + Créditos/Débitos (ex.: Excel com header correto), não converter
                def _norm_col(s: str) -> str:
                    s = (s or "").lower().replace("é", "e").replace("ê", "e").replace("á", "a").replace("ú", "u")
                    return s
                has_conta = any("conta" in _norm_col(str(c)) for c in df.columns)
                has_deb_cred = any(
                    "credito" in _norm_col(str(c)) or "debito" in _norm_col(str(c)) for c in df.columns
                )
                if not (has_conta and has_deb_cred):
                    df_balancete = _try_normalize_balancete(df)
                    if df_balancete is not None:
                        df = df_balancete
                    else:
                        df_pdf_mixed = _try_normalize_pdf_mixed_text(df)
                        if df_pdf_mixed is not None:
                            df = df_pdf_mixed

        # Renomear colunas para um padrão consistente (ex: minúsculas, sem espaços)
        # Remover caracteres especiais e normalizar
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('%', '').str.replace('.', '_')

        # Tentar mapear colunas comuns para o formato esperado (PDF, Excel, ODS, CSV)
        column_mapping = {}
        for col in df.columns:
            col_lower = col.lower()
            if 'data' in col_lower or 'date' in col_lower or 'dt' in col_lower:
                column_mapping[col] = 'data'
            elif ('descricao' in col_lower or 'desc' in col_lower or 'descrição' in col_lower or
                  'descricão' in col_lower or 'conta' in col_lower or 'historico' in col_lower or 'histórico' in col_lower):
                column_mapping[col] = 'descricao'
            elif 'tipo' in col_lower or 'type' in col_lower:
                column_mapping[col] = 'tipo'
            elif 'valor' in col_lower or 'value' in col_lower or 'amount' in col_lower or 'vlr' in col_lower:
                column_mapping[col] = 'valor'
        if column_mapping:
            df.rename(columns=column_mapping, inplace=True)
        # Excel/ODS com Débito e Crédito mas sem coluna "valor": criar valor e tipo a partir deles
        cols_after = [str(c).lower() for c in df.columns]
        if 'valor' not in cols_after and ('descricao' in cols_after or 'descricao' in df.columns):
            deb_col = None
            cred_col = None
            for c in df.columns:
                cl = str(c).lower()
                if cl in ('débito', 'debito', 'débitos', 'debitos') or 'debito' in cl.replace('é', 'e'):
                    deb_col = c
                if cl in ('crédito', 'credito', 'créditos', 'creditos') or 'credito' in cl.replace('é', 'e'):
                    cred_col = c
            if deb_col is not None or cred_col is not None:
                # Em planilhas com células mescladas, o valor pode estar na coluna ao lado (ex.: Unnamed: 7/9)
                col_list = list(df.columns)
                deb_idx = col_list.index(deb_col) if deb_col is not None else None
                cred_idx = col_list.index(cred_col) if cred_col is not None else None

                def _valor_col(row_idx: int, col_name, col_index: Optional[int]) -> float:
                    if col_index is None:
                        return 0.0
                    v = text_utils._parse_valor_monetario(df.iloc[row_idx][col_name])
                    if v != 0:
                        return v
                    if col_index + 1 < len(col_list):
                        v2 = text_utils._parse_valor_monetario(df.iloc[row_idx][col_list[col_index + 1]])
                        if v2 != 0:
                            return v2
                    return 0.0

                rows_out = []
                desc_col = 'descricao' if 'descricao' in df.columns else None
                for idx in range(len(df)):
                    descricao = str(df.iloc[idx][desc_col]) if desc_col else "Lançamento"
                    # Se descricao é vazia ou só "0", usar texto de outras colunas (ex.: Unnamed: 3 com "CONTR. PREVID. INSS...")
                    if not descricao or descricao.strip() in ('0', 'nan', ''):
                        parts = []
                        for c in df.columns:
                            if c in (desc_col, deb_col, cred_col):
                                continue
                            v = df.iloc[idx][c]
                            if pd.notna(v) and str(v).strip() and str(v).strip() not in ('0', 'nan'):
                                parts.append(str(v).strip())
                        if parts:
                            descricao = " ".join(parts)[:500]
                    deb = _valor_col(idx, deb_col, deb_idx) if deb_col is not None else 0.0
                    cred = _valor_col(idx, cred_col, cred_idx) if cred_col is not None else 0.0
                    row_data = df.iloc[idx].get("data", datetime.now()) if "data" in df.columns else datetime.now()
                    # Se débito e crédito iguais e descrição parece encargo (INSS, IRRF, FGTS, PIS, etc.), tratar só como despesa
                    desc_upper = descricao.upper()
                    encargo_like = any(
                        k in desc_upper for k in ("INSS", "IRRF", "FGTS", "PIS", "CONTRIB", "SINDICAL", "PREVID", "RECOLHIMENTO")
                    )
                    if encargo_like and deb == cred and deb > 0:
                        cred = 0.0
                    if cred > 0:
                        rows_out.append({"data": row_data, "descricao": descricao[:500], "tipo": "receita", "valor": cred})
                    if deb > 0:
                        rows_out.append({"data": row_data, "descricao": descricao[:500], "tipo": "despesa", "valor": deb})
                if rows_out:
                    df = pd.DataFrame(rows_out)
                    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('%', '').str.replace('.', '_')

        # Garantir que as colunas essenciais existam
        required_columns = ['data', 'descricao', 'tipo', 'valor']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            # Se ainda faltam colunas, criar com valores padrão
            print(f"[AVISO] Colunas faltando: {missing_columns}. Criando com valores padrao.")
            num_rows = len(df) if not df.empty else 1

            if 'data' not in df.columns:
                df['data'] = pd.to_datetime([datetime.now()] * num_rows)
            if 'descricao' not in df.columns:
                df['descricao'] = ['Transação sem descrição'] * num_rows
            if 'tipo' not in df.columns:
                df['tipo'] = ['despesa'] * num_rows
            if 'valor' not in df.columns:
                df['valor'] = [0.0] * num_rows

        # Converter 'data' para datetime
        df['data'] = pd.to_datetime(df['data'], errors='coerce')

        # Preencher valores ausentes em 'valor' com 0 e garantir tipo numérico
        # Garantir que trabalhamos com Series antes de usar fillna()
        valor_series = pd.to_numeric(df['valor'], errors='coerce')
        if not isinstance(valor_series, pd.Series):
            valor_series = pd.Series(valor_series)
        df['valor'] = valor_series.fillna(0).astype(float)

        # Validar tipos de transação
        tipos_validos = ['receita', 'despesa']
        df['tipo'] = df['tipo'].str.lower().str.strip()
        tipos_invalidos = df[~df['tipo'].isin(tipos_validos)]
        if not tipos_invalidos.empty:
            # Garantir que trabalhamos com Series antes de usar unique()
            tipo_series = tipos_invalidos['tipo']
            if isinstance(tipo_series, pd.Series):
                tipos_unicos = tipo_series.unique().tolist()
            else:
                # Se não for Series, converter para lista
                tipos_unicos = list(set(tipo_series)) if hasattr(tipo_series, '__iter__') else []
            print(f"Aviso: Encontrados tipos inválidos: {tipos_unicos}")
            # Converter tipos inválidos para 'despesa' por padrão
            df.loc[~df['tipo'].isin(tipos_validos), 'tipo'] = 'despesa'

        # Remover linhas com datas inválidas ou valores nulos essenciais
        linhas_antes = len(df)
        df.dropna(subset=['data', 'descricao', 'tipo'], inplace=True)
        linhas_depois = len(df)
        
        if linhas_antes != linhas_depois:
            print(f"Aviso: {linhas_antes - linhas_depois} linhas foram removidas por dados inválidos.")

        # Adicionar colunas para detecção de anomalias, se ainda não existirem
        if 'anomalia_detectada' not in df.columns:
            df['anomalia_detectada'] = False
        if 'justificativa_anomalia' not in df.columns:
            df['justificativa_anomalia'] = ''
        if 'categoria' not in df.columns:
            df['categoria'] = 'Não Categorizado'

        return df
        
    except Exception as e:
        raise ValueError(f"Erro ao limpar os dados: {e}")

def categorize_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Categoriza transações com base em palavras-chave na descrição."""
    # Dicionário de palavras-chave para categorias. Pode ser expandido.
    # As chaves são as categorias e os valores são listas de palavras-chave.
    category_keywords = {
        'Taxas Condominiais': ['taxa condominial', 'condominio', 'cota'],
        'Água': ['agua', 'saneamento'],
        'Eletricidade': ['luz', 'energia', 'eletricidade'],
        'Salários': ['salario', 'zelador', 'porteiro', 'funcionario', 'rh'],
        'Manutenção': ['manutencao', 'reparo', 'conserto', 'jardim', 'piscina'],
        'Material de Limpeza': ['limpeza', 'material de limpeza'],
        'Segurança': ['seguranca', 'vigilancia', 'alarme'],
        'Administração': ['administracao', 'honorarios', 'contabilidade'],
        'Obras': ['obra', 'reforma', 'construcao'],
        'Impostos e Taxas': ['imposto', 'taxa', 'tributo', 'iptu'],
        'Outras Receitas': ['aluguel', 'multa', 'juros'],
        'Outras Despesas': [] # Catch-all para despesas não categorizadas
    }

    # Função auxiliar para categorizar uma descrição
    def assign_category(description: str, transaction_type: str) -> str:
        description_lower = str(description).lower()
        for category, keywords in category_keywords.items():
            for keyword in keywords:
                if keyword in description_lower:
                    return category
        
        # Se não encontrar palavras-chave, categoriza como 'Outras Receitas' ou 'Outras Despesas'
        if transaction_type.lower() == 'receita':
            return 'Outras Receitas'
        else:
            return 'Outras Despesas'

    df['categoria'] = df.apply(lambda row: assign_category(row['descricao'], row['tipo']), axis=1)
    return df
