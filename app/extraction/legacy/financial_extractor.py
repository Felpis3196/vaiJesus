"""
Extrator Financeiro Mensal Estrito
Extrai dados financeiros mensais aplicando regras rigorosas para evitar falsos positivos.

ETAPA 1: EXTRAÇÃO (NUNCA FALHA)
REGRA 1: Holerite só existe com funcionário (nome pessoa física + evento folha + valor compatível)
REGRA 2: Linhas "TOTAIS" são sempre bloqueadas
REGRA 3: Créditos só de entrada real de dinheiro
REGRA 4: Débitos = soma de despesas individuais
REGRA 5: Valores gigantes (>5x maior débito) = acumulado
REGRA 6: Saldo final só se linha explícita "SALDO FINAL"
REGRA 7: Saída obrigatória estruturada com arrays
"""
import re
import logging
import pandas as pd
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

def _normalize_monetary_value(valor: Any) -> Optional[float]:
    """
    Extrai valor monetário com regra única: formato BR (ponto = milhar, vírgula = decimal).
    Retorna None se não conseguir parsear (NÃO APURADO).
    """
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None
    
    try:
        if isinstance(valor, str):
            valor_clean = valor.replace('R$', '').replace('$', '').strip()
            if not valor_clean:
                return None
            valor_clean = valor_clean.replace('.', '').replace(',', '.')
            valor_float = float(valor_clean)
        else:
            valor_float = float(valor)
        return round(valor_float, 2)
    except (ValueError, TypeError):
        return None


def _normalize_text(text: Any) -> str:
    """Normaliza texto para comparação."""
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    return str(text).lower().strip()


def _is_total_line_strict(row: pd.Series) -> bool:
    """
    REGRA 2: Se nome da linha contém TOTAIS/TOTAL/TOTAL GERAL/RESUMO, tratar como linha de total.
    """
    desc = str(row.get("descricao", "")).strip().upper()
    bloqueios = ["TOTAIS", "TOTAL GERAL", "RESUMO"]
    # Exato ou descrição contém a palavra (ex.: "Link TOTAIS - Período: 5313-33")
    if desc in bloqueios or desc == "TOTAL":
        return True
    return any(b in desc for b in bloqueios) or desc.startswith("TOTAL")


def _is_valid_holerite(row: pd.Series) -> bool:
    """
    REGRA 1: Holerite só existe com funcionário.
    Retorna True APENAS se TODOS existirem:
    - nome de pessoa física (não "01-ORDINARIA", não "AGUA", etc.)
    - evento explícito de folha (salário, férias, 13º)
    - valor compatível com salário (500-50000)
    - NÃO contém palavras de despesa/receita
    """
    desc = str(row.get("descricao", "")).lower()
    
    # PROIBIÇÃO: Se contém palavras de despesa/receita, NUNCA é holerite
    palavras_proibidas = [
        "agua", "energia", "administracao", "elevadores", "seguranca",
        "conservacao", "receita", "devedores", "totais", "fundo", "ordinaria"
    ]
    if any(palavra in desc for palavra in palavras_proibidas):
        return False
    
    # Verificar nome de pessoa física (deve ter pelo menos 2 palavras, começar com maiúscula)
    nome_cols = ["descricao", "funcionario", "nome"]
    tem_nome_pessoa = False
    for col in nome_cols:
        if col in row.index:
            val = str(row[col]).strip()
            palavras = val.split()
            if len(palavras) >= 2 and palavras[0][0].isupper():
                # Verificar se não é número ou código
                if not re.match(r"^\d+", palavras[0]) and palavras[0].lower() not in palavras_proibidas:
                    # Verificar se não começa com código como "01-ORDINARIA"
                    if not re.match(r"^\d+[-\.]", val):
                        tem_nome_pessoa = True
                        break
    
    # Verificar evento de folha
    eventos_folha = ["salario", "salário", "ferias", "férias", "13º", "decimo terceiro", "décimo terceiro"]
    tem_evento = any(evento in desc for evento in eventos_folha)
    
    # Verificar valor compatível
    valor = _normalize_monetary_value(row.get("valor", 0))
    valor_compativel = valor is not None and 500 <= valor <= 50000
    
    return tem_nome_pessoa and tem_evento and valor_compativel


def _is_valid_receita_mensal(row: pd.Series) -> bool:
    """
    REGRA 3: Receitas mensais SÓ de entrada real de dinheiro.
    NUNCA extrair: RECEITA PREVISTA, DEVEDORES, SALDO ANTERIOR, TOTAIS
    
    MELHORIA: Aceitar mais tipos de receitas para garantir extração completa.
    """
    # Se já foi bloqueada como TOTAL, não é receita válida
    if _is_total_line_strict(row):
        return False
    
    desc = str(row.get("descricao", "")).lower()
    
    # Bloquear palavras proibidas (valores acumulados, previstos, etc.)
    bloqueios = [
        "receita prevista", "receita previsão", "previsão",
        "devedores", "saldo anterior", "totais", "total geral",
        "acumulado", "histórico", "total histórico"
    ]
    if any(bloqueio in desc for bloqueio in bloqueios):
        return False
    
    # Verificar se há coluna "tipo" ou "credito" que indique receita
    if "tipo" in row.index:
        tipo_val = str(row.get("tipo", "")).lower().strip()
        if tipo_val in ["receita", "credito", "crédito", "entrada", "recebimento"]:
            logger.debug(f"[EXTRACTION] Receita válida por coluna 'tipo': {tipo_val}")
            return True
    
    if "credito" in row.index or "crédito" in row.index:
        credito_val = _normalize_monetary_value(row.get("credito") or row.get("crédito"))
        if credito_val is not None and credito_val > 0:
            logger.debug(f"[EXTRACTION] Receita válida por coluna 'credito': R$ {credito_val:,.2f}")
            return True
    
    # Verificar se há valor positivo na coluna "valor" e descrição não é bloqueada
    valor = _normalize_monetary_value(row.get("valor", 0))
    if valor is not None and valor > 0:
        # Se tem valor positivo e não é bloqueado, pode ser receita
        # Expandir lista de palavras permitidas
        permitidos = [
            "taxa condominial", "arrecadacao", "arrecadação",
            "recebimentos", "recebido", "entrada", "receita",
            "taxa", "condominial", "multa", "juros", "rendimento",
            "cobrança", "cobranca", "pagamento", "recebimento",
            "taxa de condomínio", "taxa de condominio",
            "ordinária", "ordinaria", "extraordinária", "extraordinaria"
        ]
        # Se descrição contém palavra permitida OU descrição está vazia mas valor existe
        if any(permitido in desc for permitido in permitidos) or (not desc.strip() and valor > 0):
            logger.debug(f"[EXTRACTION] Receita válida por descrição/permitido: '{desc[:50]}' - R$ {valor:,.2f}")
            return True
    
    return False


def _is_valid_despesa_mensal(row: pd.Series) -> bool:
    """
    REGRA 4: Débitos mensais = despesas individuais.
    Cada linha = uma despesa. Nunca usar totais prontos.
    """
    # Se já foi bloqueada como TOTAL, não é despesa válida
    if _is_total_line_strict(row):
        return False
    
    desc = str(row.get("descricao", "")).lower()
    
    # Bloquear totais
    if "total" in desc or "totais" in desc:
        return False
    
    # Permitir despesas individuais (qualquer linha que não seja TOTAL)
    return True


def _detect_accumulated_value(valor: float, debitos_individuals: List[float]) -> bool:
    """
    REGRA 5: Detecta valores acumulados usando múltiplos critérios:
    1. Se valor > 5x maior débito individual
    2. Se valor > 1.000.000 (típico de acumulados)
    3. Se valor > 10x média dos débitos individuais
    """
    if valor <= 0:
        return False
    
    # Critério 1: Valor muito grande (> 1 milhão) é quase sempre acumulado
    if valor > 1_000_000:
        return True
    
    if not debitos_individuals:
        # Sem referência, usar apenas critério de magnitude
        return valor > 1_000_000
    
    maior_debito = max(debitos_individuals)
    media_debitos = sum(debitos_individuals) / len(debitos_individuals) if debitos_individuals else 0
    
    # Critério 2: > 5x maior débito individual
    if valor > (maior_debito * 5):
        return True
    
    # Critério 3: > 10x média dos débitos
    if media_debitos > 0 and valor > (media_debitos * 10):
        return True
    
    return False


def _extract_saldo_final_explicit(df: pd.DataFrame) -> Optional[float]:
    """
    REGRA 6: Saldo final SÓ se existir linha explícita "SALDO FINAL".
    """
    for idx, row in df.iterrows():
        desc = str(row.get("descricao", "")).upper()
        if "SALDO FINAL" in desc:
            valor = _normalize_monetary_value(row.get("valor", 0))
            return valor
    return None


def _extract_totais_as_debito_fallback(
    df: pd.DataFrame,
    despesas_individuals: List[float]
) -> Optional[float]:
    """
    REGRA 4.3: Se não houver outra forma de obter total das despesas,
    usar linha "TOTAIS" como DÉBITO (nunca como crédito).
    """
    if despesas_individuals:  # Se já tem despesas individuais, não usar TOTAIS
        return None
    
    for idx, row in df.iterrows():
        desc = str(row.get("descricao", "")).upper()
        if desc in ["TOTAIS", "TOTAL", "TOTAL GERAL"]:
            valor = _normalize_monetary_value(row.get("valor", 0))
            if valor is not None and valor > 0:
                return valor
    return None


def _classify_column_purpose(
    df: pd.DataFrame,
    column_name: str
) -> str:
    """
    REGRA 2.1: Classifica coluna como:
    MOVIMENTACAO_MENSAL | SALDO_ACUMULADO | TOTAL_HISTORICO | PREVISAO | DESCONHECIDA
    """
    from services.financial_base_validator import identify_column_purpose
    
    if column_name not in df.columns:
        return "DESCONHECIDA"
    
    sample_values = []
    try:
        col_data = df[column_name].dropna()
        if len(col_data) > 0:
            numeric_values = pd.to_numeric(col_data, errors="coerce").dropna()
            if len(numeric_values) > 0:
                sample_values = numeric_values.head(10).tolist()
    except Exception:
        pass
    
    result = identify_column_purpose(df, column_name, sample_values)
    purpose = result.get("purpose", "indefinido")
    
    # Mapear para enum da REGRA 2.1
    mapping = {
        "mensal": "MOVIMENTACAO_MENSAL",
        "acumulado": "SALDO_ACUMULADO",
        "total_historico": "TOTAL_HISTORICO",
        "previsao": "PREVISAO",
        "auxiliar": "DESCONHECIDA",
        "indefinido": "DESCONHECIDA"
    }
    
    return mapping.get(purpose, "DESCONHECIDA")


def extract_monthly_financial_data(
    df: pd.DataFrame,
    sheet_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    ETAPA 1: EXTRAÇÃO (NUNCA FALHA)
    Sempre extrai valores, mesmo sem condomínio/período identificados.
    
    REGRA 1.1: Sempre extrair valores, nunca abortar.
    REGRA 1.2: Origem obrigatória completa (linha, coluna, aba, tipo preliminar).
    REGRA 2.1: Classificação de colunas (MOVIMENTACAO_MENSAL, etc.).
    
    Args:
        df: DataFrame com transações financeiras
        sheet_name: Nome da aba (opcional)
        
    Returns:
        Dict com estrutura obrigatória (REGRA 7):
        {
            "receitas_mensais_extraidas": [{"descricao": str, "valor": float, "origem": Dict}],
            "despesas_mensais_extraidas": [{"descricao": str, "valor": float, "origem": Dict}],
            "linhas_ignoradas": [{"descricao": str, "razao": str}],
            "holerites_extraidos": [{"funcionario": str, "valor": float, "origem": Dict}],
            "saldo_final_extraido": Optional[float],
            "column_classifications": Dict[str, str],  # NOVO: classificação de colunas
            "metadata": {...}
        }
    """
    if df is None or df.empty:
        return {
            "receitas_mensais_extraidas": [],
            "despesas_mensais_extraidas": [],
            "linhas_ignoradas": [],
            "holerites_extraidos": [],
            "saldo_final_extraido": None,
            "column_classifications": {},
            "metadata": {
                "total_linhas_processadas": 0,
                "linhas_bloqueadas_totais": 0,
                "linhas_bloqueadas_receita_prevista": 0,
                "valores_acumulados_detectados": 0
            }
        }
    
    # REGRA 2.1: Classificar colunas
    column_classifications = {}
    valor_col = None
    for col in df.columns:
        classification = _classify_column_purpose(df, col)
        column_classifications[col] = classification
        if classification == "MOVIMENTACAO_MENSAL" and valor_col is None:
            # Detectar coluna de valor (pode ser "valor", "credito", "debito", etc.)
            if col.lower() in ["valor", "credito", "debito", "receita", "despesa"]:
                valor_col = col
    if valor_col is None and "valor" in df.columns:
        valor_col = "valor"
    
    receitas_mensais = []
    despesas_mensais = []
    linhas_ignoradas = []
    holerites_extraidos = []
    
    # NOVO: Coletar linhas TOTAL separadamente para priorização
    total_receitas = []
    total_despesas = []
    
    metadata = {
        "total_linhas_processadas": len(df),
        "linhas_bloqueadas_totais": 0,
        "linhas_bloqueadas_receita_prevista": 0,
        "valores_acumulados_detectados": 0
    }
    
    # Primeira passada: coletar despesas individuais para REGRA 5 (excluir tabela explicativa)
    debitos_individuals = []
    for idx, row in df.iterrows():
        if "_apenas_explicativo" in df.columns and row.get("_apenas_explicativo") is True:
            continue
        if _is_valid_despesa_mensal(row):
            valor = _normalize_monetary_value(row.get("valor", 0))
            if valor is not None and valor > 0:
                debitos_individuals.append(valor)
    
    logger.info(f"[EXTRACTION] Despesas individuais coletadas para validação: {len(debitos_individuals)} itens")
    
    # Segunda passada: classificar cada linha
    for idx, row in df.iterrows():
        # Não incluir linhas de tabela explicativa (encargos) como receita/despesa do resumo
        if "_apenas_explicativo" in df.columns and row.get("_apenas_explicativo") is True:
            continue
        desc = str(row.get("descricao", ""))
        
        # Detectar coluna de valor (priorizar colunas específicas de crédito/débito)
        valor = None
        coluna_valor_usada = None
        
        # Prioridade 1: Coluna específica de crédito (para receitas)
        if "credito" in df.columns or "crédito" in df.columns:
            credito_col = "credito" if "credito" in df.columns else "crédito"
            credito_val = _normalize_monetary_value(row.get(credito_col, 0))
            if credito_val is not None and credito_val > 0:
                valor = credito_val
                coluna_valor_usada = credito_col
        
        # Prioridade 2: Coluna específica de débito (para despesas)
        if valor is None and ("debito" in df.columns or "débito" in df.columns):
            debito_col = "debito" if "debito" in df.columns else "débito"
            debito_val = _normalize_monetary_value(row.get(debito_col, 0))
            if debito_val is not None and debito_val > 0:
                valor = debito_val
                coluna_valor_usada = debito_col
        
        # Prioridade 3: Coluna "valor" genérica
        if valor is None:
            coluna_valor_usada = valor_col or "valor"
            if coluna_valor_usada in row.index:
                valor = _normalize_monetary_value(row.get(coluna_valor_usada, 0))
            else:
                # Tentar encontrar coluna numérica
                for col in df.columns:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        valor = _normalize_monetary_value(row.get(col, 0))
                        coluna_valor_usada = col
                        break
        
        # REGRA 1.2: Origem obrigatória completa
        tipo_preliminar = "desconhecido"
        
        # REGRA 2: Detectar linhas TOTAIS e coletar separadamente para priorização
        if _is_total_line_strict(row):
            tipo_preliminar = "total"
            # Verificar se a linha TOTAL tem tipo "receita" ou "despesa" e coletar o valor
            tipo_val = str(row.get("tipo", "")).lower().strip()
            if valor is not None and valor > 0:
                if tipo_val in ["receita", "credito", "crédito"]:
                    total_receitas.append({
                        "descricao": desc,
                        "valor": valor,
                        "origem": {
                            "linha": int(idx),
                            "descricao": desc,
                            "coluna_valor": coluna_valor_usada,
                            "aba": sheet_name or "desconhecida",
                            "tipo_preliminar": "total_receita"
                        }
                    })
                    logger.info(f"[EXTRACTION] Linha TOTAL de RECEITAS encontrada: R$ {valor:,.2f}")
                elif tipo_val in ["despesa", "debito", "débito"]:
                    total_despesas.append({
                        "descricao": desc,
                        "valor": valor,
                        "origem": {
                            "linha": int(idx),
                            "descricao": desc,
                            "coluna_valor": coluna_valor_usada,
                            "aba": sheet_name or "desconhecida",
                            "tipo_preliminar": "total_despesa"
                        }
                    })
                    logger.info(f"[EXTRACTION] Linha TOTAL de DESPESAS encontrada: R$ {valor:,.2f}")
                else:
                    # Se não tem tipo explícito, verificar se tem valor em coluna de crédito ou débito
                    if "credito" in df.columns or "crédito" in df.columns:
                        credito_col = "credito" if "credito" in df.columns else "crédito"
                        credito_val = _normalize_monetary_value(row.get(credito_col, 0))
                        if credito_val is not None and credito_val > 0:
                            total_receitas.append({
                                "descricao": desc,
                                "valor": credito_val,
                                "origem": {
                                    "linha": int(idx),
                                    "descricao": desc,
                                    "coluna_valor": credito_col,
                                    "aba": sheet_name or "desconhecida",
                                    "tipo_preliminar": "total_receita"
                                }
                            })
                            logger.info(f"[EXTRACTION] Linha TOTAL de RECEITAS encontrada (coluna crédito): R$ {credito_val:,.2f}")
                    if "debito" in df.columns or "débito" in df.columns:
                        debito_col = "debito" if "debito" in df.columns else "débito"
                        debito_val = _normalize_monetary_value(row.get(debito_col, 0))
                        if debito_val is not None and debito_val > 0:
                            total_despesas.append({
                                "descricao": desc,
                                "valor": debito_val,
                                "origem": {
                                    "linha": int(idx),
                                    "descricao": desc,
                                    "coluna_valor": debito_col,
                                    "aba": sheet_name or "desconhecida",
                                    "tipo_preliminar": "total_despesa"
                                }
                            })
                            logger.info(f"[EXTRACTION] Linha TOTAL de DESPESAS encontrada (coluna débito): R$ {debito_val:,.2f}")
            metadata["linhas_bloqueadas_totais"] += 1
            continue
        
        # REGRA 1: Verificar se é holerite válido
        if _is_valid_holerite(row):
            tipo_preliminar = "holerite"
            funcionario = str(row.get("funcionario", row.get("nome", desc))).strip()
            origem_completa = {
                "linha": int(idx),
                "descricao": desc,
                "coluna_valor": coluna_valor_usada,
                "aba": sheet_name or "desconhecida",
                "tipo_preliminar": tipo_preliminar
            }
            holerites_extraidos.append({
                "funcionario": funcionario,
                "valor": valor if valor is not None else 0.0,
                "origem": origem_completa
            })
            continue
        
        # REGRA 3: Verificar se é receita mensal válida
        if _is_valid_receita_mensal(row):
            tipo_preliminar = "receita"
            if valor is not None and valor > 0:
                # REGRA 5: Verificar se não é valor acumulado
                if _detect_accumulated_value(valor, debitos_individuals):
                    linhas_ignoradas.append({
                        "descricao": desc,
                        "razao": "Valor acumulado/histórico detectado (REGRA 5)"
                    })
                    metadata["valores_acumulados_detectados"] += 1
                    logger.debug(f"[EXTRACTION] Receita acumulada ignorada: {desc[:50]} - R$ {valor:,.2f}")
                else:
                    origem_completa = {
                        "linha": int(idx),
                        "descricao": desc,
                        "coluna_valor": coluna_valor_usada,
                        "aba": sheet_name or "desconhecida",
                        "tipo_preliminar": tipo_preliminar
                    }
                    receitas_mensais.append({
                        "descricao": desc,
                        "valor": valor,
                        "origem": origem_completa
                    })
                    logger.debug(f"[EXTRACTION] Receita mensal válida adicionada: {desc[:50]} - R$ {valor:,.2f}")
            continue
        
        # REGRA 3: Bloquear receita prevista/devedores
        desc_lower = desc.lower()
        if any(bloqueio in desc_lower for bloqueio in ["receita prevista", "receita previsão", "devedores"]):
            tipo_preliminar = "previsao"
            linhas_ignoradas.append({
                "descricao": desc,
                "razao": "Receita prevista ou devedores — não é entrada real de dinheiro"
            })
            metadata["linhas_bloqueadas_receita_prevista"] += 1
            continue
        
        # REGRA 4: Verificar se é despesa mensal válida
        if _is_valid_despesa_mensal(row):
            tipo_preliminar = "despesa"
            if valor is not None and valor > 0:
                # REGRA 5: Verificar se não é valor acumulado
                if _detect_accumulated_value(valor, debitos_individuals):
                    linhas_ignoradas.append({
                        "descricao": desc,
                        "razao": "Valor acumulado/histórico detectado (REGRA 5)"
                    })
                    metadata["valores_acumulados_detectados"] += 1
                    logger.debug(f"[EXTRACTION] Despesa acumulada ignorada: {desc[:50]} - R$ {valor:,.2f}")
                else:
                    origem_completa = {
                        "linha": int(idx),
                        "descricao": desc,
                        "coluna_valor": coluna_valor_usada,
                        "aba": sheet_name or "desconhecida",
                        "tipo_preliminar": tipo_preliminar
                    }
                    despesas_mensais.append({
                        "descricao": desc,
                        "valor": valor,
                        "origem": origem_completa
                    })
                    logger.debug(f"[EXTRACTION] Despesa mensal válida adicionada: {desc[:50]} - R$ {valor:,.2f}")
            continue
        
        # Se não se encaixou em nenhuma categoria, ignorar
        linhas_ignoradas.append({
            "descricao": desc,
            "razao": "Não classificada como receita/despesa mensal válida"
        })
    
    # REGRA 6: Extrair saldo final explícito
    saldo_final_extraido = _extract_saldo_final_explicit(df)
    
    # PRIORIZAÇÃO: Se encontrou linhas TOTAL, usar total geral (par mesma linha ou primeiro por ordem)
    # Regra 1: par na mesma linha (origem["linha"] igual) = total geral do balancete
    # Regra 2: senão usar o primeiro por ordem de aparição (não o máximo)
    if total_receitas:
        logger.info(f"[EXTRACTION] PRIORIZANDO linha TOTAL de receitas sobre {len(receitas_mensais)} linhas individuais")
        receitas_mensais = total_receitas if len(total_receitas) == 1 else None  # será definido abaixo se múltiplas
    if total_despesas:
        logger.info(f"[EXTRACTION] PRIORIZANDO linha TOTAL de despesas sobre {len(despesas_mensais)} linhas individuais")
        despesas_mensais = total_despesas if len(total_despesas) == 1 else None

    # Múltiplas linhas TOTAL: preferir par da mesma linha; senão primeiro de cada lista
    if (total_receitas and len(total_receitas) > 1) or (total_despesas and len(total_despesas) > 1):
        par_linha_r, par_linha_d = None, None
        for r in total_receitas:
            ln_r = r.get("origem", {}).get("linha")
            if ln_r is None or not isinstance(ln_r, int):
                continue
            for d in total_despesas:
                ln_d = d.get("origem", {}).get("linha")
                if ln_d is not None and isinstance(ln_d, int) and ln_r == ln_d:
                    par_linha_r, par_linha_d = r, d
                    break
            if par_linha_r is not None:
                break
        if par_linha_r is not None and par_linha_d is not None:
            receitas_mensais = [par_linha_r]
            despesas_mensais = [par_linha_d]
            logger.info(f"[EXTRACTION] Par TOTAL na mesma linha (linha {par_linha_r.get('origem', {}).get('linha')}): receita R$ {par_linha_r['valor']:,.2f}, despesa R$ {par_linha_d['valor']:,.2f}")
        else:
            if total_receitas and (receitas_mensais is None or len(receitas_mensais) != 1):
                receitas_mensais = [total_receitas[0]]
                logger.info(f"[EXTRACTION] Múltiplas linhas TOTAL de receitas ({len(total_receitas)}). Usando primeira por ordem: R$ {total_receitas[0]['valor']:,.2f}")
            if total_despesas and (despesas_mensais is None or len(despesas_mensais) != 1):
                despesas_mensais = [total_despesas[0]]
                logger.info(f"[EXTRACTION] Múltiplas linhas TOTAL de despesas ({len(total_despesas)}). Usando primeira por ordem: R$ {total_despesas[0]['valor']:,.2f}")
    
    # FALLBACK: Se não encontrou receitas/despesas individuais E não encontrou TOTAL, tentar somar colunas específicas
    if not receitas_mensais and not total_receitas and ("credito" in df.columns or "crédito" in df.columns):
        credito_col = "credito" if "credito" in df.columns else "crédito"
        valores_credito = df[credito_col].apply(_normalize_monetary_value).dropna()
        valores_credito_positivos = [v for v in valores_credito if v is not None and v > 0 and v < 1_000_000]
        if valores_credito_positivos:
            total_credito = sum(valores_credito_positivos)
            receitas_mensais.append({
                "descricao": f"Soma de valores da coluna '{credito_col}'",
                "valor": total_credito,
                "origem": {
                    "linha": "soma_coluna",
                    "descricao": f"Soma de {len(valores_credito_positivos)} valores da coluna '{credito_col}'",
                    "coluna_valor": credito_col,
                    "aba": sheet_name or "desconhecida",
                    "tipo_preliminar": "receita"
                }
            })
            logger.info(f"[EXTRACTION] Fallback: Soma de coluna '{credito_col}': R$ {total_credito:,.2f} ({len(valores_credito_positivos)} valores)")
    
    if not despesas_mensais and not total_despesas and ("debito" in df.columns or "débito" in df.columns):
        debito_col = "debito" if "debito" in df.columns else "débito"
        valores_debito = df[debito_col].apply(_normalize_monetary_value).dropna()
        valores_debito_positivos = [v for v in valores_debito if v is not None and v > 0 and v < 1_000_000]
        if valores_debito_positivos:
            total_debito = sum(valores_debito_positivos)
            despesas_mensais.append({
                "descricao": f"Soma de valores da coluna '{debito_col}'",
                "valor": total_debito,
                "origem": {
                    "linha": "soma_coluna",
                    "descricao": f"Soma de {len(valores_debito_positivos)} valores da coluna '{debito_col}'",
                    "coluna_valor": debito_col,
                    "aba": sheet_name or "desconhecida",
                    "tipo_preliminar": "despesa"
                }
            })
            logger.info(f"[EXTRACTION] Fallback: Soma de coluna '{debito_col}': R$ {total_debito:,.2f} ({len(valores_debito_positivos)} valores)")
    
    logger.info(
        f"[EXTRACTION] Extraídos: {len(receitas_mensais)} receitas, {len(despesas_mensais)} despesas, "
        f"{len(holerites_extraidos)} holerites, {len(linhas_ignoradas)} linhas ignoradas"
    )
    
    return {
        "receitas_mensais_extraidas": receitas_mensais,
        "despesas_mensais_extraidas": despesas_mensais,
        "linhas_ignoradas": linhas_ignoradas,
        "holerites_extraidos": holerites_extraidos,
        "saldo_final_extraido": saldo_final_extraido,
        "column_classifications": column_classifications,  # REGRA 2.1
        "metadata": metadata
    }
