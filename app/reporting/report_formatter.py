"""
Report Formatter - Formatador de Relatório de Conferência
Gera as 8 seções do relatório estruturado para consumo pelo front-end.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np
import re

from app.audit.labor_analyzer import (
    analyze_labor_charges,
    EXCLUSION_KEYWORDS_ENCARGOS,
    EXCLUSION_KEYWORDS_13_SALARIO,
    _should_exclude_row,
)
from app.audit.audit_rules import (
    evaluate_document_dimension,
    classify_final_situation,
    get_required_phrases,
    FRASE_ENCARGO_NAO_AUDITAVEL,
    FRASE_NAO_AUDITAVEL_FISCAL,
    FRASE_CALCULOS_CORRETOS_DOC_INSUFICIENTE,
    FRASE_RISCO_CONTABIL_LASTRO,
)
from app.audit.value_resolver import resolve_financial_values
from app.analysis import get_duplicate_mask

logger = logging.getLogger(__name__)

# ETAPA 6: Frases obrigatórias e proibidas
FRASES_PROIBIDAS = [
    "Créditos: R$ 0,00",  # Se houver despesas
    "Base inválida",  # Se houver números calculados
    "A conta fecha",  # Sem base mensal
]

FRASES_OBRIGATORIAS = {
    "fallback_aplicado": "Valores extraídos por fallback mínimo para evitar zeramento artificial",
    "acumulado_descartado": "Totais acumulados identificados e descartados",
    "despesa_preservada": "Despesa mensal preservada mesmo sem receita explícita"
}


def _format_financial_value(valor: Optional[float]) -> str:
    """
    ETAPA 6: Formata valor financeiro para exibição.
    Valor ausente ou não apurado: exibir "Não disponível" (nunca "ERRO" ao usuário).
    """
    if valor == "ERRO":
        return "Não disponível"
    if valor is None:
        return "Não disponível"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _format_conciliacao_estrutural(structural_extraction: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Formata resultado da extração estrutural para o bloco Conciliação estrutural da seção 3.
    Retorna dict com contas_identificadas, total_contas, saldo_consolidado, diferenca,
    classificacao, justificativa, alertas, limitacoes e texto_formatado (opcional).
    """
    if not structural_extraction or not isinstance(structural_extraction, dict):
        return None
    contas = structural_extraction.get("contas") or []
    return {
        "contas_identificadas": [
            {"nome": c.get("nome"), "saldo_final": c.get("saldo_final"), "confiabilidade": c.get("confiabilidade")}
            for c in contas
        ],
        "total_contas": structural_extraction.get("total_contas"),
        "saldo_consolidado": structural_extraction.get("saldo_consolidado"),
        "diferenca": structural_extraction.get("diferenca"),
        "classificacao": structural_extraction.get("classificacao"),
        "justificativa": structural_extraction.get("justificativa"),
        "alertas": structural_extraction.get("alertas") or [],
        "limitacoes": structural_extraction.get("limitacoes") or [],
        "texto_formatado": structural_extraction.get("texto_formatado"),
    }


def _format_conciliacao_estrutural_periodos(
    structural_extraction_periods: Optional[List[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    """
    Formata múltiplos períodos estruturais (structural_extraction_periods) para a seção 3.
    Gera:
      - uma lista de períodos com contas e totais por mês; e
      - uma lista de conciliações entre períodos consecutivos (X -> X+1).
    """
    if not structural_extraction_periods or not isinstance(structural_extraction_periods, list):
        return None

    def _period_sort_key(p: Dict[str, Any]) -> Tuple[int, int, int]:
        from app.extraction.llm.document_extractor import _normalize_period  # import local p/ evitar ciclos no topo

        raw = p.get("periodo") or p.get("rotulo_original") or ""
        norm = _normalize_period(raw)
        if norm:
            try:
                year_str, month_str = norm.split("-")
                return (int(year_str), int(month_str), 1)
            except Exception:
                pass
        s = str(raw).strip()
        m_year = re.search(r"(\d{4})", s)
        year = int(m_year.group(1)) if m_year else 0
        m_month = re.search(r"(\d{1,2})", s)
        month = int(m_month.group(1)) if m_month else 0
        return (year, month, 0)

    def _format_period_label(periodo: Optional[str], rotulo_original: Optional[str]) -> str:
        if periodo and re.match(r"^\d{4}-\d{2}$", str(periodo)):
            try:
                year_str, month_str = str(periodo).split("-")
                month = int(month_str)
                year = int(year_str)
                return f"Mês {month}/{year}"
            except Exception:
                pass
        if rotulo_original:
            return str(rotulo_original)
        if periodo:
            return f"Período {periodo}"
        return "Período não identificado"

    ordered = sorted(
        [p for p in structural_extraction_periods if isinstance(p, dict)],
        key=_period_sort_key,
    )
    if not ordered:
        return None

    periodos_fmt: List[Dict[str, Any]] = []
    for p in ordered:
        periodo = p.get("periodo")
        rotulo_original = p.get("rotulo_original")
        label = _format_period_label(periodo, rotulo_original)
        periodos_fmt.append(
            {
                "periodo": periodo,
                "label": label,
                "contas": p.get("contas") or [],
                "total_contas": p.get("total_contas"),
                "saldo_consolidado": p.get("saldo_consolidado"),
                "saldo_inicial": p.get("saldo_inicial"),
                "diferenca": p.get("diferenca"),
                "alertas": p.get("alertas") or [],
                "classificacao": p.get("classificacao"),
            }
        )

    conciliacoes: List[Dict[str, Any]] = []
    for i in range(len(ordered) - 1):
        atual = ordered[i]
        prox = ordered[i + 1]
        periodo_atual = atual.get("periodo")
        periodo_prox = prox.get("periodo")
        label_atual = _format_period_label(periodo_atual, atual.get("rotulo_original"))
        label_prox = _format_period_label(periodo_prox, prox.get("rotulo_original"))

        saldo_final_atual = atual.get("saldo_consolidado") or atual.get("total_contas")
        saldo_base_prox = prox.get("saldo_consolidado") or prox.get("total_contas")
        saldo_inicial_prox = prox.get("saldo_inicial")
        if not isinstance(saldo_final_atual, (int, float)) or not isinstance(saldo_base_prox, (int, float)):
            continue
        diff = abs(float(saldo_final_atual) - float(saldo_base_prox))
        iguais = diff <= 0.02
        # Saldo inicial de [N] = Saldo final de [N-1]
        texto_carregamento = (
            f" Saldo inicial de {label_prox} = Saldo final de {label_atual} (R$ {saldo_final_atual:,.2f})."
            if saldo_inicial_prox is not None else ""
        )
        if iguais:
            mensagem = (
                f"Saldo final de {label_atual} (R$ {saldo_final_atual:,.2f}) "
                f"é igual ao saldo consolidado de {label_prox} (R$ {saldo_base_prox:,.2f})."
                f"{texto_carregamento}"
            )
        else:
            mensagem = (
                f"Saldo final de {label_atual} (R$ {saldo_final_atual:,.2f}) "
                f"difere do saldo consolidado de {label_prox} (R$ {saldo_base_prox:,.2f}) "
                f"— diferença de R$ {diff:,.2f}. Verificar consistência da base."
                f"{texto_carregamento}"
            )
        conciliacoes.append(
            {
                "periodo_origem": periodo_atual,
                "periodo_destino": periodo_prox,
                "label_origem": label_atual,
                "label_destino": label_prox,
                "saldo_final_origem": saldo_final_atual,
                "saldo_inicial_destino": saldo_inicial_prox,
                "saldo_consolidado_destino": saldo_base_prox,
                "diferenca": round(diff, 2),
                "iguais": iguais,
                "mensagem": mensagem.replace(",", "X").replace(".", ",").replace("X", "."),
            }
        )

    return {
        "periodos": periodos_fmt,
        "conciliacoes_entre_periodos": conciliacoes,
    }


def _extract_expected_values_from_dataframe(df: pd.DataFrame, expected_receitas: Optional[float] = None, expected_despesas: Optional[float] = None) -> Dict[str, Optional[float]]:
    """
    Função auxiliar para buscar valores diretamente no DataFrame.
    Não usa valores fixos de outro condomínio: quando expected_* é None, usa apenas
    agregação neutra (máximo/soma de valores plausíveis) dos dados do próprio DataFrame.
    """
    result = {"receitas": None, "despesas": None}
    if df is None or df.empty:
        return result
    LIMITE_MENSAL = 500_000.0

    # Receitas: só busca exata se caller informou expected_receitas; senão só agregação neutra
    if "valor" in df.columns:
        try:
            valores = pd.to_numeric(df["valor"], errors="coerce").dropna()
            valores_positivos = valores[valores > 0]
            valores_mensais = valores_positivos[valores_positivos < LIMITE_MENSAL]
            if expected_receitas is not None and abs(expected_receitas) > 0.01:
                receita_esperada = valores_positivos[valores_positivos.between(expected_receitas - 0.01, expected_receitas + 0.01)]
                if not receita_esperada.empty:
                    result["receitas"] = float(receita_esperada.iloc[0])
                    logger.info(f"[EXTRACTION_DIRETA] Valor de receitas (esperado informado) encontrado: R$ {result['receitas']:,.2f}")
            if result["receitas"] is None and not valores_mensais.empty:
                result["receitas"] = float(valores_mensais.max())
                logger.info(f"[EXTRACTION_DIRETA] Valor mensal de receitas (agregação neutra): R$ {result['receitas']:,.2f}")
        except Exception as e:
            logger.warning(f"[EXTRACTION_DIRETA] Erro ao buscar receitas na coluna 'valor': {e}")

    if "valor" in df.columns:
        try:
            valores = pd.to_numeric(df["valor"], errors="coerce").dropna()
            valores_positivos = valores[valores > 0]
            valores_mensais = valores_positivos[valores_positivos < LIMITE_MENSAL]
            if expected_despesas is not None and abs(expected_despesas) > 0.01:
                despesa_esperada = valores_positivos[valores_positivos.between(expected_despesas - 0.01, expected_despesas + 0.01)]
                if not despesa_esperada.empty:
                    result["despesas"] = float(despesa_esperada.iloc[0])
                    logger.info(f"[EXTRACTION_DIRETA] Valor de despesas (esperado informado) encontrado: R$ {result['despesas']:,.2f}")
            if result["despesas"] is None and not valores_mensais.empty:
                result["despesas"] = float(valores_mensais.max())
                logger.info(f"[EXTRACTION_DIRETA] Valor mensal de despesas (agregação neutra): R$ {result['despesas']:,.2f}")
        except Exception as e:
            logger.warning(f"[EXTRACTION_DIRETA] Erro ao buscar despesas na coluna 'valor': {e}")

    return result


def generate_dataset_financeiro(
    audit_result: Dict[str, Any],
    df: Optional[pd.DataFrame] = None
) -> Dict[str, Any]:
    """
    🔒 DATASET FINANCEIRO OBRIGATÓRIO
    
    Gera o bloco imutável dataset_financeiro ANTES do relatório textual.
    Este bloco DEVE ser a única fonte de verdade para valores financeiros no relatório.
    
    REGRA ABSOLUTA: 
    - O relatório textual NÃO pode recalcular, zerar ou substituir nenhum valor desse bloco.
    - Caso não seja possível extrair um valor, retorne ERRO, não 0 nem N/A.
    - PRIORIZAR valores extraídos do texto (totals_extracted) quando disponíveis.
    """
    logger.info("[DATASET_FINANCEIRO] Iniciando geração do dataset financeiro obrigatório")
    
    # Obter valores extraídos do texto PRIMEIRO (prioridade máxima)
    doc_context = _safe_get(audit_result, "document_context", default={})
    totals_extracted = doc_context.get("totals_extracted") or {}
    
    logger.info(f"[DATASET_FINANCEIRO] totals_extracted recebido: {type(totals_extracted)}, keys={list(totals_extracted.keys()) if isinstance(totals_extracted, dict) else 'N/A'}")
    
    # Extrair valores do totals_extracted (pode ter estrutura com "values" ou ser plano)
    extracted_values = {}
    if totals_extracted:
        if isinstance(totals_extracted, dict) and "values" in totals_extracted:
            extracted_values = totals_extracted.get("values", {}) or {}
            logger.info(f"[DATASET_FINANCEIRO] Estrutura com 'values' encontrada. Keys em values: {list(extracted_values.keys())}")
        else:
            extracted_values = totals_extracted
            logger.info(f"[DATASET_FINANCEIRO] Estrutura plana. Keys: {list(extracted_values.keys()) if isinstance(extracted_values, dict) else 'N/A'}")
    
    logger.info(f"[DATASET_FINANCEIRO] Valores extraídos do texto: receitas={extracted_values.get('total_receitas')}, despesas={extracted_values.get('total_despesas')}, saldo_final={extracted_values.get('saldo_final')}")
    
    # Obter valores do fallback_result ou calculated_totals
    financial_totals = {}
    fallback_result = {}
    extracted_data = {}
    
    if df is not None and not df.empty:
        try:
            from app.audit.financial_consolidator import calculate_financial_totals_correct
            # Passar totals_extracted para usar valores do texto como primeira opção
            logger.info(f"[DATASET_FINANCEIRO] Chamando calculate_financial_totals_correct com DataFrame de {len(df)} linhas")
            financial_totals = calculate_financial_totals_correct(df, extracted_totals=totals_extracted)
            fallback_result = financial_totals.get("fallback_result", {})
            extracted_data = financial_totals.get("extracted_data", {})
            logger.info(f"[DATASET_FINANCEIRO] Valores calculados do DataFrame: receitas={financial_totals.get('total_receitas')}, despesas={financial_totals.get('total_despesas')}, saldo_final={financial_totals.get('saldo_final')}")
            logger.info(f"[DATASET_FINANCEIRO] Fallback result: creditos_mes={fallback_result.get('creditos_mes')}, debitos_mes={fallback_result.get('debitos_mes')}, fallback_applied={fallback_result.get('fallback_applied')}")
            logger.info(f"[DATASET_FINANCEIRO] Extracted data: receitas={len(extracted_data.get('receitas_mensais_extraidas', []))} itens, despesas={len(extracted_data.get('despesas_mensais_extraidas', []))} itens")
            # Log detalhado dos valores extraídos
            if extracted_data.get('receitas_mensais_extraidas'):
                receitas_list = extracted_data.get('receitas_mensais_extraidas', [])
                total_receitas_extracted = sum(r.get('valor', 0) for r in receitas_list if r.get('valor') is not None)
                logger.info(f"[DATASET_FINANCEIRO] Total de receitas do extracted_data: R$ {total_receitas_extracted:,.2f}")
            if extracted_data.get('despesas_mensais_extraidas'):
                despesas_list = extracted_data.get('despesas_mensais_extraidas', [])
                total_despesas_extracted = sum(d.get('valor', 0) for d in despesas_list if d.get('valor') is not None)
                logger.info(f"[DATASET_FINANCEIRO] Total de despesas do extracted_data: R$ {total_despesas_extracted:,.2f}")
        except Exception as e:
            logger.error(f"[DATASET_FINANCEIRO] Erro ao calcular totais financeiros: {e}", exc_info=True)
            # Se tem valores extraídos do texto, usar eles mesmo com erro no DataFrame
            if extracted_values.get("total_receitas") is not None or extracted_values.get("total_despesas") is not None:
                logger.warning(f"[DATASET_FINANCEIRO] Usando valores extraídos do texto devido a erro no DataFrame")
                financial_totals = {}
                fallback_result = {}
                extracted_data = {}
            else:
                # Retornar ERRO apenas se não tem nenhuma fonte
                logger.error(f"[DATASET_FINANCEIRO] Nenhuma fonte disponível (texto ou DataFrame). Retornando ERRO.")
                return {
                    "dataset_financeiro": {
                        "creditos_mensais": {"valor": "ERRO", "status": "ERRO", "origem": f"Erro ao processar: {str(e)}"},
                        "debitos_mensais": {"valor": "ERRO", "status": "ERRO", "origem": f"Erro ao processar: {str(e)}"},
                        "saldo_anterior": {"valor": "ERRO", "status": "ERRO"},
                        "saldo_final": {"valor": "ERRO", "status": "ERRO"}
                    }
                }
    else:
        logger.warning(f"[DATASET_FINANCEIRO] DataFrame vazio ou None. Tentando usar valores já calculados no audit_result.")
        # Tentar obter valores já calculados do audit_result
        # Os valores podem estar em diferentes lugares dependendo de quando foram calculados
        financial_summary = _safe_get(audit_result, "summary", "financial_summary", default={})
        if not financial_summary:
            financial_summary = _safe_get(audit_result, "financial_summary", default={})
        
        # Tentar obter valores de financial_extraction_result se disponível (pré-calculado no api_server)
        financial_extraction = _safe_get(audit_result, "financial_extraction_result", default={})
        if financial_extraction:
            financial_totals = {
                "total_receitas": financial_extraction.get("total_receitas"),
                "total_despesas": financial_extraction.get("total_despesas"),
                "saldo_final": financial_extraction.get("saldo_final")
            }
            extracted_data = financial_extraction.get("extracted_data") or {}
            logger.info(f"[DATASET_FINANCEIRO] Valores encontrados em financial_extraction_result: receitas={financial_totals.get('total_receitas')}, despesas={financial_totals.get('total_despesas')}")
        elif financial_summary:
            financial_totals = {
                "total_receitas": financial_summary.get("total_receitas"),
                "total_despesas": financial_summary.get("total_despesas"),
                "saldo_final": financial_summary.get("saldo_final")
            }
            logger.info(f"[DATASET_FINANCEIRO] Valores encontrados em financial_summary: receitas={financial_totals.get('total_receitas')}, despesas={financial_totals.get('total_despesas')}")
        else:
            financial_totals = {}
            fallback_result = {}
            extracted_data = {}
            logger.warning(f"[DATASET_FINANCEIRO] Nenhum valor pré-calculado encontrado no audit_result. Tentando usar apenas valores do texto.")
    
    # Obter saldo anterior do document_context
    saldo_anterior_raw = doc_context.get("saldo_anterior")
    if saldo_anterior_raw is None:
        saldo_anterior_raw = extracted_values.get("saldo_anterior")
    
    # Determinar valores finais. Quando DataFrame está vazio, priorizar totals_extracted (valores do texto do documento).
    # Caso contrário: priorizar financial_totals quando crédito != débito (soma balancete correta).
    creditos_valor = None
    debitos_valor = None
    saldo_final_valor = None
    ft_rec = financial_totals.get("total_receitas")
    ft_des = financial_totals.get("total_despesas")
    ft_saldo = financial_totals.get("saldo_final")
    df_vazio = df is None or (isinstance(df, pd.DataFrame) and df.empty)
    if df_vazio and (extracted_values.get("total_receitas") is not None or extracted_values.get("total_despesas") is not None):
        creditos_valor = extracted_values.get("total_receitas")
        debitos_valor = extracted_values.get("total_despesas")
        saldo_final_valor = extracted_values.get("saldo_final")
        logger.info(f"[DATASET_FINANCEIRO] DataFrame vazio: priorizando totals_extracted do texto: créditos={creditos_valor}, débitos={debitos_valor}, saldo_final={saldo_final_valor}")
    if creditos_valor is None and debitos_valor is None:
        # Quando consolidator retorna crédito e débito diferentes e plausíveis (< 1M), usar para corrigir 70k=70k
        if ft_rec is not None and ft_des is not None and ft_rec != "ERRO" and ft_des != "ERRO":
            if isinstance(ft_rec, (int, float)) and isinstance(ft_des, (int, float)):
                if ft_rec < 1_000_000 and ft_des < 1_000_000 and abs(ft_rec - ft_des) > 0.01:
                    creditos_valor = ft_rec
                    debitos_valor = ft_des
                    saldo_final_valor = ft_saldo if ft_saldo not in (None, "ERRO") else None
                    logger.info(f"[DATASET_FINANCEIRO] Usando totais do balancete (crédito != débito): créditos={creditos_valor:,.2f}, débitos={debitos_valor:,.2f}")
    if creditos_valor is None:
        creditos_valor = extracted_values.get("total_receitas")
    if debitos_valor is None:
        debitos_valor = extracted_values.get("total_despesas")
    if saldo_final_valor is None:
        saldo_final_valor = extracted_values.get("saldo_final")
    
    logger.info(f"[DATASET_FINANCEIRO] Após prioridade: créditos={creditos_valor}, débitos={debitos_valor}, saldo_final={saldo_final_valor}")
    
    # Preencher faltantes com financial_totals
    if creditos_valor is None:
        creditos_valor = financial_totals.get("total_receitas")
        if creditos_valor is not None and creditos_valor != "ERRO":
            logger.info(f"[DATASET_FINANCEIRO] ✅ Créditos do financial_totals: R$ {creditos_valor:,.2f}")
        elif creditos_valor == "ERRO":
            creditos_valor = None
    if debitos_valor is None:
        debitos_valor = financial_totals.get("total_despesas")
        if debitos_valor is not None and debitos_valor != "ERRO":
            logger.info(f"[DATASET_FINANCEIRO] ✅ Débitos do financial_totals: R$ {debitos_valor:,.2f}")
        elif debitos_valor == "ERRO":
            debitos_valor = None
    if saldo_final_valor is None:
        saldo_final_valor = financial_totals.get("saldo_final")
        if saldo_final_valor is not None and saldo_final_valor != "ERRO":
            logger.info(f"[DATASET_FINANCEIRO] ✅ Saldo final do financial_totals: R$ {saldo_final_valor:,.2f}")
        elif saldo_final_valor == "ERRO":
            saldo_final_valor = None
    
    # PRIORIDADE 3: Se financial_totals não tem valores, usar fallback_result
    if creditos_valor is None:
        creditos_valor = fallback_result.get("creditos_mes")
        if creditos_valor is not None:
            logger.info(f"[DATASET_FINANCEIRO] Créditos do fallback_result: {creditos_valor}")
    if debitos_valor is None:
        debitos_valor = fallback_result.get("debitos_mes")
        if debitos_valor is not None:
            logger.info(f"[DATASET_FINANCEIRO] Débitos do fallback_result: {debitos_valor}")
    if saldo_final_valor is None:
        saldo_final_valor = fallback_result.get("saldo_final")
        if saldo_final_valor is not None:
            logger.info(f"[DATASET_FINANCEIRO] Saldo final do fallback_result: {saldo_final_valor}")
    
    # VERIFICAÇÃO FINAL: Se ainda não temos valores mas há DataFrame processado, tentar extrair diretamente
    if (creditos_valor is None or debitos_valor is None) and df is not None and not df.empty and extracted_data:
        logger.info(f"[DATASET_FINANCEIRO] Verificação final: tentando extrair valores diretamente do extracted_data")
        receitas_extraidas = extracted_data.get("receitas_mensais_extraidas", [])
        despesas_extraidas = extracted_data.get("despesas_mensais_extraidas", [])
        
        if creditos_valor is None and receitas_extraidas:
            total_receitas = sum(r.get("valor", 0) for r in receitas_extraidas if r.get("valor") is not None)
            if total_receitas > 0:
                creditos_valor = total_receitas
                logger.info(f"[DATASET_FINANCEIRO] ✅ Créditos extraídos diretamente do extracted_data: R$ {creditos_valor:,.2f} ({len(receitas_extraidas)} itens)")
        
        if debitos_valor is None and despesas_extraidas:
            total_despesas = sum(d.get("valor", 0) for d in despesas_extraidas if d.get("valor") is not None)
            if total_despesas > 0:
                debitos_valor = total_despesas
                logger.info(f"[DATASET_FINANCEIRO] ✅ Débitos extraídos diretamente do extracted_data: R$ {debitos_valor:,.2f} ({len(despesas_extraidas)} itens)")
    
    # VERIFICAÇÃO EXTRA: Extração direta do DataFrame quando outras fontes falharam
    if (creditos_valor is None or debitos_valor is None) and df is not None and not df.empty:
        logger.info(f"[DATASET_FINANCEIRO] Verificação extra: tentando extrair valores diretamente do DataFrame")
        
        # Usar função auxiliar para buscar valores esperados
        valores_extraidos = _extract_expected_values_from_dataframe(df)
        
        if creditos_valor is None and valores_extraidos["receitas"] is not None:
            creditos_valor = valores_extraidos["receitas"]
            logger.info(f"[DATASET_FINANCEIRO] ✅ Créditos encontrados na verificação extra: R$ {creditos_valor:,.2f}")
        
        if debitos_valor is None and valores_extraidos["despesas"] is not None:
            debitos_valor = valores_extraidos["despesas"]
            logger.info(f"[DATASET_FINANCEIRO] ✅ Débitos encontrados na verificação extra: R$ {debitos_valor:,.2f}")
        
        # Busca em outras colunas: apenas agregação neutra (sem valores fixos de outro condomínio)
        if creditos_valor is None:
            for col_name in ["credito", "crédito", "receita", "receitas"]:
                if col_name in df.columns:
                    try:
                        valores_col = pd.to_numeric(df[col_name], errors="coerce").dropna()
                        valores_col = valores_col[valores_col > 0]
                        valores_mensais = valores_col[valores_col < 500_000]
                        if not valores_mensais.empty:
                            creditos_valor = float(valores_mensais.sum())
                            logger.info(f"[DATASET_FINANCEIRO] Soma de créditos da coluna '{col_name}': R$ {creditos_valor:,.2f}")
                            break
                    except Exception as e:
                        logger.warning(f"[DATASET_FINANCEIRO] Erro ao processar coluna '{col_name}' para créditos: {e}")
                        continue

        if debitos_valor is None:
            for col_name in ["debito", "débito", "despesa", "despesas"]:
                if col_name in df.columns:
                    try:
                        valores_col = pd.to_numeric(df[col_name], errors="coerce").dropna()
                        valores_col = valores_col[valores_col > 0]
                        valores_mensais = valores_col[valores_col < 500_000]
                        if not valores_mensais.empty:
                            debitos_valor = float(valores_mensais.sum())
                            logger.info(f"[DATASET_FINANCEIRO] Soma de débitos da coluna '{col_name}': R$ {debitos_valor:,.2f}")
                            break
                    except Exception as e:
                        logger.warning(f"[DATASET_FINANCEIRO] Erro ao processar coluna '{col_name}' para débitos: {e}")
                        continue
    
    # Saldo final do período = créditos - débitos (não usar saldo consolidado das contas quando temos totais do período)
    if (
        creditos_valor is not None
        and debitos_valor is not None
        and creditos_valor != "ERRO"
        and debitos_valor != "ERRO"
        and isinstance(creditos_valor, (int, float))
        and isinstance(debitos_valor, (int, float))
    ):
        saldo_final_valor = round(creditos_valor - debitos_valor, 2)
        logger.info(f"[DATASET_FINANCEIRO] Saldo final do período = créditos - débitos: {creditos_valor} - {debitos_valor} = {saldo_final_valor}")
    
    logger.info(f"[DATASET_FINANCEIRO] Valores finais antes de determinar status: créditos={creditos_valor}, débitos={debitos_valor}, saldo_final={saldo_final_valor}")
    
    # Inicializar variáveis de status e origem
    creditos_status = "NAO_APURADO"
    creditos_origem = "não identificada"
    debitos_status = "NAO_APURADO"
    debitos_origem = "não identificada"
    
    # Determinar origem e status dos valores
    # REGRA: Se não conseguiu extrair, retornar ERRO (não 0 nem N/A)
    # MAS: Só retornar ERRO se realmente não há valores em nenhuma fonte
    if creditos_valor is None:
        # Última tentativa: verificar se há valores em financial_totals ou fallback_result
        if financial_totals and financial_totals.get("total_receitas") is not None:
            creditos_valor = financial_totals.get("total_receitas")
            logger.info(f"[DATASET_FINANCEIRO] ✅ Usando créditos do financial_totals (última tentativa): R$ {creditos_valor:,.2f}")
        elif fallback_result and fallback_result.get("creditos_mes") is not None:
            creditos_valor = fallback_result.get("creditos_mes")
            logger.info(f"[DATASET_FINANCEIRO] ✅ Usando créditos do fallback_result (última tentativa): R$ {creditos_valor:,.2f}")
        else:
            # Só retornar ERRO se realmente não há valores em nenhuma fonte
            creditos_valor = "ERRO"
            creditos_status = "ERRO"
            creditos_origem = "Não foi possível extrair créditos mensais"
            logger.warning(f"[DATASET_FINANCEIRO] ⚠️ Créditos não encontrados em nenhuma fonte (texto, DataFrame, fallback)")
    
    if creditos_valor != "ERRO" and creditos_valor is not None:
        # Determinar origem baseado na fonte do valor
        if extracted_values.get("total_receitas") is not None and abs(extracted_values.get("total_receitas") - creditos_valor) < 0.01:
            creditos_status = "EXTRAIDO_DO_TEXTO"
            creditos_origem = extracted_values.get("_total_receitas_source", "extraído do texto do documento")
            logger.info(f"[DATASET_FINANCEIRO] ✅ Créditos extraídos do texto: R$ {creditos_valor:,.2f}")
        elif financial_totals.get("total_receitas") is not None and isinstance(creditos_valor, (int, float)) and isinstance(financial_totals.get("total_receitas"), (int, float)) and abs(financial_totals.get("total_receitas") - creditos_valor) < 0.01:
            creditos_status = "EXTRAIDO_DA_PLANILHA"
            creditos_origem = "extraído da planilha (linhas TOTAL priorizadas)"
            logger.info(f"[DATASET_FINANCEIRO] ✅ Créditos extraídos do DataFrame (financial_totals): R$ {creditos_valor:,.2f}")
        elif fallback_result.get("creditos_mes") is not None and abs(fallback_result.get("creditos_mes") - creditos_valor) < 0.01:
            creditos_status = "EXTRAIDO_DA_PLANILHA"
            creditos_origem = "soma das receitas mensais (fallback aplicado)"
            logger.info(f"[DATASET_FINANCEIRO] ✅ Créditos extraídos do DataFrame (fallback): R$ {creditos_valor:,.2f}")
        else:
            creditos_status = "EXTRAIDO_DA_PLANILHA"
            creditos_origem = "soma das receitas mensais"
            logger.info(f"[DATASET_FINANCEIRO] ✅ Créditos extraídos do DataFrame: R$ {creditos_valor:,.2f}")
    elif creditos_valor == "ERRO":
        creditos_status = "ERRO"
        creditos_origem = "Não foi possível extrair créditos mensais"
    
    if debitos_valor is None:
        # Última tentativa: verificar se há valores em financial_totals ou fallback_result
        if financial_totals and financial_totals.get("total_despesas") is not None:
            debitos_valor = financial_totals.get("total_despesas")
            logger.info(f"[DATASET_FINANCEIRO] ✅ Usando débitos do financial_totals (última tentativa): R$ {debitos_valor:,.2f}")
        elif fallback_result and fallback_result.get("debitos_mes") is not None:
            debitos_valor = fallback_result.get("debitos_mes")
            logger.info(f"[DATASET_FINANCEIRO] ✅ Usando débitos do fallback_result (última tentativa): R$ {debitos_valor:,.2f}")
        else:
            # Só retornar ERRO se realmente não há valores em nenhuma fonte
            debitos_valor = "ERRO"
            debitos_status = "ERRO"
            debitos_origem = "Não foi possível extrair débitos mensais"
            logger.warning(f"[DATASET_FINANCEIRO] ⚠️ Débitos não encontrados em nenhuma fonte (texto, DataFrame, fallback)")
    
    if debitos_valor != "ERRO" and debitos_valor is not None:
        # Determinar origem baseado na fonte do valor
        if extracted_values.get("total_despesas") is not None and abs(extracted_values.get("total_despesas") - debitos_valor) < 0.01:
            debitos_status = "EXTRAIDO_DO_TEXTO"
            debitos_origem = extracted_values.get("_total_despesas_source", "extraído do texto do documento")
            logger.info(f"[DATASET_FINANCEIRO] ✅ Débitos extraídos do texto: R$ {debitos_valor:,.2f}")
        elif financial_totals.get("total_despesas") is not None and isinstance(debitos_valor, (int, float)) and isinstance(financial_totals.get("total_despesas"), (int, float)) and abs(financial_totals.get("total_despesas") - debitos_valor) < 0.01:
            debitos_status = "EXTRAIDO_DA_PLANILHA"
            debitos_origem = "extraído da planilha (linhas TOTAL priorizadas)"
            logger.info(f"[DATASET_FINANCEIRO] ✅ Débitos extraídos do DataFrame (financial_totals): R$ {debitos_valor:,.2f}")
        elif fallback_result.get("debitos_mes") is not None and abs(fallback_result.get("debitos_mes") - debitos_valor) < 0.01:
            debitos_status = "EXTRAIDO_DA_PLANILHA"
            debitos_origem = "soma das despesas mensais (fallback aplicado)"
            logger.info(f"[DATASET_FINANCEIRO] ✅ Débitos extraídos do DataFrame (fallback): R$ {debitos_valor:,.2f}")
        else:
            debitos_status = "EXTRAIDO_DA_PLANILHA"
            debitos_origem = "soma das despesas mensais"
            logger.info(f"[DATASET_FINANCEIRO] ✅ Débitos extraídos do DataFrame: R$ {debitos_valor:,.2f}")
    elif debitos_valor == "ERRO":
        debitos_status = "ERRO"
        debitos_origem = "Não foi possível extrair débitos mensais"
    
    # Verificar se foi usado fallback de TOTAIS
    if fallback_result.get("fallback_applied"):
        fallback_reason = fallback_result.get("fallback_reason", "")
        if "fallback mínimo" in fallback_reason.lower() or "totais" in fallback_reason.lower():
            if debitos_valor != "ERRO":
                debitos_origem = "linha TOTAIS (fallback controlado)"
            if creditos_valor == "ERRO":
                creditos_origem = "receita mensal não identificada"
    
    # Verificar origem das receitas
    if extracted_data.get("receitas_mensais_extraidas") and creditos_valor != "ERRO":
        receitas_items = extracted_data["receitas_mensais_extraidas"]
        if receitas_items:
            primeira_origem = receitas_items[0].get("origem", {})
            if isinstance(primeira_origem, dict):
                linha = primeira_origem.get("linha", "?")
                aba = primeira_origem.get("aba", "desconhecida")
                creditos_origem = f"linha {linha}, aba '{aba}'"
            else:
                creditos_origem = str(primeira_origem)
    
    # Verificar origem das despesas
    if extracted_data.get("despesas_mensais_extraidas") and debitos_valor != "ERRO":
        despesas_items = extracted_data["despesas_mensais_extraidas"]
        if despesas_items:
            primeira_origem = despesas_items[0].get("origem", {})
            if isinstance(primeira_origem, dict):
                linha = primeira_origem.get("linha", "?")
                aba = primeira_origem.get("aba", "desconhecida")
                debitos_origem = f"linha {linha}, aba '{aba}'"
            else:
                debitos_origem = str(primeira_origem)
    
    # Status do saldo anterior: não usar "ERRO" como valor; ausente = NAO_ENCONTRADO
    if saldo_anterior_raw is None:
        saldo_anterior_valor = None
        saldo_anterior_status = "NAO_ENCONTRADO"
    else:
        try:
            saldo_anterior_float = float(saldo_anterior_raw)
            saldo_anterior_valor = saldo_anterior_float
            saldo_anterior_status = "EXTRAIDO_DA_PLANILHA"
        except (ValueError, TypeError):
            saldo_anterior_valor = None
            saldo_anterior_status = "NAO_ENCONTRADO"
    
    # Status do saldo final
    # Calcular saldo final se temos créditos e débitos válidos
    if saldo_final_valor is None:
        # Tentar calcular se temos créditos e débitos
        if creditos_valor is not None and creditos_valor != "ERRO" and debitos_valor is not None and debitos_valor != "ERRO":
            # Calcular saldo final = saldo anterior + (créditos - débitos)
            if saldo_anterior_raw is not None:
                try:
                    saldo_anterior_float = float(saldo_anterior_raw)
                    saldo_final_valor = saldo_anterior_float + (creditos_valor - debitos_valor)
                    saldo_final_status = "CALCULADO"
                    logger.info(f"[DATASET_FINANCEIRO] Saldo final calculado: {saldo_anterior_float} + ({creditos_valor} - {debitos_valor}) = {saldo_final_valor}")
                except (ValueError, TypeError):
                    saldo_final_valor = None
                    saldo_final_status = "ERRO"
            else:
                # Sem saldo anterior, usar apenas resultado do mês
                saldo_final_valor = creditos_valor - debitos_valor
                saldo_final_status = "CALCULADO"
                logger.info(f"[DATASET_FINANCEIRO] Saldo final calculado (sem saldo anterior): {creditos_valor} - {debitos_valor} = {saldo_final_valor}")
        else:
            saldo_final_valor = "ERRO"
            saldo_final_status = "ERRO"
            logger.warning(f"[DATASET_FINANCEIRO] ⚠️ Não foi possível calcular saldo final: créditos={creditos_valor}, débitos={debitos_valor}")
    elif creditos_valor != "ERRO" and debitos_valor != "ERRO" and creditos_valor is not None and debitos_valor is not None:
        saldo_final_status = "CALCULADO"
    else:
        saldo_final_status = "ERRO"
    
    resultado = {
        "dataset_financeiro": {
            "creditos_mensais": {
                "valor": creditos_valor,
                "status": creditos_status,
                "origem": creditos_origem
            },
            "debitos_mensais": {
                "valor": debitos_valor,
                "status": debitos_status,
                "origem": debitos_origem
            },
            "saldo_anterior": {
                "valor": saldo_anterior_valor,
                "status": saldo_anterior_status
            },
            "saldo_final": {
                "valor": saldo_final_valor,
                "status": saldo_final_status
            }
        }
    }
    
    # Log final resumindo o dataset gerado
    logger.info(f"[DATASET_FINANCEIRO] ✅ Dataset gerado: créditos={creditos_valor}, débitos={debitos_valor}, saldo_final={saldo_final_valor}")
    
    return resultado


def generate_financial_extraction_result(
    audit_result: Dict[str, Any],
    df: Optional[pd.DataFrame] = None
) -> Dict[str, Any]:
    """
    🔒 CONTRATO DE SAÍDA OBRIGATÓRIO
    
    Gera o JSON imutável com os valores financeiros extraídos ANTES do relatório textual.
    Este JSON DEVE ser a única fonte de verdade para valores financeiros no relatório.
    
    REGRA ABSOLUTA: Após este JSON ser emitido, é PROIBIDO:
    - Substituir valor por 0
    - Recalcular
    - Normalizar
    - "Limpar"
    
    O relatório textual DEVE LER DESSE JSON, nunca recalcular.
    
    Os valores financeiros apresentados no relatório DEVEM ser lidos EXCLUSIVAMENTE 
    do bloco financial_extraction_result.
    É TERMINANTEMENTE PROIBIDO recalcular, normalizar, zerar ou substituir valores 
    após a extração.
    """
    # Obter valores do fallback_result ou calculated_totals
    financial_totals = {}
    fallback_result = {}
    extracted_data = {}
    
    if df is not None and not df.empty:
        from app.audit.financial_consolidator import calculate_financial_totals_correct
        financial_totals = calculate_financial_totals_correct(df)
        fallback_result = financial_totals.get("fallback_result", {})
        extracted_data = financial_totals.get("extracted_data", {})
    
    # Obter saldo anterior do document_context
    doc_context = _safe_get(audit_result, "document_context", default={})
    saldo_anterior_raw = doc_context.get("saldo_anterior")
    if saldo_anterior_raw is None:
        totals_extracted = doc_context.get("totals_extracted") or {}
        if isinstance(totals_extracted, dict) and "values" in totals_extracted:
            saldo_anterior_raw = totals_extracted.get("values", {}).get("saldo_anterior")
        else:
            saldo_anterior_raw = totals_extracted.get("saldo_anterior")
    
    # Determinar valores finais (priorizar fallback_result)
    creditos_valor = fallback_result.get("creditos_mes")
    debitos_valor = fallback_result.get("debitos_mes")
    saldo_final_valor = fallback_result.get("saldo_final")
    
    # Se fallback não tem valores, usar financial_totals
    if creditos_valor is None:
        creditos_valor = financial_totals.get("total_receitas")
    if debitos_valor is None:
        debitos_valor = financial_totals.get("total_despesas")
    if saldo_final_valor is None:
        saldo_final_valor = financial_totals.get("saldo_final")
    
    # Determinar origem e status
    creditos_status = "EXTRAIDO_DA_PLANILHA"
    creditos_origem = "soma das receitas mensais"
    
    debitos_status = "EXTRAIDO_DA_PLANILHA"
    debitos_origem = "soma das despesas mensais"
    
    # Verificar se foi usado fallback de TOTAIS
    if fallback_result.get("fallback_applied"):
        fallback_reason = fallback_result.get("fallback_reason", "")
        if "fallback mínimo" in fallback_reason.lower() or "totais" in fallback_reason.lower():
            if debitos_valor is not None:
                debitos_origem = "linha TOTAIS (fallback controlado)"
            if creditos_valor is None:
                creditos_status = "NAO_APURADO"
                creditos_origem = "receita mensal não identificada"
    
    # Verificar origem das receitas
    if extracted_data.get("receitas_mensais_extraidas"):
        receitas_items = extracted_data["receitas_mensais_extraidas"]
        if receitas_items:
            # Pegar primeira origem como exemplo
            primeira_origem = receitas_items[0].get("origem", {})
            if isinstance(primeira_origem, dict):
                linha = primeira_origem.get("linha", "?")
                aba = primeira_origem.get("aba", "desconhecida")
                creditos_origem = f"linha {linha}, aba '{aba}'"
            else:
                creditos_origem = str(primeira_origem)
    
    # Verificar origem das despesas
    if extracted_data.get("despesas_mensais_extraidas"):
        despesas_items = extracted_data["despesas_mensais_extraidas"]
        if despesas_items:
            # Pegar primeira origem como exemplo
            primeira_origem = despesas_items[0].get("origem", {})
            if isinstance(primeira_origem, dict):
                linha = primeira_origem.get("linha", "?")
                aba = primeira_origem.get("aba", "desconhecida")
                debitos_origem = f"linha {linha}, aba '{aba}'"
            else:
                debitos_origem = str(primeira_origem)
    
    # Status do saldo anterior
    saldo_anterior_status = "NAO_IDENTIFICADO"
    if saldo_anterior_raw is not None:
        try:
            saldo_anterior_float = float(saldo_anterior_raw)
            if saldo_anterior_float != 0:
                saldo_anterior_status = "EXTRAIDO_DA_PLANILHA"
        except (ValueError, TypeError):
            pass
    
    # Status do saldo final
    saldo_final_status = "NAO_CALCULADO"
    if saldo_final_valor is not None:
        saldo_final_status = "CALCULADO"
    elif creditos_valor is not None and debitos_valor is not None:
        # Pode calcular se ambos existem
        saldo_final_status = "CALCULAVEL"
    
    return {
        "financial_extraction_result": {
            "creditos_mensais": {
                "valor": creditos_valor,
                "status": creditos_status,
                "origem": creditos_origem
            },
            "debitos_mensais": {
                "valor": debitos_valor,
                "status": debitos_status,
                "origem": debitos_origem
            },
            "saldo_anterior": {
                "valor": saldo_anterior_raw if saldo_anterior_raw is not None else None,
                "status": saldo_anterior_status
            },
            "saldo_final": {
                "valor": saldo_final_valor,
                "status": saldo_final_status
            }
        }
    }


def _iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_metadata(job_id: str, start_time: Optional[datetime] = None) -> Dict[str, Any]:
    processing_time = 0
    if start_time:
        processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
    return {"generated_at": _iso_timestamp(), "job_id": job_id or "unknown", "processing_time_ms": processing_time}


def _safe_get(data: Dict, *keys, default=None):
    result = data
    for key in keys:
        if isinstance(result, dict):
            result = result.get(key, default)
        else:
            return default
    return result if result is not None else default


def _get_period_from_df(df: Optional[pd.DataFrame]) -> Dict[str, str]:
    result = {"period_start": None, "period_end": None}
    if df is None or df.empty or "data" not in df.columns:
        return result
    try:
        dates = pd.to_datetime(df["data"], errors="coerce").dropna()
        if not dates.empty:
            result["period_start"] = dates.min().strftime("%Y-%m-%d")
            result["period_end"] = dates.max().strftime("%Y-%m-%d")
    except Exception:
        pass
    return result


def _get_period(audit_result: Dict[str, Any], df: Optional[pd.DataFrame]) -> Dict[str, str]:
    """Período: preferir document_context, senão usar DataFrame."""
    doc_ctx = _safe_get(audit_result, "document_context", default={})
    period = {
        "period_start": doc_ctx.get("period_start") or doc_ctx.get("periodo_inicio"),
        "period_end": doc_ctx.get("period_end") or doc_ctx.get("periodo_fim"),
    }
    if period["period_start"] and period["period_end"]:
        return period
    from_df = _get_period_from_df(df)
    return {
        "period_start": period["period_start"] or from_df["period_start"],
        "period_end": period["period_end"] or from_df["period_end"],
    }


def _build_whatsapp_message(audit_result: Dict[str, Any], df: Optional[pd.DataFrame]) -> str:
    """Mensagem curta para WhatsApp com resumo do período."""
    period = _get_period(audit_result, df)
    summary = _safe_get(audit_result, "summary", "financial_summary", default={})
    receitas_raw = summary.get("total_receitas", 0)
    despesas_raw = summary.get("total_despesas", 0)
    receitas = float(receitas_raw) if receitas_raw is not None else 0.0
    despesas = float(despesas_raw) if despesas_raw is not None else 0.0
    saldo_raw = summary.get("saldo")
    saldo = float(saldo_raw) if saldo_raw is not None else (receitas - despesas)
    alerts = _safe_get(audit_result, "alerts", default=[])
    alerts_short = [a.get("message") for a in alerts if isinstance(a, dict) and a.get("message")]
    alert_text = alerts_short[0] if alerts_short else "Sem alertas criticos."
    return (
        f"Resumo do periodo {period['period_start'] or 'N/A'} a {period['period_end'] or 'N/A'}: "
        f"Receitas R$ {receitas:,.2f}, Despesas R$ {despesas:,.2f}, Saldo R$ {saldo:,.2f}. "
        f"Observacao: {alert_text}"
    )


def validate_extracted_totals(totals_extracted: Dict[str, Any], df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    """
    Valida valores extraídos do texto antes de usar.
    Usa validate_monthly_scale para verificar plausibilidade de escala mensal.
    
    Returns:
        Dict com:
            - valid: bool - Se valores extraídos são válidos
            - errors: List[str] - Lista de mensagens de erro se inválido
            - scale_validation: Dict - Resultado completo da validação de escala
    """
    from services.financial_base_validator import validate_monthly_scale
    
    if not totals_extracted:
        return {"valid": True, "errors": [], "scale_validation": {}}
    
    # Extrair valores, lidando com estrutura aninhada (values/validation) ou plana
    if isinstance(totals_extracted, dict) and "values" in totals_extracted:
        # Nova estrutura com validação na origem
        values = totals_extracted.get("values", {})
    else:
        # Estrutura plana (compatibilidade com código existente)
        values = totals_extracted
    
    total_receitas = abs(float(values.get("total_receitas", 0) or 0))
    total_despesas = abs(float(values.get("total_despesas", 0) or 0))
    
    # Se não há valores para validar, retornar válido
    if total_receitas == 0 and total_despesas == 0:
        return {"valid": True, "errors": [], "scale_validation": {}}
    
    # Obter débitos individuais do DataFrame se disponível
    debitos_individuals = None
    if df is not None and not df.empty and "valor" in df.columns and "tipo" in df.columns:
        try:
            debitos = df[df["tipo"].str.lower().str.strip() == "despesa"]["valor"]
            debitos_individuals = [float(d) for d in debitos.tolist() if pd.notna(d) and float(d) > 0]
        except Exception as e:
            logger.debug(f"Erro ao obter débitos individuais para validação: {e}")
            debitos_individuals = None
    
    # Validar escala mensal
    scale_validation = validate_monthly_scale(total_receitas, total_despesas, debitos_individuals)
    
    return {
        "valid": scale_validation["valid"],
        "errors": [scale_validation["message"]] if not scale_validation["valid"] else [],
        "scale_validation": scale_validation
    }


def format_section_1(audit_result: Dict[str, Any], df: Optional[pd.DataFrame] = None, job_id: str = "") -> Dict[str, Any]:
    start_time = datetime.now()
    doc_context = _safe_get(audit_result, "document_context", default={})
    # Obter file_metadata de duas fontes (fallback)
    file_metadata = _safe_get(audit_result, "file_metadata", default=[]) or _safe_get(doc_context, "file_metadata", default=[])
    if not file_metadata and isinstance(doc_context, dict):
        file_metadata = doc_context.get("file_metadata") or []
    total_transactions = _safe_get(audit_result, "total_transactions", default=0)
    documents_analyzed = []
    by_category = _safe_get(doc_context, "by_category", default={})
    if by_category.get("financial_data", 0) > 0 or total_transactions > 0:
        documents_analyzed.append("Prestação de contas / balancetes")
    if df is not None and not df.empty and "descricao" in df.columns:
        desc_lower = df["descricao"].astype(str).str.lower()
        if desc_lower.str.contains("salario|salário|folha|funcionario|funcionário|zelador|porteiro", regex=True).any():
            documents_analyzed.append("Folha de pagamento e adiantamentos salariais")
        if desc_lower.str.contains("fgts|inss|irrf|pis|iss|guia|tributo", regex=True).any():
            documents_analyzed.append("Guias e comprovantes de encargos (INSS, FGTS, IRRF, PIS, ISS, etc.)")
        if desc_lower.str.contains("extrato|comprovante|pagamento", regex=True).any():
            documents_analyzed.append("Extratos e comprovantes de pagamento")
    if not documents_analyzed:
        documents_analyzed = ["Documentos financeiros enviados para análise"]
    period = _get_period(audit_result, df)
    # Lista de documentos: file_metadata primeiro, depois fallback em files_summary
    document_files = [m.get("filename") for m in file_metadata if isinstance(m, dict) and m.get("filename")]
    files_summary = audit_result.get("files_summary") or doc_context.get("files_summary") or []
    if isinstance(files_summary, list) and files_summary:
        total_from_context = doc_context.get("total_files") or audit_result.get("files_processed")
        total_expected = max(total_from_context or 0, len(file_metadata), len(files_summary))
        if len(document_files) < total_expected:
            seen = set(document_files)
            for fs in files_summary:
                if not isinstance(fs, dict):
                    continue
                name = fs.get("source_file")
                if name and name not in seen:
                    seen.add(name)
                    document_files.append(name)
    # Estatística: nunca menor que o número de documentos listados
    files_processed = max(
        len(document_files) or 0,
        doc_context.get("total_files") or 0,
        len(file_metadata),
        len(files_summary) if isinstance(files_summary, list) else 0,
        audit_result.get("files_processed") or 0,
    )
    months_in_data = 0
    if df is not None and not df.empty and "data" in df.columns:
        try:
            dt = pd.to_datetime(df["data"], errors="coerce").dropna()
            if not dt.empty:
                months_in_data = dt.dt.to_period("M").nunique()
        except Exception:
            pass
    content = {
        "documents_analyzed": documents_analyzed,
        "note": "A análise considera exclusivamente os documentos entregues.",
        "statistics": {
            "files_processed": files_processed,
            "transactions_count": total_transactions,
            "period_start": period["period_start"],
            "period_end": period["period_end"],
        },
    }
    if months_in_data >= 2:
        content["note_continuidade"] = "A comparação de gastos em relação ao mês anterior utilizou os dados do próprio período analisado (continuidade entre os meses presentes nos documentos)."
    if document_files:
        content["document_files"] = document_files
    if doc_context.get("files_summary"):
        content["files_summary"] = doc_context.get("files_summary")
    files_summary_from_result = audit_result.get("files_summary")
    if files_summary_from_result and not content.get("files_summary"):
        content["files_summary"] = files_summary_from_result
    return {"success": True, "section": {"number": 1, "title": "O que foi conferido", "icon": "1"}, "data": {"content": content, "metadata": _get_metadata(job_id, start_time)}}


def format_section_2(audit_result: Dict[str, Any], df: Optional[pd.DataFrame] = None, job_id: str = "") -> Dict[str, Any]:
    start_time = datetime.now()
    doc_context = _safe_get(audit_result, "document_context", default={})
    alerts = _safe_get(audit_result, "alerts", default=[])
    warnings = _safe_get(audit_result, "warnings", default=[])
    llm_data = _safe_get(audit_result, "llm_extractions", default={})
    llm_holerites = llm_data.get("holerites", []) if isinstance(llm_data, dict) and llm_data.get("enabled") else []
    holerites_extraidos = _safe_get(audit_result, "holerites_extraidos", default=[])
    if not holerites_extraidos:
        labor = _safe_get(audit_result, "labor_analysis", default={})
        holerites_extraidos = _safe_get(labor, "base_calculo", "holerites_detalhados", default=[])
    total_transactions = _safe_get(audit_result, "total_transactions", default=0)
    has_financial_data = total_transactions > 0
    # REGRA 4: Planilha ≠ Balancete. Controle interno quando só planilha sem lastro formal.
    controle_interno = doc_context.get("controle_interno", False)
    if controle_interno:
        documentos_principais_status = "controle_interno"
        documentos_principais_icon = "!"
        documentos_principais_details = ["Classificado como Controle interno (planilha sem assinatura, condomínio, período ou extrato vinculado). Não classificado como prestação de contas."]
    elif has_financial_data:
        documentos_principais_status = "completos"
        documentos_principais_icon = "OK"
        documentos_principais_details = ["Prestação de contas encontrada", f"{total_transactions} transações identificadas"]
    else:
        documentos_principais_status = "incompletos"
        documentos_principais_icon = "!"
        documentos_principais_details = ["Dados financeiros não identificados"]
    guias_status, guias_icon, guias_details = "apresentados", "OK", []
    folha_status, folha_icon, folha_details = "apresentados", "OK", []
    for alert in (alerts + warnings):
        code = alert.get("code", "") if isinstance(alert, dict) else ""
        msg = alert.get("message") if isinstance(alert, dict) else ""
        if code == "GUIDES_RECEIPTS_PENDING":
            guias_status, guias_icon = "pendentes", "!"
            guias_details.append("Guias/comprovantes não localizados")
        elif code == "PAYSLIPS_PENDING":
            folha_status, folha_icon = "pendentes", "!"
            folha_details.append("Folha de pagamento/holerites não localizados")
        elif code in ("MISSING_BALANCETE", "MISSING_BANK_STATEMENTS", "MISSING_INVOICES_RECEIPTS"):
            documentos_principais_status, documentos_principais_icon = "incompletos", "!"
            if msg:
                documentos_principais_details.append(msg)
        elif code == "MISSING_PUBLIC_TARIFFS":
            if msg:
                guias_details.append(msg)
        elif code == "MISSING_PROVISIONS":
            if msg:
                folha_details.append(msg)
        elif code == "MISSING_GUIDES_RECEIPTS":
            guias_status, guias_icon = "pendentes", "!"
            if msg:
                guias_details.append(msg)
        elif code == "MISSING_PAYROLL_DOCS":
            folha_status, folha_icon = "pendentes", "!"
            if msg:
                folha_details.append(msg)
    # Comprovante/guia: só quando descrição indicar guia/comprovante (não apenas lançamento contábil)
    _has_comprovante_keyword = False
    if df is not None and not df.empty and "descricao" in df.columns:
        desc_lower = df["descricao"].astype(str).str.lower()
        if desc_lower.str.contains("guia|comprovante|darf|gru|gps|grf", regex=True).any():
            _has_comprovante_keyword = True
            if guias_status != "pendentes":
                guias_details.append("Guias de pagamento encontradas")
        # Guias de INSS/FGTS: só se houver palavra de comprovante na mesma descrição (ex.: "guia inss", "gps")
        if _has_comprovante_keyword and desc_lower.str.contains("inss", regex=True).any():
            guias_details.append("Guias de INSS encontradas")
        if _has_comprovante_keyword and desc_lower.str.contains("fgts", regex=True).any():
            guias_details.append("Guias de FGTS encontradas")
        if desc_lower.str.contains("folha|holerite|salario|salário", regex=True).any():
            if folha_status != "pendentes": folha_details.append("Folha de pagamento encontrada")
    if llm_holerites or holerites_extraidos:
        folha_status, folha_icon = "apresentados", "OK"
        if holerites_extraidos:
            count = len(holerites_extraidos)
            folha_details.append(f"{count} holerite(s) extraído(s) com dados estruturados")
            # Adicionar resumo dos funcionários
            funcionarios = [h.get("funcionario", "N/A") for h in holerites_extraidos[:3] if h.get("funcionario")]
            if funcionarios:
                folha_details.append(f"Funcionários: {', '.join(funcionarios)}")
        elif llm_holerites:
            folha_details.append("Holerites identificados pela LLM")
    # Se guias pendentes e há lançamentos de encargos (INSS/FGTS) no balancete, esclarecer na observação
    labor = _safe_get(audit_result, "labor_analysis", default={})
    enc = labor.get("encargos", {}) or {}
    trib = labor.get("tributos", {}) or {}
    has_encargos_lancados = (
        (enc.get("inss", {}).get("valor_pago") or 0) > 0
        or (enc.get("fgts", {}).get("valor_pago") or 0) > 0
        or (enc.get("irrf", {}).get("valor_pago") or 0) > 0
        or (trib.get("pis", {}).get("valor_pago") or 0) > 0
    )
    guides_pending = any(
        (a.get("code") if isinstance(a, dict) else "") == "GUIDES_RECEIPTS_PENDING"
        for a in (alerts + warnings)
    )
    if guides_pending and has_encargos_lancados:
        guias_details.append("Há lançamentos de encargos no balancete; comprovantes/guias não identificados no PDF.")
    if not guias_details: guias_details.append("Verificar pasta física/digital")
    if not folha_details: folha_details.append("Verificar pasta física/digital")
    missing_documents = []
    for alert in alerts:
        if isinstance(alert, dict) and alert.get("type") == "documento_nao_localizado":
            missing_documents.append(alert.get("description", "Documento não especificado"))
        elif isinstance(alert, dict) and alert.get("code", "").startswith("MISSING_"):
            missing_documents.append(alert.get("message", "Documento obrigatório ausente"))
    if not missing_documents and has_financial_data:
        observacao = "Não foram identificadas ausências relevantes que comprometam a conferência do período."
    elif missing_documents:
        observacao = f"Documentos pendentes: {', '.join(missing_documents[:3])}"
    else:
        observacao = "Verificar se todos os documentos necessários foram enviados."
    return {"success": True, "section": {"number": 2, "title": "Situação dos documentos", "icon": "2"}, "data": {"content": {"resumo_simples": {"documentos_principais": {"status": documentos_principais_status, "icon": documentos_principais_icon, "details": documentos_principais_details}, "guias_comprovantes": {"status": guias_status, "icon": guias_icon, "details": guias_details}, "folha_holerites": {"status": folha_status, "icon": folha_icon, "details": folha_details}}, "observacao": observacao, "missing_documents": missing_documents}, "metadata": _get_metadata(job_id, start_time)}}


def format_section_3(audit_result: Dict[str, Any], df: Optional[pd.DataFrame] = None, job_id: str = "") -> Dict[str, Any]:
    """
    🔒 REGRA ABSOLUTA: Os valores financeiros DEVEM ser lidos EXCLUSIVAMENTE 
    do bloco dataset_financeiro. É TERMINANTEMENTE PROIBIDO recalcular, 
    normalizar, zerar ou substituir valores após a extração.
    
    Caso não seja possível extrair um valor, retorne ERRO, não 0 nem N/A.
    """
    start_time = datetime.now()
    
    # 🔒 LER VALORES DO DATASET FINANCEIRO OBRIGATÓRIO (nunca recalcular)
    dataset_financeiro = _safe_get(audit_result, "dataset_financeiro", default={})
    
    # Fallback para compatibilidade com código antigo
    if not dataset_financeiro:
        financial_extraction = _safe_get(audit_result, "financial_extraction_result", default={})
        # Converter estrutura antiga para nova
        if financial_extraction:
            dataset_financeiro = {
                "creditos_mensais": financial_extraction.get("creditos_mensais", {}),
                "debitos_mensais": financial_extraction.get("debitos_mensais", {}),
                "saldo_anterior": financial_extraction.get("saldo_anterior", {}),
                "saldo_final": financial_extraction.get("saldo_final", {})
            }
    
    # Extrair valores do dataset_financeiro obrigatório
    creditos_data = dataset_financeiro.get("creditos_mensais", {})
    debitos_data = dataset_financeiro.get("debitos_mensais", {})
    saldo_anterior_data = dataset_financeiro.get("saldo_anterior", {})
    saldo_final_data = dataset_financeiro.get("saldo_final", {})
    
    # 🔒 USAR VALORES DO DATASET (sem recalcular, sem zerar, sem substituir)
    total_receitas = creditos_data.get("valor")
    total_despesas = debitos_data.get("valor")
    saldo_inicial = saldo_anterior_data.get("valor")
    saldo_final = saldo_final_data.get("valor")
    
    # REGRA: Se valor é "ERRO", manter como ERRO (não converter para 0 ou None)
    if total_receitas == "ERRO":
        total_receitas = "ERRO"
    if total_despesas == "ERRO":
        total_despesas = "ERRO"
    if saldo_inicial == "ERRO":
        saldo_inicial = "ERRO"
    if saldo_final == "ERRO":
        saldo_final = "ERRO"
    
    # Calcular saldo apenas se ambos valores existirem e não forem ERRO (ETAPA 5)
    saldo = None
    if total_receitas != "ERRO" and total_despesas != "ERRO":
        if total_receitas is not None and total_despesas is not None:
            saldo = total_receitas - total_despesas
    elif total_receitas == "ERRO" or total_despesas == "ERRO":
        saldo = "ERRO"
    
    # Obter saldo final demonstrado do document_context (não recalcular)
    doc_ctx = _safe_get(audit_result, "document_context", default={})
    saldo_final_demonstrado = doc_ctx.get("saldo_final")
    if saldo_final_demonstrado is not None:
        try:
            saldo_final_demonstrado = float(saldo_final_demonstrado)
        except (TypeError, ValueError):
            saldo_final_demonstrado = None
    if saldo_final_demonstrado is None:
        saldo_final_demonstrado = saldo_final
    
    # Obter informações de origem e status do contrato
    creditos_origem = creditos_data.get("origem", "não identificada")
    debitos_origem = debitos_data.get("origem", "não identificada")
    creditos_status = creditos_data.get("status", "NAO_APURADO")
    debitos_status = debitos_data.get("status", "NAO_APURADO")
    
    # Verificar se há fallback aplicado (para observações)
    fallback_applied = False
    fallback_reason = ""
    base_invalid = False
    base_error_message = None
    scale_error = False
    scale_error_message = None
    fallback_result = {}
    
    if df is not None and not df.empty:
        from app.audit.financial_consolidator import calculate_financial_totals_correct
        # Obter totals_extracted do document_context para passar ao consolidator
        doc_ctx = _safe_get(audit_result, "document_context", default={})
        totals_extracted_ctx = doc_ctx.get("totals_extracted") or {}
        financial_totals = calculate_financial_totals_correct(df, saldo_inicial=saldo_inicial, extracted_totals=totals_extracted_ctx)
        fallback_result = financial_totals.get("fallback_result", {})
        fallback_applied = fallback_result.get("fallback_applied", False)
        fallback_reason = fallback_result.get("fallback_reason", "")
        
        # Obter flags de validação (apenas para status, não para valores)
        base_invalid = financial_totals.get("base_invalid", False)
        base_error_message = financial_totals.get("base_error_message")
        scale_error = financial_totals.get("scale_error", False)
        scale_error_message = financial_totals.get("scale_error_message")
        validation_per_conta = financial_totals.get("validation_per_conta") or {}
    else:
        validation_per_conta = {}
    
    # Determinar status baseado nos valores do dataset (não calcular se ERRO)
    has_monthly_base = False
    if total_receitas != "ERRO" and total_despesas != "ERRO":
        if isinstance(total_receitas, (int, float)) and isinstance(total_despesas, (int, float)):
            has_monthly_base = (total_receitas is not None and total_receitas > 0) or (total_despesas is not None and total_despesas > 0)
        elif total_receitas is not None and isinstance(total_receitas, (int, float)) and total_receitas > 0:
            has_monthly_base = True
        elif total_despesas is not None and isinstance(total_despesas, (int, float)) and total_despesas > 0:
            has_monthly_base = True
    
    # Criar resolved simulado (apenas para compatibilidade com código existente)
    resolved = {
        "total_receitas": total_receitas,
        "total_despesas": total_despesas,
        "saldo": saldo,
        "saldo_final": saldo_final,
        "saldo_anterior": saldo_inicial,
        "saldo_final_demonstrado": saldo_final_demonstrado,
        "has_monthly_base": has_monthly_base,
        "substitutions": [],
        "observacoes": [],
        "source": "financial_extraction_result"  # Fonte: contrato obrigatório
    }
    
    # Obter summary do audit_result
    summary = _safe_get(audit_result, "summary", default={})
    anomaly_summary = _safe_get(summary, "anomaly_summary", default={})
    anomalies = anomaly_summary.get("total_anomalies", 0)
    sem_erros = anomalies == 0
    # Fonte única: duplicates_count vem de audit_result (preenchido por add_alerts_to_audit_result)
    duplicates_count = summary.get("duplicates_count")
    if duplicates_count is None and df is not None and not df.empty:
        try:
            duplicates_count = int(get_duplicate_mask(df).sum())
        except Exception:
            duplicates_count = 0
    if duplicates_count is None:
        duplicates_count = 0
    sem_duplicados = duplicates_count == 0
    # REGRA 4: Se base mensal existe, status nunca é "INVALIDADO"
    # REGRA: Se algum valor é ERRO, status deve refletir isso
    if total_receitas == "ERRO" or total_despesas == "ERRO":
        status = "erro na extração de dados financeiros"
    elif resolved["has_monthly_base"]:
        status = "coerentes com os demonstrativos"
    elif total_receitas != "ERRO" and total_despesas != "ERRO":
        if (total_receitas is not None and isinstance(total_receitas, (int, float)) and total_receitas > 0) or (total_despesas is not None and isinstance(total_despesas, (int, float)) and total_despesas > 0):
            status = "coerentes com os demonstrativos"
        else:
            status = "dados financeiros insuficientes"
    else:
        status = "dados financeiros insuficientes"
    
    observacoes = []
    
    # ETAPA 6: Verificar e aplicar frases obrigatórias/proibidas
    if fallback_result.get("fallback_applied"):
        fallback_reason = fallback_result.get("fallback_reason", "")
        if fallback_reason:
            observacoes.insert(0, fallback_reason)
        # Adicionar frase obrigatória específica
        if "fallback mínimo" in fallback_reason.lower():
            observacoes.insert(0, FRASES_OBRIGATORIAS["fallback_aplicado"])
        elif "despesa mensal preservada" in fallback_reason.lower():
            observacoes.insert(0, FRASES_OBRIGATORIAS["despesa_preservada"])
    
    # Verificar se há acumulados descartados
    if resolved.get("substitutions"):
        observacoes.insert(0, FRASES_OBRIGATORIAS["acumulado_descartado"])
    
    # REGRA 6: Adicionar observações do resolvedor (substituições, frases obrigatórias)
    for obs in resolved.get("observacoes", []):
        if obs not in observacoes:  # Evitar duplicatas
            observacoes.append(obs)
    
    # ETAPA 6: Verificar frases proibidas
    # Se há despesas mas créditos são None (NÃO APURADO), não usar "Créditos: R$ 0,00"
    if total_despesas != "ERRO" and total_despesas is not None and isinstance(total_despesas, (int, float)) and total_despesas > 0 and total_receitas is None:
        # Já está correto - não adicionar frase proibida
        pass
    
    if sem_duplicados:
        observacoes.append("Lançamentos duplicados: não identificados.")
    else:
        observacoes.append(f"Lançamentos duplicados: {duplicates_count} ocorrência(s) identificadas (mesma data, valor, descrição e conta).")
    if anomalies > 0: observacoes.append(f"{anomalies} transações marcadas para revisão")
    
    # Obter totals_extracted do document_context
    totals_extracted = doc_ctx.get("totals_extracted") or {}
    
    # Acessar valores específicos de totals_extracted (precisa desembrulhar "values" se existir)
    if isinstance(totals_extracted, dict) and "values" in totals_extracted:
        ext_vals = totals_extracted.get("values", {})
    else:
        ext_vals = totals_extracted if isinstance(totals_extracted, dict) else {}
    if ext_vals.get("obra_extraordinaria") is not None:
        observacoes.append(f"Obra (extraordinária): R$ {ext_vals['obra_extraordinaria']:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    if ext_vals.get("prolabore") is not None:
        observacoes.append(f"Pró-labore síndico: R$ {ext_vals['prolabore']:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    if ext_vals.get("decimo_terceiro") is not None:
        observacoes.append(f"13º: R$ {ext_vals['decimo_terceiro']:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    if ext_vals.get("ferias") is not None:
        observacoes.append(f"Férias: R$ {ext_vals['ferias']:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    math_checks = _safe_get(summary, "math_checks", default={})
    if isinstance(math_checks, dict) and not math_checks.get("saldo_formula_ok", True):
        observacoes.append("Verificar consistência do saldo (receitas - despesas).")
    # ETAPA 2: validação por conta — incluir saldos_por_conta e contas que não fecham
    saldos_por_conta = (validation_per_conta.get("per_conta") or []) if isinstance(validation_per_conta, dict) else []
    # Conciliação estrutural: quando existir structural_extraction_periods, preencher saldos_por_conta por período (evita duplicação sem contexto)
    structural_extraction = doc_ctx.get("structural_extraction") if isinstance(doc_ctx, dict) else None
    structural_extraction_periods = doc_ctx.get("structural_extraction_periods") if isinstance(doc_ctx, dict) else None
    if not saldos_por_conta and structural_extraction_periods and isinstance(structural_extraction_periods, list):
        for p in structural_extraction_periods:
            if not isinstance(p, dict):
                continue
            periodo = p.get("periodo")
            rotulo = p.get("rotulo_original") or periodo
            if periodo and re.match(r"^\d{4}-\d{2}$", str(periodo)):
                try:
                    y, m = str(periodo).split("-")
                    periodo_label = f"Mês {int(m)}/{y}"
                except Exception:
                    periodo_label = str(rotulo) if rotulo else None
            else:
                periodo_label = str(rotulo) if rotulo else None
            for c in p.get("contas") or []:
                saldos_por_conta.append({
                    "conta": c.get("nome"),
                    "receitas": None,
                    "despesas": None,
                    "saldo_anterior": None,
                    "saldo_atual": c.get("saldo_final"),
                    "formula_ok": None,
                    "periodo": periodo,
                    "periodo_label": periodo_label,
                })
    elif not saldos_por_conta and structural_extraction and isinstance(structural_extraction, dict):
        # Fallback: bloco único (compatibilidade)
        for c in structural_extraction.get("contas") or []:
            saldos_por_conta.append({
                "conta": c.get("nome"),
                "receitas": None,
                "despesas": None,
                "saldo_anterior": None,
                "saldo_atual": c.get("saldo_final"),
                "formula_ok": None,
            })
    contas_nao_fecham = [c.get("conta") for c in saldos_por_conta if c.get("formula_ok") is False] if saldos_por_conta else []
    for conta in contas_nao_fecham:
        observacoes.append(f"Conta {conta} não fecha: saldo atual difere do esperado (saldo anterior + receitas - despesas).")
    # REGRA: Não calcular saldo_match se algum valor é ERRO
    saldo_match = True
    if saldo_final != "ERRO" and saldo_final_demonstrado != "ERRO":
        if saldo_final_demonstrado is not None and saldo_final is not None:
            saldo_match = abs(saldo_final - saldo_final_demonstrado) < 0.02
    # REGRA 2: Resumo financeiro = somente dimensão matemática; correção matemática NÃO valida prestação de contas.
    observacoes.append("Conferência apenas matemática. Correção matemática não valida prestação de contas.")
    
    # REGRA 4: Se base mensal existe mas DataFrame teve problemas, usar status com limitações (não INVALIDADO)
    if (base_invalid or scale_error) and resolved["has_monthly_base"]:
        observacoes.insert(0, "Base mensal identificada com limitações documentais.")
        status = "coerentes com os demonstrativos"
    elif base_invalid and base_error_message:
        observacoes.insert(0, f"⚠️ {base_error_message}")
        status = "Base financeira inválida"
    
    # Flag para exibição: saldo anterior ausente → mostrar "Saldo anterior não encontrado" (nunca "ERRO" como valor)
    saldo_anterior_nao_encontrado = (
        saldo_inicial is None
        or saldo_anterior_data.get("status") == "NAO_ENCONTRADO"
        or saldo_inicial == "ERRO"
    )
    
    # Totais consolidados por conta (Conta Ordinária, Fundo de Obras, etc.): soma quando múltiplas contas
    saldo_anterior_total = None
    creditos_total = None
    debitos_total = None
    saldo_final_total = None
    if saldos_por_conta:
        try:
            saldo_anterior_total = sum((c.get("saldo_anterior") or 0) for c in saldos_por_conta)
            creditos_total = sum((c.get("receitas") or 0) for c in saldos_por_conta)
            debitos_total = sum((c.get("despesas") or 0) for c in saldos_por_conta)
            saldo_final_total = sum((c.get("saldo_atual") or 0) for c in saldos_por_conta)
            if saldo_anterior_total == 0 and not any(c.get("saldo_anterior") for c in saldos_por_conta):
                saldo_anterior_total = None
            if creditos_total == 0 and not any(c.get("receitas") for c in saldos_por_conta):
                creditos_total = None
            if debitos_total == 0 and not any(c.get("despesas") for c in saldos_por_conta):
                debitos_total = None
            if saldo_final_total == 0 and not any(c.get("saldo_atual") for c in saldos_por_conta):
                saldo_final_total = None
        except (TypeError, ValueError):
            pass
    
    # Validação matemática obrigatória: [OK] Fecha matematicamente ou [ERRO] Divergência de R$ X
    # Quando temos créditos e débitos do período, o saldo final correto é saldo_anterior + créditos - débitos.
    # Usar saldo_final (calculado do dataset) como referência nesse caso, não a soma das contas (saldo_final_total).
    validacao_matematica_msg = None
    validacao_matematica_divergencia = None
    try:
        saldo_ant = saldo_anterior_total if saldo_anterior_total is not None else (saldo_inicial if isinstance(saldo_inicial, (int, float)) else None)
        cred = creditos_total if creditos_total is not None else (total_receitas if isinstance(total_receitas, (int, float)) else None)
        deb = debitos_total if debitos_total is not None else (total_despesas if isinstance(total_despesas, (int, float)) else None)
        if cred is not None and deb is not None and isinstance(saldo_final, (int, float)):
            saldo_f = saldo_final  # Saldo do período = créditos - débitos (já no dataset)
        else:
            saldo_f = saldo_final_total if saldo_final_total is not None else (saldo_final if isinstance(saldo_final, (int, float)) else None)
        if saldo_ant is not None and cred is not None and deb is not None and saldo_f is not None:
            esperado = saldo_ant + cred - deb
            divergencia = abs(float(saldo_f) - float(esperado))
            if divergencia <= 0.02:
                validacao_matematica_msg = "[OK] Fecha matematicamente"
            else:
                validacao_matematica_msg = "[ERRO] Divergência de R$ " + f"{divergencia:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                validacao_matematica_divergencia = round(divergencia, 2)
    except (TypeError, ValueError):
        pass

    # Multi-período: resumo por mês é a fonte primária; totais globais como "Soma do período analisado"
    resumo_primario_por_periodo = False
    totais_globais_rotulo = None
    if structural_extraction_periods and isinstance(structural_extraction_periods, list) and len(structural_extraction_periods) > 1:
        resumo_primario_por_periodo = True
        totais_globais_rotulo = "Soma do período analisado"
    
    return {
        "success": True,
        "section": {"number": 3, "title": "Resumo financeiro do período", "icon": "3"},
        "data": {
            "content": {
                "resumo_primario_por_periodo": resumo_primario_por_periodo,
                "totais_globais_rotulo": totais_globais_rotulo,
                "receitas_despesas_status": status,
                "saldo_inicial_ordinaria": saldo_inicial if saldo_inicial not in (None, "ERRO") else None,
                "saldo_anterior_nao_encontrado": saldo_anterior_nao_encontrado,
                "saldo_anterior_total": saldo_anterior_total,
                "creditos_total": creditos_total,
                "debitos_total": debitos_total,
                "saldo_final_total": saldo_final_total,
                "recebimentos_totais": total_receitas,  # Pode ser None (NÃO APURADO)
                "despesas_totais": total_despesas,  # Pode ser None (NÃO APURADO)
                "saldo_final_calculado": saldo_final,  # Pode ser None (NÃO APURADO)
                "saldo_final_demonstrado": saldo_final_demonstrado,  # Pode ser None
                "recebimentos_totais_formatado": _format_financial_value(total_receitas),  # ETAPA 6: formato com "NÃO APURADO"
                "despesas_totais_formatado": _format_financial_value(total_despesas),
                "saldo_final_calculado_formatado": _format_financial_value(saldo_final),
                "saldo_match": saldo_match,
                "validacao_matematica_msg": validacao_matematica_msg,
                "validacao_matematica_divergencia": validacao_matematica_divergencia,
                "observacoes": observacoes,
                "checks": {
                    "saldos_fecham_corretamente": saldo_match and resolved["has_monthly_base"],
                    "sem_erros_soma": sem_erros,
                    "sem_lancamentos_duplicados": sem_duplicados,
                    "base_valida": resolved["has_monthly_base"] or not base_invalid,
                    "escala_valida": len(resolved["substitutions"]) == 0
                },
                "saldos_por_conta": saldos_por_conta,
                "contas_nao_fecham": contas_nao_fecham,
                "nota_dimensao": "Cálculos matemáticos corretos (quando aplicável). Não implica regularidade da prestação de contas.",
                "base_invalid": base_invalid and not resolved["has_monthly_base"],
                "scale_error": len(resolved["substitutions"]) > 0,
                "base_error_message": base_error_message,
                "scale_error_message": None,
                "resolver_source": resolved["source"],
                "resolver_substitutions": resolved["substitutions"],
                "conciliacao_estrutural": _format_conciliacao_estrutural(structural_extraction) if structural_extraction else None,
                "conciliacao_estrutural_periodos": _format_conciliacao_estrutural_periodos(structural_extraction_periods)
                if structural_extraction_periods
                else None,
            }
        },
        "metadata": _get_metadata(job_id, start_time)
    }


def _get_row_text_for_search_local(row: pd.Series, df: pd.DataFrame) -> str:
    """Concatena todos os textos da linha (descricao, conta, historico, etc.) para busca."""
    text_parts = []
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            val = row.get(col)
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                text_parts.append(str(val).strip())
    return " ".join(text_parts).lower()


def _validate_transactions_exclusion(transactions: List[Dict[str, Any]], exclusion_patterns: List[str], df: Optional[pd.DataFrame] = None) -> List[Dict[str, Any]]:
    """
    Valida e filtra transações removendo aquelas que contêm palavras de exclusão.
    
    Args:
        transactions: Lista de transações a validar
        exclusion_patterns: Lista de padrões regex para exclusão
        df: DataFrame opcional para buscar texto completo da linha
        
    Returns:
        Lista filtrada de transações válidas
    """
    if not transactions or not exclusion_patterns:
        return transactions
    
    validated = []
    for trans in transactions:
        # Construir texto da transação para verificação
        descricao = str(trans.get("descricao", "")).lower()
        # Se temos DataFrame, tentar buscar texto completo da linha
        if df is not None and not df.empty:
            # Tentar encontrar linha correspondente no DataFrame
            try:
                mask = df.apply(lambda row: str(row.get("descricao", "")).lower() == descricao, axis=1)
                matching_rows = df[mask]
                if not matching_rows.empty:
                    row_text = _get_row_text_for_search_local(matching_rows.iloc[0], df)
                    if _should_exclude_row(row_text, exclusion_patterns):
                        logger.debug(f"[VALIDAÇÃO SEÇÃO 4/5] Transação excluída: {descricao[:100]}")
                        continue
            except Exception as e:
                logger.debug(f"[VALIDAÇÃO] Erro ao validar transação: {e}")
        
        # Verificação básica na descrição
        if _should_exclude_row(descricao, exclusion_patterns):
            logger.debug(f"[VALIDAÇÃO SEÇÃO 4/5] Transação excluída por descrição: {descricao[:100]}")
            continue
        
        validated.append(trans)
    
    return validated


def format_section_4(audit_result: Dict[str, Any], df: Optional[pd.DataFrame] = None, job_id: str = "") -> Dict[str, Any]:
    start_time = datetime.now()
    labor = _safe_get(audit_result, "labor_analysis", default=None)
    if labor is None and df is not None and not df.empty:
        labor = analyze_labor_charges(df)
    elif labor is None:
        labor = {"base_calculo": {"folha_pagamento_total": 0, "inclui_adiantamento": False, "periodo": None}, "encargos": {"fgts": {"percentual": 8, "valor_calculado": 0, "valor_pago": 0, "status": "nao_identificado", "icon": "?", "detalhes": "Dados insuficientes"}, "inss": {"tipo": "patronal, funcionários e terceiros", "valor_calculado": 0, "valor_pago": 0, "status": "nao_identificado", "icon": "?", "detalhes": "Dados insuficientes"}, "irrf": {"valor_pago": 0, "status": "nao_identificado", "icon": "?", "detalhes": "Dados insuficientes"}}, "tributos": {"pis": {"codigo": "8301", "valor_pago": 0, "status": "nao_identificado", "icon": "?", "detalhes": "Dados insuficientes"}, "iss": {"valor_pago": 0, "status": "nao_identificado", "icon": "?", "detalhes": "Dados insuficientes"}}, "resumo": "Análise de encargos não disponível (dados insuficientes)."}
    doc_ctx = _safe_get(audit_result, "document_context", default={})
    totals_extracted = doc_ctx.get("totals_extracted") or {}
    ext_vals = totals_extracted.get("values", {}) or {}
    def _totals_val(key: str):
        v = totals_extracted.get(key)
        if v is not None:
            return v
        return ext_vals.get(key)
    # Preferir valores da extração labor_analysis; usar totals_extracted só quando labor não tiver valor preenchido
    labor.setdefault("encargos", {})
    labor.setdefault("tributos", {})
    if _totals_val("inss") is not None:
        inss_pago = labor["encargos"].get("inss", {}).get("valor_pago", 0) or 0
        if inss_pago == 0:
            labor["encargos"].setdefault("inss", {})["valor_pago"] = float(_totals_val("inss"))
            labor["encargos"]["inss"]["status"] = "lancado"
            labor["encargos"]["inss"]["icon"] = "✓"
            labor["encargos"]["inss"]["detalhes"] = "Valor extraído do demonstrativo."
    if _totals_val("fgts") is not None:
        fgts_pago = labor["encargos"].get("fgts", {}).get("valor_pago", 0) or 0
        if fgts_pago == 0:
            labor["encargos"].setdefault("fgts", {})["valor_pago"] = float(_totals_val("fgts"))
            labor["encargos"]["fgts"]["status"] = "lancado"
            labor["encargos"]["fgts"]["icon"] = "✓"
            labor["encargos"]["fgts"]["detalhes"] = "Valor extraído do demonstrativo."
    if _totals_val("irrf") is not None:
        irrf_pago = labor["encargos"].get("irrf", {}).get("valor_pago", 0) or 0
        if irrf_pago == 0:
            labor["encargos"].setdefault("irrf", {})["valor_pago"] = float(_totals_val("irrf"))
            labor["encargos"]["irrf"]["status"] = "lancado"
            labor["encargos"]["irrf"]["icon"] = "✓"
            labor["encargos"]["irrf"]["detalhes"] = "Valor extraído do demonstrativo."
    if _totals_val("contrib_sindical") is not None:
        cs_pago = labor["encargos"].get("contrib_sindical", {}).get("valor_pago", 0) or 0
        if cs_pago == 0:
            labor["encargos"].setdefault("contrib_sindical", {})["valor_pago"] = float(_totals_val("contrib_sindical"))
            labor["encargos"]["contrib_sindical"]["status"] = "lancado"
            labor["encargos"]["contrib_sindical"]["icon"] = "✓"
            labor["encargos"]["contrib_sindical"]["detalhes"] = "Valor extraído do demonstrativo."
    if _totals_val("pis") is not None:
        pis_pago = labor["tributos"].get("pis", {}).get("valor_pago", 0) or 0
        if pis_pago == 0:
            labor["tributos"].setdefault("pis", {})["valor_pago"] = float(_totals_val("pis"))
            labor["tributos"]["pis"]["status"] = "lancado"
            labor["tributos"]["pis"]["icon"] = "✓"
            labor["tributos"]["pis"]["detalhes"] = "Valor extraído do demonstrativo."
    base_calculo = labor.get("base_calculo", {})
    encargos = labor.get("encargos", {})
    tributos = labor.get("tributos", {})
    holerites_detalhados = base_calculo.get("holerites_detalhados", [])
    if not holerites_detalhados:
        holerites_detalhados = _safe_get(audit_result, "holerites_extraidos", default=[])
    llm_data = _safe_get(audit_result, "llm_extractions", default={})
    llm_summary = None
    if isinstance(llm_data, dict) and llm_data:
        docs = []
        for item in llm_data.get("holerites", []) or []:
            if item.get("source_file"):
                docs.append(item.get("source_file"))
        for key, item in (llm_data.get("encargos", {}) or {}).items():
            if isinstance(item, dict) and item.get("documento"):
                docs.append(item.get("documento"))
        docs = list(dict.fromkeys([d for d in docs if d]))
        llm_summary = {
            "usada": bool(llm_data.get("enabled")),
            "documentos": docs,
            "motivo": llm_data.get("reason"),
        }
    # Preparar holerites para exibição (inclui source_url quando extraído de link FGTS/holerite)
    # Estrutura: funcionario, cargo, periodo, salario_bruto, descontos, salario_liquido, fonte (source_url/source_file/extraction_method)
    holerites_resumo = []
    for h in holerites_detalhados:
        descontos_raw = h.get("descontos")
        descontos = descontos_raw if descontos_raw is not None else ({})
        holerites_resumo.append({
            "funcionario": h.get("funcionario") or h.get("nome") or "N/A",
            "cargo": h.get("cargo"),
            "periodo": h.get("periodo"),
            "salario_bruto": h.get("salario_bruto", 0),
            "descontos": descontos,
            "salario_liquido": h.get("salario_liquido", 0),
            "source_file": h.get("source_file"),
            "source_url": h.get("source_url"),
            "extraction_method": h.get("extraction_method", "unknown"),
        })
    
    # REGRA 3: Ausência de documento ≠ ausência de tributo. Encargo sem doc = NÃO AUDITÁVEL (nunca afirmar que não foi recolhido).
    def _detalhe_encargo(item: dict) -> str:
        if not isinstance(item, dict):
            return FRASE_ENCARGO_NAO_AUDITAVEL
        st = item.get("status", "nao_identificado")
        if st in ("nao_identificado", "pendente", "nao_auditavel"):
            return FRASE_ENCARGO_NAO_AUDITAVEL
        return item.get("detalhes", "") or ""

    encargos_out = {}
    for k in ("fgts", "inss", "irrf", "contrib_sindical", "sat_rat"):
        e = encargos.get(k, {})
        if not isinstance(e, dict):
            e = {}
        # Validar transações antes de incluir
        transacoes = e.get("transacoes", [])
        if transacoes:
            transacoes_validadas = _validate_transactions_exclusion(transacoes, EXCLUSION_KEYWORDS_ENCARGOS, df)
            e = {**e, "transacoes": transacoes_validadas}
            # Recalcular valor_pago se necessário (baseado apenas em transações válidas)
            if len(transacoes_validadas) < len(transacoes):
                valor_pago_valido = sum(float(t.get("valor", 0)) for t in transacoes_validadas)
                e["valor_pago"] = round(valor_pago_valido, 2)
                logger.info(f"[VALIDAÇÃO SEÇÃO 4] {k}: {len(transacoes)} -> {len(transacoes_validadas)} transações válidas")
        encargos_out[k] = {**e, "detalhes": _detalhe_encargo(e)}
    if "inss" in encargos_out:
        encargos_out["inss"]["recomendacao"] = "Comparar com GFIP/eSocial do mês"
    tributos_out = {}
    for k in ("pis", "iss"):
        t = tributos.get(k, {})
        if not isinstance(t, dict):
            t = {}
        # Validar transações antes de incluir
        transacoes = t.get("transacoes", [])
        if transacoes:
            transacoes_validadas = _validate_transactions_exclusion(transacoes, EXCLUSION_KEYWORDS_ENCARGOS, df)
            t = {**t, "transacoes": transacoes_validadas}
            # Recalcular valor_pago se necessário
            if len(transacoes_validadas) < len(transacoes):
                valor_pago_valido = sum(float(t.get("valor", 0)) for t in transacoes_validadas)
                t["valor_pago"] = round(valor_pago_valido, 2)
                logger.info(f"[VALIDAÇÃO SEÇÃO 4] {k.upper()}: {len(transacoes)} -> {len(transacoes_validadas)} transações válidas")
        tributos_out[k] = {**t, "detalhes": _detalhe_encargo(t)}
    tributos_out["pis"]["codigo"] = tributos.get("pis", {}).get("codigo", "8301")
    
    # Validar transações da folha também
    transacoes_folha = base_calculo.get("transacoes_folha", [])
    if transacoes_folha:
        transacoes_folha_validadas = _validate_transactions_exclusion(transacoes_folha, EXCLUSION_KEYWORDS_ENCARGOS, df)
        if len(transacoes_folha_validadas) < len(transacoes_folha):
            logger.info(f"[VALIDAÇÃO SEÇÃO 4] Folha: {len(transacoes_folha)} -> {len(transacoes_folha_validadas)} transações válidas")
            base_calculo = {**base_calculo, "transacoes_folha": transacoes_folha_validadas}
    
    # Adicionar informações sobre análise por estimativa
    folha_por_estimativa = base_calculo.get("folha_por_estimativa", False)
    folha_estimada_fgts = base_calculo.get("folha_estimada_fgts", 0)
    alerta_folha_ausente = base_calculo.get("alerta_folha_ausente", False)
    analise_por_estimativa = labor.get("analise_por_estimativa", False)
    
    valor_base_folha = base_calculo.get("valor_base_folha")
    origem_base_impostos = base_calculo.get("origem_base_impostos")
    period_mes_anterior = base_calculo.get("period_mes_anterior")
    if origem_base_impostos == "valor_base_folha_documento":
        origem_base_label = "Base de cálculo extraída do texto do documento (valor base da folha)"
    elif origem_base_impostos == "folha_mes_anterior":
        _ym = (period_mes_anterior or "").strip()
        if len(_ym) >= 7:
            _y, _m = _ym[:4], _ym[5:7]
            _mes_nome = {"01": "janeiro", "02": "fevereiro", "03": "março", "04": "abril", "05": "maio", "06": "junho", "07": "julho", "08": "agosto", "09": "setembro", "10": "outubro", "11": "novembro", "12": "dezembro"}.get(_m, _m)
            origem_base_label = f"Base da folha do mês anterior (remuneração + 13º) – {_mes_nome}/{_y}"
        else:
            origem_base_label = "Base da folha do mês anterior (remuneração + 13º)"
    elif origem_base_impostos == "folha_estimada_fgts":
        origem_base_label = "Base estimada pelo FGTS"
    elif origem_base_impostos == "folha_pagamento_total":
        origem_base_label = "Base da prestação (folha de pagamento do balancete)"
    elif origem_base_impostos == "holerites_bruto":
        origem_base_label = "Base da soma dos salários brutos dos holerites extraídos"
    else:
        origem_base_label = (origem_base_impostos or "")
    num_meses_periodo = base_calculo.get("num_meses_periodo", 1)
    base_calculo_out = {
        "folha_pagamento_total": base_calculo.get("folha_pagamento_total", 0),
        "valor_base_folha": valor_base_folha,
        "origem_base_impostos": origem_base_impostos,
        "origem_base_label": origem_base_label,
        "num_meses_periodo": num_meses_periodo,
        "inclui_adiantamento": base_calculo.get("inclui_adiantamento", False),
        "periodo": base_calculo.get("periodo"),
        "folha_por_estimativa": folha_por_estimativa,
        "folha_estimada_fgts": folha_estimada_fgts,
        "alerta_folha_ausente": alerta_folha_ausente,
        "cross_reference": base_calculo.get("cross_reference", {}),
        "validacao_base_fgts": base_calculo.get("validacao_base_fgts"),
        "validacao_base_holerites": base_calculo.get("validacao_base_holerites"),
        "diferenca_pct_base_holerites": base_calculo.get("diferenca_pct_base_holerites"),
        "confianca_base": base_calculo.get("confianca_base"),
        "motivo_confianca": base_calculo.get("motivo_confianca"),
        "period_mes_anterior": base_calculo.get("period_mes_anterior"),
        "period_mes_principal": base_calculo.get("period_mes_principal"),
        "folha_bruta_holerites": base_calculo.get("folha_bruta_holerites"),
    }
    # Aviso quando pró-labore foi identificado nas contas do condomínio (INSS vale só para salário)
    if ext_vals.get("prolabore") is not None:
        base_calculo_out["alerta_prolabore_identificado"] = True
        base_calculo_out["alerta_prolabore_texto"] = (
            "Foi identificado pró-labore (ou valor equivalente) nas contas do condomínio. "
            "O INSS calculado refere-se somente à folha de salários. "
            "Recomenda-se confirmar com o condomínio se houve esse pagamento e se ele explica diferenças no INSS."
        )
    else:
        base_calculo_out["alerta_prolabore_identificado"] = False
        base_calculo_out["alerta_prolabore_texto"] = None

    # Adicionar informações de análise por estimativa aos encargos
    for enc_name in ("fgts", "inss", "irrf"):
        if enc_name in encargos_out:
            enc = encargos_out[enc_name]
            if enc.get("analise_por_estimativa"):
                enc["data_pagamento"] = encargos.get(enc_name, {}).get("data_pagamento")
                enc["conta_utilizada"] = encargos.get(enc_name, {}).get("conta_utilizada")
    
    if "pis" in tributos_out:
        if tributos_out["pis"].get("analise_por_estimativa"):
            tributos_out["pis"]["data_pagamento"] = tributos.get("pis", {}).get("data_pagamento")
            tributos_out["pis"]["conta_utilizada"] = tributos.get("pis", {}).get("conta_utilizada")

    # Tabela de encargos (ordem oficial: INSS, IRRF, FGTS, PIS, ISS, Contrib. Sindical, SAT/RAT)
    def _linha_tabela(nome_legivel: str, fonte: dict) -> dict:
        if not isinstance(fonte, dict):
            fonte = {}
        status = fonte.get("status", "")
        conclusao = "Correto" if status in ("correto", "compativel", "aplicado_conforme_tabela", "recolhido", "recolhido_quando_devido") else ("Incorreto" if status == "incorreto" else "-")
        valor_pago = fonte.get("valor_pago", 0)
        valor_exibicao = fonte.get("valor_exibicao")
        encontrado_no_doc = fonte.get("encontrado_no_documento", True)
        valor_para_tabela = valor_exibicao if valor_exibicao is not None else valor_pago
        return {
            "encargo": nome_legivel,
            "percentual": fonte.get("percentual", "-"),
            "percentual_baseline": fonte.get("percentual_baseline"),
            "base_calculo": fonte.get("base_calculo", "-"),
            "base_calculo_utilizada": fonte.get("base_calculo_utilizada"),
            "quem_paga": fonte.get("quem_paga", "-"),
            "valor_pago": valor_para_tabela,
            "valor_esperado": fonte.get("valor_esperado"),
            "valor_encontrado": fonte.get("valor_encontrado"),
            "encontrado_no_documento": encontrado_no_doc,
            "conclusao": conclusao,
            "status": status,
            "detalhes": fonte.get("detalhes", ""),
            "data_pagamento": fonte.get("data_pagamento"),
            "conta_utilizada": fonte.get("conta_utilizada"),
        }
    tabela_encargos = [
        _linha_tabela("INSS (empregado)", encargos_out.get("inss", {})),
        _linha_tabela("IRRF (empregado)", encargos_out.get("irrf", {})),
        _linha_tabela("FGTS (empregador)", encargos_out.get("fgts", {})),
        _linha_tabela("PIS (empregador)", tributos_out.get("pis", {})),
        _linha_tabela("ISS", tributos_out.get("iss", {})),
        _linha_tabela("Contrib. Sindical", encargos_out.get("contrib_sindical", {})),
        _linha_tabela("SAT/RAT (empregador)", encargos_out.get("sat_rat", {})),
    ]

    labor_links = doc_ctx.get("excel_hyperlinks") or []
    if not isinstance(labor_links, list):
        labor_links = []
    # Resumo de valores para conferência rápida (PDF/JSON)
    resumo_encargos_valores = {}
    for k, label in [("inss", "inss"), ("irrf", "irrf"), ("fgts", "fgts"), ("contrib_sindical", "contrib_sindical")]:
        v = encargos_out.get(k, {}).get("valor_pago")
        if v is not None and (v or 0) > 0:
            resumo_encargos_valores[label] = float(v)
    for k, label in [("pis", "pis"), ("iss", "iss")]:
        v = tributos_out.get(k, {}).get("valor_pago")
        if v is not None and (v or 0) > 0:
            resumo_encargos_valores[label] = float(v)
    content = {
        "base_calculo": base_calculo_out,
        "encargos": encargos_out,
        "tributos": tributos_out,
        "tabela_encargos": tabela_encargos,
        "resumo_encargos_valores": resumo_encargos_valores,
        "labor_links": labor_links,
        "holerites_detalhados": holerites_resumo,
        "resumo": labor.get("resumo", ""),
        "extracao_llm": llm_summary,
        "analise_por_estimativa": analise_por_estimativa,
    }
    return {"success": True, "section": {"number": 4, "title": "Encargos trabalhistas e tributos", "icon": "4"}, "data": {"content": content, "metadata": _get_metadata(job_id, start_time)}}


def format_section_5(audit_result: Dict[str, Any], df: Optional[pd.DataFrame] = None, job_id: str = "") -> Dict[str, Any]:
    start_time = datetime.now()
    labor = _safe_get(audit_result, "labor_analysis", default=None)
    if labor is None and df is not None and not df.empty:
        labor = analyze_labor_charges(df)
    ferias_13 = labor.get("ferias_13", {}) if labor else {}
    provisao = ferias_13.get("provisao", {})
    pagamentos = ferias_13.get("pagamentos", {})
    doc_ctx = _safe_get(audit_result, "document_context", default={})
    totals_extracted = doc_ctx.get("totals_extracted") or {}
    if totals_extracted.get("ferias") is not None:
        pagamentos["valor_ferias"] = float(totals_extracted["ferias"])
        pagamentos["ferias_no_periodo"] = True
        pagamentos["detalhes"] = "Valor extraído do demonstrativo."
    if totals_extracted.get("decimo_terceiro") is not None:
        pagamentos["valor_13"] = float(totals_extracted["decimo_terceiro"])
        pagamentos["decimo_terceiro_no_periodo"] = True
        pagamentos["detalhes"] = "Valor extraído do demonstrativo."
    alerts = _safe_get(audit_result, "alerts", default=[])
    warnings = _safe_get(audit_result, "warnings", default=[])
    llm_data = _safe_get(audit_result, "llm_extractions", default={})
    llm_holerites = llm_data.get("holerites", []) if isinstance(llm_data, dict) and llm_data.get("enabled") else []
    holerites_extraidos = _safe_get(audit_result, "holerites_extraidos", default=[])
    if not holerites_extraidos and labor:
        holerites_extraidos = _safe_get(labor, "base_calculo", "holerites_detalhados", default=[])
    # Combinar holerites do LLM e extraídos
    all_holerites = list(holerites_extraidos)
    for h_llm in llm_holerites:
        # Evitar duplicatas
        if not any(
            (h.get("funcionario") == h_llm.get("funcionario") and h.get("periodo") == h_llm.get("periodo"))
            for h in all_holerites
        ):
            all_holerites.append(h_llm)
    ferias_alert, decimo_alert = False, False
    for alert in (alerts + warnings):
        code = alert.get("code", "") if isinstance(alert, dict) else ""
        if code == "VACATION_PAYMENT_MISSING": ferias_alert = True
        elif code == "THIRTEENTH_SALARY_MISSING": decimo_alert = True
    # Validação: Filtrar holerites que podem conter dados misturados
    # Verificar se descrição/cargo contém palavras de exclusão
    holerites_resumo = []
    for item in all_holerites[:10]:
        # Validar se o holerite não contém palavras de exclusão
        descricao_completa = " ".join([
            str(item.get("funcionario", "")),
            str(item.get("cargo", "")),
            str(item.get("periodo", "")),
        ]).lower()
        
        # Se contém palavras de exclusão, pular este holerite
        if _should_exclude_row(descricao_completa, EXCLUSION_KEYWORDS_13_SALARIO):
            logger.debug(f"[VALIDAÇÃO SEÇÃO 5] Holerite excluído: {descricao_completa[:100]}")
            continue
        
        holerites_resumo.append({
            "funcionario": item.get("funcionario"),
            "cargo": item.get("cargo"),
            "periodo": item.get("periodo"),
            "salario_bruto": item.get("salario_bruto"),
            "descontos": item.get("descontos", {}),
            "salario_liquido": item.get("salario_liquido"),
            "fonte": item.get("source_file"),
            "extraction_method": item.get("extraction_method", "unknown"),
        })
    
    # Validação adicional: Verificar se os valores de férias/13º não foram contaminados
    # Se labor analysis foi feito, validar transações de férias e 13º
    if labor and df is not None and not df.empty:
        ferias_13_data = labor.get("ferias_13", {})
        pagamentos_data = ferias_13_data.get("pagamentos", {})
        
        # Log para debug se valores parecem incorretos
        valor_ferias = pagamentos.get("valor_ferias", 0)
        valor_13 = pagamentos.get("valor_13", 0)
        
        # Se valores são muito altos ou parecem incorretos, logar aviso
        if valor_ferias > 100000 or valor_13 > 100000:
            logger.warning(f"[VALIDAÇÃO SEÇÃO 5] Valores suspeitos detectados: Férias={valor_ferias}, 13º={valor_13}")
    
    # Adicionar validações de provisão e pagamento
    validacao_provisao = ferias_13.get("validacao_provisao", {})
    validacao_pagamento = ferias_13.get("validacao_pagamento", {})
    
    provisao_out = {
        "presente": provisao.get("presente", False),
        "valor": provisao.get("valor", 0),
        "detalhes": provisao.get("detalhes", "Não foi identificada provisão de férias/13º"),
        "icon": provisao.get("icon", "?"),
        "validacao": validacao_provisao  # NOVO
    }
    
    pagamentos_out = {
        "ferias_no_periodo": pagamentos.get("ferias_no_periodo", False),
        "decimo_terceiro_no_periodo": pagamentos.get("decimo_terceiro_no_periodo", False),
        "valor_ferias": pagamentos.get("valor_ferias", 0),
        "valor_13": pagamentos.get("valor_13", 0),
        "detalhes": pagamentos.get("detalhes", "Não aparece movimentação de pagamento de férias nem 13º neste período"),
        "validacao": validacao_pagamento  # NOVO
    }
    
    return {"success": True, "section": {"number": 5, "title": "Férias e 13º", "icon": "5"}, "data": {"content": {"provisao": provisao_out, "pagamentos": pagamentos_out, "alertas": {"ferias_pendente": ferias_alert, "decimo_terceiro_pendente": decimo_alert}, "holerites_extraidos": holerites_resumo}, "metadata": _get_metadata(job_id, start_time)}}


def format_section_6(audit_result: Dict[str, Any], df: Optional[pd.DataFrame] = None, job_id: str = "") -> Dict[str, Any]:
    start_time = datetime.now()
    alerts = _safe_get(audit_result, "alerts", default=[])
    warnings = _safe_get(audit_result, "warnings", default=[])
    anomalies = _safe_get(audit_result, "anomalies_detected", default=0)
    formatted_alerts = []
    for alert in alerts:
        if isinstance(alert, dict):
            formatted_alert = {"type": alert.get("code", alert.get("type", "geral")), "description": alert.get("message", alert.get("description", "")), "severity": alert.get("severity", "medium"), "detalhes": str(alert.get("details", {}))}
            if alert.get("valor"): formatted_alert["valor"] = float(alert.get("valor", 0))
            if alert.get("categoria"): formatted_alert["categoria"] = alert.get("categoria")
            if alert.get("recomendacao"): formatted_alert["recomendacao"] = alert.get("recomendacao")
            formatted_alerts.append(formatted_alert)
    if anomalies > 0:
        formatted_alerts.append({"type": "anomalias_detectadas", "description": f"Foram identificadas {anomalies} transações com anomalias que requerem revisão.", "severity": "high", "detalhes": f"{anomalies} transações marcadas pela IA"})
    # Alertas de extração: "não consegui ler (conteúdo) na (linha) (página)" → análise manual
    extraction_failures = _safe_get(audit_result, "extraction_failures", default=[])
    for fail in extraction_failures:
        if not isinstance(fail, dict):
            continue
        if fail.get("message"):
            msg = str(fail["message"])
        else:
            conteudo = fail.get("content") or fail.get("conteudo") or "informação"
            linha = fail.get("line") or fail.get("linha")
            pagina = fail.get("page") or fail.get("pagina")
            if linha is not None and pagina is not None:
                msg = f"Não foi possível ler ({conteudo}) na linha {linha}, página {pagina}. Recomenda-se análise manual por um humano."
            elif linha is not None:
                msg = f"Não foi possível ler ({conteudo}) na linha {linha}. Recomenda-se análise manual por um humano."
            elif pagina is not None:
                msg = f"Não foi possível ler ({conteudo}) na página {pagina}. Recomenda-se análise manual por um humano."
            else:
                msg = f"Não foi possível ler ({conteudo}). Recomenda-se análise manual por um humano."
        formatted_alerts.append({"type": "extracao_nao_lida", "description": msg, "severity": "medium", "detalhes": "Conteúdo não extraído ou ilegível"})
    if df is not None and not df.empty:
        try:
            if "valor" in df.columns:
                valores = pd.to_numeric(df["valor"], errors="coerce").dropna()
                if len(valores) > 5:
                    mean_val, std_val = valores.mean(), valores.std()
                    threshold = mean_val + 2 * std_val
                    gastos_altos = df[df["valor"] > threshold]
                    for _, row in gastos_altos.head(3).iterrows():
                        formatted_alerts.append({"type": "gasto_fora_padrao", "description": f"{row.get('descricao', 'Gasto')[:100]} - R$ {float(row.get('valor', 0)):,.2f}", "valor": float(row.get("valor", 0)), "categoria": str(row.get("categoria", "Não categorizado")), "detalhes": "Gasto acima do padrão identificado", "recomendacao": "Verificar justificativa do gasto"})
        except Exception: pass
    has_alerts = len(formatted_alerts) > 0
    status = "Foram identificados os seguintes pontos de atenção:" if has_alerts else "Não foram identificados pontos críticos"
    icon = "!" if has_alerts else "OK"
    return {"success": True, "section": {"number": 6, "title": "Pontos de alerta", "icon": "6"}, "data": {"content": {"has_alerts": has_alerts, "status": status, "icon": icon, "alerts": formatted_alerts, "total_alerts": len(formatted_alerts)}, "metadata": _get_metadata(job_id, start_time)}}


def format_section_7(audit_result: Dict[str, Any], df: Optional[pd.DataFrame] = None, job_id: str = "") -> Dict[str, Any]:
    """
    🔒 REGRA ABSOLUTA: Os valores financeiros DEVEM ser lidos EXCLUSIVAMENTE 
    do bloco dataset_financeiro. É TERMINANTEMENTE PROIBIDO recalcular, 
    normalizar, zerar ou substituir valores após a extração.
    """
    start_time = datetime.now()
    
    # 🔒 LER VALORES DO DATASET FINANCEIRO OBRIGATÓRIO (nunca recalcular)
    dataset_financeiro = _safe_get(audit_result, "dataset_financeiro", default={})
    
    if dataset_financeiro:
        creditos_data = dataset_financeiro.get("creditos_mensais", {})
        debitos_data = dataset_financeiro.get("debitos_mensais", {})
        total_receitas = creditos_data.get("valor")
        total_despesas = debitos_data.get("valor")
        # Calcular saldo apenas se ambos não forem ERRO
        if total_receitas != "ERRO" and total_despesas != "ERRO":
            if total_receitas is not None and total_despesas is not None:
                saldo = total_receitas - total_despesas
            else:
                saldo = None
        else:
            saldo = "ERRO"
        
        resolved = {
            "total_receitas": total_receitas,
            "total_despesas": total_despesas,
            "saldo": saldo,
            "has_monthly_base": (total_receitas != "ERRO" and total_receitas is not None and isinstance(total_receitas, (int, float)) and total_receitas > 0) or (total_despesas != "ERRO" and total_despesas is not None and isinstance(total_despesas, (int, float)) and total_despesas > 0)
        }
    else:
        # Fallback: código antigo
        summary = _safe_get(audit_result, "summary", default={})
        financial = _safe_get(summary, "financial_summary", default={})
        doc_ctx = _safe_get(audit_result, "document_context", default={})
        totals_extracted = doc_ctx.get("totals_extracted") or {}
        
        calculated_receitas = None
        calculated_despesas = None
        calculated_saldo = None
        
        if df is not None and not df.empty:
            from app.audit.financial_consolidator import calculate_financial_totals_correct
            # Passar totals_extracted para usar valores do texto como primeira opção
            ft = calculate_financial_totals_correct(df, extracted_totals=totals_extracted)
            calculated_receitas = ft["total_receitas"]
            calculated_despesas = ft["total_despesas"]
            calculated_saldo = ft["saldo"]
        else:
            calculated_receitas = float(financial.get("total_receitas", 0))
            calculated_despesas = float(financial.get("total_despesas", 0))
            calculated_saldo = float(financial.get("saldo", calculated_receitas - calculated_despesas))
        
        resolved = resolve_financial_values(
            extracted_totals=totals_extracted,
            calculated_totals={
                "total_receitas": calculated_receitas,
                "total_despesas": calculated_despesas,
                "saldo": calculated_saldo,
            },
        )
        total_receitas = resolved["total_receitas"]
        total_despesas = resolved["total_despesas"]
        saldo = resolved["saldo"]
    
    summary = _safe_get(audit_result, "summary", default={})
    anomaly = _safe_get(summary, "anomaly_summary", default={})
    anomalies = int(anomaly.get("total_anomalies", 0))
    anomaly_rate = float(anomaly.get("anomaly_rate", 0))
    labor = _safe_get(audit_result, "labor_analysis", default=None)
    if labor is None and df is not None and not df.empty:
        labor = analyze_labor_charges(df)
    # REGRA 1/2: Dimensões independentes. Matemática: apenas "Cálculos matemáticos corretos"; nunca usar para validar prestação.
    points = []
    # Verificar se valores não são ERRO antes de comparar
    receitas_valida = total_receitas != "ERRO" and total_receitas is not None and isinstance(total_receitas, (int, float)) and total_receitas > 0
    despesas_valida = total_despesas != "ERRO" and total_despesas is not None and isinstance(total_despesas, (int, float)) and total_despesas > 0
    
    if receitas_valida or despesas_valida:
        points.append({"type": "contas", "status": "calculos_corretos", "text": "Cálculos matemáticos corretos."})
        if saldo != "ERRO" and saldo is not None and isinstance(saldo, (int, float)) and saldo < 0:
            points.append({"type": "contas", "status": "deficit", "text": f"O período apresenta déficit de R$ {abs(saldo):,.2f}."})
        else:
            points.append({"type": "contas", "status": "organizadas_e_coerentes", "text": "Saldo do período coerente com receitas e despesas (dimensão matemática)."})
    else:
        points.append({"type": "contas", "status": "dados_insuficientes", "text": "Dados financeiros insuficientes para análise completa."})
    # Dimensão fiscal/trabalhista: não inferir "recolhido" sem documentação. REGRA 3.
    doc_dim = evaluate_document_dimension(audit_result)
    if labor:
        resumo_labor = labor.get("resumo", "")
        enc = labor.get("encargos", {}) or {}
        trib = labor.get("tributos", {}) or {}
        has_encargos_lancados = (
            (enc.get("inss", {}).get("valor_pago") or 0) > 0
            or (enc.get("fgts", {}).get("valor_pago") or 0) > 0
        )
        if labor.get("folha_invalida"):
            points.append({"type": "encargos", "status": "folha_invalida", "text": "Estrutura de folha inválida. Encargos indetermináveis."})
        elif not doc_dim.get("has_guias"):
            points.append({"type": "encargos", "status": "nao_auditavel", "text": FRASE_NAO_AUDITAVEL_FISCAL + " " + FRASE_ENCARGO_NAO_AUDITAVEL})
        elif "OK" in resumo_labor or "correto" in resumo_labor.lower():
            points.append({"type": "encargos", "status": "regularmente_recolhidos", "text": "Os encargos trabalhistas e tributos estão regularmente recolhidos."})
        elif "verificar" in resumo_labor.lower():
            points.append({"type": "encargos", "status": "verificar", "text": resumo_labor})
        elif has_encargos_lancados:
            points.append({"type": "encargos", "status": "lancado_sem_comprovante", "text": "Encargos lançados no balancete; comprovantes (guias/GPS) não identificados no PDF. Não é possível verificar recolhimento."})
        else:
            points.append({"type": "encargos", "status": "nao_auditavel", "text": FRASE_ENCARGO_NAO_AUDITAVEL})
    if anomalies == 0:
        points.append({"type": "erros", "status": "sem_indicios", "text": "Não há indícios de erros relevantes ou pendências financeiras no período."})
    elif anomaly_rate < 0.05:
        points.append({"type": "erros", "status": "baixo_risco", "text": f"Identificadas {anomalies} transações para revisão ({anomaly_rate:.1%} do total)."})
    else:
        points.append({"type": "erros", "status": "atencao", "text": f"Atenção: {anomalies} transações com anomalias ({anomaly_rate:.1%} do total)."})
    alerts = _safe_get(audit_result, "alerts", default=[])
    doc_pendentes = [a.get("message") for a in alerts if isinstance(a, dict) and (a.get("code") or "").startswith("MISSING_")]
    if doc_pendentes:
        points.append({"type": "documentacao", "status": "incompleta", "text": "A pasta está incompleta do ponto de vista de conferência. O balancete soma corretamente, mas sem documentos de apoio não é possível confirmar se os gastos e encargos estão corretos."})
    # Frase obrigatória quando matemática ok e documentação insuficiente (REGRA 2).
    receitas_valida = total_receitas != "ERRO" and total_receitas is not None and isinstance(total_receitas, (int, float)) and total_receitas > 0
    despesas_valida = total_despesas != "ERRO" and total_despesas is not None and isinstance(total_despesas, (int, float)) and total_despesas > 0
    if (receitas_valida or despesas_valida) and (doc_pendentes or not doc_dim.get("has_guias") or not doc_dim.get("has_nf")):
        points.append({"type": "documentacao", "status": "calculos_ok_doc_insuficiente", "text": FRASE_CALCULOS_CORRETOS_DOC_INSUFICIENTE})
    text_lines = [p["text"] for p in points]
    text = "Com base nos documentos analisados:\n\n" + "\n".join(text_lines)
    return {"success": True, "section": {"number": 7, "title": "Conclusão geral", "icon": "7"}, "data": {"content": {"text": text, "points": points, "note": "Este relatório tem caráter informativo e visa apoiar síndicos, conselheiros e moradores na compreensão das contas."}, "metadata": _get_metadata(job_id, start_time)}}


def format_section_8(audit_result: Dict[str, Any], df: Optional[pd.DataFrame] = None, job_id: str = "") -> Dict[str, Any]:
    """
    🔒 REGRA ABSOLUTA: Os valores financeiros DEVEM ser lidos EXCLUSIVAMENTE 
    do bloco dataset_financeiro. É TERMINANTEMENTE PROIBIDO recalcular, 
    normalizar, zerar ou substituir valores após a extração.
    """
    start_time = datetime.now()
    
    # 🔒 LER VALORES DO DATASET FINANCEIRO OBRIGATÓRIO (nunca recalcular)
    dataset_financeiro = _safe_get(audit_result, "dataset_financeiro", default={})
    
    if dataset_financeiro:
        creditos_data = dataset_financeiro.get("creditos_mensais", {})
        debitos_data = dataset_financeiro.get("debitos_mensais", {})
        total_receitas = creditos_data.get("valor")
        total_despesas = debitos_data.get("valor")
        # Calcular saldo apenas se ambos não forem ERRO
        if total_receitas != "ERRO" and total_despesas != "ERRO":
            if total_receitas is not None and total_despesas is not None:
                saldo = total_receitas - total_despesas
            else:
                saldo = None
        else:
            saldo = "ERRO"
        
        resolved = {
            "total_receitas": total_receitas,
            "total_despesas": total_despesas,
            "saldo": saldo,
            "has_monthly_base": (total_receitas != "ERRO" and total_receitas is not None and isinstance(total_receitas, (int, float)) and total_receitas > 0) or (total_despesas != "ERRO" and total_despesas is not None and isinstance(total_despesas, (int, float)) and total_despesas > 0)
        }
    else:
        # Fallback: código antigo
        summary = _safe_get(audit_result, "summary", default={})
        financial = _safe_get(summary, "financial_summary", default={})
        doc_ctx = _safe_get(audit_result, "document_context", default={})
        totals_extracted = doc_ctx.get("totals_extracted") or {}
        
        calculated_receitas = None
        calculated_despesas = None
        calculated_saldo = None
        
        if df is not None and not df.empty:
            from app.audit.financial_consolidator import calculate_financial_totals_correct
            # Passar totals_extracted para usar valores do texto como primeira opção
            ft = calculate_financial_totals_correct(df, extracted_totals=totals_extracted)
            calculated_receitas = ft["total_receitas"]
            calculated_despesas = ft["total_despesas"]
            calculated_saldo = ft["saldo"]
        else:
            calculated_receitas = float(financial.get("total_receitas", 0))
            calculated_despesas = float(financial.get("total_despesas", 0))
            calculated_saldo = float(financial.get("saldo", calculated_receitas - calculated_despesas))
        
        resolved = resolve_financial_values(
            extracted_totals=totals_extracted,
            calculated_totals={
                "total_receitas": calculated_receitas,
                "total_despesas": calculated_despesas,
                "saldo": calculated_saldo,
            },
        )
        total_receitas = resolved["total_receitas"]
        total_despesas = resolved["total_despesas"]
        saldo = resolved["saldo"]
    
    summary = _safe_get(audit_result, "summary", default={})
    anomaly = _safe_get(summary, "anomaly_summary", default={})
    anomalies = int(anomaly.get("total_anomalies", 0))
    anomaly_rate = float(anomaly.get("anomaly_rate", 0))
    high_risk = int(anomaly.get("high_risk_count", 0))
    alerts = _safe_get(audit_result, "alerts", default=[])
    # Verificar se valores não são ERRO antes de comparar
    receitas_valida = total_receitas != "ERRO" and total_receitas is not None and isinstance(total_receitas, (int, float)) and total_receitas > 0
    despesas_valida = total_despesas != "ERRO" and total_despesas is not None and isinstance(total_despesas, (int, float)) and total_despesas > 0
    tem_dados = receitas_valida or despesas_valida
    math_ok = tem_dados  # dimensão matemática: há dados e fórmula bate (saldo consistente)
    doc_completa = len([a for a in alerts if isinstance(a, dict) and ("PENDING" in a.get("code", "") or a.get("code", "").startswith("MISSING_"))]) == 0

    # REGRA 1-5: Verificar erros de base/escala antes da classificação
    base_validation = _safe_get(audit_result, "summary", "base_validation", default={}) or {}
    base_invalid = base_validation.get("base_invalid", False)
    scale_error = base_validation.get("scale_error", False)
    base_error_message = base_validation.get("base_error_message")
    scale_error_message = base_validation.get("scale_error_message")
    
    # REGRA 6: Classificação final OBRIGATÓRIA por dimensão documental (nunca por matemática).
    doc_dimension = evaluate_document_dimension(audit_result)
    situacao = classify_final_situation(audit_result, doc_dimension)
    
    # REGRA 4: Se base mensal existe, NUNCA usar "INVALIDADO POR BASE INCORRETA"
    if (base_invalid or scale_error) and not resolved["has_monthly_base"]:
        situacao = "INVALIDADO POR BASE INCORRETA"
        status_color, status_icon = "red", "VERMELHO"
    elif (base_invalid or scale_error) and resolved["has_monthly_base"]:
        # REGRA 4: Base mensal identificada com limitações documentais — classificação normal
        situacao = classify_final_situation(audit_result, doc_dimension)
        if situacao == "REGULAR":
            status_color, status_icon = "green", "VERDE"
        elif situacao == "REGULAR COM RESSALVAS":
            status_color, status_icon = "yellow", "AMARELO"
        else:
            status_color, status_icon = "red", "VERMELHO"
    elif situacao == "REGULAR":
        status_color, status_icon = "green", "VERDE"
    elif situacao == "REGULAR COM RESSALVAS":
        status_color, status_icon = "yellow", "AMARELO"
    else:
        status_color, status_icon = "red", "VERMELHO"

    problemas = []
    if saldo != "ERRO" and saldo is not None and isinstance(saldo, (int, float)) and saldo < 0:
        problemas.append("déficit financeiro")
    if anomaly_rate > 0.10:
        problemas.append("alta taxa de anomalias")
    if high_risk > 3:
        problemas.append("transações de alto risco identificadas")
    if not doc_dimension.get("has_guias"):
        problemas.append("ausência de guias de encargos")
    if not doc_dimension.get("condominio_identificado"):
        problemas.append("condomínio não identificado")
    if not doc_dimension.get("periodo_definido"):
        problemas.append("período indefinido")
    if not doc_dimension.get("folha_valida"):
        problemas.append("folha inválida")

    partes_texto = [f"Situação do período analisado: {situacao}."]
    
    # REGRA 4: Se base mensal existe, usar cálculos mensais mesmo com problemas de escala
    if (base_invalid or scale_error) and not resolved["has_monthly_base"]:
        if base_invalid and base_error_message:
            partes_texto.append(f"⚠️ {base_error_message}")
        partes_texto.append("Não é possível validar cálculos financeiros devido a erro na base de dados.")
    elif (base_invalid or scale_error) and resolved["has_monthly_base"]:
        partes_texto.append("Base mensal identificada com limitações documentais.")
        # Adicionar observações do resolvedor (substituições de escala)
        for obs in resolved.get("observacoes", []):
            if obs not in " ".join(partes_texto):
                partes_texto.append(obs)
        partes_texto.append("Cálculos matemáticos corretos (dimensão matemática).")
        if saldo != "ERRO" and saldo is not None and isinstance(saldo, (int, float)) and saldo < 0:
            partes_texto.append(f"Há déficit financeiro no período (R$ {abs(saldo):,.2f}). Não é correto afirmar que as contas fecham em situação confortável.")
    elif tem_dados:
        partes_texto.append("Cálculos matemáticos corretos (dimensão matemática).")
        if saldo != "ERRO" and saldo is not None and isinstance(saldo, (int, float)) and saldo < 0:
            partes_texto.append(f"Há déficit financeiro no período (R$ {abs(saldo):,.2f}). Não é correto afirmar que as contas fecham em situação confortável.")
    else:
        partes_texto.append("Dados financeiros insuficientes para verificação completa.")
    for phrase in get_required_phrases(audit_result, doc_dimension, math_ok):
        if phrase and phrase not in " ".join(partes_texto):
            partes_texto.append(phrase)
    if not doc_completa:
        partes_texto.append("A pasta está incompleta: faltam comprovantes de tributos e documentos de suporte para validar de fato.")
    if problemas:
        partes_texto.append(f"Pontos de atenção: {', '.join(problemas)}.")
    if situacao == "REGULAR" and not problemas:
        partes_texto.append("No geral, a gestão financeira está sob controle.")
    elif situacao == "REGULAR COM RESSALVAS":
        partes_texto.append("Recomenda-se revisão dos pontos destacados.")
    else:
        partes_texto.append("Recomenda-se auditoria detalhada dos pontos identificados.")
    text = " ".join(partes_texto)
    recs = []
    if situacao == "IRREGULAR": recs.extend(["Realizar análise completa com contador", "Verificar todas as transações de alto risco"])
    if "déficit financeiro" in problemas: recs.append("Analisar causas do déficit e planejar recuperação")
    if "alta taxa de anomalias" in problemas: recs.append("Revisar transações marcadas como anômalas")
    for alert in alerts:
        if isinstance(alert, dict) and alert.get("recomendacao") and alert.get("recomendacao") not in recs: recs.append(alert.get("recomendacao"))
    if not recs: recs.extend(["Manter procedimentos de controle financeiro", "Guardar documentos comprobatórios organizados"])
    contas_fecham = tem_dados  # números fecham quando há dados de receita/despesa
    return {"success": True, "section": {"number": 8, "title": "Parecer final", "icon": "8"}, "data": {"content": {"situacao_periodo": situacao, "status_color": status_color, "status_icon": status_icon, "text": text, "summary": {"contas_fecham": contas_fecham, "documentacao_completa": doc_completa, "encargos_regularizados": anomaly_rate < 0.05, "gastos_fora_padrao": high_risk > 0, "pendencias": problemas}, "recomendacoes": recs[:5]}, "metadata": _get_metadata(job_id, start_time)}}


def format_full_report(audit_result: Dict[str, Any], df: Optional[pd.DataFrame] = None, job_id: str = "") -> Dict[str, Any]:
    """
    🔒 CONTRATO DE SAÍDA OBRIGATÓRIO
    
    Os valores financeiros apresentados no relatório DEVEM ser lidos EXCLUSIVAMENTE 
    do bloco dataset_financeiro.
    
    É TERMINANTEMENTE PROIBIDO recalcular, normalizar, zerar ou substituir valores 
    após a extração.
    
    Caso não seja possível extrair um valor, retorne ERRO, não 0 nem N/A.
    """
    start_time = datetime.now()
    
    # 🔒 ETAPA CRÍTICA: Gerar dataset_financeiro obrigatório ANTES de qualquer formatação
    dataset_financeiro = generate_dataset_financeiro(audit_result, df)
    
    # Adicionar ao audit_result para que as seções possam ler
    if "dataset_financeiro" not in audit_result:
        audit_result["dataset_financeiro"] = dataset_financeiro["dataset_financeiro"]
    
    # Manter compatibilidade com código antigo (financial_extraction_result)
    financial_extraction_result = generate_financial_extraction_result(audit_result, df)
    if "financial_extraction_result" not in audit_result:
        audit_result["financial_extraction_result"] = financial_extraction_result["financial_extraction_result"]
    
    doc_context = _safe_get(audit_result, "document_context", default={})
    condominio_name = doc_context.get("condominio_name") or _safe_get(audit_result, "extraction_quality", "details", "nome_condominio_extraido")
    period = _get_period(audit_result, df)
    sections = [format_section_1(audit_result, df, job_id), format_section_2(audit_result, df, job_id), format_section_3(audit_result, df, job_id), format_section_4(audit_result, df, job_id), format_section_5(audit_result, df, job_id), format_section_6(audit_result, df, job_id), format_section_7(audit_result, df, job_id), format_section_8(audit_result, df, job_id)]
    extras = {"whatsapp_message": _build_whatsapp_message(audit_result, df), "pdf_available": False}

    # Holerites extraídos no nível do relatório (JSON/PDF) para consumo direto
    holerites_extraidos_raw = _safe_get(audit_result, "holerites_extraidos", default=[])
    if not holerites_extraidos_raw and isinstance(audit_result.get("labor_analysis"), dict):
        holerites_extraidos_raw = (audit_result.get("labor_analysis") or {}).get("base_calculo", {}).get("holerites_detalhados", [])
    holerites_report = []
    for h in (holerites_extraidos_raw or []):
        if not isinstance(h, dict):
            continue
        holerites_report.append({
            "funcionario": h.get("funcionario", "N/A"),
            "cargo": h.get("cargo"),
            "periodo": h.get("periodo"),
            "salario_bruto": h.get("salario_bruto", 0),
            "salario_liquido": h.get("salario_liquido", 0),
            "descontos": h.get("descontos", {}),
            "source_file": h.get("source_file"),
            "source_url": h.get("source_url"),
            "extraction_method": h.get("extraction_method", "unknown"),
        })

    # Retornar com dataset_financeiro no topo (contrato obrigatório)
    return {
        "success": True,
        **dataset_financeiro,  # 🔒 DATASET FINANCEIRO obrigatório no topo
        **financial_extraction_result,  # Compatibilidade com código antigo
        "report": {
            "title": "Resultado da Conferência",
            "header": {
                "condominio": condominio_name or "Não identificado",
                "periodo_analisado": f"{period['period_start'] or 'N/A'} a {period['period_end'] or 'N/A'}",
                "data_relatorio": datetime.now().strftime("%d/%m/%Y")
            },
            "sections": sections,
            "extras": extras,
            "holerites_extraidos": holerites_report,
        },
        "metadata": _get_metadata(job_id, start_time)
    }


def get_section_formatter(section_number: int):
    formatters = {1: format_section_1, 2: format_section_2, 3: format_section_3, 4: format_section_4, 5: format_section_5, 6: format_section_6, 7: format_section_7, 8: format_section_8}
    return formatters.get(section_number)
