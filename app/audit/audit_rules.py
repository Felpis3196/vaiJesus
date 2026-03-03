"""
Regras de auditoria condominial (não negociáveis).
Separação rigorosa: dimensão matemática, contábil, documental, fiscal e trabalhista.
Jamais usar resultado de uma dimensão para validar outra.
"""
from typing import Dict, Any, List

# Frases OBRIGATÓRIAS quando aplicável
FRASE_CALCULOS_CORRETOS_DOC_INSUFICIENTE = "Cálculos corretos, porém documentação insuficiente."
FRASE_NAO_AUDITAVEL_FISCAL = "Não auditável do ponto de vista fiscal."
FRASE_ENCARGO_NAO_AUDITAVEL = "Não é possível verificar recolhimento por ausência de documentação e estrutura válida."
FRASE_RISCO_CONTABIL_LASTRO = "Risco contábil elevado por ausência de lastro."

# Frases PROIBIDAS (nunca usar)
FRASES_PROIBIDAS = [
    "encargos não existem",
    "tributos não foram pagos",
    "documento regular porque os valores fecham",
    "não foi recolhido",
    "não existem encargos",
    "tributos não existem",
]


def _safe_get(data: Dict, *keys, default=None):
    result = data
    for key in keys:
        if isinstance(result, dict):
            result = result.get(key, default)
        else:
            return default
    return result if result is not None else default


def evaluate_document_dimension(audit_result: Dict[str, Any]) -> Dict[str, bool]:
    """
    Avalia a dimensão documental de forma independente.
    REGRA 6: classificação final depende destes itens; não da matemática.
    """
    doc_ctx = _safe_get(audit_result, "document_context", default={}) or {}
    alerts = _safe_get(audit_result, "alerts", default=[]) or []
    alerts_codes = {a.get("code", "") for a in alerts if isinstance(a, dict)}

    # Balancete: considerado presente se há dados financeiros/transações (prestação de contas)
    total_transactions = int(audit_result.get("total_transactions") or 0)
    has_financial_data = total_transactions > 0
    # Ausência de balancete = alerta MISSING_BALANCETE ou sem dados
    has_balancete = not ("MISSING_BALANCETE" in alerts_codes) and has_financial_data

    # Extrato bancário
    has_extrato = not ("MISSING_BANK_STATEMENTS" in alerts_codes)

    # Notas fiscais / comprovantes
    has_nf = not ("MISSING_INVOICES_RECEIPTS" in alerts_codes)

    # Guias de encargos (INSS, FGTS, etc.)
    has_guias = not any(
        c in alerts_codes for c in ("GUIDES_RECEIPTS_PENDING", "MISSING_GUIDES_RECEIPTS", "MISSING_PUBLIC_TARIFFS")
    )

    # Condomínio identificado
    condominio_name = doc_ctx.get("condominio_name") or _safe_get(audit_result, "extraction_quality", "details", "nome_condominio_extraido")
    condominio_identificado = bool(condominio_name and str(condominio_name).strip() and str(condominio_name).strip().lower() not in ("não identificado", "nao identificado", "n/a"))

    # Período definido
    period_start = doc_ctx.get("period_start") or doc_ctx.get("periodo_inicio")
    period_end = doc_ctx.get("period_end") or doc_ctx.get("periodo_fim")
    periodo_definido = bool(period_start and period_end)

    # Folha válida: não inválida (holerite com líquido=0, descontos=0, rubricas genéricas, sem CPF/cargo)
    labor = _safe_get(audit_result, "labor_analysis", default={}) or {}
    folha_invalida_flag = labor.get("folha_invalida", False)
    folha_valida = not folha_invalida_flag

    # REGRA 4: planilha sem lastro = controle interno (não prestação de contas)
    controle_interno = doc_ctx.get("controle_interno", False)

    return {
        "has_balancete": has_balancete,
        "has_extrato": has_extrato,
        "has_nf": has_nf,
        "has_guias": has_guias,
        "condominio_identificado": condominio_identificado,
        "periodo_definido": periodo_definido,
        "folha_valida": folha_valida,
        "controle_interno": controle_interno,
    }


def classify_final_situation(audit_result: Dict[str, Any], doc_dimension: Dict[str, bool]) -> str:
    """
    REGRA 6 — Classificação final OBRIGATÓRIA.
    Uma única categoria: REGULAR | REGULAR COM RESSALVAS | IRREGULAR.
    Conta que fecha NÃO é conta regular.
    """
    if doc_dimension.get("controle_interno"):
        return "IRREGULAR"

    # IRREGULAR: se QUALQUER item abaixo ocorrer
    if not doc_dimension.get("has_balancete"):
        return "IRREGULAR"
    if not doc_dimension.get("has_extrato"):
        return "IRREGULAR"
    if not doc_dimension.get("has_nf"):
        return "IRREGULAR"
    if not doc_dimension.get("has_guias"):
        return "IRREGULAR"
    if not doc_dimension.get("periodo_definido"):
        return "IRREGULAR"
    if not doc_dimension.get("condominio_identificado"):
        return "IRREGULAR"
    if not doc_dimension.get("folha_valida"):
        return "IRREGULAR"

    # REGULAR: somente se TODOS existirem
    if (
        doc_dimension.get("has_balancete")
        and doc_dimension.get("has_extrato")
        and doc_dimension.get("has_nf")
        and doc_dimension.get("has_guias")
        and doc_dimension.get("condominio_identificado")
        and doc_dimension.get("periodo_definido")
        and doc_dimension.get("folha_valida")
    ):
        # Pequenas falhas pontuais (ex.: alertas de anomalia baixa) → COM RESSALVAS
        anomaly = _safe_get(audit_result, "summary", "anomaly_summary", default={}) or {}
        anomaly_rate = float(anomaly.get("anomaly_rate", 0))
        high_risk = int(anomaly.get("high_risk_count", 0))
        alerts = _safe_get(audit_result, "alerts", default=[]) or []
        missing_count = sum(1 for a in alerts if isinstance(a, dict) and (a.get("code") or "").startswith("MISSING_"))
        if missing_count > 0 or anomaly_rate > 0.10 or high_risk > 3:
            return "REGULAR COM RESSALVAS"
        return "REGULAR"

    # REGULAR COM RESSALVAS: balancete e extratos existem, pequenas falhas pontuais
    if doc_dimension.get("has_balancete") and doc_dimension.get("has_extrato"):
        return "REGULAR COM RESSALVAS"

    return "IRREGULAR"


def get_required_phrases(audit_result: Dict[str, Any], doc_dimension: Dict[str, bool], math_ok: bool) -> List[str]:
    """Retorna frases obrigatórias a incluir no parecer quando aplicável."""
    phrases = []
    if math_ok and not all([
        doc_dimension.get("has_guias"),
        doc_dimension.get("has_nf"),
        doc_dimension.get("has_extrato"),
    ]):
        phrases.append(FRASE_CALCULOS_CORRETOS_DOC_INSUFICIENTE)
    if not doc_dimension.get("has_guias") or not doc_dimension.get("has_nf"):
        phrases.append(FRASE_NAO_AUDITAVEL_FISCAL)
    if not doc_dimension.get("has_extrato") and doc_dimension.get("has_balancete"):
        phrases.append(FRASE_RISCO_CONTABIL_LASTRO)
    return phrases
