"""
Gerador de Alertas da Auditoria
Gera alertas quando: documentos principais incompletos, guias/comprovantes pendentes,
folhas/holerites pendentes, ausência de movimentação de férias/13º no mês vigente,
gasto >5% acima do mês passado e acima da inflação do mês, e outros valores anômalos.
"""
import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, cast

import pandas as pd

from app.audit.audit_structures import make_warning
from app.audit.financial_consolidator import check_gasto_acima_5pct_e_inflacao
from app.analysis import get_duplicate_mask

logger = logging.getLogger(__name__)


def _get_inflation_monthly_decimal() -> float:
    """Inflação mensal em decimal (ex.: 0.005 = 0,5%). Variável INFLATION_MONTHLY_PCT em %."""
    try:
        pct = os.environ.get("INFLATION_MONTHLY_PCT", "0.5")
        return float(pct) / 100.0
    except (ValueError, TypeError):
        return 0.005


# Palavras-chave para detecção nas descrições
KEYWORDS_GUIAS_COMPROVANTES = [
    r'\bguia\b', r'\bcomprovante\b', r'\bgru\b', r'\bdarf\b', r'\bgare\b',
    r'\bgrrf\b', r'\bcomprovante\s+de\s+pagamento\b', r'\brecibo\b', r'\bnota\s+fiscal\b',
    r'\bgps\b', r'\bgfip\b', r'\besocial\b', r'\be-social\b', r'\bsefip\b', r'\bgrf\b',
    r'\bguia\s+fgts\b', r'\bguia\s+inss\b', r'\bcomprovante\s+de\s+recolhimento\b'
]
KEYWORDS_FOLHAS_HOLERITES = [
    r'\bfolha\s+de\s+pagamento\b', r'\bholerite\b', r'\bcontracheque\b',
    r'\bholerites\b', r'\bfolha\s+salarial\b', r'\bfolha\s+pagamento\b',
    r'\bdemonstrativo\s+de\s+pagamento\b', r'\bproventos\s+e\s+descontos\b',
    r'\bproventos\b', r'\bdescontos\b', r'\be-social\b', r'\besocial\b'
]
KEYWORDS_FERIAS = [
    r'\bferias\b', r'\bférias\b', r'\bferias\s+proporcionais\b', r'\bdecimo\s+ferias\b'
]
KEYWORDS_DECIMO_TERCEIRO = [
    r'\b13[°ºº]\s*salario\b', r'\b13\s*salario\b', r'\bdecimo\s+terceiro\b',
    r'\bdécimo\s+terceiro\b', r'\b13o\s*salario\b', r'\b13º\s*salário\b',
    r'\bgratificacao\s+natalina\b', r'\bgratificação\s+natalina\b', r'\b13\s*\.?\s*sal\b'
]
# Documentos obrigatorios (detecção por descrições/nomes de arquivo)
KEYWORDS_BALANCETE = [
    r'\bbalancete\b', r'\bprestac[aã]o\s+de\s+contas\b', r'\bdemonstrativo\b',
    r'\bposi[cç][aã]o\s+financeira\b'
]
KEYWORDS_EXTRATOS = [
    r'\bextrato\b', r'\bextratos\b', r'\bbanc[aá]rio\b', r'\bconta\s+corrente\b'
]
KEYWORDS_NOTAS = [
    r'\bnota\s+fiscal\b', r'\bnf\b', r'\brecibo\b', r'\bfatura\b'
]
KEYWORDS_TARIFAS = [
    r'\benel\b', r'\bsabesp\b', r'\benergia\b', r'\bagua\b', r'\bágua\b',
    r'\btelefone\b', r'\bcelular\b', r'\bg[aá]s\b', r'\binternet\b'
]
KEYWORDS_PROVISOES = [
    r'\bprovis[aã]o\b', r'\bfundo\s+de\s+obras\b', r'\bfundo\s+obras\b',
    r'\b13[°ºº]\s*sal[aá]rio\b', r'\bd[eé]cimo\s+terceiro\b', r'\bf[eé]rias\b'
]
# Despesas que costumam exigir guia/comprovante ou folha
KEYWORDS_DESPESA_COM_COMPROVANTE = [
    r'\bsalario\b', r'\bsalário\b', r'\bfgts\b', r'\binss\b', r'\birrf\b', r'\bpis\b', r'\biss\b',
    r'\bimposto\b', r'\bcontribuicao\b', r'\bencargos\s+sociais\b', r'\bhonorarios\b', r'\bprolabore\b',
    r'\bgps\b', r'\bgfip\b', r'\bgrf\b', r'\bsefip\b', r'\besocial\b', r'\be-social\b', r'\bdarf\b'
]


def _safe_series(value: Any) -> pd.Series:
    """Garante que temos uma Series para usar em máscaras."""
    if isinstance(value, pd.Series):
        return value
    if hasattr(value, '__iter__') and not isinstance(value, (str, dict)):
        return pd.Series(value)
    return pd.Series([value])


def _norm_text(s: Any) -> str:
    """Normaliza texto para comparação (minúsculo, sem acentos opcionais)."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    t = str(s).lower().strip()
    # Normalizar alguns acentos comuns
    for old, new in [('é', 'e'), ('á', 'a'), ('í', 'i'), ('ó', 'o'), ('ú', 'u'), ('ã', 'a'), ('õ', 'o'), ('ç', 'c')]:
        t = t.replace(old, new)
    return t


def _description_matches_keywords(descricao: str, patterns: List[str]) -> bool:
    """Verifica se a descrição contém algum dos padrões (regex)."""
    if not descricao:
        return False
    text = _norm_text(descricao)
    for pattern in patterns:
        try:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        except re.error:
            if pattern.lower().replace(r'\b', '') in text:
                return True
    return False


def _has_keyword_in_dataframe(df: pd.DataFrame, patterns: List[str], column: str = "descricao") -> bool:
    """Retorna True se alguma linha tiver a coluna indicada com match em algum padrão."""
    if df.empty or column not in df.columns:
        return False
    for idx, row in df.iterrows():
        val = row.get(column)
        if _description_matches_keywords(str(val) if val is not None else "", patterns):
            return True
    return False


def _has_keyword_in_filenames(file_metadata: List[Dict[str, Any]], patterns: List[str]) -> bool:
    """Retorna True se algum filename em file_metadata combinar com padrões."""
    for item in file_metadata:
        filename = str(item.get("filename") or "")
        if _description_matches_keywords(filename, patterns):
            return True
    return False


def _count_matches_in_dataframe(df: pd.DataFrame, patterns: List[str], column: str = "descricao") -> int:
    """Conta quantas linhas têm a coluna com match em algum padrão."""
    if df.empty or column not in df.columns:
        return 0
    count = 0
    for idx, row in df.iterrows():
        val = row.get(column)
        if _description_matches_keywords(str(val) if val is not None else "", patterns):
            count += 1
    return count


def _get_reference_month_from_df(df: pd.DataFrame) -> Optional[tuple]:
    """Obtém (ano, mês) de referência a partir da coluna data (mês mais recente)."""
    if df is None or df.empty:
        return None
    col = "data"
    if col not in df.columns:
        return None
    try:
        series = pd.to_datetime(df[col], errors="coerce").dropna()
        if series.empty:
            return None
        last = series.max()
        if hasattr(last, "year") and hasattr(last, "month"):
            return (int(last.year), int(last.month))
        return None
    except Exception:
        return None


def generate_alerts(
    df: Optional[pd.DataFrame] = None,
    *,
    document_context: Optional[Dict[str, Any]] = None,
    reference_month: Optional[tuple] = None,
    main_documents_expected: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Gera lista de alertas com base em regras de negócio.

    Args:
        df: DataFrame com colunas data, descricao, tipo, valor (e opcionalmente categoria, anomalia_detectada).
        document_context: Dados opcionais sobre documentos (ex.: file_categorization, documents_processed,
            listas de nomes de arquivos ou tipos encontrados).
        reference_month: (ano, mês) para checagem de férias/13º; se None, usa o mês máximo em df.
        main_documents_expected: Lista de tipos/nomes de documentos considerados principais (ex.: ['extrato', 'planilha']).

    Returns:
        Dict com:
          - alerts: lista de strings (mensagens de alerta para warnings);
          - alerts_structured: lista de dicts com code, message, severity (opcional para API).
    """
    alerts: List[str] = []
    alerts_structured: List[Dict[str, str]] = []
    duplicates_count = 0
    doc_ctx = document_context or {}
    file_metadata = doc_ctx.get("file_metadata") or []
    extraction_quality = doc_ctx.get("extraction_quality") or {}

    # ETAPA 1: Gerar alerta se documento estiver ilegível ou sem sentido
    if isinstance(extraction_quality, dict) and extraction_quality.get("ok") is False:
        errors = extraction_quality.get("errors") or []
        msg = "Foram identificados problemas na extração dos dados (documento ilegível, sem sentido ou confuso)."
        if errors:
            msg = f"{msg} Detalhes: {', '.join(errors[:3])}"
        alerts.append(msg)
        alerts_structured.append(make_warning("EXTRACTION_QUALITY_ISSUE", msg, details={"errors": errors}, severity="high"))

    # --- 1) Documentos principais incompletos ---
    total_files = doc_ctx.get("total_files") or doc_ctx.get("documents_count") or 0
    has_financial_data = doc_ctx.get("has_financial_data")
    # Se foi informado que documentos principais são esperados e não há dado financeiro útil
    if main_documents_expected is not None and len(main_documents_expected) > 0:
        if total_files == 0:
            msg = "Documentos principais incompletos: nenhum documento foi enviado para análise."
            alerts.append(msg)
            alerts_structured.append(make_warning("MAIN_DOCUMENTS_MISSING", msg, details={"expected": main_documents_expected}, severity="high"))
        elif has_financial_data is False:
            msg = "Documentos principais incompletos: nenhum dado financeiro estruturado foi encontrado nos arquivos (ex.: planilha ou extrato com colunas data, descrição, valor)."
            alerts.append(msg)
            alerts_structured.append(make_warning("MAIN_DOCUMENTS_INCOMPLETE", msg, details={}, severity="high"))

    # Se temos contexto de categorização: financeiros = 0 pode indicar incompletude
    by_category = doc_ctx.get("by_category") or doc_ctx.get("file_categorization", {}).get("by_category") or {}
    financial_count = by_category.get("financial_data", 0) if isinstance(by_category, dict) else 0
    has_financial_data_ctx = doc_ctx.get("has_financial_data")
    if total_files > 0 and financial_count == 0 and has_financial_data_ctx is False:
        msg = "Documentos principais incompletos: nenhum dado financeiro estruturado foi identificado na análise."
        if msg not in alerts:
            alerts.append(msg)
            alerts_structured.append(make_warning("MAIN_DOCUMENTS_NO_FINANCIAL", msg, details={"by_category": by_category}, severity="medium"))

    if df is None or df.empty:
        col_desc = None
    else:
        col_desc = "descricao" if "descricao" in df.columns else None

    # --- 1b) Documentos obrigatorios (balancete, extratos, notas, folha, guias, tarifas, provisoes) ---
    if total_files > 0:
        def has_doc(patterns: List[str]) -> bool:
            by_df = _has_keyword_in_dataframe(df, patterns, col_desc) if col_desc and df is not None and not df.empty else False
            by_file = _has_keyword_in_filenames(file_metadata, patterns) if file_metadata else False
            return by_df or by_file

        if not has_doc(KEYWORDS_BALANCETE):
            msg = "Documento obrigatório ausente: balancete/prestação de contas."
            alerts.append(msg)
            alerts_structured.append(make_warning("MISSING_BALANCETE", msg, details={}, severity="high"))
        if not has_doc(KEYWORDS_EXTRATOS):
            msg = "Documento obrigatório ausente: extratos bancários."
            alerts.append(msg)
            alerts_structured.append(make_warning("MISSING_BANK_STATEMENTS", msg, details={}, severity="high"))
        if not has_doc(KEYWORDS_NOTAS):
            msg = "Documento obrigatório ausente: notas fiscais e recibos."
            alerts.append(msg)
            alerts_structured.append(make_warning("MISSING_INVOICES_RECEIPTS", msg, details={}, severity="medium"))
        if not has_doc(KEYWORDS_FOLHAS_HOLERITES):
            msg = "Documento obrigatório ausente: folha de pagamento e holerites."
            alerts.append(msg)
            alerts_structured.append(make_warning("MISSING_PAYROLL_DOCS", msg, details={}, severity="medium"))
        if not has_doc(KEYWORDS_GUIAS_COMPROVANTES):
            msg = "Documento obrigatório ausente: guias e comprovantes (FGTS/INSS/PIS/IRRF)."
            alerts.append(msg)
            alerts_structured.append(make_warning("MISSING_GUIDES_RECEIPTS", msg, details={}, severity="medium"))
        if not has_doc(KEYWORDS_TARIFAS):
            msg = "Documento obrigatório ausente: tarifas públicas (energia/água/telefone)."
            alerts.append(msg)
            alerts_structured.append(make_warning("MISSING_PUBLIC_TARIFFS", msg, details={}, severity="low"))
        if not has_doc(KEYWORDS_PROVISOES):
            msg = "Documento obrigatório ausente: provisões (13º, férias, fundo de obras)."
            alerts.append(msg)
            alerts_structured.append(make_warning("MISSING_PROVISIONS", msg, details={}, severity="low"))

    if not col_desc:
        return {"alerts": alerts, "alerts_structured": alerts_structured, "duplicates_count": 0}

    # --- 2) Guias e comprovantes pendentes ---
    has_guia_comprovante = _has_keyword_in_dataframe(df, KEYWORDS_GUIAS_COMPROVANTES, col_desc)
    df_despesas: pd.DataFrame = cast(
        pd.DataFrame,
        df[df["tipo"].astype(str).str.lower().str.contains("despesa", na=False)] if "tipo" in df.columns else df,
    )
    has_despesa_comprovante = _has_keyword_in_dataframe(df_despesas, KEYWORDS_DESPESA_COM_COMPROVANTE, col_desc)
    if has_despesa_comprovante and not has_guia_comprovante:
        msg = "Há lançamentos de encargos (ex.: salário, INSS, impostos) no balancete; não foram identificados comprovantes ou guias de recolhimento (GPS, GRF, DARFs) no PDF."
        alerts.append(msg)
        alerts_structured.append(make_warning("GUIDES_RECEIPTS_PENDING", msg, details={}, severity="medium"))

    # --- 3) Folhas e holerites pendentes ---
    has_folha_holerite = _has_keyword_in_dataframe(df, KEYWORDS_FOLHAS_HOLERITES, col_desc)
    if has_despesa_comprovante and not has_folha_holerite:
        msg = "Folhas e holerites apresentados estão pendentes: há despesas com pessoal/salários, porém não foi identificada movimentação de folha de pagamento ou holerites nas descrições."
        alerts.append(msg)
        alerts_structured.append(make_warning("PAYSLIPS_PENDING", msg, details={}, severity="medium"))

    # --- 3b) Gastos sem nota fiscal/recibo ---
    has_notas = _has_keyword_in_dataframe(df, KEYWORDS_NOTAS, col_desc)
    if has_despesa_comprovante and not has_notas:
        msg = "Há despesas no período, mas não foi identificada menção a notas fiscais ou recibos nas descrições."
        alerts.append(msg)
        alerts_structured.append(make_warning("EXPENSES_WITHOUT_RECEIPTS", msg, details={}, severity="low"))

    # --- 4) Férias e 13º no mês vigente ---
    ref = reference_month or _get_reference_month_from_df(df)
    if ref is not None:
        ano, mes = ref
        df["_dt"] = pd.to_datetime(df["data"], errors="coerce")
        _df_month = df[(df["_dt"].dt.year == ano) & (df["_dt"].dt.month == mes)]
        assert isinstance(_df_month, pd.DataFrame), "Filtro por mês deve retornar DataFrame"
        df_month: pd.DataFrame = _df_month
        has_ferias = _has_keyword_in_dataframe(df_month, KEYWORDS_FERIAS, col_desc)
        has_13 = _has_keyword_in_dataframe(df_month, KEYWORDS_DECIMO_TERCEIRO, col_desc)
        if not has_ferias:
            msg = f"Não foi identificada movimentação de pagamento de férias no mês vigente ({mes:02d}/{ano}). Verifique se há lançamentos de férias neste período."
            alerts.append(msg)
            alerts_structured.append(make_warning("VACATION_PAYMENT_MISSING", msg, details={"month": mes, "year": ano}, severity="low"))
        if not has_13:
            msg = f"Não foi identificada movimentação de pagamento de 13º salário no mês vigente ({mes:02d}/{ano}). Verifique se há lançamentos de décimo terceiro neste período."
            alerts.append(msg)
            alerts_structured.append(make_warning("THIRTEENTH_SALARY_MISSING", msg, details={"month": mes, "year": ano}, severity="low"))
        if "_dt" in df.columns:
            df.drop(columns=["_dt"], inplace=True, errors="ignore")

    # --- 5) Valores considerados com anomalia ---
    if "anomalia_detectada" in df.columns:
        try:
            anomalias = df[df["anomalia_detectada"] == True]
            if not anomalias.empty:
                n = len(anomalias)
                msg = f"Existem {n} transação(ões) com anomalia que requer(em) revisão."
                alerts.append(msg)
                alerts_structured.append(make_warning("ANOMALIES_REQUIRE_REVIEW", msg, details={"count": int(n)}, severity="high"))
        except Exception as e:
            logger.debug("Alert check anomalia_detectada: %s", e)

    # --- 6) Transações duplicadas ---
    try:
        duplicates_count = int(get_duplicate_mask(df).sum())
        if duplicates_count > 0:
            msg = f"Lançamentos duplicados: {duplicates_count} ocorrência(s) identificadas (mesma data, valor e descrição)."
            alerts.append(msg)
            alerts_structured.append(make_warning("DUPLICATE_TRANSACTIONS", msg, details={"count": duplicates_count}, severity="medium"))
    except Exception as e:
        logger.debug("Alert check duplicates: %s", e)

    # --- 7) Gastos fora do padrão: >5% acima do mês passado E acima da inflação do mês ---
    try:
        if "data" in df.columns and "valor" in df.columns:
            df_tmp = df.copy()
            df_tmp["_dt"] = pd.to_datetime(df_tmp["data"], errors="coerce")
            df_tmp = df_tmp[df_tmp["_dt"].notna()]
            if not df_tmp.empty:
                df_tmp["_month"] = df_tmp["_dt"].dt.to_period("M").astype(str)
                months_sorted = sorted(df_tmp["_month"].unique())
                last_month = months_sorted[-1]
                df_desp = df_tmp[df_tmp["tipo"].astype(str).str.lower() == "despesa"] if "tipo" in df_tmp.columns else df_tmp
                total_atual = float(df_desp[df_desp["_month"] == last_month]["valor"].sum())
                total_mes_anterior = None
                prev_month = None
                if len(months_sorted) >= 2:
                    prev_month = months_sorted[-2]
                    total_mes_anterior = float(df_desp[df_desp["_month"] == prev_month]["valor"].sum())
                else:
                    # Um único mês: usar document_context se informado (ex.: gasto_mes_anterior, inflacao_mes)
                    total_mes_anterior = doc_ctx.get("gasto_mes_anterior") or doc_ctx.get("previous_month_expenses")
                    if total_mes_anterior is not None:
                        try:
                            total_mes_anterior = float(total_mes_anterior)
                        except (TypeError, ValueError):
                            total_mes_anterior = None
                inflacao_decimal = _get_inflation_monthly_decimal()
                inflacao_override = doc_ctx.get("inflacao_mes") or doc_ctx.get("inflation_monthly_pct")
                if inflacao_override is not None:
                    try:
                        v = float(inflacao_override)
                        inflacao_decimal = v / 100.0 if v > 1 else v  # aceita 0.5 ou 0.005
                    except (TypeError, ValueError):
                        pass
                if total_mes_anterior is not None and total_mes_anterior > 0 and total_atual > 0:
                    check = check_gasto_acima_5pct_e_inflacao(
                        total_despesas_atual=total_atual,
                        total_despesas_mes_anterior=total_mes_anterior,
                        inflacao_pct_mes=inflacao_decimal,
                        mes_referencia=last_month,
                    )
                    details = dict(check.get("details", {}))
                    details["base_mes_anterior"] = "dados_consolidados" if len(months_sorted) >= 2 else ("contexto" if doc_ctx.get("gasto_mes_anterior") or doc_ctx.get("previous_month_expenses") else "unico_mes")
                    if check.get("alert"):
                        msg = check.get("message") or (
                            f"Gasto do mês {last_month} está mais de 5% acima do mês anterior e acima da inflação do mês."
                        )
                        alerts.append(msg)
                        alerts_structured.append(
                            make_warning(
                                "GASTO_ACIMA_5PCT_E_INFLACAO",
                                msg,
                                details=details,
                                severity="medium",
                            )
                        )
                if len(months_sorted) >= 2:
                    # Por categoria: mesmo critério (5% e acima da inflação)
                    group_cols = ["categoria", "_month"] if "categoria" in df_tmp.columns else ["_month"]
                    totals = df_desp.groupby(group_cols)["valor"].sum().reset_index()
                    alerts_added = 0
                    for cat in (totals[group_cols[0]].unique() if "categoria" in totals.columns else [None]):
                        if "categoria" in totals.columns:
                            sub = totals[totals["categoria"] == cat]
                        else:
                            sub = totals
                        if last_month not in set(sub["_month"]) or prev_month not in set(sub["_month"]):
                            continue
                        last_total = float(sub[sub["_month"] == last_month]["valor"].sum())
                        prev_total = float(sub[sub["_month"] == prev_month]["valor"].sum())
                        if prev_total <= 0:
                            continue
                        check_cat = check_gasto_acima_5pct_e_inflacao(
                            total_despesas_atual=last_total,
                            total_despesas_mes_anterior=prev_total,
                            inflacao_pct_mes=inflacao_decimal,
                            mes_referencia=last_month,
                        )
                        if check_cat.get("alert"):
                            pct = check_cat["details"].get("variacao_pct", (last_total / prev_total - 1) * 100)
                            sev = "high" if pct >= 12.0 else "medium"
                            label = f"categoria {cat}" if cat else "total de despesas"
                            msg = f"Gasto fora do padrão em {label}: aumento de {pct:.1f}% no mês {last_month} (acima de 5% e acima da inflação)."
                            alerts.append(msg)
                            alerts_structured.append(
                                make_warning(
                                    "ABNORMAL_SPENDING",
                                    msg,
                                    details={"month": last_month, "category": cat, "pct_increase": pct, "above_inflation": True},
                                    severity=sev,
                                )
                            )
                            alerts_added += 1
                        if alerts_added >= 5:
                            break
            if "_dt" in df_tmp.columns:
                df_tmp.drop(columns=["_dt"], inplace=True, errors="ignore")
    except Exception as e:
        logger.debug("Alert check abnormal spending: %s", e)

    return {"alerts": alerts, "alerts_structured": alerts_structured, "duplicates_count": duplicates_count}


def add_alerts_to_audit_result(
    audit_result: Dict[str, Any],
    df: Optional[pd.DataFrame] = None,
    document_context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Adiciona os alertas gerados aos campos warnings e alerts do audit_result (in-place).

    - audit_result['warnings']: recebe itens estruturados (code, message, details, timestamp, severity).
    - audit_result['alerts']: mesma lista de itens estruturados dos alertas (para referência por código).
    """
    out = generate_alerts(
        df=df,
        document_context=document_context,
        reference_month=None,
        main_documents_expected=None,
    )
    structured = out.get("alerts_structured", [])
    if not audit_result.get("warnings"):
        audit_result["warnings"] = []
    audit_result["warnings"].extend(structured)
    audit_result["alerts"] = list(structured)
    # Fonte única para duplicados: Seção 3 e Seção 6 usam este valor
    audit_result.setdefault("summary", {})["duplicates_count"] = out.get("duplicates_count", 0)
