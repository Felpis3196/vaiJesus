"""
Qualidade da extração: check_extraction_quality.
"""
import pandas as pd
from typing import Optional

from .text_utils import extract_condominio_name

def check_extraction_quality(
    df: pd.DataFrame,
    source_hint: str = "",
    condominio_name_from_raw: Optional[str] = None,
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
    ocr_used: Optional[bool] = None,
    ocr_text_len: Optional[int] = None,
) -> dict:
    """
    Verifica se os dados extraídos (após clean_data) estão coerentes antes de gerar o relatório.
    Útil para detectar extração falha (ex.: PDF/ODS com valores zerados ou colunas faltando).
    condominio_name_from_raw: nome extraído do DataFrame bruto (antes de clean_data); usado quando
    o DataFrame limpo já não contém a linha "Condomínio: NOME" (ex.: PDF).

    Returns:
        Dict com: ok (bool), errors (list), warnings (list), details (dict com totais, nome condomínio, etc.)
    """
    result = {
        "ok": True,
        "errors": [],
        "warnings": [],
        "details": {},
    }
    if df is None or not isinstance(df, pd.DataFrame):
        result["ok"] = False
        result["errors"].append("DataFrame inválido ou nulo.")
        return result

    if df.empty:
        result["ok"] = False
        result["errors"].append("Nenhum dado extraído (DataFrame vazio). Verifique o documento ou formato.")
        result["details"]["total_rows"] = 0
        return result

    result["details"]["total_rows"] = len(df)

    # Colunas obrigatórias
    cols_lower = [str(c).lower() for c in df.columns]
    for col in ("data", "descricao", "tipo", "valor"):
        if col not in cols_lower:
            result["ok"] = False
            result["errors"].append(f"Coluna obrigatória ausente: '{col}'. A extração pode ter falhado.")

    # Totais financeiros (se coluna valor existir)
    if "valor" in df.columns:
        try:
            v = pd.to_numeric(df["valor"], errors="coerce").fillna(0)
            total_geral = float(v.sum())
            result["details"]["soma_valores"] = total_geral
        except Exception:
            total_geral = 0.0
            result["details"]["soma_valores"] = 0.0

        if total_geral == 0.0 and len(df) > 0:
            result["warnings"].append(
                "Soma dos valores é zero. O documento pode não ter valores monetários extraídos corretamente (ex.: PDF/ODS)."
            )
            result["details"]["valores_zerados"] = True

    if "tipo" in df.columns and "valor" in df.columns:
        try:
            rec = df[df["tipo"].astype(str).str.lower().str.strip() == "receita"]["valor"]
            desp = df[df["tipo"].astype(str).str.lower().str.strip() == "despesa"]["valor"]
            total_receitas = float(pd.to_numeric(rec, errors="coerce").fillna(0).sum())
            total_despesas = float(pd.to_numeric(desp, errors="coerce").fillna(0).sum())
            result["details"]["total_receitas"] = total_receitas
            result["details"]["total_despesas"] = total_despesas
            result["details"]["saldo"] = total_receitas - total_despesas
        except Exception:
            result["details"]["total_receitas"] = None
            result["details"]["total_despesas"] = None
            result["details"]["saldo"] = None

    # Nome do condomínio: preferir o extraído do bruto (PDF); senão tentar do DataFrame atual
    nome = condominio_name_from_raw or extract_condominio_name(df)
    result["details"]["nome_condominio_extraido"] = nome if nome else None
    # Periodo (se informado)
    if period_start or period_end:
        result["details"]["period_start"] = period_start
        result["details"]["period_end"] = period_end
    # OCR (se informado)
    if ocr_used is not None:
        result["details"]["ocr_used"] = bool(ocr_used)
        result["details"]["ocr_text_len"] = int(ocr_text_len or 0)
        if ocr_used and (ocr_text_len or 0) < 50:
            result["warnings"].append("OCR executado, mas o texto extraido foi insuficiente. O documento pode estar ilegivel.")
    if source_hint and ("pdf" in source_hint.lower() or "ods" in source_hint.lower()) and not nome:
        result["warnings"].append(
            "Nome do condomínio não encontrado nos dados. Comum em PDF/ODS; o relatório pode mostrar campo em branco."
        )

    if result["errors"]:
        result["ok"] = False

    return result
