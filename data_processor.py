"""
Backward-compatibility wrapper. Prefer: from app.extraction.legacy import ...
O pipeline principal de extração é 100% LLM (api_server, data_input_manager, main);
estas funções permanecem para scripts de diagnóstico e testes legados.
"""
from app.extraction.legacy.data_processor import (
    load_data,
    load_document,
    load_document_from_bytes,
    clean_data,
    categorize_transactions,
    check_extraction_quality,
    extract_condominio_name,
    extract_period_from_text,
    extract_period_from_filename,
    get_month_from_filename,
    extract_saldos_from_text,
    extract_financial_totals_from_text,
    dataframe_to_text_br,
    extract_hyperlinks_from_excel,
    extract_hyperlinks_from_ods,
    extract_folha_value_from_text,
)

__all__ = [
    "load_data",
    "load_document",
    "load_document_from_bytes",
    "clean_data",
    "categorize_transactions",
    "check_extraction_quality",
    "extract_condominio_name",
    "extract_period_from_text",
    "extract_period_from_filename",
    "get_month_from_filename",
    "extract_saldos_from_text",
    "extract_financial_totals_from_text",
    "dataframe_to_text_br",
    "extract_hyperlinks_from_excel",
    "extract_hyperlinks_from_ods",
    "extract_folha_value_from_text",
]
