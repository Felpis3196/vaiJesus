"""
Testes do contrato de processamento de dados da extração LLM.
Garantem que build_dataframe_and_context produz DataFrame e document_context
com totals_extracted, encargos_from_llm e holerites_from_llm conforme esperado pelo pipeline.
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pytest
import pandas as pd
from app.extraction.llm.document_extractor import (
    build_dataframe_and_context,
    extract,
    _parse_llm_json,
    _normalize_raw_chunk,
)


# Mock de resultado da LLM para testes do contrato
EXTRACTION_RESULT_FULL = {
    "success": True,
    "transacoes": [
        {"data": "2025-01-05", "descricao": "Taxa condominial", "tipo": "receita", "valor": 5000.0},
        {"data": "2025-01-10", "descricao": "Salário zelador", "tipo": "despesa", "valor": 1200.0},
        {"data": "2025-01-15", "descricao": "Conta de água", "tipo": "despesa", "valor": 180.50},
    ],
    "condominio_name": "Condomínio Teste",
    "period_start": "2025-01-01",
    "period_end": "2025-01-31",
    "saldos": {"saldo_anterior": 10000.0, "saldo_final": 13619.50},
    "holerites": [
        {"source_file": "folha.pdf", "periodo": "01/2025", "funcionario": "João", "salario_bruto": 1200.0, "descontos": 100.0, "salario_liquido": 1100.0, "confidence": 0.9},
    ],
    "encargos": {
        "fgts": {"valor_pago": 96.0, "periodo": "01/2025", "documento": "GFIP", "confidence": 0.9},
        "inss": {"valor_pago": 180.0, "periodo": "01/2025", "documento": "GPS", "confidence": 0.9},
    },
    "errors": [],
    "confidence": "high",
}

DOCUMENT_TEXTS = [{"filename": "prestacao.pdf", "text": "Texto do documento"}]


def test_build_dataframe_and_context_dataframe_columns_and_rows():
    """DataFrame deve ter colunas data, descricao, tipo, valor e número de linhas igual ao de transações."""
    df, doc_ctx = build_dataframe_and_context(EXTRACTION_RESULT_FULL, DOCUMENT_TEXTS)
    assert isinstance(df, pd.DataFrame)
    for col in ("data", "descricao", "tipo", "valor"):
        assert col in df.columns, f"Coluna esperada: {col}"
    assert len(df) == len(EXTRACTION_RESULT_FULL["transacoes"])
    assert list(df["tipo"]) == ["receita", "despesa", "despesa"]
    assert list(df["valor"]) == [5000.0, 1200.0, 180.50]


def test_build_dataframe_and_context_totals_extracted():
    """document_context deve conter totals_extracted com values (total_receitas, total_despesas, saldo_final) coerentes."""
    df, doc_ctx = build_dataframe_and_context(EXTRACTION_RESULT_FULL, DOCUMENT_TEXTS)
    assert "totals_extracted" in doc_ctx
    values = doc_ctx["totals_extracted"].get("values") or {}
    assert values.get("total_receitas") == 5000.0
    assert values.get("total_despesas") == pytest.approx(1380.50, abs=0.01)  # 1200 + 180.50
    assert values.get("saldo_final") == 13619.50
    assert values.get("saldo_anterior") == 10000.0


def test_build_dataframe_and_context_encargos_and_holerites():
    """document_context deve conter encargos_from_llm e holerites_from_llm quando presentes no resultado."""
    df, doc_ctx = build_dataframe_and_context(EXTRACTION_RESULT_FULL, DOCUMENT_TEXTS)
    assert doc_ctx.get("encargos_from_llm") == EXTRACTION_RESULT_FULL["encargos"]
    assert doc_ctx.get("holerites_from_llm") == EXTRACTION_RESULT_FULL["holerites"]


def test_build_dataframe_and_context_period_and_condominio():
    """document_context deve conter period_start, period_end e condominio_name conforme mock."""
    df, doc_ctx = build_dataframe_and_context(EXTRACTION_RESULT_FULL, DOCUMENT_TEXTS)
    assert doc_ctx.get("period_start") == "2025-01-01"
    assert doc_ctx.get("period_end") == "2025-01-31"
    assert doc_ctx.get("condominio_name") == "Condomínio Teste"
    assert doc_ctx.get("saldo_anterior") == 10000.0
    assert doc_ctx.get("saldo_final") == 13619.50


def test_build_dataframe_and_context_empty_transacoes():
    """Com transacoes vazias, DataFrame vazio e totals_extracted ausente ou vazio."""
    result = {"transacoes": [], "saldos": {}, "holerites": [], "encargos": {}}
    df, doc_ctx = build_dataframe_and_context(result, DOCUMENT_TEXTS)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
    for col in ("data", "descricao", "tipo", "valor"):
        assert col in df.columns
    assert doc_ctx.get("totals_extracted") is None or doc_ctx.get("totals_extracted", {}).get("values") == {}


def test_build_dataframe_and_context_no_encargos_no_holerites():
    """Sem encargos/holerites no resultado, chaves não devem quebrar."""
    result = {
        "transacoes": [{"data": "2025-01-01", "descricao": "X", "tipo": "receita", "valor": 100.0}],
        "period_start": "2025-01-01",
        "period_end": "2025-01-31",
    }
    df, doc_ctx = build_dataframe_and_context(result, [])
    assert "encargos_from_llm" not in doc_ctx or doc_ctx.get("encargos_from_llm") is None
    assert "holerites_from_llm" not in doc_ctx or doc_ctx.get("holerites_from_llm") is None
    assert doc_ctx["totals_extracted"]["values"]["total_receitas"] == 100.0


def test_extract_with_mock_llm():
    """extract() com chat_completion mockado retorna estrutura válida e build_dataframe_and_context produz contrato."""
    import json
    from unittest.mock import patch
    mock_response = json.dumps({
        "condominio_name": "Mock Cond",
        "period_start": "2025-02-01",
        "period_end": "2025-02-28",
        "transacoes": [
            {"data": "2025-02-01", "descricao": "Receita", "tipo": "receita", "valor": 200.0},
            {"data": "2025-02-02", "descricao": "Despesa", "tipo": "despesa", "valor": 50.0},
        ],
        "saldos": {"saldo_anterior": 100.0, "saldo_final": 250.0},
        "holerites": [],
        "encargos": {},
        "errors": [],
    })
    with patch("app.extraction.llm.client.is_llm_available", return_value=True), \
         patch("app.extraction.llm.client.chat_completion", return_value=mock_response):
        doc_texts = [{"filename": "test.pdf", "text": "Conteúdo"}]
        out = extract(doc_texts)
    assert out.get("success") is True
    assert isinstance(out.get("transacoes"), list)
    assert len(out["transacoes"]) == 2
    assert out.get("saldos") and out["saldos"].get("saldo_final") == 250.0
    df, doc_ctx = build_dataframe_and_context(out, doc_texts)
    assert len(df) == 2
    assert doc_ctx.get("totals_extracted", {}).get("values", {}).get("total_receitas") == 200.0
    assert doc_ctx.get("totals_extracted", {}).get("values", {}).get("total_despesas") == 50.0
    assert doc_ctx.get("totals_extracted", {}).get("values", {}).get("saldo_final") == 250.0


@pytest.mark.skipif(
    not os.environ.get("LLM_BASE_URL") and not os.environ.get("OPENAI_API_KEY"),
    reason="LLM não configurada (LLM_BASE_URL ou OPENAI_API_KEY); teste de integração opcional",
)
def test_extract_integration_llm_available():
    """Com LLM configurada, extract() retorna estrutura com success, transacoes, saldos; build_dataframe_and_context produz contrato."""
    doc_texts = [{"filename": "sample.txt", "text": "Prestação de contas Janeiro 2025. Condomínio Exemplo. Receitas: 10.000,00. Despesas: 8.500,00. Saldo anterior: 5.000,00. Saldo final: 6.500,00."}]
    out = extract(doc_texts)
    assert "success" in out
    assert "transacoes" in out
    assert "saldos" in out
    assert "errors" in out
    df, doc_ctx = build_dataframe_and_context(out, doc_texts)
    assert doc_ctx.get("llm_extraction") is True
    if out.get("transacoes"):
        assert "totals_extracted" in doc_ctx or doc_ctx.get("has_financial_data")


def test_parse_llm_json_plain_object():
    """_parse_llm_json aceita JSON puro."""
    raw, err = _parse_llm_json('{"transacoes": [{"data": "2025-01-01", "valor": 100}], "condominio_name": "Test"}')
    assert err is None
    assert raw["condominio_name"] == "Test"
    assert len(raw["transacoes"]) == 1
    assert raw["transacoes"][0]["valor"] == 100


def test_parse_llm_json_markdown_block():
    """_parse_llm_json extrai JSON de bloco ```json ... ```."""
    content = 'Aqui está o resultado:\n```json\n{"transacoes": [], "condominio_name": "Bloco"}\n```'
    raw, err = _parse_llm_json(content)
    assert err is None
    assert raw["condominio_name"] == "Bloco"
    assert raw["transacoes"] == []


def test_parse_llm_json_fallback_curly():
    """_parse_llm_json usa primeiro { até último } quando não começa com {."""
    content = 'Texto antes {"transacoes": [{"valor": 50}], "period_start": "2025-01-01"} texto depois'
    raw, err = _parse_llm_json(content)
    assert err is None
    assert len(raw["transacoes"]) == 1
    assert raw["period_start"] == "2025-01-01"


def test_parse_llm_json_empty_or_invalid():
    """_parse_llm_json retorna erro para vazio ou JSON inválido."""
    _, err = _parse_llm_json("")
    assert err is not None
    _, err = _parse_llm_json("not json at all")
    assert err is not None


def test_normalize_raw_chunk_transactions_to_transacoes():
    """_normalize_raw_chunk mapeia 'transactions' para 'transacoes'."""
    raw = {"transactions": [{"data": "2025-01-01", "descricao": "x", "tipo": "receita", "valor": 10}], "period_start": "2025-01-01"}
    _normalize_raw_chunk(raw)
    assert "transacoes" in raw
    assert len(raw["transacoes"]) == 1
    assert raw["transacoes"][0]["valor"] == 10


def test_normalize_raw_chunk_data_as_transacoes():
    """_normalize_raw_chunk mapeia 'data' (lista com valor/descricao) para 'transacoes'."""
    raw = {"data": [{"data": "2025-01-01", "descricao": "y", "tipo": "despesa", "valor": 20}], "condominio_name": "Y"}
    _normalize_raw_chunk(raw)
    assert "transacoes" in raw
    assert len(raw["transacoes"]) == 1
    assert raw["transacoes"][0]["valor"] == 20


def test_build_dataframe_and_context_structural_extraction_from_contas_and_totais():
    """Quando a LLM retorna contas/totais por período, document_context deve incluir structural_extraction e totals_extracted.saldo_final."""
    result = {
        "success": True,
        "transacoes": [],
        "saldos": {},
        "contas": [
            {"nome": "Conta ordinária", "saldo_final": -1398.29, "periodo": "2025-05"},
            {"nome": "Fundo de reserva", "saldo_final": 80090.98, "periodo": "2025-05"},
            {"nome": "Espaço festa", "saldo_final": 10306.43, "periodo": "2025-05"},
            {"nome": "Lavanderia", "saldo_final": 48183.80, "periodo": "2025-05"},
        ],
        "totais_por_periodo": [
            {"periodo": "2025-05", "total": 137182.82, "conta_consolidada": 137182.82, "alerta_conta_ordinaria_negativa": True}
        ],
        "errors": [],
        "confidence": "high",
    }
    df, ctx = build_dataframe_and_context(result, DOCUMENT_TEXTS)
    assert isinstance(df, pd.DataFrame)
    assert ctx.get("llm_extraction") is True
    assert "structural_extraction" in ctx
    se = ctx["structural_extraction"]
    assert isinstance(se, dict)
    # total_contas é soma dos saldos das contas; saldo_consolidado vem do totals_por_periodo
    assert se.get("total_contas") == 137182.92
    assert se.get("saldo_consolidado") == 137182.82
    assert any("Conta ordinária negativa" in a for a in (se.get("alertas") or []))
    totals = ctx.get("totals_extracted") or {}
    values = totals.get("values", totals)
    assert values.get("saldo_final") == 137182.82


def test_build_dataframe_and_context_structural_extraction_multiple_periods():
    """
    Quando a LLM retorna contas/totais para dois períodos, structural_extraction_periods
    deve conter ambos com total_contas coerente e totals_extracted.period deve apontar
    para o último período ordenado.
    """
    result = {
        "success": True,
        "transacoes": [],
        "saldos": {},
        "contas": [
            # Mês 5
            {"nome": "Conta ordinária", "saldo_final": -1398.29, "periodo": "2025-05"},
            {"nome": "Fundo de reserva", "saldo_final": 80090.98, "periodo": "2025-05"},
            {"nome": "Espaço festa", "saldo_final": 10306.43, "periodo": "2025-05"},
            {"nome": "Lavanderia", "saldo_final": 48183.80, "periodo": "2025-05"},
            # Mês 6
            {"nome": "Conta ordinária", "saldo_final": -20357.53, "periodo": "2025-06"},
            {"nome": "Fundo de reserva", "saldo_final": 72482.07, "periodo": "2025-06"},
            {"nome": "Espaço festa", "saldo_final": 10536.18, "periodo": "2025-06"},
            {"nome": "Lavanderia", "saldo_final": 51953.40, "periodo": "2025-06"},
        ],
        "totais_por_periodo": [
            {"periodo": "2025-05", "total": 137182.82, "conta_consolidada": 137182.82, "alerta_conta_ordinaria_negativa": True},
            {"periodo": "2025-06", "total": 114614.12, "conta_consolidada": 114614.12, "alerta_conta_ordinaria_negativa": True},
        ],
        "errors": [],
        "confidence": "high",
    }
    df, ctx = build_dataframe_and_context(result, DOCUMENT_TEXTS)
    assert isinstance(df, pd.DataFrame)
    periods = ctx.get("structural_extraction_periods")
    assert isinstance(periods, list)
    assert len(periods) == 2
    # períodos distintos e ordenáveis
    period_keys = [p.get("periodo") for p in periods]
    assert len(set(period_keys)) == 2
    # total_contas de cada período coincide com soma das contas daquele período
    for p in periods:
        contas_list = p.get("contas") or []
        soma = sum(c.get("saldo_final") or 0 for c in contas_list)
        assert p.get("total_contas") == pytest.approx(soma, abs=0.01)
    # totals_extracted deve refletir o último período (2025-06)
    totals = ctx.get("totals_extracted") or {}
    values = totals.get("values", totals)
    assert values.get("saldo_final") == pytest.approx(114614.12, abs=0.01)
    assert values.get("period") == "2025-06"
