"""
Testes de carregamento unificado (load_document / load_document_from_bytes + clean_data).
Caminho legado: o pipeline principal usa extração 100% LLM; estes testes validam o fluxo
legado para diagnóstico e compatibilidade.
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pytest
import pandas as pd
from app.extraction.legacy import load_document, load_document_from_bytes, clean_data


def test_load_document_csv():
    """CSV: load_document retorna DataFrame; clean_data preserva colunas padrão."""
    path = os.path.join(PROJECT_ROOT, "data", "sample_condominium_accounts.csv")
    if not os.path.exists(path):
        pytest.skip(f"Arquivo de exemplo não encontrado: {path}")
    result = load_document(path)
    df = result[0] if isinstance(result, tuple) else result
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    df_clean = clean_data(df)
    for col in ("data", "descricao", "tipo", "valor"):
        assert col in df_clean.columns, f"Coluna esperada: {col}"


def test_load_document_from_bytes_csv():
    """Upload (bytes): load_document_from_bytes com CSV."""
    path = os.path.join(PROJECT_ROOT, "data", "sample_condominium_accounts.csv")
    if not os.path.exists(path):
        pytest.skip(f"Arquivo de exemplo não encontrado: {path}")
    with open(path, "rb") as f:
        content = f.read()
    df = load_document_from_bytes(content, "sample_condominium_accounts.csv")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    df_clean = clean_data(df)
    assert "valor" in df_clean.columns


def test_load_document_pdf_balancete():
    """PDF balancete: load_document + clean_data produz colunas padrão e valores."""
    path = os.path.join(PROJECT_ROOT, "Docs", "Prest. Contas- Balancete Junho25 Acapulco Beach-2.pdf")
    if not os.path.exists(path):
        pytest.skip(f"PDF de teste não encontrado: {path}")
    result = load_document(path)
    df = result[0] if isinstance(result, tuple) else result
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    df_clean = clean_data(df)
    for col in ("data", "descricao", "tipo", "valor"):
        assert col in df_clean.columns, f"Coluna esperada: {col}"
    # Deve haver algum valor monetário extraído
    assert "valor" in df_clean.columns
    valor_sum = df_clean["valor"].sum()
    assert valor_sum != 0, "Esperado pelo menos algum valor extraído do balancete"


def test_load_document_ods():
    """ODS: load_document + clean_data (formato balancete quando aplicável)."""
    path = os.path.join(PROJECT_ROOT, "Docs", "Ed Led prestacao_contas_8_2025.ods")
    if not os.path.exists(path):
        pytest.skip(f"ODS de teste não encontrado: {path}")
    result = load_document(path)
    df = result[0] if isinstance(result, tuple) else result
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    df_clean = clean_data(df)
    for col in ("data", "descricao", "tipo", "valor"):
        assert col in df_clean.columns, f"Coluna esperada: {col}"
    valor_sum = df_clean["valor"].sum()
    assert valor_sum != 0, "Esperado pelo menos algum valor extraído do ODS"


def test_clean_data_with_few_rows_no_exception():
    """Relatório deve ser gerável mesmo com poucas linhas (evitar quebra por ML/stratify)."""
    df = pd.DataFrame({
        "data": ["2025-01-01", "2025-01-02"],
        "descricao": ["Receita teste", "Despesa teste"],
        "tipo": ["receita", "despesa"],
        "valor": [100.0, 50.0],
    })
    df_clean = clean_data(df)
    assert len(df_clean) <= 2
    assert "valor" in df_clean.columns
    assert df_clean["valor"].sum() == 150.0
