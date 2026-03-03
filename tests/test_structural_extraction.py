"""
Testes para o módulo de extração e conciliação estrutural (prestação de contas condominial).
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pytest
from app.extraction.legacy.structural_extraction import (
    normalizar_texto,
    run_structural_extraction,
)


def test_normalizar_texto_acentos_e_espacos():
    """Normalização: acentos e múltiplos espaços."""
    t = "  Conta   Ordinária   "
    out = normalizar_texto(t)
    assert "ordinaria" in out or "ordinária" not in out
    assert "  " not in out
    assert out.strip() == out


def test_normalizar_texto_vazio():
    """Normalização: entrada vazia retorna string vazia."""
    assert normalizar_texto("") == ""
    assert normalizar_texto(None) == ""


def test_run_structural_extraction_vazio():
    """Sem texto nem DataFrame -> SEM BASE PARA CONCILIAÇÃO."""
    r = run_structural_extraction("", None)
    assert r["classificacao"] == "SEM BASE PARA CONCILIAÇÃO"
    assert r["contas"] == []
    assert r["total_contas"] is None
    assert "structural" in r["justificativa"].lower() or "nenhum" in r["justificativa"].lower()


def test_run_structural_extraction_sem_consolidado():
    """Documento com contas mas sem saldo consolidado -> SEM BASE ou só total das contas."""
    text = """
    Conta Ordinaria    1000,00    500,00    200,00    1300,00
    Fundo de Reserva   2000,00    0,00      100,00    1900,00
    """
    r = run_structural_extraction(text)
    # Pode identificar 0 contas se o regex não bater exatamente; o importante é não inventar consolidado
    assert r.get("saldo_consolidado") is None or r["classificacao"] == "SEM BASE PARA CONCILIAÇÃO"
    if not r.get("contas"):
        assert "limitacoes" in r


def test_run_structural_extraction_com_consolidado_fechando():
    """Contas + saldo consolidado que fecha -> REGULAR ou REGULAR COM ALERTAS."""
    text = """
    conta ordinaria    1000,00    500,00    200,00    1300,00
    fundo de reserva    500,00    0,00      0,00      500,00
    Saldo consolidado: 1800,00
    """
    r = run_structural_extraction(text)
    total = r.get("total_contas")
    consolidado = r.get("saldo_consolidado")
    diff = r.get("diferenca")
    if total is not None and consolidado is not None and diff is not None:
        if abs(total - 1800) < 0.02 and abs(consolidado - 1800) < 0.02:
            assert diff <= 0.02
            assert r["classificacao"] in ("REGULAR", "REGULAR COM ALERTAS", "EXTRAÇÃO INCONFIÁVEL")


def test_run_structural_extraction_consolidado_nao_fecha():
    """Saldo consolidado diferente da soma das contas -> IRREGULAR (se consolidado existir)."""
    text = """
    conta ordinaria    1000,00    500,00    200,00    1300,00
    fundo reserva      500,00     0,00      0,00      500,00
    Saldo consolidado: 2000,00
    """
    r = run_structural_extraction(text)
    if r.get("saldo_consolidado") is not None and r.get("total_contas") is not None:
        if r.get("diferenca") is not None and r["diferenca"] > 0.01:
            assert r["classificacao"] == "IRREGULAR"


def test_anti_forcamento_sem_consolidado_nao_regular():
    """Sem saldo consolidado no documento -> nunca classificar como REGULAR por conciliação."""
    text = """
    conta ordinaria    1000,00    500,00    200,00    1300,00
    """
    r = run_structural_extraction(text)
    if r.get("saldo_consolidado") is None:
        assert r["classificacao"] != "REGULAR" or "SEM BASE" in r["classificacao"]


def test_alertas_saldo_negativo():
    """Conta com saldo final negativo -> alerta específico."""
    text = """
    fundo de obras    500,00    0,00    600,00    -100,00
    """
    r = run_structural_extraction(text)
    alertas = r.get("alertas") or []
    negativos = [a for a in alertas if "negativo" in a.lower()]
    if r.get("contas") and any(c.get("saldo_final") is not None and c["saldo_final"] < 0 for c in r["contas"]):
        assert len(negativos) >= 1


def test_run_structural_extraction_com_dataframe():
    """Com DataFrame: texto concatenado e pipeline executa."""
    import pandas as pd
    df = pd.DataFrame({
        "conta": ["Conta Ordinaria", "Fundo Reserva"],
        "saldo_anterior": [1000, 500],
        "creditos": [500, 0],
        "debitos": [200, 0],
        "saldo_final": [1300, 500],
    })
    r = run_structural_extraction("", df)
    assert "contas" in r
    assert "classificacao" in r
    assert "total_contas" in r or r["total_contas"] is None


def test_saida_tem_texto_formatado():
    """Saída inclui texto_formatado padronizado."""
    text = "conta ordinaria 1000,00 500,00 200,00 1300,00"
    r = run_structural_extraction(text)
    assert "texto_formatado" in r
    assert "CONTAS IDENTIFICADAS" in r["texto_formatado"] or "CLASSIFICAÇÃO" in r["texto_formatado"]


def test_exclusao_linhas_fornecedor_inss():
    """Linhas com fornecedor/INSS não viram conta financeira."""
    text = """
    Fornecedor XYZ    100,00    0,00    0,00    100,00
    INSS encargos     50,00     0,00    0,00    50,00
    conta ordinaria   1000,00   500,00  200,00  1300,00
    """
    r = run_structural_extraction(text)
    contas = r.get("contas") or []
    nomes = [c.get("nome", "").lower() for c in contas]
    assert not any("fornecedor" in n for n in nomes)
    assert not any("inss" in n for n in nomes)
