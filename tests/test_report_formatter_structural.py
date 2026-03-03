import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pytest

from app.reporting.report_formatter import format_section_3


def _make_structural_period(periodo, total, consolidado, alerta_ordinaria=False):
    contas = []
    if periodo == "2025-05":
        contas = [
            {"nome": "Conta ordinária", "saldo_final": -1398.29, "confiabilidade": None},
            {"nome": "Fundo de reserva", "saldo_final": 80090.98, "confiabilidade": None},
            {"nome": "Espaço festa", "saldo_final": 10306.43, "confiabilidade": None},
            {"nome": "Lavanderia", "saldo_final": 48183.80, "confiabilidade": None},
        ]
    elif periodo == "2025-06":
        contas = [
            {"nome": "Conta ordinária", "saldo_final": -20357.53, "confiabilidade": None},
            {"nome": "Fundo de reserva", "saldo_final": 72482.07, "confiabilidade": None},
            {"nome": "Espaço festa", "saldo_final": 10536.18, "confiabilidade": None},
            {"nome": "Lavanderia", "saldo_final": 51953.40, "confiabilidade": None},
        ]
    elif periodo == "2025-07":
        contas = [
            {"nome": "Conta ordinária", "saldo_final": 21901.55, "confiabilidade": None},
            {"nome": "Fundo de reserva", "saldo_final": 75524.55, "confiabilidade": None},
            {"nome": "Espaço festa", "saldo_final": 10842.90, "confiabilidade": None},
            {"nome": "Lavanderia", "saldo_final": 57538.28, "confiabilidade": None},
        ]
    alertas = []
    if alerta_ordinaria:
        alertas.append(f"Conta ordinária negativa no período {periodo}.")
    return {
        "periodo": periodo,
        "rotulo_original": periodo,
        "contas": contas,
        "total_contas": total,
        "saldo_consolidado": consolidado,
        "diferenca": round(abs(total - consolidado), 2),
        "alertas": alertas,
        "classificacao": "REGULAR",
        "justificativa": "Mock de teste estrutural por período.",
        "limitacoes": [],
        "texto_formatado": None,
    }


def test_format_section_3_structural_extraction_periods_multi_month():
    """
    format_section_3 deve expor múltiplos períodos estruturais e conciliações entre meses consecutivos
    quando document_context.structural_extraction_periods estiver presente.
    """
    structural_periods = [
        _make_structural_period("2025-05", 137182.82, 137182.82, alerta_ordinaria=True),
        _make_structural_period("2025-06", 114614.12, 114614.12, alerta_ordinaria=True),
        _make_structural_period("2025-07", 165806.92, 165806.92, alerta_ordinaria=False),
    ]

    audit_result = {
        "dataset_financeiro": {
            "creditos_mensais": {"valor": 0.0, "status": "OK", "origem": "teste"},
            "debitos_mensais": {"valor": 0.0, "status": "OK", "origem": "teste"},
            "saldo_anterior": {"valor": 0.0, "status": "OK"},
            "saldo_final": {"valor": 0.0, "status": "OK"},
        },
        "document_context": {
            "saldo_final": 0.0,
            "structural_extraction_periods": structural_periods,
            "structural_extraction": structural_periods[-1],
        },
        "summary": {
            "anomaly_summary": {"total_anomalies": 0},
            "duplicates_count": 0,
        },
    }

    result = format_section_3(audit_result, df=None, job_id="test")
    assert result.get("success") is True
    content = result.get("data", {}).get("content", {})
    conciliacao_periodos = content.get("conciliacao_estrutural_periodos")
    assert conciliacao_periodos is not None

    periodos = conciliacao_periodos.get("periodos") or []
    assert len(periodos) == 3
    labels = {p.get("label") for p in periodos}
    # Deve haver rótulos derivados dos períodos normalizados
    assert any("Mês 5/2025" in (lbl or "") for lbl in labels)
    assert any("Mês 6/2025" in (lbl or "") for lbl in labels)

    conciliacoes = conciliacao_periodos.get("conciliacoes_entre_periodos") or []
    # Esperamos conciliações 5->6 e 6->7
    pares = {(c.get("periodo_origem"), c.get("periodo_destino")) for c in conciliacoes}
    assert ("2025-05", "2025-06") in pares
    assert ("2025-06", "2025-07") in pares
    # Pelo menos uma mensagem de conciliação deve mencionar "Saldo final de"
    assert any("Saldo final de" in (c.get("mensagem") or "") for c in conciliacoes)

