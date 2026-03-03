import os
import sys

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.reporting.report_generator import generate_full_report


def test_generate_full_report_without_anomalia_detectada_column():
    """generate_full_report deve funcionar mesmo sem coluna anomalia_detectada (modo estrutural)."""
    df = pd.DataFrame(
        {
            "data": [],
            "descricao": [],
            "tipo": [],
            "valor": [],
        }
    )
    out = generate_full_report(df)
    assert isinstance(out, str)
    assert "Relatório de Anomalias Detectadas" in out
    assert "Nenhuma anomalia detectada" in out

