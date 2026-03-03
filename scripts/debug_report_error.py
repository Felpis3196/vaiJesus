"""Debug: reproduz erro 'truth value of array ambiguous' com 1 linha."""
import os
import sys
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

# DataFrame com 1 linha (despesa)
df = pd.DataFrame({
    "data": [pd.Timestamp("2026-01-29")],
    "descricao": ["Item PDF"],
    "tipo": ["despesa"],
    "valor": [22261.97],
    "anomalia_detectada": [False],
    "justificativa_anomalia": [""],
    "categoria": ["Outras Despesas"],
    "nivel_risco": ["BAIXO"],
})
audit_results = {
    "summary": {
        "financial_summary": {
            "total_receitas": 0.0,
            "total_despesas": 22261.97,
            "saldo": -22261.97,
        }
    },
    "document_context": {},
    "alerts": [],
    "warnings": [],
    "anomalies_detected": 0,
    "total_transactions": 1,
    "ai_analysis": {"total_anomalies": 0, "high_confidence_anomalies": 0, "model_agreement": {}},
    "nlp_analysis": {"suspicious_patterns": {}, "high_suspicion_count": 0},
    "predictive_analysis": {"error": "test"},
}

from app.reporting.report_generator import generate_conference_report, generate_full_report

print("Chamando generate_conference_report...")
try:
    out = generate_conference_report(df, audit_results)
    print("OK, len=", len(out))
except Exception as e:
    import traceback
    traceback.print_exc()

print("\nChamando generate_full_report...")
try:
    out2 = generate_full_report(df)
    print("OK, len=", len(out2))
except Exception as e:
    import traceback
    traceback.print_exc()
