"""
Regressões do fluxo de holerite para garantir:
1) a auditoria não quebra por chamada incompatível de logger.warning;
2) holerites propagam para a seção trabalhista do relatório.
"""
import os
import sys
import unittest
from unittest.mock import patch

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


class TestHoleritePdfRegression(unittest.TestCase):
    def test_run_comprehensive_audit_does_not_fail_with_doc_texts_and_zero_holerites(self):
        """
        Regressão do erro:
        'warning() takes 2 positional arguments but 3 were given'
        quando há doc_texts e nenhum holerite estruturado extraído.
        """
        from app.audit import AdvancedAuditSystem

        df_input = pd.DataFrame(
            [
                {
                    "descricao": "Lançamento teste",
                    "valor": 100.0,
                    "anomalia_detectada": False,
                    "nivel_risco": "BAIXO",
                }
            ]
        )
        doc_ctx = {
            "document_texts": [
                {
                    "filename": "folha de dezembro pdf.pdf",
                    "text": "Documento sem padrão claro de holerite, com conteúdo textual para debug.",
                }
            ]
        }

        system = AdvancedAuditSystem()

        with patch.object(AdvancedAuditSystem, "_validate_dataframe", return_value=[]), \
             patch.object(AdvancedAuditSystem, "_process_data", return_value=(df_input, {})), \
             patch.object(AdvancedAuditSystem, "_run_advanced_ai_analysis", return_value=df_input), \
             patch.object(AdvancedAuditSystem, "_extract_ai_insights", return_value={}), \
             patch.object(AdvancedAuditSystem, "_run_nlp_analysis", return_value=df_input), \
             patch.object(AdvancedAuditSystem, "_extract_nlp_insights", return_value={}), \
             patch.object(AdvancedAuditSystem, "_run_predictive_analysis", return_value={}), \
             patch.object(AdvancedAuditSystem, "_consolidate_results", return_value=df_input), \
             patch.object(AdvancedAuditSystem, "_generate_comprehensive_summary", return_value={}), \
             patch.object(AdvancedAuditSystem, "_generate_advanced_report", return_value="dummy.md"), \
             patch("advanced_audit_system.analyze_labor_charges", return_value={"base_calculo": {}, "encargos": {}, "tributos": {}, "ferias_13": {}}), \
             patch("advanced_audit_system.should_trigger_llm", return_value=False), \
             patch("advanced_audit_system.add_alerts_to_audit_result", return_value=None):
            result = system.run_comprehensive_audit(df_input=df_input, document_context=doc_ctx)

        self.assertTrue(result.get("success"), "Auditoria não deve falhar por erro de logger.warning")
        self.assertEqual(result.get("errors"), [], "Não deve haver erro de execução neste cenário")
        self.assertIn("holerite_extraction_debug", result)
        self.assertIsInstance(result["holerite_extraction_debug"], list)
        self.assertGreaterEqual(len(result["holerite_extraction_debug"]), 1)

    def test_section_4_receives_holerites_for_pdf_rendering(self):
        """
        Garante que holerites extraídos chegam em section 4 -> content.holerites_detalhados,
        estrutura lida pelo gerador de PDF.
        """
        from app.reporting.report_formatter import format_section_4

        holerite = {
            "funcionario": "Maria Silva",
            "cargo": "Zeladora",
            "periodo": "12/2025",
            "salario_bruto": 3200.0,
            "descontos": 410.0,
            "salario_liquido": 2790.0,
            "source_file": "folha de dezembro pdf.pdf",
            "extraction_method": "regex_text",
        }
        audit_result = {
            "labor_analysis": {
                "base_calculo": {"holerites_detalhados": [holerite]},
                "encargos": {},
                "tributos": {},
                "resumo": "",
            },
            "document_context": {},
        }

        section = format_section_4(audit_result, df=pd.DataFrame([{"descricao": "x", "valor": 1.0}]), job_id="job-test")
        content = ((section.get("data") or {}).get("content") or {})
        hols = content.get("holerites_detalhados") or []

        self.assertGreaterEqual(len(hols), 1, "section 4 deve incluir holerites para exibição no PDF")
        self.assertEqual(hols[0].get("funcionario"), "Maria Silva")
        self.assertGreater(float(hols[0].get("salario_bruto") or 0), 0)


if __name__ == "__main__":
    unittest.main()
