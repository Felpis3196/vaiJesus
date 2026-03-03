"""
Testes que garantem que a extração de holerite retorna dados quando o conteúdo
contém texto típico de holerite (funcionário, salário bruto, líquido, etc.).
"""
import os
import sys
import unittest
from unittest.mock import Mock, patch

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# Texto mínimo que deve gerar pelo menos um holerite (indicadores + valores no formato esperado).
# Usar "Salário bruto" e "Salário líquido" que são reconhecidos pelos padrões do extractor.
TEXTO_HOLERITE_REAL = """
Folha de pagamento - Condomínio Exemplo
Funcionário: João da Silva
Cargo: Porteiro
Período: 01/2025
Salário bruto: R$ 2.500,00
Total de descontos: R$ 320,00
Salário líquido: R$ 2.180,00
"""


class TestHoleriteExtractionReturnsData(unittest.TestCase):
    """Garante que a extração de holerite retorna algo quando há conteúdo válido."""

    def test_extract_holerites_from_text_returns_at_least_one(self):
        """extract_holerites_from_text com texto de holerite real retorna lista não vazia."""
        from app.extraction.legacy.holerite_extractor import extract_holerites_from_text

        result = extract_holerites_from_text(TEXTO_HOLERITE_REAL, "teste.txt")
        self.assertIsInstance(result, list, "Deve retornar lista")
        self.assertGreaterEqual(len(result), 1, "Texto com holerite deve retornar ao menos 1 item")
        h = result[0]
        self.assertIn("salario_bruto", h)
        self.assertIn("salario_liquido", h)
        self.assertIn("funcionario", h)
        self.assertIn("extraction_method", h)
        self.assertGreater(h.get("salario_bruto", 0) + h.get("salario_liquido", 0), 0,
                           "Pelo menos um valor de salário deve ser extraído")

    def test_extract_holerites_from_text_alternative_labels(self):
        """Extrai com labels alternativos: vencimentos, liquido a receber."""
        from app.extraction.legacy.holerite_extractor import extract_holerites_from_text

        text = """
        Holerite - Competência 12/2024
        Nome: Maria Santos
        Vencimentos: R$ 4.200,00
        Total de descontos: R$ 580,00
        Líquido a receber: R$ 3.620,00
        """
        result = extract_holerites_from_text(text, "holerite.pdf")
        self.assertGreaterEqual(len(result), 1, "Deve extrair com vencimentos/líquido a receber")
        h = result[0]
        self.assertGreater(h.get("salario_bruto", 0) or h.get("salario_liquido", 0), 0)

    def test_fetch_holerites_from_url_returns_holerites_when_html_has_holerite(self):
        """fetch_holerites_from_url com HTML de holerite retorna lista não vazia e campos corretos."""
        from app.services.fgts_link_fetcher import fetch_holerites_from_url

        html = """
        <html><body>
        <h1>Folha de pagamento</h1>
        <p>Funcionário: Carlos Souza</p>
        <p>Salário bruto: R$ 3.500,00</p>
        <p>Descontos: R$ 450,00</p>
        <p>Salário líquido: R$ 3.050,00</p>
        <p>Período: 01/2025</p>
        </body></html>
        """
        url = "https://exemplo.com/holerite"

        with patch("app.services.fgts_link_fetcher.requests.get") as m_get:
            resp = Mock()
            resp.raise_for_status = Mock()
            resp.headers = {"Content-Type": "text/html"}
            resp.encoding = "utf-8"
            resp.text = html
            m_get.return_value = resp

            result = fetch_holerites_from_url(url)

        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 1,
                                "HTML com holerite deve retornar ao menos 1 holerite extraído")
        h = result[0]
        self.assertEqual(h.get("extraction_method"), "fgts_link")
        self.assertEqual(h.get("source_url"), url)
        self.assertGreater(h.get("salario_bruto", 0) + h.get("salario_liquido", 0), 0,
                           "Valores de salário devem ser extraídos")

    def test_extract_holerites_from_text_empty_without_indicators(self):
        """Texto sem indicadores de holerite retorna lista vazia."""
        from app.extraction.legacy.holerite_extractor import extract_holerites_from_text

        result = extract_holerites_from_text(
            "Relatório financeiro geral. Total de despesas: R$ 10.000,00. Fim.",
            "outro.txt"
        )
        self.assertEqual(result, [])

    def test_extract_holerites_from_text_short_returns_empty(self):
        """Texto muito curto retorna lista vazia."""
        from app.extraction.legacy.holerite_extractor import extract_holerites_from_text

        result = extract_holerites_from_text("Pouco texto.", "x.txt")
        self.assertEqual(result, [])

    # ------------------------------------------------------------------
    # Novos testes: dict format, múltiplos funcionários, tabela HTML
    # ------------------------------------------------------------------

    def test_extract_holerites_hybrid_with_dict_format(self):
        """
        extract_holerites_hybrid deve aceitar lista de dicts {"filename", "text"}
        — o formato produzido por api_server para document_texts — e retornar
        ao menos 1 holerite.
        """
        from app.extraction.legacy.holerite_extractor import extract_holerites_hybrid

        doc_texts = [{"filename": "folha.pdf", "text": TEXTO_HOLERITE_REAL}]
        result = extract_holerites_hybrid(doc_texts)
        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 1,
                                "dict format deve retornar ao menos 1 holerite")
        h = result[0]
        self.assertGreater(h.get("salario_bruto", 0) + h.get("salario_liquido", 0), 0)
        # filename real deve aparecer como source_file, não "doc_text_0"
        self.assertIn("folha.pdf", h.get("source_file", ""),
                      "source_file deve conter o nome do arquivo real")

    def test_extract_holerites_hybrid_multiple_employees_in_blocks(self):
        """
        Texto com dois funcionários separados por linha em branco deve produzir
        ao menos 2 holerites distintos.
        """
        from app.extraction.legacy.holerite_extractor import extract_holerites_hybrid

        texto_dois_funcionarios = """
Folha de pagamento - Condomínio Exemplo

Funcionário: João da Silva
Cargo: Porteiro
Período: 01/2025
Salário bruto: R$ 2.500,00
Total de descontos: R$ 320,00
Salário líquido: R$ 2.180,00


Funcionário: Maria Oliveira
Cargo: Zeladora
Período: 01/2025
Salário bruto: R$ 1.900,00
Total de descontos: R$ 240,00
Salário líquido: R$ 1.660,00
"""
        doc_texts = [{"filename": "folha_jan.pdf", "text": texto_dois_funcionarios}]
        result = extract_holerites_hybrid(doc_texts)
        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 2,
                                "Dois blocos de funcionário devem gerar ao menos 2 holerites")
        nomes = [h.get("funcionario", "").lower() for h in result]
        self.assertTrue(any("jo" in n for n in nomes), "João deve ser encontrado")
        self.assertTrue(any("mari" in n for n in nomes), "Maria deve ser encontrada")

    def test_fetch_holerites_from_url_table_html(self):
        """
        HTML com holerite em tabela HTML (<table><tr><th>/<td>) deve ter os
        valores extraídos corretamente após a melhoria em _html_table_to_text.
        """
        from app.services.fgts_link_fetcher import fetch_holerites_from_url

        html_tabela = """
        <html><body>
        <h1>Folha de Pagamento</h1>
        <table>
          <tr><th>Campo</th><th>Valor</th></tr>
          <tr><td>Funcionário</td><td>Carlos Souza</td></tr>
          <tr><td>Período</td><td>12/2024</td></tr>
          <tr><td>Salário bruto</td><td>R$ 4.200,00</td></tr>
          <tr><td>Total de descontos</td><td>R$ 630,00</td></tr>
          <tr><td>Salário líquido</td><td>R$ 3.570,00</td></tr>
        </table>
        </body></html>
        """
        url = "https://exemplo.com/holerite-tabela"

        with patch("app.services.fgts_link_fetcher.requests.get") as m_get:
            resp = Mock()
            resp.raise_for_status = Mock()
            resp.headers = {"Content-Type": "text/html; charset=utf-8"}
            resp.encoding = "utf-8"
            resp.text = html_tabela
            m_get.return_value = resp

            result = fetch_holerites_from_url(url)

        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 1,
                                "HTML com holerite em tabela deve retornar ao menos 1 holerite")
        h = result[0]
        self.assertEqual(h.get("extraction_method"), "fgts_link")
        self.assertEqual(h.get("source_url"), url)
        self.assertGreater(h.get("salario_bruto", 0), 0,
                           "Salário bruto deve ser extraído da tabela HTML")
        self.assertGreater(h.get("salario_liquido", 0), 0,
                           "Salário líquido deve ser extraído da tabela HTML")


if __name__ == "__main__":
    unittest.main()
