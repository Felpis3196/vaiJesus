"""
Testes do fetcher de holerites a partir de URL FGTS/holerite.
Usa mock de requests para não acessar rede.
"""
import os
import sys
import unittest
from unittest.mock import Mock, patch

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


class TestFgtsLinkFetcher(unittest.TestCase):
    """Testes para fetch_holerites_from_url e helpers."""

    def test_fetch_holerites_from_url_invalid_url(self):
        """URL inválida ou não HTTP retorna lista vazia."""
        from app.services.fgts_link_fetcher import fetch_holerites_from_url

        self.assertEqual(fetch_holerites_from_url(""), [])
        self.assertEqual(fetch_holerites_from_url("ftp://example.com/doc.pdf"), [])
        self.assertEqual(fetch_holerites_from_url("not-a-url"), [])

    def test_fetch_holerites_from_url_http_error_returns_empty(self):
        """Resposta HTTP 404/500 retorna lista vazia."""
        from app.services.fgts_link_fetcher import fetch_holerites_from_url
        import requests

        with patch("app.services.fgts_link_fetcher.requests.get") as m_get:
            m_get.return_value.raise_for_status.side_effect = requests.HTTPError("404")
            m_get.return_value.headers = {"Content-Type": "text/html"}
            result = fetch_holerites_from_url("https://example.com/folha")
        self.assertEqual(result, [])

    def test_fetch_holerites_from_url_timeout_returns_empty(self):
        """Timeout na requisição retorna lista vazia."""
        from app.services.fgts_link_fetcher import fetch_holerites_from_url
        import requests

        with patch("app.services.fgts_link_fetcher.requests.get") as m_get:
            m_get.side_effect = requests.Timeout()
            result = fetch_holerites_from_url("https://example.com/folha")
        self.assertEqual(result, [])

    def test_fetch_holerites_from_url_html_with_holerite_text(self):
        """HTML com texto de holerite retorna lista com itens e extraction_method fgts_link."""
        from app.services.fgts_link_fetcher import fetch_holerites_from_url

        html_body = """
    <body>
    <h1>Folha de pagamento</h1>
    <p>Funcionário: João da Silva</p>
    <p>Salário bruto: R$ 3.500,00</p>
    <p>Descontos: R$ 450,00</p>
    <p>Salário líquido: R$ 3.050,00</p>
    <p>Período: 01/2025</p>
    </body>
    """
        url = "https://example.com/holerite"

        with patch("app.services.fgts_link_fetcher.requests.get") as m_get:
            resp = Mock()
            resp.raise_for_status = Mock()
            resp.headers = {"Content-Type": "text/html"}
            resp.encoding = "utf-8"
            resp.text = "<html>" + html_body + "</html>"
            m_get.return_value = resp

            result = fetch_holerites_from_url(url)

        self.assertIsInstance(result, list)
        for h in result:
            self.assertEqual(h.get("extraction_method"), "fgts_link")
            self.assertEqual(h.get("source_url"), url)
            self.assertEqual(h.get("source_file"), url)

    def test_fetch_holerites_from_url_insufficient_content_returns_empty(self):
        """Conteúdo extraído com menos de 50 caracteres retorna lista vazia."""
        from app.services.fgts_link_fetcher import fetch_holerites_from_url

        with patch("app.services.fgts_link_fetcher.requests.get") as m_get:
            resp = Mock()
            resp.raise_for_status = Mock()
            resp.headers = {"Content-Type": "text/html"}
            resp.encoding = "utf-8"
            resp.text = "<html><body>OK</body></html>"
            m_get.return_value = resp

            result = fetch_holerites_from_url("https://example.com/short")

        self.assertEqual(result, [])

    def test_pdf_content_to_text_empty(self):
        """_pdf_content_to_text com bytes não-PDF retorna string vazia."""
        from app.services.fgts_link_fetcher import _pdf_content_to_text

        out = _pdf_content_to_text(b"not a pdf")
        self.assertIsInstance(out, str)
        self.assertEqual(out, "")

    def test_html_content_to_text_extracts_body(self):
        """_html_content_to_text extrai texto do body."""
        from app.services.fgts_link_fetcher import _html_content_to_text

        html = "<html><body><p>Folha de pagamento</p><p>Funcionário: Maria</p></body></html>"
        text = _html_content_to_text(html)
        self.assertIsInstance(text, str)
        self.assertTrue("Folha" in text or "Funcionário" in text or "Maria" in text)


if __name__ == "__main__":
    unittest.main()
