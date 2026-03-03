"""
Acessa URL do FGTS (ou holerite) e extrai dados de holerite do conteúdo (PDF ou HTML).
Reutiliza extract_holerites_from_text do holerite_extractor.
"""
import io
import logging
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _default_headers() -> Dict[str, str]:
    return {"User-Agent": USER_AGENT}


def _pdf_content_to_text(content: bytes) -> str:
    """Extrai texto de um PDF em bytes (sem OCR)."""
    try:
        import pdfplumber
    except ImportError:
        logger.warning("pdfplumber não disponível para extrair texto do PDF")
        return ""
    text_parts: List[str] = []
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                tx = page.extract_text()
                if tx and tx.strip():
                    text_parts.append(tx.strip())
    except Exception as e:
        logger.warning("Erro ao extrair texto do PDF: %s", e)
        return ""
    return "\n".join(text_parts) if text_parts else ""


def _html_table_to_text(table_tag) -> str:
    """
    Serializa uma tag <table> do BeautifulSoup como linhas delimitadas por '|'.
    Cada linha da tabela vira "| cell1 | cell2 | …" para que os padrões de regex
    de salário consigam identificar pares rótulo-valor em colunas adjacentes.
    """
    lines: List[str] = []
    for row in table_tag.find_all("tr"):
        cells = [cell.get_text(separator=" ", strip=True) for cell in row.find_all(["th", "td"])]
        if any(c for c in cells):
            lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _html_content_to_text(html: str) -> str:
    """
    Extrai texto relevante do HTML preservando a estrutura de tabelas.
    Tabelas são serializadas com delimitadores '|' (um por linha) para que
    os padrões de regex consigam localizar pares rótulo-valor em colunas.
    Conteúdo não-tabular é extraído normalmente com get_text().
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("beautifulsoup4 não disponível para extrair texto do HTML")
        return html[:50000] if html else ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        body = soup.find("body") or soup
        main = body.find("main")
        root = main if main else body
        if not root:
            return html[:50000] if html else ""

        parts: List[str] = []
        for element in root.children:
            # NavigableString (text nodes) têm .name == None — tratá-los como texto simples
            if not hasattr(element, "name") or element.name is None:
                t = str(element).strip()
                if t:
                    parts.append(t)
                continue
            if element.name == "table":
                parts.append(_html_table_to_text(element))
            else:
                inner_tables = element.find_all("table")
                if inner_tables:
                    non_table = element.get_text(separator="\n", strip=True)
                    if non_table:
                        parts.append(non_table)
                    for tbl in inner_tables:
                        parts.append(_html_table_to_text(tbl))
                else:
                    t = element.get_text(separator="\n", strip=True)
                    if t:
                        parts.append(t)

        text = "\n\n".join(parts)
        return text[:80000] if text else ""
    except Exception as e:
        logger.warning("Erro ao extrair texto do HTML: %s", e)
        return html[:50000] if html else ""


# Indicadores de que a página retornada é de login/autenticação (não o conteúdo do holerite).
_LOGIN_PAGE_INDICATORS = (
    "faça login", "faca login", "faça o login", "fazer login",
    "entre com", "acesse com", "autenticação", "autenticacao",
    "sign in", "log in", "entrar", "acessar sua conta",
    "usuário e senha", "usuario e senha", "cpf e senha",
    "sessão expirada", "sessao expirada", "sua sessão",
    "gov.br", "acesso restrito", "área restrita", "area restrita",
)


def _looks_like_login_page(text: str) -> bool:
    """Retorna True se o texto extraído parece ser uma página de login em vez de holerite."""
    if not text or len(text) < 100:
        return False
    lower = text.lower().strip()
    return any(ind in lower for ind in _LOGIN_PAGE_INDICATORS)


def fetch_holerites_from_url(
    url: str,
    timeout: int = DEFAULT_TIMEOUT,
    headers: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Acessa a URL (GET), identifica se o conteúdo é PDF ou HTML, extrai texto
    e retorna lista de holerites no formato padrão do projeto.

    Funciona para URLs públicas (ex.: link direto para PDF). Portais que exigem
    login (FGTS/Caixa/Meu INSS) podem não retornar conteúdo útil; headers/cookies
    podem ser passados no futuro para sessão autenticada.

    Args:
        url: URL do link FGTS/holerite.
        timeout: Timeout da requisição em segundos.
        headers: Headers opcionais (ex.: Authorization, Cookie).

    Returns:
        Lista de dicionários no formato holerite (funcionario, cargo, periodo,
        salario_bruto, descontos, salario_liquido, source_url, extraction_method).
    """
    if not url or not url.strip().startswith(("http://", "https://")):
        logger.warning("[ALERTA] URL inválida ou não HTTP: %s", url[:80] if url else "")
        return []

    req_headers = headers or _default_headers()
    if "User-Agent" not in req_headers:
        req_headers = {**req_headers, "User-Agent": USER_AGENT}

    try:
        response = requests.get(url, timeout=timeout, headers=req_headers)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.warning(
            "[ALERTA] Falha ao acessar link (timeout, rede ou servidor exige login): %s | URL: %s",
            e, url[:80],
        )
        return []

    # Redirecionamento para página de login (ex.: 200 com HTML de login)
    content_type = (response.headers.get("Content-Type") or "").lower()
    is_pdf = "application/pdf" in content_type or (
        not content_type and len(response.content) >= 4 and response.content[:4] == b"%PDF"
    )

    if is_pdf:
        text = _pdf_content_to_text(response.content)
    else:
        response.encoding = response.encoding or "utf-8"
        try:
            html = response.text
        except Exception as e:
            logger.warning("[ALERTA] Erro ao decodificar resposta da URL como texto: %s | URL: %s", e, url[:80])
            return []
        text = _html_content_to_text(html)

    if not text or len(text.strip()) < 50:
        logger.warning(
            "[ALERTA] Link aberto mas conteúdo insuficiente para holerite (%d caracteres). "
            "Pode ser página em branco ou exige login. URL: %s",
            len(text or ""), url[:80],
        )
        return []

    if _looks_like_login_page(text):
        logger.warning(
            "[ALERTA] Conteúdo do link parece ser página de LOGIN (não holerite). "
            "Portal pode exigir autenticação. URL: %s",
            url[:80],
        )
        return []

    from app.extraction.legacy.holerite_extractor import extract_holerites_from_text

    holerites = extract_holerites_from_text(text, filename=url)
    if not holerites:
        logger.warning(
            "[ALERTA] Link aberto e texto extraído (%d chars), mas NENHUM holerite reconhecido. "
            "Formato do documento pode ser diferente do esperado. URL: %s",
            len(text), url[:80],
        )
        return []

    for h in holerites:
        h["source_url"] = url
        h["source_file"] = url
        h["extraction_method"] = "fgts_link"
    return holerites
