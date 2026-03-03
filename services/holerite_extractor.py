"""
Extrator de Holerites
Extrai dados estruturados de holerites (contracheques) a partir de texto (PDF/HTML)
e DataFrames (ODS/Excel). Inclui deduplicação de entradas 100% idênticas da mesma origem.
"""
import re
import logging
import math
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# Limite de holerites extraídos em modo permissivo (sem coluna explícita de folha)
MAX_PERMISSIVE = 50

# ---------------------------------------------------------------------------
# Parsing de valores monetários (formato BR)
# ---------------------------------------------------------------------------

def _parse_br_value(s: Any) -> float:
    """Converte string monetária BR (ponto=milhar, vírgula=decimal) para float."""
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        try:
            v = float(s)
            return 0.0 if math.isnan(v) else v
        except (ValueError, TypeError):
            return 0.0
    try:
        clean = str(s).replace("R$", "").replace("$", "").strip()
        clean = clean.replace(".", "").replace(",", ".")
        return float(clean)
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Deduplicação
# ---------------------------------------------------------------------------

def _holerite_identity_key(h: Dict) -> Tuple:
    """Chave de identidade: (source_file, funcionario, periodo, bruto, liquido)."""
    source = str(h.get("source_file") or h.get("source_url") or "").strip()
    funcionario = str(h.get("funcionario") or h.get("nome") or "").lower().strip()
    periodo_raw = h.get("periodo")
    periodo = ""
    if periodo_raw is not None and str(periodo_raw).strip().lower() not in ("", "none", "nan"):
        periodo = str(periodo_raw).strip().lower()
    try:
        bruto = round(float(h.get("salario_bruto") or 0), 2)
    except (ValueError, TypeError):
        bruto = 0.0
    try:
        liquido = round(float(h.get("salario_liquido") or 0), 2)
    except (ValueError, TypeError):
        liquido = 0.0
    return (source, funcionario, periodo, bruto, liquido)


def deduplicate_holerites(holerites: List[Dict]) -> Tuple[List[Dict], int]:
    """
    Remove holerites 100% idênticos da mesma origem (source_file).
    Retorna (lista_deduplicada, n_removidos).
    """
    seen: set = set()
    result: List[Dict] = []
    n_dup = 0
    for h in holerites:
        key = _holerite_identity_key(h)
        if key in seen:
            n_dup += 1
            logger.debug(
                "Holerite duplicado ignorado: funcionario=%s periodo=%s bruto=%.2f",
                key[1], key[2], key[3],
            )
        else:
            seen.add(key)
            result.append(h)
    return result, n_dup


# ---------------------------------------------------------------------------
# Regex para extração de texto
# ---------------------------------------------------------------------------

_RE_FUNCIONARIO = re.compile(
    r'(?:funcion[aá]rio|nome\s*do\s*funcion[aá]rio|nome\s*do\s*empregado|'
    r'nome\s*do\s*trabalhador|nome\s*completo|nome|empregado|trabalhador|colaborador)'
    r'\s*[:\-]?\s*([A-ZÀ-Ú][a-zA-ZÀ-úàáâãäéèêëíìîïóòôõöúùûüç\s]{3,60})',
    re.IGNORECASE,
)
_RE_CARGO = re.compile(
    r'(?:cargo|fun[çc][aã]o|ocupa[çc][aã]o|posto)'
    r'\s*[:\-]?\s*([A-Za-zÀ-úàáâãäéèêëíìîïóòôõöúùûüç\s]{3,50})',
    re.IGNORECASE,
)
_RE_PERIODO = re.compile(
    r'(?:per[ií]odo|compet[eê]ncia|m[eê]s\s+(?:de\s+)?refer[eê]ncia|'
    r'refer[eê]ncia|m[eê]s\s+base|m[eê]s)'
    r'\s*[:\-|]?\s*(\d{2}[/\-.]\d{4}|\d{4}[/\-.]\d{2}|\w+\s+\d{4})',
    re.IGNORECASE,
)
_RE_SALARIO_BRUTO = re.compile(
    r'(?:sal[aá]rio\s+bruto|total\s+de\s+vencimentos?|vencimentos?\s+totais?|'
    r'total\s+proventos?|vencimentos?|proventos?|sal\.\s*bruto|'
    r'remun(?:era(?:[çc][aã]o)?)?\s*total|total\s+remunera|'
    r'bruto\s+(?:do\s+)?m[eê]s|total\s+bruto|'
    r'(?<!\w)bruto(?!\s*[a-z]))'
    r'\s*[:\-|]?\s*R?\$?\s*([\d.]+,\d{2})',
    re.IGNORECASE,
)
_RE_SALARIO_LIQUIDO = re.compile(
    r'(?:sal[aá]rio\s+l[ií]quido|l[ií]quido\s+a\s+receber|total\s+l[ií]quido|'
    r'l[ií]quido\s+final|valor\s+l[ií]quido|sal\.\s*l[ií]quido|'
    r'total\s+a\s+(?:receber|pagar)|valor\s+a\s+(?:receber|pagar)|'
    r'l[ií]quido\s+(?:do\s+)?m[eê]s|'
    r'(?<!\w)l[ií]quido(?!\s*[a-z]))'
    r'\s*[:\-|]?\s*R?\$?\s*([\d.]+,\d{2})',
    re.IGNORECASE,
)
_RE_DESCONTOS = re.compile(
    r'(?:total\s+de\s+descontos?|descontos?\s+totais?|total\s+desconto|'
    r'total\s+(?:dos\s+)?descontos?|descontos?)'
    r'\s*[:\-|]?\s*R?\$?\s*([\d.]+,\d{2})',
    re.IGNORECASE,
)
_RE_VALOR_MONETARIO = re.compile(r'R\$\s*([\d.]+,\d{2})', re.IGNORECASE)
_RE_PERIODO_SIMPLES = re.compile(r'\b(\d{2}[/\-]\d{4})\b')

_HOLERITE_INDICATORS = [
    'salário bruto', 'salario bruto', 'sal. bruto',
    'salário líquido', 'salario liquido',
    'vencimentos', 'proventos',
    'líquido a receber', 'liquido a receber',
    'holerite', 'contracheque', 'contra-cheque', 'contra cheque',
    'folha de pagamento', 'folha pagamento',
    'recibo de pagamento', 'recibo salarial',
    'demonstrativo salarial', 'demonstrativo de pagamento',
    'demonstrativo de vencimentos',
    'remunera', 'remuneração', 'remuneracao',
    'sal. líquido', 'sal. liquido',
    'total líquido', 'total liquido',
    'total a receber', 'valor a receber',
]


# ---------------------------------------------------------------------------
# Extração de texto
# ---------------------------------------------------------------------------

def extract_holerites_from_text(text: str, filename: str = "") -> List[Dict]:
    """
    Extrai holerites de texto livre (PDF/HTML).
    Modo estrito quando há indicadores; fallback permissivo para conteúdo atípico.
    """
    if not text or len(text.strip()) < 50:
        return []

    text_lower = text.lower()
    has_indicator = any(ind in text_lower for ind in _HOLERITE_INDICATORS)

    if has_indicator:
        results = _extract_strict(text, filename)
        if results:
            return results

    # Fallback: texto >= 200 chars com padrões monetários
    if len(text.strip()) >= 200:
        return _extract_permissive(text, filename)

    return []


def _extract_strict(text: str, filename: str) -> List[Dict]:
    func_matches = list(_RE_FUNCIONARIO.finditer(text))
    if not func_matches:
        # Primeiro, tentar extração em bloco único
        single = _extract_single(text, filename)
        if single:
            return single
        # Fallback: dividir em blocos e tentar cada um individualmente
        blocks = _split_into_blocks(text)
        if len(blocks) > 1:
            results: List[Dict] = []
            seen: set = set()
            for block in blocks:
                for h in _extract_single(block, filename):
                    key = _holerite_identity_key(h)
                    if key not in seen:
                        seen.add(key)
                        results.append(h)
            return results
        return []

    results = []
    for i, m in enumerate(func_matches):
        start = m.start()
        end = func_matches[i + 1].start() if i + 1 < len(func_matches) else len(text)
        h = _extract_single(text[start:end], filename, funcionario_override=m.group(1).strip())
        results.extend(h)
    return results


_RE_BLOCK_SEPARATOR = re.compile(
    r'(?:\n\s*[-=_]{5,}\s*\n|\n{3,})',
)


def _split_into_blocks(text: str) -> List[str]:
    """
    Divide o texto em blocos candidatos a holerites individuais.
    Tenta separadores explícitos primeiro; fallback em duas linhas em branco.
    """
    blocks = _RE_BLOCK_SEPARATOR.split(text)
    # Se o split produzir apenas um bloco (sem separadores encontrados),
    # tentar dividir em parágrafos por linha em branco dupla.
    if len(blocks) <= 1:
        blocks = re.split(r'\n{2,}', text)
    # Descartar blocos muito curtos (< 60 chars) ou sem nenhum valor monetário
    result = []
    for b in blocks:
        b = b.strip()
        if len(b) >= 60 and _RE_VALOR_MONETARIO.search(b):
            result.append(b)
    return result or [text]


def _extract_permissive(text: str, filename: str) -> List[Dict]:
    """
    Extração permissiva que tenta retornar um holerite por bloco de funcionário.
    Para textos com múltiplos funcionários separados por linhas em branco ou
    delimitadores, encontra um holerite por bloco (até MAX_PERMISSIVE).
    """
    blocks = _split_into_blocks(text)

    results: List[Dict] = []
    for block in blocks:
        if len(results) >= MAX_PERMISSIVE:
            break
        h = _extract_permissive_single_block(block, filename)
        if h:
            results.append(h)

    # Se nenhum bloco rendeu resultado, tentar o texto inteiro como bloco único
    if not results:
        h = _extract_permissive_single_block(text, filename)
        if h:
            results.append(h)

    return results


def _extract_permissive_single_block(text: str, filename: str) -> Optional[Dict]:
    """Extrai um único holerite de um bloco de texto (modo permissivo)."""
    valores = [_parse_br_value(m.group(1)) for m in _RE_VALOR_MONETARIO.finditer(text)]
    valores_plaus = sorted([v for v in valores if 300.0 <= v <= 100_000.0], reverse=True)
    if not valores_plaus:
        return None

    bruto = valores_plaus[0]
    liquido = valores_plaus[1] if len(valores_plaus) > 1 else 0.0
    descontos = round(bruto - liquido, 2) if bruto > liquido else 0.0
    periodo_m = _RE_PERIODO_SIMPLES.search(text)
    func_m = _RE_FUNCIONARIO.search(text)

    return {
        "funcionario": func_m.group(1).strip() if func_m else "",
        "cargo": "",
        "periodo": periodo_m.group(1) if periodo_m else "",
        "salario_bruto": bruto,
        "descontos": descontos,
        "salario_liquido": liquido,
        "source_file": filename,
        "extraction_method": "regex_text_permissive",
    }


def _extract_single(
    text: str, filename: str = "", funcionario_override: Optional[str] = None
) -> List[Dict]:
    bruto_m = _RE_SALARIO_BRUTO.search(text)
    liquido_m = _RE_SALARIO_LIQUIDO.search(text)
    bruto = _parse_br_value(bruto_m.group(1)) if bruto_m else 0.0
    liquido = _parse_br_value(liquido_m.group(1)) if liquido_m else 0.0

    if bruto == 0.0 and liquido == 0.0:
        return []

    desc_m = _RE_DESCONTOS.search(text)
    cargo_m = _RE_CARGO.search(text)
    periodo_m = _RE_PERIODO.search(text)
    func_m = _RE_FUNCIONARIO.search(text)

    funcionario = funcionario_override or (func_m.group(1).strip() if func_m else "")
    descontos = _parse_br_value(desc_m.group(1)) if desc_m else 0.0
    if descontos == 0.0 and bruto > liquido > 0.0:
        descontos = round(bruto - liquido, 2)

    return [{
        "funcionario": funcionario,
        "cargo": cargo_m.group(1).strip() if cargo_m else "",
        "periodo": periodo_m.group(1).strip() if periodo_m else "",
        "salario_bruto": bruto,
        "descontos": descontos,
        "salario_liquido": liquido,
        "source_file": filename,
        "extraction_method": "regex_text",
    }]


# ---------------------------------------------------------------------------
# Extração de DataFrame (ODS / Excel)
# ---------------------------------------------------------------------------

_COL_FUNCIONARIO = {"funcionario", "nome", "nome_funcionario", "empregado", "trabalhador", "colaborador"}
_COL_BRUTO = {"salario_bruto", "salariobruto", "bruto", "vencimentos", "proventos", "sal_bruto"}
_COL_LIQUIDO = {"salario_liquido", "salarioliquido", "liquido", "liquido_receber", "sal_liquido", "net"}
_COL_DESCONTOS = {"descontos", "desconto", "total_descontos", "total_desconto"}
_COL_PERIODO = {"periodo", "competencia", "mes", "mes_ano", "data", "referencia"}
_COL_CARGO = {"cargo", "funcao", "ocupacao"}

_RE_NOME_PESSOA = re.compile(
    r'^[A-ZÀ-Ú][a-zA-ZÀ-úàáâãäéèêëíìîïóòôõöúùûüç]{2,}\s+[A-ZÀ-Úa-zA-ZÀ-ú]{2,}',
    re.UNICODE,
)
_RE_VALOR_CELULA = re.compile(r'(\d{1,3}(?:\.\d{3})*,\d{2})')


def _normalize_col(col: str) -> str:
    return re.sub(r'[^a-z0-9]', '_', str(col).lower().strip())


def extract_holerites_from_dataframe(df: pd.DataFrame, filename: str = "") -> List[Dict]:
    """
    Extrai holerites de DataFrame bruto (ODS/Excel).
    Modo estrito (colunas nomeadas) → modo permissivo (nome+valor em qualquer linha).
    """
    if df is None or df.empty:
        return []

    norm_cols = {_normalize_col(c): c for c in df.columns}

    col_func = next((norm_cols[n] for n in _COL_FUNCIONARIO if n in norm_cols), None)
    col_bruto = next((norm_cols[n] for n in _COL_BRUTO if n in norm_cols), None)
    col_liquido = next((norm_cols[n] for n in _COL_LIQUIDO if n in norm_cols), None)
    col_descontos = next((norm_cols[n] for n in _COL_DESCONTOS if n in norm_cols), None)
    col_periodo = next((norm_cols[n] for n in _COL_PERIODO if n in norm_cols), None)
    col_cargo = next((norm_cols[n] for n in _COL_CARGO if n in norm_cols), None)

    results: List[Dict] = []

    # Modo estrito
    if col_func and (col_bruto or col_liquido):
        for _, row in df.iterrows():
            funcionario = str(row.get(col_func, "")).strip()
            if not funcionario or funcionario.lower() in ("nan", "none", ""):
                continue
            if len(funcionario.split()) < 2:
                continue

            bruto = _parse_br_value(row.get(col_bruto)) if col_bruto else 0.0
            liquido = _parse_br_value(row.get(col_liquido)) if col_liquido else 0.0
            if bruto == 0.0 and liquido == 0.0:
                continue

            descontos = _parse_br_value(row.get(col_descontos)) if col_descontos else 0.0
            if descontos == 0.0 and bruto > liquido > 0.0:
                descontos = round(bruto - liquido, 2)

            results.append({
                "funcionario": funcionario,
                "cargo": str(row.get(col_cargo, "")).strip() if col_cargo else "",
                "periodo": str(row.get(col_periodo, "")).strip() if col_periodo else "",
                "salario_bruto": bruto,
                "descontos": descontos,
                "salario_liquido": liquido,
                "source_file": filename,
                "extraction_method": "dataframe_strict",
            })

        if results:
            logger.info("extract_holerites_from_dataframe (strict): %d holerite(s) de '%s'", len(results), filename)
            return results

    # Modo permissivo
    count = 0
    for _, row in df.iterrows():
        if count >= MAX_PERMISSIVE:
            break

        cells = [str(c) for c in row.values if c is not None and str(c).strip() not in ("", "nan", "None")]

        nome = ""
        for cell in cells:
            if _RE_NOME_PESSOA.match(cell.strip()):
                nome = cell.strip()
                break
        if not nome:
            continue

        valores = []
        for cell in cells:
            for m in _RE_VALOR_CELULA.finditer(cell):
                v = _parse_br_value(m.group(1))
                if 300.0 <= v <= 100_000.0:
                    valores.append(v)
        if not valores:
            continue

        vs = sorted(set(valores), reverse=True)
        bruto = vs[0]
        liquido = vs[1] if len(vs) > 1 else 0.0
        descontos = round(bruto - liquido, 2) if bruto > liquido else 0.0

        results.append({
            "funcionario": nome,
            "cargo": "",
            "periodo": "",
            "salario_bruto": bruto,
            "descontos": descontos,
            "salario_liquido": liquido,
            "source_file": filename,
            "extraction_method": "dataframe_permissive",
        })
        count += 1

    if results:
        logger.info("extract_holerites_from_dataframe (permissive): %d holerite(s) de '%s'", len(results), filename)
    return results


# ---------------------------------------------------------------------------
# Extração híbrida
# ---------------------------------------------------------------------------

def extract_holerites_hybrid(doc_texts: List, force_llm: bool = False) -> List[Dict]:
    """
    Orquestra extração de holerites a partir de lista de textos.
    Aceita tanto lista de strings simples quanto lista de dicts
    ``{"filename": str, "text": str}`` (formato usado por document_texts no api_server).
    """
    results: List[Dict] = []
    seen_keys: set = set()

    for i, item in enumerate(doc_texts):
        # Suporta tanto string direta quanto dict {"filename": ..., "text": ...}
        if isinstance(item, dict):
            text = item.get("text") or ""
            fname = item.get("filename") or f"doc_text_{i}"
        else:
            text = item or ""
            fname = f"doc_text_{i}"

        if not text or not isinstance(text, str):
            continue

        for h in extract_holerites_from_text(text, filename=fname):
            key = _holerite_identity_key(h)
            if key not in seen_keys:
                seen_keys.add(key)
                results.append(h)

    if not results and force_llm:
        logger.info("extract_holerites_hybrid: nenhum holerite via regex; LLM necessário se disponível")

    return results


def collect_holerite_extraction_debug(doc_texts: List) -> List[Dict]:
    """
    Gera diagnóstico por documento: quantos holerites foram extraídos de cada um.
    Retorna lista de dicts com filename, count, status para uso no frontend/API.
    """
    debug: List[Dict] = []
    for i, item in enumerate(doc_texts):
        if isinstance(item, dict):
            text = item.get("text") or ""
            fname = item.get("filename") or f"doc_text_{i}"
        else:
            text = item or ""
            fname = f"doc_text_{i}"
        if not text or not isinstance(text, str):
            debug.append({"filename": fname, "count": 0, "status": "empty_or_invalid"})
            continue
        holerites = extract_holerites_from_text(text, filename=fname)
        n = len(holerites)
        debug.append({
            "filename": fname,
            "count": n,
            "status": "ok" if n > 0 else "no_matches",
        })
    return debug
