"""
Extração e Conciliação Estrutural Generalista — Prestação de Contas Condominial.

Pipeline orientado pela estrutura do documento: detecção dinâmica de contas,
extração de saldos por conta sem hardcode, consolidação, validação matemática
e classificação (REGULAR/IRREGULAR/SEM BASE). Não depende de valores específicos
nem número fixo de contas.
"""
import re
import logging
import unicodedata
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# --- Constantes semânticas (pesos para classificação de conta, não lista fixa de contas) ---
CONTA_KEYWORDS = {
    "conta": 3,
    "fundo": 3,
    "reserva": 2,
    "obras": 2,
    "investimento": 2,
    "festa": 1,
    "lavanderia": 1,
}
EXCLUDE_KEYWORDS = [
    "fornecedor", "inss", "fgts", "pis", "salario", "salário", "imposto", "encargos",
]
CONSOLIDADO_PATTERNS = [
    r"saldo\s+consolidado",
    r"saldo\s+geral",
    r"saldo\s+total",
    r"saldo\s+final\s+banco",
    r"disponibilidade\s+total",
]
RE_VALOR_BR = re.compile(r"-?\d{1,3}(?:\.\d{3})*,\d{2}|\(\d{1,3}(?:\.\d{3})*,\d{2}\)")
TOLERANCIA_CONCILIACAO = 0.01
SCORE_MIN_CONTA_FINANCEIRA = 3
SCORE_ALTA_CONFIABILIDADE = 7
SCORE_MEDIA_CONFIABILIDADE_MIN = 4
SCORE_MEDIA_CONFIABILIDADE_MAX = 6


def _parse_valor_br(raw: str) -> float:
    """Converte valor BR (1.234,56 ou (1.234,56)) para float."""
    if not raw or not isinstance(raw, str):
        return 0.0
    s = raw.strip().strip("()")
    s = s.replace(".", "").replace(",", ".")
    try:
        val = float(s)
        return -abs(val) if raw.strip().startswith("(") else val
    except ValueError:
        return 0.0


def normalizar_texto(texto: str) -> str:
    """
    Etapa 0 — Normalização do texto.
    UTF-8, múltiplos espaços, separadores monetários, quebras no meio de números, acentos.
    """
    if not texto or not isinstance(texto, str):
        return ""
    # Remover acentos para comparação semântica (NFC)
    try:
        texto = unicodedata.normalize("NFD", texto)
        texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    except Exception:
        pass
    texto = texto.replace("\r", "\n")
    # Remover quebras no meio de números (ex.: 1.\n234,56 -> 1.234,56)
    texto = re.sub(r"(\d)\s*\n\s*(\d)", r"\1\2", texto)
    texto = re.sub(r"\s+", " ", texto)
    # Padronizar R$ 1.234,56 mantendo vírgula para regex (não converter aqui; regex usa ,\d{2})
    return texto.strip().lower()


def _semantic_score_conta(nome: str) -> int:
    """Score por palavras-chave; >= SCORE_MIN_CONTA_FINANCEIRA => conta financeira."""
    if not nome:
        return 0
    nome_lower = nome.lower().strip()
    score = 0
    for kw, peso in CONTA_KEYWORDS.items():
        if kw in nome_lower:
            score += peso
    return score


def _is_excluded_line(line_lower: str) -> bool:
    """Ignorar linhas que contenham fornecedor, inss, fgts, etc."""
    return any(ex in line_lower for ex in EXCLUDE_KEYWORDS)


def _extract_account_blocks(text: str) -> List[Dict[str, Any]]:
    """
    Detecta blocos com estrutura financeira: conta + saldo_anterior/creditos/debitos/saldo_final.
    Retorna lista de candidatos com nome e valores extraídos.
    """
    lines = text.split("\n")
    candidates = []
    # Regex para linha tipo planilha: nome (3-40 chars) seguido de valores BR
    # Texto já normalizado (acentos removidos) -> conta só [a-z\s]
    re_block = re.compile(
        r"(?P<conta>[a-z\s]{3,50}?)\s+"
        r"(?P<saldo_anterior>-?\d{1,3}(?:\.\d{3})*,\d{2}|\(\d{1,3}(?:\.\d{3})*,\d{2}\))?\s*"
        r"(?P<creditos>-?\d{1,3}(?:\.\d{3})*,\d{2}|\(\d{1,3}(?:\.\d{3})*,\d{2}\))?\s*"
        r"(?P<debitos>-?\d{1,3}(?:\.\d{3})*,\d{2}|\(\d{1,3}(?:\.\d{3})*,\d{2}\))?\s*"
        r"(?P<saldo_final>-?\d{1,3}(?:\.\d{3})*,\d{2}|\(\d{1,3}(?:\.\d{3})*,\d{2}\))"
    )
    for i, line in enumerate(lines):
        line_clean = line.strip()
        if not line_clean or _is_excluded_line(line_clean.lower()):
            continue
        m = re_block.search(line_clean)
        if not m:
            continue
        conta_nome = m.group("conta").strip()
        if len(conta_nome) < 3:
            continue
        saldo_ant = m.group("saldo_anterior")
        creditos = m.group("creditos")
        debitos = m.group("debitos")
        saldo_fin = m.group("saldo_final")
        candidates.append({
            "nome": conta_nome,
            "saldo_anterior": _parse_valor_br(saldo_ant) if saldo_ant else None,
            "creditos": _parse_valor_br(creditos) if creditos else None,
            "debitos": _parse_valor_br(debitos) if debitos else None,
            "saldo_final_raw": _parse_valor_br(saldo_fin) if saldo_fin else None,
            "linha": i + 1,
        })
    return candidates


def _normalize_for_match(s: str) -> str:
    """Normaliza string para comparação (remove acentos)."""
    if not s:
        return ""
    try:
        s = unicodedata.normalize("NFD", s)
        s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    except Exception:
        pass
    return s.lower().strip()


def _find_explicit_saldo_final(text: str, conta_nome: str) -> Optional[float]:
    """Prioridade 1: coluna/linha explícita 'Saldo Final' associada à conta."""
    re_saldo_final = re.compile(
        r"saldo\s*(?:final|atual|do\s*mes)\s*[:\-]?\s*"
        r"(-?\d{1,3}(?:\.\d{3})*,\d{2}|\(\d{1,3}(?:\.\d{3})*,\d{2}\))",
        re.IGNORECASE
    )
    lines = text.split("\n")
    conta_ref = _normalize_for_match(conta_nome)
    for line in lines:
        if conta_ref not in _normalize_for_match(line):
            continue
        m = re_saldo_final.search(line)
        if m:
            return _parse_valor_br(m.group(1))
    return None


def _find_totalizer_line(text: str, conta_nome: str) -> Optional[float]:
    """Prioridade 2: linha totalizadora da conta (total + nome conta + valor)."""
    re_total = re.compile(
        r"total\s+(?:da\s+)?(?:conta\s+)?(?:[a-záéíóúãõç\s]+)\s+"
        r"(-?\d{1,3}(?:\.\d{3})*,\d{2}|\(\d{1,3}(?:\.\d{3})*,\d{2}\))",
        re.IGNORECASE
    )
    lines = text.split("\n")
    conta_ref = _normalize_for_match(conta_nome)
    for line in lines:
        if conta_ref not in _normalize_for_match(line) or "total" not in line.lower():
            continue
        m = re_total.search(line)
        if m:
            return _parse_valor_br(m.group(1))
        # Fallback: último valor monetário da linha
        vals = RE_VALOR_BR.findall(line)
        if vals:
            return _parse_valor_br(vals[-1])
    return None


def _detect_contas(text: str) -> List[Dict[str, Any]]:
    """
    Etapa 1–2: Detecção dinâmica de contas e extração do saldo final por conta.
    Retorna lista de contas com nome, saldo_final, score e metadados.
    """
    text_norm = normalizar_texto(text)
    blocks = _extract_account_blocks(text_norm)
    # Também procurar por seções nomeadas (ex.: "Conta Ordinária", "Fundo de Reserva")
    re_section = re.compile(
        r"\b(conta\s+ordin[aá]ria|fundo\s+de\s+reserva|fundo\s+de\s+obras|fundo\s+obras|"
        r"espa[cç]o\s+festa|lavanderia|conta\s+investimento|reserva\s+geral)\b",
        re.IGNORECASE
    )
    seen_names = set()
    contas = []
    for block in blocks:
        nome = block["nome"]
        if _is_excluded_line(nome):
            continue
        score_semantic = _semantic_score_conta(nome)
        if score_semantic < SCORE_MIN_CONTA_FINANCEIRA:
            if not re_section.search(nome):
                continue
            score_semantic = SCORE_MIN_CONTA_FINANCEIRA
        nome_key = nome.lower()[:50]
        if nome_key in seen_names:
            continue
        seen_names.add(nome_key)
        saldo_final = None
        source = None
        score = score_semantic
        if block.get("saldo_final_raw") is not None:
            saldo_final = block["saldo_final_raw"]
            source = "bloco_regex"
            score += 3
        if saldo_final is None:
            saldo_final = _find_explicit_saldo_final(text, nome)
            if saldo_final is not None:
                source = "explicito"
                score += 3
        if saldo_final is None:
            saldo_final = _find_totalizer_line(text, nome)
            if saldo_final is not None:
                source = "totalizador"
                score += 2
        if saldo_final is None and (
            block.get("saldo_anterior") is not None
            or block.get("creditos") is not None
            or block.get("debitos") is not None
        ):
            ant = block.get("saldo_anterior") or 0.0
            cred = block.get("creditos") or 0.0
            deb = block.get("debitos") or 0.0
            saldo_final = ant + cred - deb
            source = "calculado"
            score -= 1
            if block.get("saldo_final_raw") is not None:
                if abs(saldo_final - block["saldo_final_raw"]) <= TOLERANCIA_CONCILIACAO:
                    score += 2
                else:
                    score -= 3
        if saldo_final is None:
            continue
        if block.get("saldo_anterior") is not None and block.get("creditos") is not None and block.get("debitos") is not None:
            score += 2
        confiabilidade = "Alta" if score >= SCORE_ALTA_CONFIABILIDADE else (
            "Média" if SCORE_MEDIA_CONFIABILIDADE_MIN <= score <= SCORE_MEDIA_CONFIABILIDADE_MAX else "Baixa"
        )
        contas.append({
            "nome": nome.strip(),
            "saldo_final": round(float(saldo_final), 2),
            "confiabilidade": confiabilidade,
            "score": min(10, max(0, score)),
            "source": source or "inferido",
        })
    return contas


def _detect_saldo_consolidado(text: str) -> Tuple[Optional[float], bool]:
    """
    Etapa 3: Detecta saldo consolidado (Saldo Consolidado, Saldo Total, etc.).
    Retorna (valor ou None, existe_no_documento).
    """
    text_norm = normalizar_texto(text)
    candidates = []
    for pattern in CONSOLIDADO_PATTERNS:
        re_cons = re.compile(
            pattern + r"\s*[:\-]?\s*(-?\d{1,3}(?:\.\d{3})*,\d{2}|\(\d{1,3}(?:\.\d{3})*,\d{2}\))",
            re.IGNORECASE
        )
        for m in re_cons.finditer(text_norm):
            val = _parse_valor_br(m.group(1))
            candidates.append((val, m.group(0)))
    if not candidates:
        return None, False
    if len(candidates) == 1:
        return round(candidates[0][0], 2), True
    # Múltiplos: escolher maior valor positivo (não soma de receitas/despesas)
    positive = [(v, s) for v, s in candidates if v > 0]
    if positive:
        best = max(positive, key=lambda x: x[0])
        return round(best[0], 2), True
    return round(candidates[0][0], 2), True


def _compute_reconciliation(
    contas: List[Dict[str, Any]],
    saldo_consolidado: Optional[float],
    consolidado_existe: bool,
) -> Tuple[float, Optional[float], bool]:
    """
    Etapa 4: TOTAL_CONTAS = sum(saldo_final); diferença; conciliacao_ok.
    Anti-forçamento: se diferença == 0 e saldo_consolidado is None -> não considerar conciliação OK.
    """
    total_contas = sum(c.get("saldo_final") or 0 for c in contas)
    total_contas = round(total_contas, 2)
    if not consolidado_existe or saldo_consolidado is None:
        return total_contas, None, False
    diferenca = abs(total_contas - saldo_consolidado)
    conciliacao_ok = diferenca <= TOLERANCIA_CONCILIACAO
    if diferenca == 0.0 and saldo_consolidado is None:
        conciliacao_ok = False
    return total_contas, round(diferenca, 2), conciliacao_ok


def _build_alertas(contas: List[Dict[str, Any]], consolidado_existe: bool) -> List[str]:
    """Etapa 5: Alertas para saldos negativos e ausência de consolidado."""
    alertas = []
    for c in contas:
        sf = c.get("saldo_final")
        if sf is not None and sf < 0:
            alertas.append(f"Alerta: {c.get('nome', 'Conta')} com saldo negativo.")
    if not consolidado_existe:
        alertas.append("Não há referência de saldo consolidado no documento.")
    return alertas


def _score_relatorio(
    contas: List[Dict[str, Any]],
    consolidado_existe: bool,
    conciliacao_ok: bool,
) -> float:
    """Etapa 6: Score do relatório (média score contas * 0.6 + consolidado * 2 + conciliação * 3)."""
    if not contas:
        return 0.0
    media_score = sum(c.get("score", 0) for c in contas) / len(contas)
    presenca = 2.0 if consolidado_existe else 0.0
    conc = 3.0 if conciliacao_ok else 0.0
    return round(media_score * 0.6 + presenca + conc, 2)


def _classificar(
    conciliacao_ok: bool,
    score_relatorio: float,
    consolidado_existe: bool,
    alertas_negativos: bool,
) -> Tuple[str, str]:
    """
    Etapa 7: Classificação estrutural.
    REGULAR / REGULAR COM ALERTAS / IRREGULAR / EXTRAÇÃO INCONFIÁVEL / SEM BASE PARA CONCILIAÇÃO.
    """
    if not consolidado_existe:
        return "SEM BASE PARA CONCILIAÇÃO", (
            "Não há saldo consolidado no documento. Conciliação estrutural não aplicável."
        )
    if score_relatorio < SCORE_MEDIA_CONFIABILIDADE_MIN:
        return "EXTRAÇÃO INCONFIÁVEL", (
            f"Score de confiança da extração ({score_relatorio}) abaixo do mínimo. "
            "Recomenda-se validar manualmente os dados extraídos."
        )
    if not conciliacao_ok:
        return "IRREGULAR", (
            "A soma dos saldos finais das contas não confere com o saldo consolidado do documento."
        )
    if score_relatorio >= SCORE_ALTA_CONFIABILIDADE and not alertas_negativos:
        return "REGULAR", "Conciliação matemática OK e extração de alta confiança."
    if conciliacao_ok and (SCORE_MEDIA_CONFIABILIDADE_MIN <= score_relatorio <= SCORE_MEDIA_CONFIABILIDADE_MAX or alertas_negativos):
        return "REGULAR COM ALERTAS", (
            "Conciliação matemática OK, com alertas (saldos negativos ou confiança média)."
        )
    return "REGULAR", "Conciliação matemática OK."


def _checkpoint(contas: List, total_contas: float, saldo_consolidado: Optional[float], consolidado_existe: bool) -> List[str]:
    """Checkpoint interno: limitações a declarar se algo incerto."""
    limitacoes = []
    if len(contas) == 0:
        limitacoes.append("Nenhuma conta financeira identificada no documento.")
    else:
        sem_saldo = [c.get("nome") for c in contas if c.get("saldo_final") is None]
        if sem_saldo:
            limitacoes.append(f"Contas sem saldo final válido: {', '.join(sem_saldo[:5])}.")
    if not consolidado_existe and saldo_consolidado is not None:
        limitacoes.append("Saldo consolidado usado sem referência explícita no documento.")
    return limitacoes


def _format_output_text(result: Dict[str, Any]) -> str:
    """Formato de saída padronizado em texto."""
    lines = ["CONTAS IDENTIFICADAS", ""]
    for c in result.get("contas", []):
        nome = c.get("nome", "—")
        sf = c.get("saldo_final")
        val = f"R$ {sf:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if sf is not None else "N/A"
        lines.append(f"  {nome}: {val}")
    total = result.get("total_contas")
    lines.append("")
    lines.append(f"TOTAL DAS CONTAS: R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if total is not None else "TOTAL DAS CONTAS: N/A")
    sc = result.get("saldo_consolidado")
    if sc is not None:
        lines.append(f"Saldo Consolidado: R$ {sc:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    diff = result.get("diferenca")
    if diff is not None:
        lines.append(f"Diferença: R$ {diff:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    lines.append("")
    lines.append("ALERTAS")
    for a in result.get("alertas", []):
        lines.append(f"  - {a}")
    if not result.get("alertas"):
        lines.append("  (nenhum)")
    lines.append("")
    lines.append("CLASSIFICAÇÃO")
    lines.append(result.get("classificacao", "N/A"))
    if result.get("justificativa"):
        lines.append(result["justificativa"])
    return "\n".join(lines)


def run_structural_extraction(
    text: str,
    df: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Pipeline completo de extração e conciliação estrutural.

    Args:
        text: Texto do documento (prestação de contas / balancete).
        df: DataFrame opcional; se fornecido, convertido em texto e concatenado ao text.

    Returns:
        Dict com: contas, total_contas, saldo_consolidado, diferenca, confiabilidade_geral,
        classificacao, justificativa, alertas, limitacoes, texto_formatado (e campos JSON).
    """
    if not text and df is None:
        return {
            "contas": [],
            "total_contas": None,
            "saldo_consolidado": None,
            "diferenca": None,
            "confiabilidade_geral": 0.0,
            "classificacao": "SEM BASE PARA CONCILIAÇÃO",
            "justificativa": "Nenhum texto ou planilha fornecida.",
            "alertas": ["Nenhum dado para análise estrutural."],
            "limitacoes": ["Documento vazio ou não fornecido."],
            "texto_formatado": "CONTAS IDENTIFICADAS\n\n(nenhuma)\n\nCLASSIFICAÇÃO\nSEM BASE PARA CONCILIAÇÃO",
        }
    texto_final = text or ""
    if df is not None:
        try:
            from app.extraction.legacy import dataframe_to_text_br
            df_text = dataframe_to_text_br(df)
            if df_text:
                texto_final = (texto_final + "\n" + df_text).strip()
        except Exception as e:
            logger.warning(f"[STRUCTURAL] Erro ao converter DataFrame em texto: {e}")
    if not texto_final.strip():
        return {
            "contas": [],
            "total_contas": None,
            "saldo_consolidado": None,
            "diferenca": None,
            "confiabilidade_geral": 0.0,
            "classificacao": "SEM BASE PARA CONCILIAÇÃO",
            "justificativa": "Texto resultante vazio.",
            "alertas": ["Nenhum conteúdo para análise."],
            "limitacoes": [],
            "texto_formatado": "CONTAS IDENTIFICADAS\n\n(nenhuma)\n\nCLASSIFICAÇÃO\nSEM BASE PARA CONCILIAÇÃO",
        }
    contas = _detect_contas(texto_final)
    saldo_consolidado, consolidado_existe = _detect_saldo_consolidado(texto_final)
    total_contas, diferenca, conciliacao_ok = _compute_reconciliation(
        contas, saldo_consolidado, consolidado_existe
    )
    alertas = _build_alertas(contas, consolidado_existe)
    score_relatorio = _score_relatorio(contas, consolidado_existe, conciliacao_ok)
    alertas_negativos = any("saldo negativo" in a.lower() for a in alertas)
    classificacao, justificativa = _classificar(
        conciliacao_ok, score_relatorio, consolidado_existe, alertas_negativos
    )
    limitacoes = _checkpoint(contas, total_contas, saldo_consolidado, consolidado_existe)
    result = {
        "contas": [
            {
                "nome": c.get("nome"),
                "saldo_final": c.get("saldo_final"),
                "confiabilidade": c.get("confiabilidade"),
                "score": c.get("score"),
            }
            for c in contas
        ],
        "total_contas": total_contas,
        "saldo_consolidado": saldo_consolidado,
        "diferenca": diferenca,
        "confiabilidade_geral": score_relatorio,
        "classificacao": classificacao,
        "justificativa": justificativa,
        "alertas": alertas,
        "limitacoes": limitacoes,
    }
    result["texto_formatado"] = _format_output_text(result)
    logger.info(
        f"[STRUCTURAL] Contas={len(contas)}, total_contas={total_contas}, "
        f"consolidado={saldo_consolidado}, diferenca={diferenca}, classificacao={classificacao}"
    )
    return result
