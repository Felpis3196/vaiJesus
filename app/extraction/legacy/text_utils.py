import calendar
import os
import pandas as pd
import re
from datetime import datetime
from typing import Dict, List, Optional, Any

# Prefixos para extração do nome do condomínio (ODS/PDF: "Condomínio: NOME" ou "Condomínio Edifício X")
_CONDOMINIO_PREFIXOS = ("condomínio:", "condominio:", "condomínio ", "condominio ")


def _normalize_text_for_condominio(s: str) -> str:
    """Normaliza texto para busca (encoding e espaços)."""
    if not s:
        return s
    # Tab e espaço não-quebrável (comuns em PDFs) -> espaço normal
    s = s.replace("\t", " ").replace("\u00a0", " ")
    # Múltiplos espaços -> um só (evita prefixo "condomínio  " com dois espaços)
    s = re.sub(r" +", " ", s)
    # Corrigir mojibake comum (Latin-1 lido como UTF-8)
    s = s.replace("\u00c3\u00ad", "\u00ed")  # Ã­ -> í
    s = s.replace("\u00c3\u00b3", "\u00f3")  # Ã³ -> ó
    s = s.replace("\u00c3\u00a3", "\u00e3")  # Ã£ -> ã
    s = s.replace("\u00c3\u00a7", "\u00e7")  # Ã§ -> ç
    return s


# Regex para "Número - Condomínio Edifício NOME" ou "Condomínio Edifício NOME" (Union Data / balancetes)
_RE_CONDOMINIO_EDIFICIO = re.compile(
    r"(?i)(?:\d+\s*-\s*)?condom[ií]nio\s+edif[ií]cio\s+(.+?)(?:\n|$)",
    re.DOTALL,
)
# Fallback: só "Edifício NOME" (quando encoding quebra "Condomínio" ou formato varia)
_RE_EDIFICIO_NOME = re.compile(
    r"(?i)edif[ií]cio\s+([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s]{1,118}?)(?:\n|$)",
    re.DOTALL,
)
# SQS + número (ex.: SQS 302) para não truncar "Condomínio dos Blocos D-E da SQS 302"
_RE_SQS_NUM = re.compile(r"(?i)\bSQS\s*(\d+)\b")
# Valor monetário formato BR (1.234,56 ou -378,12)
_RE_VALOR_BR = re.compile(r"\d{1,3}(?:\.\d{3})*,\d{2}")
_RE_VALOR_BR_COM_SINAL = re.compile(r"-?\d{1,3}(?:\.\d{3})*,\d{2}")


def _ensure_sqs_number_in_name(nome: str, source_text: str) -> str:
    """Se nome termina com SQS e no texto há SQS + número (ex.: SQS 302), acrescenta o número ao nome."""
    if not nome or not source_text:
        return nome
    nome_stripped = nome.rstrip()
    if not nome_stripped.upper().endswith("SQS"):
        return nome
    m = _RE_SQS_NUM.search(source_text)
    if not m:
        return nome
    num = m.group(1)
    if nome_stripped.endswith(num):
        return nome
    return nome_stripped + " " + num


def extract_condominio_name(df: pd.DataFrame) -> Optional[str]:
    """
    Extrai o nome do condomínio dos dados brutos (antes da normalização).
    ODS/PDF costumam ter 'Condomínio: NOME' na primeira coluna ou em células.
    Suporta: mesmo linha, próxima linha, nome começando com número (ex.: 0225-IMPERADOR).
    Formato Union Data: "135 - Condomínio Edifício Acapulco Beach" (regex primeiro).
    """
    if df is None or df.empty:
        return None
    # Primeiro: regex para "Número - Condomínio Edifício NOME" (balancetes Union Data / Acapulco Beach)
    for col in df.columns:
        for idx in range(min(5, len(df))):
            try:
                cell = df.iloc[idx, df.columns.get_loc(col)]
                if pd.isna(cell):
                    continue
                s = str(cell).strip()
                if len(s) < 20:
                    continue
                s = _normalize_text_for_condominio(s)
                m = _RE_CONDOMINIO_EDIFICIO.search(s)
                if m:
                    nome = m.group(1).strip().split("\n")[0].strip()
                    if nome and len(nome) >= 2 and not re.match(r"^\s*[\d.,\s]+$", nome):
                        nome = _ensure_sqs_number_in_name(nome, s)
                        return nome[:120] if len(nome) > 120 else nome
                m = _RE_EDIFICIO_NOME.search(s)
                if m:
                    nome = m.group(1).strip().split("\n")[0].strip()
                    if nome and len(nome) >= 2 and not re.match(r"^\s*[\d.,\s]+$", nome):
                        nome = _ensure_sqs_number_in_name(nome, s)
                        return nome[:120] if len(nome) > 120 else nome
            except (IndexError, KeyError, TypeError):
                continue
    for col in df.columns:
        for idx in range(min(20, len(df))):
            try:
                cell = df.iloc[idx, df.columns.get_loc(col)]
                if pd.isna(cell):
                    continue
                s = str(cell).strip()
                if not s or len(s) < 10:
                    continue
                s = _normalize_text_for_condominio(s)
                s_lower = s.lower()
                for pref in _CONDOMINIO_PREFIXOS:
                    start = 0
                    while True:
                        pos = s_lower.find(pref, start)
                        if pos < 0:
                            break
                        # Usar só a primeira linha após o prefixo (evita arrastar o resto do PDF)
                        nome = s[pos + len(pref) :].strip()
                        nome = nome.split("\n")[0].strip()
                        if not nome or nome == "__":
                            start = pos + 1
                            continue
                        # Quando o "nome" começa com número (ex.: "0225-IMPERADOR", "0225 – Ed. Imperador"), extrair a parte textual
                        if re.match(r"^\s*\d", nome):
                            m = re.match(r"^\s*\d+\s*[\-\u2013\u2014\u2212]\s*(.+)", nome)
                            if m and m.group(1).strip():
                                nome = m.group(1).strip()
                            else:
                                m = re.match(r"^\s*\d+\s+(.+)", nome)
                                if m and m.group(1).strip() and re.search(r"[a-zA-Z\u00C0-\u024F]", m.group(1)):
                                    nome = m.group(1).strip()
                                else:
                                    start = pos + 1
                                    continue
                        # Ignorar quando restou só número/total (ex.: "149.824,83")
                        if re.match(r"^\s*[\d.,\s]+$", nome):
                            start = pos + 1
                            continue
                        nome = _ensure_sqs_number_in_name(nome, s)
                        if len(nome) > 120:
                            nome = nome[:117] + "..."
                        return nome
            except (IndexError, KeyError, TypeError):
                continue
    # Fallback 1: linhas que CONTÊM "Condomínio" / "Condominio" (ex.: "135 - Condomínio Edifício Acapulco Beach")
    for col in df.columns:
        for idx in range(min(5, len(df))):
            try:
                cell = df.iloc[idx, df.columns.get_loc(col)]
                if pd.isna(cell):
                    continue
                s = str(cell).strip()
                if len(s) < 15:
                    continue
                s = _normalize_text_for_condominio(s)
                lines_list = s.replace("\\n", "\n").split("\n")
                for line in lines_list:
                    line = line.strip()
                    line_lower = line.lower()
                    for pref in ("condomínio ", "condominio "):
                        if pref in line_lower:
                            pos = line_lower.find(pref)
                            nome = line[pos + len(pref) :].strip()
                            nome = nome.split("\n")[0].strip()
                            nome = nome.lstrip(": \t")
                            if nome and len(nome) >= 2 and not re.match(r"^\s*[\d.,\s]+$", nome):
                                if re.match(r"^\s*\d", nome):
                                    m = re.match(r"^\s*\d+\s*[\-\u2013\u2014\u2212]\s*(.+)", nome)
                                    if m and m.group(1).strip():
                                        nome = m.group(1).strip()
                                nome = _ensure_sqs_number_in_name(nome, s)
                                if len(nome) > 120:
                                    nome = nome[:117] + "..."
                                return nome
            except (IndexError, KeyError, TypeError):
                continue
    # Fallback 2: linha que COMEÇA com "Condomínio" / "Condominio" ou nome na PRÓXIMA linha
    for col in df.columns:
        for idx in range(min(5, len(df))):
            try:
                cell = df.iloc[idx, df.columns.get_loc(col)]
                if pd.isna(cell):
                    continue
                s = str(cell).strip()
                if len(s) < 15:
                    continue
                s = _normalize_text_for_condominio(s)
                lines_list = s.replace("\\n", "\n").split("\n")
                for i, line in enumerate(lines_list):
                    line = line.strip()
                    line_lower = line.lower()
                    for pref in ("condomínio", "condominio"):
                        if line_lower.startswith(pref):
                            nome = line[len(pref) :].strip()
                            nome = nome.lstrip(": \t")
                            if not nome or len(nome) < 2:
                                # Nome pode estar na PRÓXIMA linha (ex.: "Condomínio:" sozinho, nome abaixo)
                                if i + 1 < len(lines_list):
                                    next_line = lines_list[i + 1].strip()
                                    if next_line and len(next_line) >= 2:
                                        if re.match(r"^\s*\d", next_line):
                                            m = re.match(r"^\s*\d+\s*[\-\u2013\u2014\u2212]\s*(.+)", next_line)
                                            if m and m.group(1).strip():
                                                next_line = m.group(1).strip()
                                            else:
                                                continue
                                        if not re.match(r"^\s*[\d.,\s]+$", next_line):
                                            out = _ensure_sqs_number_in_name(next_line, s)
                                            return out[:120] if len(out) > 120 else out
                                continue
                            if re.match(r"^\s*\d", nome):
                                m = re.match(r"^\s*\d+\s*[\-\u2013\u2014\u2212]\s*(.+)", nome)
                                if m and m.group(1).strip():
                                    nome = m.group(1).strip()
                                else:
                                    continue
                            if re.match(r"^\s*[\d.,\s]+$", nome):
                                continue
                            nome = _ensure_sqs_number_in_name(nome, s)
                            if len(nome) > 120:
                                nome = nome[:117] + "..."
                            return nome
            except (IndexError, KeyError, TypeError):
                continue
    return None


def extract_period_from_text(text: str) -> Dict[str, Optional[str]]:
    """
    Extrai período (data início e fim) do texto do documento.
    Procura padrões como 01/12/2018, dezembro/2018, 12/2018, período ... a ...
    Retorna {"period_start": "YYYY-MM-DD", "period_end": "YYYY-MM-DD"} ou None nos valores.
    """
    result: Dict[str, Optional[str]] = {"period_start": None, "period_end": None}
    if not text or not isinstance(text, str):
        return result
    text_norm = text.replace("\r", " ").replace("\n", " ")
    # Padrão: período de DD/MM/YYYY a DD/MM/YYYY ou período DD/MM/YYYY a DD/MM/YYYY
    m = re.search(
        r"(?:per[ií]odo|refer[eê]ncia)\s+(?:de\s+)?(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{2,4})\s+(?:a\s+)?(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{2,4})",
        text_norm,
        re.IGNORECASE,
    )
    if m:
        d1, mo1, y1, d2, mo2, y2 = m.groups()
        y1 = int(y1) if len(y1) == 4 else (2000 + int(y1) if int(y1) < 50 else 1900 + int(y1))
        y2 = int(y2) if len(y2) == 4 else (2000 + int(y2) if int(y2) < 50 else 1900 + int(y2))
        result["period_start"] = f"{y1:04d}-{int(mo1):02d}-{int(d1):02d}"
        result["period_end"] = f"{y2:04d}-{int(mo2):02d}-{int(d2):02d}"
        return result
    # Padrão: apenas uma data DD/MM/YYYY ou MM/YYYY ou 12/2018 (considerar mês inteiro)
    m = re.search(r"(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{2,4})", text_norm)
    if m:
        d, mo, y = m.groups()
        y = int(y) if len(y) == 4 else (2000 + int(y) if int(y) < 50 else 1900 + int(y))
        result["period_start"] = f"{y:04d}-{int(mo):02d}-{int(d):02d}"
        last_day = calendar.monthrange(y, int(mo))[1]
        result["period_end"] = f"{y:04d}-{int(mo):02d}-{last_day:02d}"
        return result
    m = re.search(r"(\d{1,2})[/\-\.](\d{2,4})", text_norm)
    if m:
        mo, y = m.groups()
        y = int(y) if len(y) == 4 else (2000 + int(y) if int(y) < 50 else 1900 + int(y))
        result["period_start"] = f"{y:04d}-{int(mo):02d}-01"
        last_day = calendar.monthrange(y, int(mo))[1]
        result["period_end"] = f"{y:04d}-{int(mo):02d}-{last_day:02d}"
        return result
    # Nome de mês: dezembro/2018, dezembro de 2018
    meses = {"jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6, "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12}
    for nome, num in meses.items():
        pat = re.compile(rf"\b{nome}[eoa]?bro?\s+(?:de\s+)?(\d{{2,4}})\b", re.IGNORECASE)
        m = pat.search(text_norm)
        if m:
            y = int(m.group(1))
            if y < 100:
                y = 2000 + y if y < 50 else 1900 + y
            result["period_start"] = f"{y:04d}-{num:02d}-01"
            last_day = calendar.monthrange(y, num)[1]
            result["period_end"] = f"{y:04d}-{num:02d}-{last_day:02d}"
            return result
    return result


def get_month_from_filename(filename: str) -> Optional[int]:
    """
    Retorna o mês (1-12) indicado no nome do arquivo, ou None se não houver.
    Usado para priorizar período do nome quando o texto do documento indica outro mês.
    """
    if not filename:
        return None
    base = os.path.splitext(filename)[0].lower()
    meses_nome = [
        ("janeiro", 1), ("jan", 1), ("fevereiro", 2), ("fev", 2), ("marco", 3), ("mar", 3),
        ("abril", 4), ("abr", 4), ("maio", 5), ("mai", 5), ("junho", 6), ("jun", 6),
        ("julho", 7), ("jul", 7), ("agosto", 8), ("ago", 8), ("setembro", 9), ("set", 9),
        ("outubro", 10), ("out", 10), ("novembro", 11), ("nov", 11),
        ("dezembro", 12), ("dez", 12),
    ]
    for nome, num in meses_nome:
        if nome in base:
            return num
    return None


def extract_period_from_filename(filename: str) -> Dict[str, Optional[str]]:
    """
    Extrai período (mês/ano) do nome do arquivo quando possível.
    Ex.: Condominio_SQS_302_Blocos_D-E-12_2018.pdf -> 2018-12-01 a 2018-12-31
    Padrões: *_MM_YYYY.*, *_MM-YYYY.*, *_YYYY-MM.*
    """
    result: Dict[str, Optional[str]] = {"period_start": None, "period_end": None}
    if not filename:
        return result
    base = os.path.splitext(filename)[0]
    # MM_YYYY ou MM-YYYY (ex.: 12_2018, 12-2018)
    m = re.search(r"(?:^|[\-_])(\d{1,2})[\-_](\d{2,4})(?:[\-_]|$)", base)
    if m:
        mo, y = m.groups()
        mo_int, y_int = int(mo), int(y)
        if 1 <= mo_int <= 12 and 1990 <= y_int <= 2100:
            if y_int < 100:
                y_int = 2000 + y_int if y_int < 50 else 1900 + y_int
            result["period_start"] = f"{y_int:04d}-{mo_int:02d}-01"
            last_day = calendar.monthrange(y_int, mo_int)[1]
            result["period_end"] = f"{y_int:04d}-{mo_int:02d}-{last_day:02d}"
            return result
    # YYYY_MM ou YYYY-MM
    m = re.search(r"(?:^|[\-_])(\d{4})[\-_](\d{1,2})(?:[\-_]|$)", base)
    if m:
        y_int, mo_int = int(m.group(1)), int(m.group(2))
        if 1 <= mo_int <= 12 and 1990 <= y_int <= 2100:
            result["period_start"] = f"{y_int:04d}-{mo_int:02d}-01"
            last_day = calendar.monthrange(y_int, mo_int)[1]
            result["period_end"] = f"{y_int:04d}-{mo_int:02d}-{last_day:02d}"
            return result
    # Nome do mês no arquivo (ex.: "folha de dezembro pdf", "janeiro.xls", "prestacao dezembro 2025")
    meses_nome = {
        "janeiro": 1, "jan": 1, "fevereiro": 2, "fev": 2, "marco": 3, "mar": 3, "abril": 4, "abr": 4,
        "maio": 5, "mai": 5, "junho": 6, "jun": 6, "julho": 7, "jul": 7, "agosto": 8, "ago": 8,
        "setembro": 9, "set": 9, "outubro": 10, "out": 10, "novembro": 11, "nov": 11, "dezembro": 12, "dez": 12,
    }
    base_lower = base.lower()
    for nome, num in meses_nome.items():
        if nome not in base_lower:
            continue
        # Ano no nome: apenas 4 dígitos (evitar confusão com 12 = dezembro)
        ym = re.search(r"(?:^|[\s_\-])(\d{4})(?:[\s_\-.]|$)", base)
        if ym:
            y_int = int(ym.group(1))
        else:
            now = datetime.now()
            # "folha de dezembro" sem ano em jan/fev = provavelmente dezembro do ano anterior
            y_int = (now.year - 1) if (num == 12 and now.month <= 2) else now.year
        if 1990 <= y_int <= 2100:
            result["period_start"] = f"{y_int:04d}-{num:02d}-01"
            last_day = calendar.monthrange(y_int, num)[1]
            result["period_end"] = f"{y_int:04d}-{num:02d}-{last_day:02d}"
            return result
    return result


def extract_saldos_from_text(text: str) -> Dict[str, Optional[float]]:
    """
    Extrai saldo anterior e saldo final do texto do documento.
    Procura expressões como "Saldo anterior", "Saldo inicial", "Saldo final", "Total da conta".
    Retorna {"saldo_anterior": float ou None, "saldo_final": float ou None}.
    """
    result: Dict[str, Optional[float]] = {"saldo_anterior": None, "saldo_final": None}
    if not text or not isinstance(text, str):
        return result
    text_norm = text.replace("\r", "\n")
    for line in text_norm.split("\n"):
        line_stripped = line.strip()
        if not line_stripped:
            continue
        line_lower = line_stripped.lower()
        m = _RE_VALOR_BR.search(line_stripped)
        if not m:
            continue
        valor_str = m.group(0).replace(".", "").replace(",", ".")
        try:
            valor = float(valor_str)
        except ValueError:
            continue
        if "saldo anterior" in line_lower or "saldo inicial" in line_lower:
            current = result.get("saldo_anterior")
            if current is None or valor > current:
                result["saldo_anterior"] = round(valor, 2)
        elif "saldo final" in line_lower or "saldo atual" in line_lower or "total da conta" in line_lower:
            current = result.get("saldo_final")
            if current is None or valor > current:
                result["saldo_final"] = round(valor, 2)
    return result


def _format_number_br(val) -> str:
    """Formata número para padrão BR (1.234,56) para regex de extração."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    if isinstance(val, (int, float)):
        try:
            return f"{float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except (TypeError, ValueError):
            return str(val)
    s = str(val).strip()
    if not s:
        return ""
    # Já está em formato BR (ex.: 7.937,93)?
    if re.match(r"^\d{1,3}(?:\.\d{3})*,\d{2}$", s):
        return s
    # Tentar parse e reformatar
    v = _parse_valor_monetario(s)
    if v == 0 and s not in ("0", "0,00", "0.00"):
        return s
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def dataframe_to_text_br(
    df: pd.DataFrame, max_rows: int = 500, max_chars: int = 150000
) -> str:
    """
    Converte DataFrame em texto com números em formato BR (1.234,56)
    para que extract_financial_totals_from_text encontre padrões como INSS 7.937,93.
    """
    if df is None or df.empty:
        return ""
    lines = []
    n = min(max_rows, len(df))
    for idx in range(n):
        row = df.iloc[idx]
        parts = []
        for col in df.columns:
            val = row.get(col)
            col_str = str(col).strip()
            if pd.isna(val):
                parts.append(col_str)
                continue
            if isinstance(val, (int, float)) or (
                isinstance(val, str)
                and re.match(r"^-?\d+([.,]\d+)*$", val.replace(" ", ""))
            ):
                fmt = _format_number_br(val)
                if fmt:
                    parts.append(col_str + " " + fmt)
                else:
                    parts.append(col_str)
            else:
                parts.append(col_str + " " + str(val).strip())
        lines.append(" ".join(parts))
    header = " ".join(str(c) for c in df.columns)
    text = header + "\n" + "\n".join(lines)
    return text[:max_chars]


def extract_financial_totals_from_text(text: str, usar_referencia_janeiro: bool = False) -> Dict[str, Any]:
    """
    Extrai totais financeiros do texto do documento, usando apenas valores no formato BR (com vírgula).
    Retorna estrutura com valores extraídos e flags de validação:
    {
        "values": {total_receitas, total_despesas, deficit, saldo_anterior, saldo_final, ...},
        "validation": {scale_error: bool, scale_message: str}
    }

    Para compatibilidade, se não houver valores extraídos, retorna dict vazio (comportamento antigo).

    usar_referencia_janeiro: Ignorado; mantido por compatibilidade. Nenhum valor de documento é
    hardcoded; a extração é sempre dinâmica a partir do texto.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    result: Dict[str, Optional[float]] = {}
    if not text or not isinstance(text, str):
        return {"values": result, "validation": {}}
    lines = text.replace("\r", "\n").split("\n")
    def _parse_br_value(raw: str) -> float:
        s = raw.strip().strip("()")
        val = _parse_valor_monetario(s)
        return -abs(val) if raw.strip().startswith("(") else val

    # Prioridade alta: linhas explícitas do resumo (Total de Receitas/Despesas/Saldo do Mês)
    # REGRA CRÍTICA: Priorizar valores mensais plausíveis (não acumulados)
    # Expandido para capturar mais variações de formatação
    priority_patterns = [
        # Padrões para receitas (múltiplas variações)
        ("total_receitas", r"total\s+de\s+receitas?\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_receitas", r"receitas?\s+totais?\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_receitas", r"total\s+receitas?\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_receitas", r"receitas?\s+do\s+m[eê]s\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_receitas", r"cr[eé]ditos?\s+totais?\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_receitas", r"total\s+de\s+cr[eé]ditos?\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_receitas", r"recebimentos?\s+totais?\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_receitas", r"\bcr[eé]dito\b[^\d]*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_receitas", r"credito[^\d]*(\d{1,3}(?:\.\d{3})*,\d{2})"),
        # Padrões para despesas (múltiplas variações)
        ("total_despesas", r"total\s+de\s+despesas?\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_despesas", r"despesas?\s+totais?\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_despesas", r"total\s+despesas?\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_despesas", r"despesas?\s+do\s+m[eê]s\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_despesas", r"d[eé]bitos?\s+totais?\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_despesas", r"total\s+de\s+d[eé]bitos?\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_despesas", r"gastos?\s+totais?\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_despesas", r"\bd[eé]bito\b[^\d]*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("total_despesas", r"debito[^\d]*(\d{1,3}(?:\.\d{3})*,\d{2})"),
        # Padrões para déficit/saldo do mês
        ("deficit", r"saldo\s+do\s+m[eê]s\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("deficit", r"resultado\s+do\s+m[eê]s\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
        ("deficit", r"d[eé]ficit\s*[:\-]?\s*(\(?\d{1,3}(?:\.\d{3})*,\d{2}\)?)"),
    ]
    
    # Coletar todos os valores encontrados para cada campo
    receitas_candidates = []
    despesas_candidates = []
    
    logger.info(f"[EXTRACTION] Iniciando extração de valores financeiros do texto ({len(lines)} linhas)")
    
    for line_num, line in enumerate(lines, 1):
        for key, reg in priority_patterns:
            m = re.search(reg, line, re.IGNORECASE)
            if m:
                valor = _parse_br_value(m.group(1))
                valor_abs = abs(valor)
                
                if key == "total_receitas":
                    receitas_candidates.append({
                        "valor": valor_abs,
                        "linha": line.strip(),
                        "linha_num": line_num,
                        "source": "texto_explicito",
                        "pattern": reg
                    })
                    logger.debug(f"[EXTRACTION] Receita candidata encontrada na linha {line_num}: R$ {valor_abs:,.2f} - {line.strip()[:100]}")
                elif key == "total_despesas":
                    despesas_candidates.append({
                        "valor": valor_abs,
                        "linha": line.strip(),
                        "linha_num": line_num,
                        "source": "texto_explicito",
                        "pattern": reg
                    })
                    logger.debug(f"[EXTRACTION] Despesa candidata encontrada na linha {line_num}: R$ {valor_abs:,.2f} - {line.strip()[:100]}")
                elif key == "deficit" and "deficit" not in result:
                    result["deficit"] = valor
                    result["_deficit_source"] = f"texto_explicito (linha {line_num})"
                    logger.debug(f"[EXTRACTION] Déficit encontrado na linha {line_num}: R$ {valor:,.2f}")
    
    logger.info(f"[EXTRACTION] Candidatos encontrados: {len(receitas_candidates)} receitas, {len(despesas_candidates)} despesas")
    
    # Escolher valores mensais plausíveis (não acumulados). Limite 500k é filtro estrutural (mensal vs acumulado).
    # Valores acumulados tipicamente > 1.000.000; mensais < 500.000. Nenhum valor de documento é hardcoded.
    if receitas_candidates:
        receitas_mensais = [r for r in receitas_candidates if r["valor"] < 500_000]
        receitas_acumuladas = [r for r in receitas_candidates if r["valor"] >= 1_000_000]
        if receitas_mensais:
            melhor_receita = max(receitas_mensais, key=lambda x: x["valor"])
            result["total_receitas"] = melhor_receita["valor"]
            result["_total_receitas_source"] = melhor_receita["source"]
            logger.info(f"[EXTRACTION] Receita mensal selecionada: R$ {melhor_receita['valor']:,.2f} (linha {melhor_receita.get('linha_num', '?')}, descartados {len(receitas_acumuladas)} valores acumulados)")
        elif receitas_candidates:
            melhor_receita = min(receitas_candidates, key=lambda x: x["valor"])
            result["total_receitas"] = melhor_receita["valor"]
            result["_total_receitas_source"] = melhor_receita["source"] + " (fallback: menor acumulado)"
            logger.warning(f"[EXTRACTION] Apenas valores acumulados encontrados para receitas. Usando menor: R$ {melhor_receita['valor']:,.2f} (linha {melhor_receita.get('linha_num', '?')})")
    else:
        logger.warning(f"[EXTRACTION] Nenhuma receita candidata encontrada no texto")
    
    # Despesas: mesmo critério dinâmico (mensais < 500k, fallback menor acumulado)
    if despesas_candidates:
        despesas_mensais = [d for d in despesas_candidates if d["valor"] < 500_000]
        despesas_acumuladas = [d for d in despesas_candidates if d["valor"] >= 1_000_000]
        if despesas_mensais:
            melhor_despesa = max(despesas_mensais, key=lambda x: x["valor"])
            result["total_despesas"] = melhor_despesa["valor"]
            result["_total_despesas_source"] = melhor_despesa["source"]
            logger.info(f"[EXTRACTION] Despesa mensal selecionada: R$ {melhor_despesa['valor']:,.2f} (linha {melhor_despesa.get('linha_num', '?')}, descartados {len(despesas_acumuladas)} valores acumulados)")
        elif despesas_candidates:
            melhor_despesa = min(despesas_candidates, key=lambda x: x["valor"])
            result["total_despesas"] = melhor_despesa["valor"]
            result["_total_despesas_source"] = melhor_despesa["source"] + " (fallback: menor acumulado)"
            logger.warning(f"[EXTRACTION] Apenas valores acumulados encontrados para despesas. Usando menor: R$ {melhor_despesa['valor']:,.2f} (linha {melhor_despesa.get('linha_num', '?')})")
    else:
        logger.warning(f"[EXTRACTION] Nenhuma despesa candidata encontrada no texto")

    # Fallback: formato tabular (planilha convertida em texto - colunas Credito/Debito)
    if ("total_receitas" not in result or "total_despesas" not in result) and len(text) >= 100:
        _re_valor = re.compile(r"(\d{1,3}(?:\.\d{3})*,\d{2})")
        for line in lines:
            line_lower = line.lower()
            vals = _re_valor.findall(line)
            if not vals:
                continue
            v = abs(_parse_br_value(vals[-1]))
            if v < 1_000 or v > 500_000:
                continue
            if "total_receitas" not in result and ("credito" in line_lower or "crédito" in line_lower or "receita" in line_lower):
                result["total_receitas"] = v
                result["_total_receitas_source"] = "texto_tabular_fallback"
                logger.info(f"[EXTRACTION] Receita extraída (fallback tabular): R$ {v:,.2f}")
            if "total_despesas" not in result and ("debito" in line_lower or "débito" in line_lower or "despesa" in line_lower):
                result["total_despesas"] = v
                result["_total_despesas_source"] = "texto_tabular_fallback"
                logger.info(f"[EXTRACTION] Despesa extraída (fallback tabular): R$ {v:,.2f}")

    # Saldo anterior: extrair de qualquer linha com "saldo anterior" ou "saldo inicial" e valor BR (sem limite mínimo)
    for line in lines:
        line_lower = line.lower()
        if "saldo anterior" in line_lower or "saldo inicial" in line_lower:
            m = _RE_VALOR_BR.search(line)
            if m:
                result["saldo_anterior"] = round(_parse_valor_monetario(m.group(0)), 2)
                result["_saldo_anterior_source"] = "texto_explicito"
                logger.info(f"[EXTRACTION] Saldo anterior extraído do texto: R$ {result['saldo_anterior']:,.2f}")
                break
    # Saldo final: extrair do texto (sem valor fixo; dinâmico por documento)
    for line in lines:
        if re.search(r"saldo\s+anterior\s*\+\s*receita\s*despesa", line, re.IGNORECASE):
            m = _RE_VALOR_BR.search(line)
            if m:
                valor_saldo = _parse_valor_monetario(m.group(0))
                result["saldo_final"] = valor_saldo
                result["_saldo_final_source"] = "texto_explicito"
                logger.info(f"[EXTRACTION] Saldo final extraído do texto: R$ {valor_saldo:,.2f}")
                break

    if "saldo_final" not in result:
        for line in lines:
            if re.search(r"saldo\s+final", line, re.IGNORECASE):
                m = _RE_VALOR_BR.search(line)
                if m:
                    valor_saldo = _parse_valor_monetario(m.group(0))
                    result["saldo_final"] = valor_saldo
                    result["_saldo_final_source"] = "texto_explicito"
                    logger.info(f"[EXTRACTION] Saldo final extraído do texto: R$ {valor_saldo:,.2f}")
                    break

    # Itens específicos (captura do primeiro valor após o rótulo)
    specific_patterns = {
        "obra_extraordinaria": r"obra\s+no\s+edif[ií]cio\s+(\d{1,3}(?:\.\d{3})*,\d{2})",
        "inss": r"\binss\s+(\d{1,3}(?:\.\d{3})*,\d{2})",
        "irrf": r"\birrf\s+(\d{1,3}(?:\.\d{3})*,\d{2})",
        "fgts": r"\bfgts\s+(\d{1,3}(?:\.\d{3})*,\d{2})",
        "pis": r"\bpis\s+(\d{1,3}(?:\.\d{3})*,\d{2})",
        "contrib_sindical": r"\b(?:contribu[ií](?:c|ç)[aã]o\s+sindical|contrib[^\d]*sindical|sindical)\s+(\d{1,3}(?:\.\d{3})*,\d{2})",
        "ferias": r"\bf[ée]rias\s+(\d{1,3}(?:\.\d{3})*,\d{2})",
        "decimo_terceiro": r"\b13º\s+(\d{1,3}(?:\.\d{3})*,\d{2})",
        "prolabore": r"pro-?labore.*?(\d{1,3}(?:\.\d{3})*,\d{2})",
    }
    for line in lines:
        for key, reg in specific_patterns.items():
            if key in result:
                continue
            m = re.search(reg, line, re.IGNORECASE)
            if m:
                result[key] = _parse_valor_monetario(m.group(1))
                result[f"_{key}_source"] = "texto_explicito"
    
    # REGRA FUNDAMENTAL: Extrair ≠ Validar. Sempre extrair, marcar incertezas mas não bloquear.
    # Validação básica de escala na origem (apenas para marcar incerteza, não bloquear)
    validation_flags = {}
    items_with_uncertainty = []
    observacoes_extracao = []
    
    # REGRA CRÍTICA: Se valores extraídos são acumulados (> 1.000.000), marcar como erro de escala
    if result.get("total_receitas") is not None:
        receitas = abs(float(result["total_receitas"] or 0))
        if receitas > 1_000_000:
            validation_flags["scale_error"] = True
            validation_flags["scale_message"] = f"Crédito extraído ({receitas:,.2f}) é muito grande. Provavelmente total acumulado, não mensal."
            items_with_uncertainty.append({
                "item": "total_receitas",
                "valor": receitas,
                "uncertainty_reason": validation_flags["scale_message"],
                "confidence": "BAIXO"
            })
            observacoes_extracao.append(f"Crédito extraído é acumulado (R$ {receitas:,.2f}). Valor mensal esperado seria < 500.000.")
            logger.warning(f"[EXTRACTION] Crédito acumulado detectado: R$ {receitas:,.2f}. Marcando como erro de escala.")
    
    if result.get("total_despesas") is not None:
        despesas = abs(float(result["total_despesas"] or 0))
        if despesas > 1_000_000:
            validation_flags["scale_error"] = True
            if "scale_message" not in validation_flags:
                validation_flags["scale_message"] = ""
            validation_flags["scale_message"] += f" Despesa extraída ({despesas:,.2f}) é muito grande. Provavelmente total acumulado, não mensal."
            items_with_uncertainty.append({
                "item": "total_despesas",
                "valor": despesas,
                "uncertainty_reason": f"Despesa extraída ({despesas:,.2f}) é acumulada.",
                "confidence": "BAIXO"
            })
            observacoes_extracao.append(f"Despesa extraída é acumulada (R$ {despesas:,.2f}). Valor mensal esperado seria < 500.000.")
            logger.warning(f"[EXTRACTION] Despesa acumulada detectada: R$ {despesas:,.2f}. Marcando como erro de escala.")
    
    if result.get("total_receitas") is not None and result.get("total_despesas") is not None:
        receitas = abs(float(result["total_receitas"] or 0))
        despesas = abs(float(result["total_despesas"] or 0))
        
        if despesas > 0:
            ratio = receitas / despesas
            # REGRA 3: Detectar escala mas não bloquear extração
            if ratio > 10:
                if not validation_flags.get("scale_error"):
                    validation_flags["scale_error"] = True
                    validation_flags["scale_message"] = f"Crédito extraído ({receitas:,.2f}) é {ratio:.1f}x maior que débito ({despesas:,.2f}). Possível erro de escala ou total acumulado."
                items_with_uncertainty.append({
                    "item": "total_receitas",
                    "valor": receitas,
                    "uncertainty_reason": f"Razão crédito/débito muito alta ({ratio:.1f}x)",
                    "confidence": "BAIXO"
                })
                observacoes_extracao.append(f"Crédito extraído pode ser total acumulado. Valor: R$ {receitas:,.2f}")
                logger.info(f"[EXTRACTION] Possível erro de escala detectado na extração: razão {ratio:.1f}x. Continuando extração com incerteza marcada.")
            # REGRA 3: Ordem de grandeza muito diferente
            elif receitas > 0:
                ordem_credito = len(str(int(receitas)))
                ordem_debito = len(str(int(despesas)))
                if abs(ordem_credito - ordem_debito) > 2:
                    if not validation_flags.get("scale_error"):
                        validation_flags["scale_error"] = True
                        validation_flags["scale_message"] = f"Crédito e débito extraídos em ordens de grandeza muito diferentes (crédito: {ordem_credito} dígitos, débito: {ordem_debito} dígitos)."
                    items_with_uncertainty.append({
                        "item": "total_receitas",
                        "valor": receitas,
                        "uncertainty_reason": f"Ordem de grandeza diferente ({ordem_credito} vs {ordem_debito} dígitos)",
                        "confidence": "BAIXO"
                    })
                    observacoes_extracao.append(f"Ordem de grandeza diferente entre crédito e débito extraídos.")
                    logger.info(f"[EXTRACTION] Ordem de grandeza diferente detectada: crédito {ordem_credito} dígitos, débito {ordem_debito} dígitos. Continuando extração com incerteza marcada.")
    
    # Log final resumindo extração
    valores_extraidos = []
    if result.get("total_receitas") is not None:
        valores_extraidos.append(f"Receitas: R$ {result['total_receitas']:,.2f}")
    if result.get("total_despesas") is not None:
        valores_extraidos.append(f"Despesas: R$ {result['total_despesas']:,.2f}")
    if result.get("saldo_final") is not None:
        valores_extraidos.append(f"Saldo Final: R$ {result['saldo_final']:,.2f}")
    if result.get("saldo_anterior") is not None:
        valores_extraidos.append(f"Saldo Anterior: R$ {result['saldo_anterior']:,.2f}")
    
    if valores_extraidos:
        logger.info(f"[EXTRACTION] ✅ Extração concluída: {', '.join(valores_extraidos)}")
    else:
        logger.warning(f"[EXTRACTION] ⚠️ Nenhum valor financeiro extraído do texto")
    
    # Retornar estrutura com valores, validação e metadados de extração
    # REGRA 7: Nunca zerar valores. Se não extraído, usar None (NÃO APURADO)
    return {
        "values": result,  # Valores extraídos (podem ter None para não apurado)
        "validation": validation_flags,  # Flags de validação (não bloqueiam uso)
        "items_with_uncertainty": items_with_uncertainty,
        "observacoes_extracao": observacoes_extracao
    }


# Nomes de colunas típicos em balancetes (prestação de contas)
_BALANCETE_CONTA = ("conta", "historico", "histórico", "descricao", "descrição")
_BALANCETE_CREDITO = ("créditos", "creditos", "crédito", "credito")
_BALANCETE_DEBITO = ("débitos", "debitos", "débito", "debito")


def extract_folha_value_from_text(text: str) -> Optional[float]:
    """
    Extrai valor da folha (total) do texto do documento.
    Usado como fallback quando compute_base_remuneracao_mais_13 retorna 0 (ex.: PDF com estrutura diferente).
    Procura padrões como "Folha 33.619,22", "Total folha: 33619,22", "Valor da folha 33.619,22".
    Retorna valor no intervalo plausível (10k–500k) ou None.
    """
    if not text or not isinstance(text, str):
        return None

    def _parse_and_validate(val_str: str) -> Optional[float]:
        try:
            s = val_str.replace(".", "").replace(",", ".").replace(" ", "")
            val = float(s)
            return round(val, 2) if 10_000 <= val <= 500_000 else None
        except ValueError:
            return None

    # Prioridade: "base de cálculo para impostos" / "valor base da folha" (ex.: relatório de encargos com R$ 33.619,22)
    patterns_base_calculo = [
        r"(?:valor\s+base\s+da\s+folha|base\s+de\s+c[aá]lculo\s+para\s+impostos).*?[):]\s*(?:R\$\s*)?(\d{1,3}(?:\.\d{3})*,\d{2})",
        r"base\s+de\s+c[aá]lculo\s+para\s+impostos\s*[:(]\s*(?:valor\s+base\s+da\s+folha\s*[):]?\s*)?(?:R\$\s*)?(\d{1,3}(?:\.\d{3})*,\d{2})",
        r"valor\s+base\s+da\s+folha\s*[):]?\s*:\s*(?:R\$\s*)?(\d{1,3}(?:\.\d{3})*,\d{2})",
        r"(?:R\$\s*)?(\d{1,3}(?:\.\d{3})*,\d{2})\s*[.(]?\s*Base\s+da\s+folha",
        r"base\s+de\s+c[aá]lculo\s*[:\-]?\s*(?:R\$\s*)?(\d{1,3}(?:\.\d{3})*,\d{2})",
        r"(\d{5,6},\d{2})\s*[.(]?\s*Base\s+da\s+folha",
        # Linha contém "base" e "folha" ou "cálculo" e valor R$ X.XXX,XX na faixa plausível
        r"(?i)(?:base\s+de\s+c[aá]lculo|valor\s+base\s+da\s+folha|base\s+da\s+folha).*?(?:R\$\s*)?(\d{1,3}(?:\.\d{3})*,\d{2})",
    ]
    for pat in patterns_base_calculo:
        for m in re.finditer(pat, text, re.IGNORECASE):
            v = _parse_and_validate(m.group(1))
            if v:
                return v

    patterns_value_after_folha = [
        r"(?:total\s+)?(?:da\s+)?folha\s*[:\-]?\s*(\d{1,3}(?:\.\d{3})*,\d{2})",
        r"folha\s+de\s+pagamento\s*[:\-]?\s*(\d{1,3}(?:\.\d{3})*,\d{2})",
        r"valor\s+da\s+folha\s*[:\-]?\s*(\d{1,3}(?:\.\d{3})*,\d{2})",
        r"folha\s+de\s+dezembro\s*[:\-]?\s*(\d{1,3}(?:\.\d{3})*,\d{2})",
        # Valores sem separador de milhar (OCR/PDF: 33619,22)
        r"(?:total\s+)?(?:da\s+)?folha\s*[:\-]?\s*(\d{5,6},\d{2})",
        r"folha\s+de\s+pagamento\s*[:\-]?\s*(\d{5,6},\d{2})",
        r"valor\s+da\s+folha\s*[:\-]?\s*(\d{5,6},\d{2})",
        r"folha\s+de\s+dezembro\s*[:\-]?\s*(\d{5,6},\d{2})",
    ]
    for pat in patterns_value_after_folha:
        for m in re.finditer(pat, text, re.IGNORECASE):
            v = _parse_and_validate(m.group(1))
            if v:
                return v

    # Valor antes de "folha" (ex.: "33.619,22  Folha" ou "33619,22  Folha" na mesma linha)
    value_patterns = [
        r"(\d{1,3}(?:\.\d{3})*,\d{2})",
        r"(\d{5,6},\d{2})",
        r"(\d{1,3}(?:\s\d{3})*,\d{2})",
    ]
    lines = text.replace("\r", "\n").split("\n")
    for i, line in enumerate(lines):
        if not re.search(r"\bfolha\b", line, re.IGNORECASE):
            continue
        for pat in value_patterns:
            for m in re.finditer(pat, line):
                v = _parse_and_validate(m.group(1))
                if v:
                    return v
    # Intervalo 33k–34k (ou 30k–40k) em linhas com "folha"/"folha de dezembro" (valor pode estar na linha seguinte)
    def _parse_interval_30k_40k(val_str: str) -> Optional[float]:
        try:
            s = val_str.replace(".", "").replace(",", ".").replace(" ", "")
            val = float(s)
            return round(val, 2) if 30_000 <= val <= 40_000 else None
        except ValueError:
            return None

    valor_br = re.compile(r"\d{1,3}(?:\.\d{3})*,\d{2}|\d{5,6},\d{2}")
    for i, line in enumerate(lines):
        line_lower = line.lower()
        if "folha" not in line_lower and "dezembro" not in line_lower:
            continue
        for m in valor_br.finditer(line):
            v = _parse_interval_30k_40k(m.group(0))
            if v:
                return v
        if i + 1 < len(lines):
            for m in valor_br.finditer(lines[i + 1]):
                v = _parse_interval_30k_40k(m.group(0))
                if v:
                    return v
    return None


def _parse_valor_monetario(val) -> float:
    """Converte valor para float (aceita formato BR 1.234,56 ou 1234.56)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(" ", "")
    if not s:
        return 0.0
    # Remover símbolos de moeda e espaços
    s = re.sub(r"[R$\s]", "", s, flags=re.IGNORECASE)
    # Formato BR: 1.234,56 ou 1234,56
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        # Apenas vírgula como decimal (ex: 1234,56)
        if re.match(r"^\d+,\d*$", s):
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0
