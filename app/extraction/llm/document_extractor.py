"""
Extração de documento completo via LLM local ou OpenAI.
Preenche o contrato: transacoes (DataFrame), document_context (período, saldos, holerites, encargos).
"""
import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Schema esperado da LLM (em português para o prompt)
SCHEMA_TRANSACAO = {
    "data": "YYYY-MM-DD",
    "descricao": "string",
    "tipo": "receita ou despesa",
    "valor": "número",
    "categoria": "string opcional",
    "source_file": "nome do arquivo opcional",
}

# Configuração via ambiente
def _config_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or not str(v).strip().isdigit():
        return default
    return int(v)


def _config_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None:
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _chunk_documents(
    document_texts: List[Dict[str, str]],
    max_chars_per_chunk: int,
) -> List[List[Dict[str, str]]]:
    """Agrupa documentos em chunks. Com 2+ documentos: um chunk por doc (N-1 antes de N). Com 1 doc: por tamanho."""
    if len(document_texts) > 1:
        return [[{"filename": str(d.get("filename", "documento")), "text": str(d.get("text", "")).strip()}] for d in document_texts]
    chunks: List[List[Dict[str, str]]] = []
    current: List[Dict[str, str]] = []
    current_len = 0
    for doc in document_texts:
        filename = str(doc.get("filename", "documento"))
        text = str(doc.get("text", "")).strip()
        if not text:
            continue
        doc_block_len = len(f"### Documento: {filename}\n{text}\n")
        if current_len + doc_block_len > max_chars_per_chunk and current:
            chunks.append(current)
            current = []
            current_len = 0
        current.append({"filename": filename, "text": text})
        current_len += doc_block_len
    if current:
        chunks.append(current)
    return chunks if chunks else []


def _normalize_date(val: Any) -> Optional[str]:
    """Tenta converter valor para YYYY-MM-DD."""
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ("null", "none", ""):
        return None
    # Já ISO
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    # DD/MM/YYYY ou DD-MM-YYYY
    m = re.match(r"^(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})$", s)
    if m:
        d, mo, y = m.groups()
        y = int(y)
        if y < 100:
            y += 2000 if y < 50 else 1900
        try:
            return f"{y:04d}-{int(mo):02d}-{int(d):02d}"
        except ValueError:
            return None
    return None


def _infer_period_from_filename(filename: str) -> Optional[str]:
    """
    Infere período YYYY-MM a partir do nome do arquivo.
    Ex.: 'Prest. Contas - 05_2025_.pdf' -> '2025-05'; '06_2025' -> '2025-06'.
    Procura padrões MM_YYYY, MM-YYYY, MM/YYYY no filename.
    """
    if not filename or not isinstance(filename, str):
        return None
    s = str(filename).strip()
    # MM_YYYY ou M_YYYY (ex.: 05_2025, 5_2025)
    m = re.search(r"(?<!\d)(\d{1,2})_(\d{4})(?!\d)", s)
    if m:
        month, year = int(m.group(1)), int(m.group(2))
        if 1 <= month <= 12 and 2000 <= year <= 2100:
            return f"{year:04d}-{month:02d}"
    # YYYY-MM ou YYYY-MM
    m = re.search(r"(?<!\d)(\d{4})[-_](\d{1,2})(?!\d)", s)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        if 1 <= month <= 12 and 2000 <= year <= 2100:
            return f"{year:04d}-{month:02d}"
    # MM-YYYY ou MM/YYYY
    m = re.search(r"(?<!\d)(\d{1,2})[-/](\d{4})(?!\d)", s)
    if m:
        month, year = int(m.group(1)), int(m.group(2))
        if 1 <= month <= 12 and 2000 <= year <= 2100:
            return f"{year:04d}-{month:02d}"
    return None


def _normalize_period(val: Any) -> Optional[str]:
    """
    Normaliza representações de período (mês/ano) para o formato 'YYYY-MM' sempre que possível.
    Aceita variações como:
      - '2025-05', '2025/5'
      - '05/2025', '5/2025'
      - 'mês 5/2025', 'mes 5 2025'
      - 'maio/2025', 'junho 2025', etc.
    Retorna None quando não for possível normalizar.
    """
    if val is None:
        return None
    s = str(val).strip().lower()
    if not s:
        return None

    # yyyy-mm ou yyyy/m
    m = re.search(r"(\d{4})[-/](\d{1,2})", s)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        if 1 <= month <= 12:
            return f"{year:04d}-{month:02d}"

    # mm/yyyy ou m/yyyy
    m = re.search(r"(\d{1,2})[-/](\d{4})", s)
    if m:
        month = int(m.group(1))
        year = int(m.group(2))
        if 1 <= month <= 12:
            return f"{year:04d}-{month:02d}"

    # 'mes 5/2025', 'mês 5 2025', etc.
    m = re.search(r"m[eê]s\s*(\d{1,2})[^\d]?(\d{4})?", s)
    if m:
        month = int(m.group(1))
        year_group = m.group(2)
        if 1 <= month <= 12:
            if year_group:
                year = int(year_group)
                return f"{year:04d}-{month:02d}"
            # Sem ano definido – ainda assim normaliza mês para ordenação relativa
            return f"0000-{month:02d}"

    # Nomes de meses em português, ex.: "maio/2025", "junho 2025"
    MESES = {
        "janeiro": 1,
        "fevereiro": 2,
        "marco": 3,
        "março": 3,
        "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        "agosto": 8,
        "setembro": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
    }
    for nome, mes in MESES.items():
        if nome in s:
            ano_match = re.search(r"(\d{4})", s)
            if ano_match:
                year = int(ano_match.group(1))
                return f"{year:04d}-{mes:02d}"
            # Sem ano explícito: usar ano 0000 para manter ordenação relativa
            return f"0000-{mes:02d}"

    return None


def _normalize_valor(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        try:
            f = float(val)
            return round(f, 2) if not (f != f) else None
        except (ValueError, TypeError):
            return None
    s = str(val).strip().replace("R$", "").replace("$", "").strip()
    s = s.replace(".", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except ValueError:
        return None


def _normalize_tipo(tipo: Any) -> str:
    if tipo is None:
        return "despesa"
    t = str(tipo).strip().lower()
    if t in ("receita", "credito", "crédito"):
        return "receita"
    return "despesa"


def _validate_and_normalize_transacoes(transacoes: Any) -> List[Dict[str, Any]]:
    """Garante lista de dicts com data, descricao, tipo, valor; descarta inválidos."""
    if not isinstance(transacoes, list):
        return []
    out = []
    for row in transacoes:
        if not isinstance(row, dict):
            continue
        valor = _normalize_valor(row.get("valor"))
        if valor is None:
            continue
        tipo = _normalize_tipo(row.get("tipo"))
        data = _normalize_date(row.get("data"))
        if not data:
            data = datetime.now().strftime("%Y-%m-%d")
        descricao = str(row.get("descricao") or "").strip() or "Sem descrição"
        out.append({
            "data": data,
            "descricao": descricao,
            "tipo": tipo,
            "valor": valor,
            "categoria": str(row.get("categoria") or "").strip() or None,
            "source_file": str(row.get("source_file") or "").strip() or None,
        })
    return out


def _parse_llm_json(content: str) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Extrai e parseia JSON da resposta da LLM (pode vir com markdown ou texto ao redor).
    Returns:
        (parsed_dict, None) em sucesso; ({}, error_msg) em falha.
    """
    if not content or not isinstance(content, str):
        return {}, "Resposta vazia ou inválida"
    text = content.strip()
    # Remover bloco ```json ... ``` ou ``` ... ```
    if "```" in text:
        first = text.find("```")
        rest = text[first + 3 :].strip()
        if rest.lower().startswith("json"):
            rest = rest[4:].strip()
        end_block = rest.find("```")
        if end_block >= 0:
            rest = rest[:end_block].strip()
        if rest.startswith("{"):
            text = rest
    # Se ainda não começa com {, tentar extrair primeiro { até último }
    if not text.startswith("{"):
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            text = text[start : end + 1]
        else:
            return {}, "Nenhum objeto JSON encontrado na resposta"
    try:
        return json.loads(text), None
    except json.JSONDecodeError as e:
        snippet = (content[:300] + "..." if len(content) > 300 else content)
        logger.warning(
            "LLM response JSON parse failed: %s. Snippet: %s",
            e,
            snippet.replace("\n", " "),
        )
        return {}, f"JSON inválido: {e}"


def _normalize_raw_chunk(raw: Dict[str, Any]) -> None:
    """
    Normaliza chaves alternativas no resultado do chunk para o formato esperado.
    Modifica raw in-place: transactions -> transacoes; data (lista de transações) -> transacoes.
    """
    if not isinstance(raw, dict):
        return
    # Chaves alternativas em inglês / variações comuns
    if "totais_por_periodo" not in raw and isinstance(raw.get("totals_by_period"), list):
        raw["totais_por_periodo"] = raw.get("totals_by_period")
    if "contas" not in raw and isinstance(raw.get("accounts"), list):
        raw["contas"] = raw.get("accounts")

    if "transacoes" not in raw or not raw["transacoes"]:
        if "transactions" in raw and isinstance(raw["transactions"], list):
            raw["transacoes"] = raw.pop("transactions", [])
    if "transacoes" not in raw or not raw["transacoes"]:
        data = raw.get("data")
        if isinstance(data, list) and data:
            # Verificar se parece lista de transações (tem valor ou descricao/description)
            def looks_like_transaction(item: Any) -> bool:
                if not isinstance(item, dict):
                    return False
                return "valor" in item or "value" in item or "descricao" in item or "description" in item
            if all(looks_like_transaction(x) for x in data[:3]):
                trans = []
                for row in data:
                    if not isinstance(row, dict):
                        continue
                    t = {
                        "data": row.get("data") or row.get("date"),
                        "descricao": row.get("descricao") or row.get("description") or "",
                        "tipo": row.get("tipo") or row.get("type") or "despesa",
                        "valor": row.get("valor") if "valor" in row else row.get("value"),
                    }
                    if t.get("valor") is not None:
                        trans.append(t)
                if trans:
                    raw["transacoes"] = trans


def _merge_chunk_results(
    chunk_results: List[Dict[str, Any]],
    chunks: Optional[List[List[Dict[str, str]]]] = None,
) -> Dict[str, Any]:
    """Mescla resultados de múltiplos chunks: transacoes, holerites, encargos; metadados do primeiro não vazio.
    Se chunks for fornecido, atribui período inferido pelo filename às contas de cada chunk cujo periodo esteja vazio.
    Deduplica contas por (nome normalizado, periodo)."""
    merged: Dict[str, Any] = {
        "estrutura_tipo": None,
        "condominio_name": None,
        "period_start": None,
        "period_end": None,
        "transacoes": [],
        "saldos": {},
        "holerites": [],
        "encargos": {},
        "contas": [],
        "totais_por_periodo": [],
        "errors": [],
    }
    for i, r in enumerate(chunk_results):
        if not isinstance(r, dict):
            continue
        merged["transacoes"].extend(_validate_and_normalize_transacoes(r.get("transacoes")))
        merged["holerites"].extend(r.get("holerites") or [])
        # Contas: atribuir período inferido pelo chunk quando periodo estiver vazio
        chunk_contas = r.get("contas") or []
        if isinstance(chunk_contas, list) and chunks and i < len(chunks):
            chunk_docs = chunks[i]
            periodo_inferido = None
            if chunk_docs and isinstance(chunk_docs[0], dict):
                first_filename = chunk_docs[0].get("filename") or ""
                periodo_inferido = _infer_period_from_filename(first_filename)
                if periodo_inferido:
                    periodo_inferido = _normalize_period(periodo_inferido) or periodo_inferido
            for c in chunk_contas:
                if not isinstance(c, dict):
                    continue
                conta = dict(c)
                if not conta.get("periodo") and periodo_inferido:
                    conta["periodo"] = periodo_inferido
                merged["contas"].append(conta)
        elif isinstance(chunk_contas, list):
            merged["contas"].extend(chunk_contas)
        if isinstance(r.get("totais_por_periodo"), list):
            merged["totais_por_periodo"].extend(r.get("totais_por_periodo") or [])
        enc = r.get("encargos") or {}
        if isinstance(enc, dict):
            for k, v in enc.items():
                if v and (merged["encargos"].get(k) or {}).get("valor_pago", 0) <= 0:
                    merged["encargos"].setdefault(k, {}).update(v if isinstance(v, dict) else {})
        saldos = r.get("saldos") or {}
        if isinstance(saldos, dict):
            if merged["saldos"].get("saldo_anterior") is None and saldos.get("saldo_anterior") is not None:
                merged["saldos"]["saldo_anterior"] = saldos["saldo_anterior"]
            if merged["saldos"].get("saldo_final") is None and saldos.get("saldo_final") is not None:
                merged["saldos"]["saldo_final"] = saldos["saldo_final"]
        if not merged["condominio_name"] and r.get("condominio_name"):
            merged["condominio_name"] = str(r.get("condominio_name")).strip()
        if not merged["period_start"] and r.get("period_start"):
            merged["period_start"] = _normalize_date(r.get("period_start")) or str(r.get("period_start"))[:10]
        if not merged["period_end"] and r.get("period_end"):
            merged["period_end"] = _normalize_date(r.get("period_end")) or str(r.get("period_end"))[:10]
        merged["errors"].extend(r.get("errors") or [])
    for i in range(len(chunk_results) - 1, -1, -1):
        r = chunk_results[i] if isinstance(chunk_results[i], dict) else {}
        if r.get("estrutura_tipo") in ("SIMPLES", "MULTICONTAS", "INDEFINIDA"):
            merged["estrutura_tipo"] = r.get("estrutura_tipo")
            break
    if merged["estrutura_tipo"] is None:
        merged["estrutura_tipo"] = "INDEFINIDA"
    # Totais do período analisado = último chunk (não somar dois meses)
    for i in range(len(chunk_results) - 1, -1, -1):
        r = chunk_results[i] if isinstance(chunk_results[i], dict) else {}
        saldos = r.get("saldos") or {}
        if isinstance(saldos, dict):
            for key in ("total_creditos_mes", "total_debitos_mes"):
                v = saldos.get(key)
                if v is not None:
                    try:
                        merged["saldos"][key] = round(float(v), 2)
                    except (TypeError, ValueError):
                        pass
            if merged["saldos"].get("total_creditos_mes") is not None or merged["saldos"].get("total_debitos_mes") is not None:
                break
    # Deduplicar transações por (data, valor, descrição) para evitar lançamentos duplicados da LLM
    if merged["transacoes"]:
        seen_keys: set = set()
        deduped_trans: List[Dict[str, Any]] = []
        for t in merged["transacoes"]:
            if not isinstance(t, dict):
                continue
            data = str(t.get("data") or "")[:10]
            try:
                val = round(float(t.get("valor", 0)), 2)
            except (TypeError, ValueError):
                val = 0.0
            desc = (str(t.get("descricao") or "").strip().lower())[:200]
            key = (data, val, desc)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped_trans.append(t)
        merged["transacoes"] = deduped_trans
    # Saldo anterior do período analisado = saldo final do primeiro chunk (mês N-1)
    if len(chunk_results) >= 2:
        first_saldos = chunk_results[0].get("saldos") or {} if isinstance(chunk_results[0], dict) else {}
        saldo_final_n1 = first_saldos.get("saldo_final")
        if saldo_final_n1 is not None:
            try:
                merged["saldos"]["saldo_anterior"] = round(float(saldo_final_n1), 2)
            except (TypeError, ValueError):
                pass
    # Deduplicar contas por (nome normalizado, periodo): manter primeira ocorrência
    if merged["contas"]:
        seen: set = set()
        deduped: List[Dict[str, Any]] = []
        for c in merged["contas"]:
            if not isinstance(c, dict):
                continue
            nome = str(c.get("nome") or "").strip().lower()
            periodo = str(c.get("periodo") or "").strip() or "periodo_indefinido"
            key = (nome, periodo)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(c)
        merged["contas"] = deduped
    return merged


def _build_system_prompt() -> str:
    return (
        "Você é um analista contábil. Sua tarefa é extrair dados financeiros de documentos de prestação de contas de condomínios. "
        "Extraia SOMENTE dados explicitamente presentes no texto. Não invente valores, datas nem categorias. "
        "Nunca crie contas cujo nome não exista no texto (ex.: Lavanderia, Fundo de reserva, Espaço festa, Conta ordinária somente se aparecerem literalmente). "
        "Se o documento tiver apenas débito, crédito e saldo (sem nomes de subcontas), retorne estrutura_tipo: SIMPLES e uma única conta com nome 'Conta Única' e os valores de saldos. "
        "Quando não puder extrair um campo, use null ou []/{} conforme o tipo. "
        "Sua resposta deve ser APENAS um único objeto JSON válido, sem texto antes ou depois, sem markdown. "
        "Use exatamente estas chaves no JSON: estrutura_tipo, condominio_name, period_start, period_end, transacoes, saldos, holerites, encargos, contas, totais_por_periodo, errors. "
        "Não responda com descrições de schema nem com chaves como 'key' ou 'type'; extraia os dados reais do documento. "
        "Inclua cada transação apenas uma vez; não duplique linhas com mesma data, valor e descrição."
    )


def _build_user_prompt(chunk_docs: List[Dict[str, str]], schema_text: str) -> str:
    parts = [
        "Extraia do(s) documento(s) abaixo:",
        "1. Nome do condomínio (condominio_name), se aparecer.",
        "2. Período de referência (period_start e period_end no formato YYYY-MM-DD), se aparecer.",
        "3. Todas as transações (receitas e despesas) com: data, descrição, tipo (receita ou despesa), valor numérico. Inclua categoria quando identificável e source_file com o nome do arquivo de origem.",
        "4. Saldo anterior e saldo final (saldos.saldo_anterior, saldos.saldo_final). Se o documento tiver um resumo ou 'Total geral' com 'Créditos no mês' e 'Débitos no mês', preencha também saldos.total_creditos_mes e saldos.total_debitos_mes com esses valores numéricos.",
        "5. Holerites (lista com source_file, periodo, funcionario, salario_bruto, descontos, salario_liquido), se houver.",
        "6. Encargos (fgts, inss, irrf, pis, iss) com valor_pago, periodo, documento, confidence, se houver.",
        "7. Classifique o documento em estrutura_tipo:",
        "   - SIMPLES: o texto menciona apenas Débito, Crédito, Saldo (Saldo final, Saldo anterior) e NÃO contém nomes de subcontas (Lavanderia, Fundo de reserva, Espaço festa, Conta ordinária). Nesse caso preencha 'contas' com UM ÚNICO item: nome = 'Conta Única', saldo_final = saldo do documento, periodo = YYYY-MM. Use saldos.saldo_final e saldos.total_creditos_mes/total_debitos_mes quando existirem. NÃO crie Conta ordinária, Fundo de reserva, Espaço festa ou Lavanderia se esses nomes não estiverem no texto.",
        "   - MULTICONTAS: o texto contém nomes literais de contas/subcontas. Liste em 'contas' APENAS as contas cujo nome aparece literalmente no documento. Preencha totais_por_periodo com periodo (YYYY-MM), total, conta_consolidada e alerta_conta_ordinaria_negativa apenas quando a Conta ordinária estiver no texto e negativa.",
        "   - INDEFINIDA: não foi possível classificar com segurança.",
        "   Nunca invente nomes de contas. Se o documento for simples (só débito/crédito/saldo), use sempre estrutura_tipo SIMPLES e uma única conta 'Conta Única'.",
        "",
        "Estrutura obrigatória do JSON (use exatamente estas chaves):",
        schema_text,
        "",
        "Documentos:",
    ]
    for doc in chunk_docs:
        parts.append(f"### Documento: {doc.get('filename', 'documento')}")
        parts.append(doc.get("text", ""))
        parts.append("")
    parts.append(
        "Responda APENAS com o objeto JSON (chaves: estrutura_tipo, condominio_name, period_start, period_end, transacoes, saldos, holerites, encargos, contas, totais_por_periodo, errors). "
        "Sem markdown, sem texto antes ou depois. Se não houver dados para algum campo, use [] ou {} ou null conforme o tipo."
    )
    parts.append(
        "Exemplo SIMPLES (documento só com débito/crédito/saldo): "
        "{\"estrutura_tipo\": \"SIMPLES\", \"condominio_name\": null, \"period_start\": null, \"period_end\": null, "
        "\"transacoes\": [], \"saldos\": {\"saldo_anterior\": null, \"saldo_final\": -4700.33, \"total_creditos_mes\": 64395.04, \"total_debitos_mes\": 70095.37}, "
        "\"holerites\": [], \"encargos\": {}, "
        "\"contas\": [{\"nome\": \"Conta Única\", \"saldo_final\": -4700.33, \"periodo\": \"2026-01\"}], "
        "\"totais_por_periodo\": [{\"periodo\": \"2026-01\", \"total\": -4700.33, \"conta_consolidada\": -4700.33, \"alerta_conta_ordinaria_negativa\": false}], "
        "\"errors\": []}"
    )
    parts.append(
        "Exemplo MULTICONTAS (somente quando o documento listar explicitamente contas por nome): "
        "Use contas com os nomes que aparecem no texto. Não use Conta ordinária, Fundo de reserva, Espaço festa ou Lavanderia se não aparecerem no documento."
    )
    return "\n".join(parts)


def _get_schema_text() -> str:
    return '''{
  "estrutura_tipo": "SIMPLES | MULTICONTAS | INDEFINIDA",
  "condominio_name": "string ou null",
  "period_start": "YYYY-MM-DD ou null",
  "period_end": "YYYY-MM-DD ou null",
  "transacoes": [
    { "data": "YYYY-MM-DD", "descricao": "string", "tipo": "receita ou despesa", "valor": número, "categoria": "string ou null", "source_file": "string ou null" }
  ],
  "saldos": { "saldo_anterior": número ou null, "saldo_final": número ou null, "total_creditos_mes": número ou null, "total_debitos_mes": número ou null },
  "holerites": [
    { "source_file": "string", "periodo": "string", "funcionario": "string", "salario_bruto": número, "descontos": número, "salario_liquido": número, "confidence": número }
  ],
  "encargos": {
    "fgts": { "valor_pago": número, "periodo": "string", "documento": "string", "confidence": número },
    "inss": { "valor_pago": número, "periodo": "string", "documento": "string", "confidence": número },
    "irrf": { "valor_pago": número, "periodo": "string", "documento": "string", "confidence": número },
    "pis": { "valor_pago": número, "periodo": "string", "documento": "string", "confidence": número },
    "iss": { "valor_pago": número, "periodo": "string", "documento": "string", "confidence": número }
  },
  "contas": [
    { "nome": "string", "saldo_final": número, "periodo": "string OBRIGATÓRIO formato YYYY-MM (ex.: 2025-05)" }
  ],
  "totais_por_periodo": [
    { "periodo": "string YYYY-MM", "total": número, "conta_consolidada": número, "alerta_conta_ordinaria_negativa": boolean }
  ],
  "errors": []
}'''


def extract(
    document_texts: List[Dict[str, str]],
    *,
    model: Optional[str] = None,
    max_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    timeout: Optional[int] = None,
    max_chars_per_chunk: Optional[int] = None,
    retries: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Extrai transações, saldos, holerites e encargos dos textos via LLM.

    Args:
        document_texts: Lista de {"filename": str, "text": str}
        model, max_tokens, temperature, timeout, max_chars_per_chunk, retries: opcionais (senão usa env)

    Returns:
        {
            "success": bool,
            "transacoes": list[dict],
            "condominio_name": str | None,
            "period_start": str | None,
            "period_end": str | None,
            "saldos": {"saldo_anterior": float?, "saldo_final": float?},
            "holerites": list,
            "encargos": dict,
            "errors": list,
            "confidence": str ("high"|"medium"|"low"),
        }
    """
    from .client import chat_completion, is_llm_available

    if not is_llm_available():
        return {
            "success": False,
            "transacoes": [],
            "condominio_name": None,
            "period_start": None,
            "period_end": None,
            "saldos": {},
            "holerites": [],
            "encargos": {},
            "errors": ["LLM não configurada (defina LLM_BASE_URL ou OPENAI_API_KEY)."],
            "confidence": "low",
        }
    if not document_texts:
        return {
            "success": False,
            "transacoes": [],
            "condominio_name": None,
            "period_start": None,
            "period_end": None,
            "saldos": {},
            "holerites": [],
            "encargos": {},
            "errors": ["Nenhum texto de documento fornecido."],
            "confidence": "low",
        }

    # Ordenar por período inferido (filename) para que N-1 venha antes de N
    document_texts = sorted(
        document_texts,
        key=lambda d: (_infer_period_from_filename(d.get("filename")) or "9999-12", d.get("filename") or ""),
    )

    max_tok = max_tokens if max_tokens is not None else _config_int("LLM_EXTRACTION_MAX_TOKENS", 8192)
    temp = temperature if temperature is not None else _config_float("LLM_EXTRACTION_TEMPERATURE", 0.0)
    timeo = timeout if timeout is not None else _config_int("LLM_EXTRACTION_TIMEOUT", 120)
    max_chars = max_chars_per_chunk if max_chars_per_chunk is not None else _config_int("LLM_EXTRACTION_MAX_CHARS_PER_CHUNK", 100000)
    retr = retries if retries is not None else _config_int("LLM_EXTRACTION_RETRIES", 2)

    chunks = _chunk_documents(document_texts, max_chars)
    schema_text = _get_schema_text()
    chunk_results: List[Dict[str, Any]] = []
    all_errors: List[str] = []

    for i, chunk_docs in enumerate(chunks):
        try:
            messages = [
                {"role": "system", "content": _build_system_prompt()},
                {"role": "user", "content": _build_user_prompt(chunk_docs, schema_text)},
            ]
            content = chat_completion(
                messages,
                model=model,
                max_tokens=max_tok,
                temperature=temp,
                timeout=timeo,
                retries=retr,
                response_format={"type": "json_object"},
            )
            raw, parse_err = _parse_llm_json(content or "")
            if parse_err:
                msg = f"Chunk {i + 1}/{len(chunks)}: {parse_err}"
                logger.warning("LLM document extraction chunk failed: %s", msg)
                all_errors.append(msg)
                chunk_results.append({"transacoes": [], "errors": [msg]})
            else:
                _normalize_raw_chunk(raw)
                chunk_results.append(raw)
        except Exception as e:
            msg = f"Chunk {i + 1}/{len(chunks)}: {e}"
            logger.warning("LLM document extraction chunk failed: %s", msg)
            all_errors.append(msg)
            chunk_results.append({"transacoes": [], "errors": [msg]})

    # Diagnóstico: quando todos os chunks resultam em transações vazias, logar estrutura
    all_empty = all(
        not (r.get("transacoes") or r.get("holerites"))
        for r in chunk_results
        if isinstance(r, dict)
    )
    if all_empty and chunk_results:
        first = next((r for r in chunk_results if isinstance(r, dict)), None)
        if first:
            keys_info = {k: type(v).__name__ for k, v in first.items()}
            logger.info(
                "LLM extraction: all %s chunk(s) have no transacoes/holerites. First chunk keys: %s",
                len(chunk_results),
                keys_info,
            )
            # Se a resposta não tem a estrutura esperada (ex.: modelo retornou "key"/"type"), logar amostra
            expected_keys = {"transacoes", "transactions", "data", "saldos", "holerites"}
            if not (expected_keys & set(first.keys())):
                try:
                    sample = json.dumps(first, ensure_ascii=False)[:400]
                    logger.warning(
                        "LLM returned unexpected structure (missing transacoes/saldos). Sample: %s",
                        sample,
                    )
                except Exception:
                    pass

    merged = _merge_chunk_results(chunk_results, chunks=chunks)
    transacoes = merged["transacoes"]
    contas = merged.get("contas") or []
    totais_por_periodo = merged.get("totais_por_periodo") or []
    saldos = merged.get("saldos") or {}
    if isinstance(saldos, dict):
        for k in ("saldo_anterior", "saldo_final"):
            v = saldos.get(k)
            if v is not None:
                try:
                    saldos[k] = round(float(v), 2)
                except (TypeError, ValueError):
                    saldos[k] = None

    # Confiança simples
    confidence = "high"
    if not transacoes and not merged.get("holerites"):
        confidence = "low"
    elif len(all_errors) > 0 or merged.get("errors"):
        confidence = "medium"

    return {
        "success": (
            len(transacoes) > 0
            or bool(merged.get("holerites"))
            or (isinstance(contas, list) and len(contas) > 0)
            or (isinstance(totais_por_periodo, list) and len(totais_por_periodo) > 0)
        ),
        "estrutura_tipo": merged.get("estrutura_tipo") or "INDEFINIDA",
        "transacoes": transacoes,
        "condominio_name": merged.get("condominio_name"),
        "period_start": merged.get("period_start"),
        "period_end": merged.get("period_end"),
        "saldos": saldos,
        "holerites": merged.get("holerites") or [],
        "encargos": merged.get("encargos") or {},
        "contas": contas if isinstance(contas, list) else [],
        "totais_por_periodo": totais_por_periodo if isinstance(totais_por_periodo, list) else [],
        "errors": all_errors + (merged.get("errors") or []),
        "confidence": confidence,
    }


def build_dataframe_and_context(
    extraction_result: Dict[str, Any],
    document_texts: List[Dict[str, str]],
) -> Tuple[Any, Dict[str, Any]]:
    """
    Converte o resultado de extract() em (DataFrame, document_context) para o pipeline.

    Returns:
        (pd.DataFrame com colunas data, descricao, tipo, valor, categoria?, source_file?,
         document_context dict com period_start, period_end, condominio_name, saldos, etc.)
    """
    import pandas as pd

    transacoes = extraction_result.get("transacoes") or []
    contas = extraction_result.get("contas") or []
    totais_por_periodo = extraction_result.get("totais_por_periodo") or []
    if not transacoes:
        df = pd.DataFrame(columns=["data", "descricao", "tipo", "valor"])
    else:
        df = pd.DataFrame(transacoes)
        for col in ("data", "descricao", "tipo", "valor"):
            if col not in df.columns:
                df[col] = "" if col != "valor" else 0.0
        if "categoria" not in df.columns:
            df["categoria"] = None
        if "source_file" not in df.columns:
            df["source_file"] = ""

    doc_ctx: Dict[str, Any] = {
        "total_files": len(document_texts),
        "has_financial_data": len(df) > 0,
        "document_texts": document_texts,
        "file_metadata": [],
        "llm_extraction": True,
    }
    if extraction_result.get("period_start"):
        doc_ctx["period_start"] = extraction_result["period_start"]
    if extraction_result.get("period_end"):
        doc_ctx["period_end"] = extraction_result["period_end"]
    if extraction_result.get("condominio_name"):
        doc_ctx["condominio_name"] = extraction_result["condominio_name"]
    saldos = extraction_result.get("saldos") or {}
    if saldos.get("saldo_anterior") is not None:
        doc_ctx["saldo_anterior"] = saldos["saldo_anterior"]
    if saldos.get("saldo_final") is not None:
        doc_ctx["saldo_final"] = saldos["saldo_final"]
    estrutura_tipo = extraction_result.get("estrutura_tipo") or "INDEFINIDA"
    # Safeguard: se o texto não contém nomes de multicontas, tratar como SIMPLES
    text_concatenated = " ".join(
        (d.get("text") or "") for d in document_texts if isinstance(d, dict)
    ).lower()
    multicontas_keywords = ("lavanderia", "fundo de reserva", "espaço festa", "conta ordinária")
    text_has_multicontas = any(kw in text_concatenated for kw in multicontas_keywords)
    if estrutura_tipo == "SIMPLES" or not text_has_multicontas:
        estrutura_tipo = "SIMPLES"
        periodo_doc = None
        if isinstance(totais_por_periodo, list) and totais_por_periodo:
            last_t = sorted(totais_por_periodo, key=lambda x: str(x.get("periodo") or ""))[-1]
            periodo_doc = _normalize_period(last_t.get("periodo")) or str(last_t.get("periodo") or "").strip() or None
        if not periodo_doc and extraction_result.get("period_end"):
            pe = extraction_result["period_end"]
            if isinstance(pe, str) and len(pe) >= 7:
                periodo_doc = pe[:7]
        if not periodo_doc:
            periodo_doc = "periodo_indefinido"
        saldo_final_val = saldos.get("saldo_final")
        if saldo_final_val is None and isinstance(totais_por_periodo, list) and totais_por_periodo:
            last_t = sorted(totais_por_periodo, key=lambda x: str(x.get("periodo") or ""))[-1]
            total_val = last_t.get("total") or last_t.get("conta_consolidada")
            if total_val is not None:
                try:
                    saldo_final_val = round(float(total_val), 2)
                except (TypeError, ValueError):
                    saldo_final_val = None
        if saldo_final_val is not None:
            try:
                saldo_final_val = round(float(saldo_final_val), 2)
            except (TypeError, ValueError):
                saldo_final_val = None
        contas = [{"nome": "Conta Única", "saldo_final": saldo_final_val, "periodo": periodo_doc}]
    doc_ctx["estrutura_tipo"] = estrutura_tipo
    if extraction_result.get("holerites"):
        doc_ctx["holerites_from_llm"] = extraction_result["holerites"]
    if extraction_result.get("encargos") and isinstance(extraction_result["encargos"], dict):
        doc_ctx["encargos_from_llm"] = extraction_result["encargos"]
    if isinstance(contas, list) and contas:
        doc_ctx["contas_from_llm"] = contas
    if isinstance(totais_por_periodo, list) and totais_por_periodo:
        doc_ctx["totais_por_periodo_from_llm"] = totais_por_periodo

    # Preencher totals_extracted para o consolidator (contrato do pipeline)
    def _to_float(v: Any) -> Optional[float]:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            try:
                return round(float(v), 2)
            except (TypeError, ValueError):
                return None
        s = str(v).strip().replace("R$", "").replace("$", "").strip()
        if not s:
            return None
        # aceitar BR e US
        if re.search(r"\d+,\d{2}$", s):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
        try:
            return round(float(s), 2)
        except ValueError:
            return None

    # Preferir totals_por_periodo (quando documentos trazem contas por mês e total consolidado)
    extracted_saldo_final: Optional[float] = None
    extracted_saldo_anterior: Optional[float] = None
    extracted_total_receitas: Optional[float] = None
    extracted_total_despesas: Optional[float] = None
    extracted_period: Optional[str] = None

    if isinstance(totais_por_periodo, list) and totais_por_periodo:
        # escolher o último item com base no periodo normalizado; fallback para último da lista
        def _period_sort_key(p: Any) -> Tuple[int, int, int]:
            norm = _normalize_period(p)
            if norm:
                try:
                    year_str, month_str = norm.split("-")
                    year = int(year_str)
                    month = int(month_str)
                    return (year, month, 1)
                except Exception:
                    pass
            # Fallback simples: tentar extrair ano e mês de qualquer forma
            s = str(p or "").strip().lower()
            m_year = re.search(r"(\d{4})", s)
            year = int(m_year.group(1)) if m_year else 0
            m_month = re.search(r"(\d{1,2})", s)
            month = int(m_month.group(1)) if m_month else 0
            return (year, month, 0)

        valid_items = [x for x in totais_por_periodo if isinstance(x, dict)]
        if valid_items:
            chosen = sorted(valid_items, key=lambda x: _period_sort_key(x.get("periodo")))[-1]
            extracted_saldo_final = _to_float(chosen.get("conta_consolidada"))
            if extracted_saldo_final is None:
                extracted_saldo_final = _to_float(chosen.get("total"))
            chosen_period_raw = chosen.get("periodo")
            extracted_period = _normalize_period(chosen_period_raw) or (str(chosen_period_raw).strip() or None)

    saldos = extraction_result.get("saldos") or {}
    saldo_anterior = None
    if isinstance(saldos, dict) and saldos.get("saldo_anterior") is not None:
        try:
            saldo_anterior = round(float(saldos["saldo_anterior"]), 2)
        except (TypeError, ValueError):
            pass
    saldo_final = None
    if isinstance(saldos, dict) and saldos.get("saldo_final") is not None:
        try:
            saldo_final = round(float(saldos["saldo_final"]), 2)
        except (TypeError, ValueError):
            pass
    extracted_saldo_anterior = saldo_anterior
    # se totals_por_periodo trouxe saldo final consolidado, sobrescrever saldo_final no contexto
    if extracted_saldo_final is not None:
        saldo_final = extracted_saldo_final
        doc_ctx["saldo_final"] = extracted_saldo_final
    values = {}
    # Preferir totais explícitos do documento (Total geral / Créditos-Débitos no mês) quando presentes
    saldos_for_totals = extraction_result.get("saldos") or {}
    if isinstance(saldos_for_totals, dict):
        cred_explicit = _to_float(saldos_for_totals.get("total_creditos_mes"))
        deb_explicit = _to_float(saldos_for_totals.get("total_debitos_mes"))
        if cred_explicit is not None:
            extracted_total_receitas = cred_explicit
            values["total_receitas"] = extracted_total_receitas
        if deb_explicit is not None:
            extracted_total_despesas = deb_explicit
            values["total_despesas"] = extracted_total_despesas
    # Fallback: total_receitas/total_despesas a partir das transações quando não houver totais explícitos
    if extracted_total_receitas is None or extracted_total_despesas is None:
        if transacoes:
            total_receitas: Optional[float] = None
            total_despesas: Optional[float] = None
            for t in transacoes:
                if not isinstance(t, dict):
                    continue
                valor = _normalize_valor(t.get("valor"))
                if valor is None:
                    continue
                tipo = _normalize_tipo(t.get("tipo"))
                if tipo == "receita":
                    total_receitas = (total_receitas or 0.0) + valor
                else:
                    total_despesas = (total_despesas or 0.0) + valor
            if total_receitas is not None and extracted_total_receitas is None:
                extracted_total_receitas = round(total_receitas, 2)
                values["total_receitas"] = extracted_total_receitas
            if total_despesas is not None and extracted_total_despesas is None:
                extracted_total_despesas = round(total_despesas, 2)
                values["total_despesas"] = extracted_total_despesas
    if saldo_final is not None:
        values["saldo_final"] = saldo_final
    if extracted_saldo_anterior is not None:
        values["saldo_anterior"] = extracted_saldo_anterior
    if extracted_period is not None:
        values["period"] = extracted_period
    if values:
        doc_ctx["totals_extracted"] = {"values": values}

    # Conciliação estrutural via LLM: preencher no formato esperado pelo report_formatter
    if isinstance(contas, list) and contas:
        contas_norm = []
        for c in contas:
            if not isinstance(c, dict):
                continue
            nome = str(c.get("nome") or "").strip()
            if not nome:
                continue
            periodo = str(c.get("periodo") or "").strip() or None
            saldo_c = _to_float(c.get("saldo_final"))
            if saldo_c is None:
                continue
            contas_norm.append({"nome": nome, "saldo_final": saldo_c, "periodo": periodo})

        if contas_norm:
            # Agrupar por período normalizado (quando possível), para permitir exibição multi-período no relatório
            by_period: Dict[str, Dict[str, Any]] = {}
            for c in contas_norm:
                raw_periodo = c.get("periodo")
                norm_periodo = _normalize_period(raw_periodo)
                key = norm_periodo or raw_periodo or "periodo_indefinido"
                entry = by_period.setdefault(
                    key,
                    {"periodo": norm_periodo or raw_periodo or "periodo_indefinido", "rotulo_original": raw_periodo, "contas": []},
                )
                entry["contas"].append(c)

            periods_struct: List[Dict[str, Any]] = []
            for key, entry in by_period.items():
                per_norm = entry.get("periodo")
                rotulo_original = entry.get("rotulo_original")
                contas_list = entry["contas"]
                total_contas = round(sum(x.get("saldo_final") or 0 for x in contas_list), 2)
                saldo_consolidado = None
                if isinstance(totais_por_periodo, list):
                    for t in totais_por_periodo:
                        if not isinstance(t, dict):
                            continue
                        tp_raw = t.get("periodo")
                        tp_norm = _normalize_period(tp_raw)
                        same_period = False
                        if tp_norm and per_norm and tp_norm == per_norm:
                            same_period = True
                        elif (tp_norm is None or per_norm is None) and str(tp_raw).strip() == str(key):
                            same_period = True
                        if same_period:
                            saldo_consolidado = _to_float(t.get("conta_consolidada")) or _to_float(t.get("total"))
                            break
                if saldo_consolidado is None:
                    saldo_consolidado = total_contas

                alertas: List[str] = []
                if isinstance(totais_por_periodo, list):
                    for t in totais_por_periodo:
                        if not isinstance(t, dict):
                            continue
                        tp_raw = t.get("periodo")
                        tp_norm = _normalize_period(tp_raw)
                        same_period = False
                        if tp_norm and per_norm and tp_norm == per_norm:
                            same_period = True
                        elif (tp_norm is None or per_norm is None) and str(tp_raw).strip() == str(key):
                            same_period = True
                        if same_period:
                            if bool(t.get("alerta_conta_ordinaria_negativa")):
                                label = rotulo_original or per_norm or key
                                alertas.append(f"Conta ordinária negativa no período {label}.")
                            break

                periods_struct.append(
                    {
                        "periodo": per_norm or key,
                        "rotulo_original": rotulo_original,
                        "contas": [
                            {
                                "nome": x["nome"],
                                "saldo_final": x["saldo_final"],
                                "confiabilidade": None,
                            }
                            for x in contas_list
                        ],
                        "total_contas": total_contas,
                        "saldo_consolidado": saldo_consolidado,
                        "diferenca": round(abs(total_contas - (saldo_consolidado or 0.0)), 2) if saldo_consolidado is not None else None,
                        "alertas": alertas,
                        "classificacao": "REGULAR"
                        if (saldo_consolidado is not None and abs(total_contas - (saldo_consolidado or 0.0)) <= 0.02)
                        else "SEM_BASE",
                        "justificativa": "Extraído via LLM (contas/subcontas) a partir do documento.",
                        "limitacoes": [],
                        "texto_formatado": None,
                    }
                )

            # Saldo inicial do mês N = saldo final do mês N-1 (após ordenar por período)
            if periods_struct:
                periods_struct_sorted = sorted(
                    periods_struct,
                    key=lambda x: (0 if x.get("periodo") in (None, "periodo_indefinido") else 1, str(x.get("periodo"))),
                )
                for i, p in enumerate(periods_struct_sorted):
                    if i == 0:
                        p["saldo_inicial"] = None  # primeiro período: não há anterior
                    else:
                        prev = periods_struct_sorted[i - 1]
                        p["saldo_inicial"] = prev.get("saldo_consolidado") if prev.get("saldo_consolidado") is not None else prev.get("total_contas")
                doc_ctx["structural_extraction_periods"] = periods_struct_sorted
                doc_ctx["structural_extraction"] = periods_struct_sorted[-1]

    return df, doc_ctx
