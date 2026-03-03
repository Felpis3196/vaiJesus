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
    """Agrupa documentos em chunks que não excedam max_chars_per_chunk."""
    chunks: List[List[Dict[str, str]]] = []
    current: List[Dict[str, str]] = []
    current_len = 0
    for doc in document_texts:
        filename = str(doc.get("filename", "documento"))
        text = str(doc.get("text", "")).strip()
        if not text:
            continue
        # Cada doc no prompt vira "### Documento: X\n{text}\n"
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


def _merge_chunk_results(chunk_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Mescla resultados de múltiplos chunks: transacoes, holerites, encargos; metadados do primeiro não vazio."""
    merged: Dict[str, Any] = {
        "condominio_name": None,
        "period_start": None,
        "period_end": None,
        "transacoes": [],
        "saldos": {},
        "holerites": [],
        "encargos": {},
        "errors": [],
    }
    for r in chunk_results:
        if not isinstance(r, dict):
            continue
        merged["transacoes"].extend(_validate_and_normalize_transacoes(r.get("transacoes")))
        merged["holerites"].extend(r.get("holerites") or [])
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
    return merged


def _build_system_prompt() -> str:
    return (
        "Você é um analista contábil. Sua tarefa é extrair dados financeiros de documentos de prestação de contas de condomínios. "
        "Extraia apenas o que houver evidência clara no texto. Não invente valores nem datas. "
        "Quando não puder extrair um campo, use null. "
        "Retorne sempre um único JSON válido, sem texto antes ou depois."
    )


def _build_user_prompt(chunk_docs: List[Dict[str, str]], schema_text: str) -> str:
    parts = [
        "Extraia do(s) documento(s) abaixo:",
        "1. Nome do condomínio (condominio_name), se aparecer.",
        "2. Período de referência (period_start e period_end no formato YYYY-MM-DD), se aparecer.",
        "3. Todas as transações (receitas e despesas) com: data, descrição, tipo (receita ou despesa), valor numérico. Inclua categoria quando identificável e source_file com o nome do arquivo de origem.",
        "4. Saldo anterior e saldo final (saldos.saldo_anterior, saldos.saldo_final), se aparecerem.",
        "5. Holerites (lista com source_file, periodo, funcionario, salario_bruto, descontos, salario_liquido), se houver.",
        "6. Encargos (fgts, inss, irrf, pis, iss) com valor_pago, periodo, documento, confidence, se houver.",
        "",
        "Schema do JSON de resposta:",
        schema_text,
        "",
        "Documentos:",
    ]
    for doc in chunk_docs:
        parts.append(f"### Documento: {doc.get('filename', 'documento')}")
        parts.append(doc.get("text", ""))
        parts.append("")
    return "\n".join(parts)


def _get_schema_text() -> str:
    return '''{
  "condominio_name": "string ou null",
  "period_start": "YYYY-MM-DD ou null",
  "period_end": "YYYY-MM-DD ou null",
  "transacoes": [
    { "data": "YYYY-MM-DD", "descricao": "string", "tipo": "receita ou despesa", "valor": número, "categoria": "string ou null", "source_file": "string ou null" }
  ],
  "saldos": { "saldo_anterior": número ou null, "saldo_final": número ou null },
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
    from services.llm_client import chat_completion, is_llm_available

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
            raw = json.loads(content or "{}")
            chunk_results.append(raw)
        except Exception as e:
            msg = f"Chunk {i + 1}/{len(chunks)}: {e}"
            logger.warning("LLM document extraction chunk failed: %s", msg)
            all_errors.append(msg)
            chunk_results.append({"transacoes": [], "errors": [msg]})

    merged = _merge_chunk_results(chunk_results)
    transacoes = merged["transacoes"]
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
        "success": len(transacoes) > 0 or bool(merged.get("holerites")),
        "transacoes": transacoes,
        "condominio_name": merged.get("condominio_name"),
        "period_start": merged.get("period_start"),
        "period_end": merged.get("period_end"),
        "saldos": saldos,
        "holerites": merged.get("holerites") or [],
        "encargos": merged.get("encargos") or {},
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
    if extraction_result.get("holerites"):
        doc_ctx["holerites_from_llm"] = extraction_result["holerites"]
    return df, doc_ctx
