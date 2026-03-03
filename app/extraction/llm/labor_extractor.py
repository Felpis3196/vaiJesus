import json
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _truncate_text(text: str, max_chars: int = 12000) -> str:
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    head = text[: int(max_chars * 0.7)]
    tail = text[-int(max_chars * 0.2) :]
    return head + "\n...\n" + tail


def _normalize_llm_response(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    data.setdefault("holerites", [])
    data.setdefault("encargos", {})
    data.setdefault("evidencias", [])
    return data


def extract_labor_data_from_docs(
    document_texts: List[Dict[str, str]],
    model: Optional[str] = None,
    max_chars: int = 12000,
    max_tokens: int = 1200,
    timeout: int = 30,
) -> Dict[str, Any]:
    from .client import is_llm_available, get_default_model, chat_completion

    if not is_llm_available():
        return {
            "enabled": False,
            "reason": "missing_llm_config",
            "holerites": [],
            "encargos": {},
            "errors": ["Nenhuma LLM configurada (defina LLM_BASE_URL ou OPENAI_API_KEY)."],
        }

    if not document_texts:
        return {
            "enabled": False,
            "reason": "no_document_texts",
            "holerites": [],
            "encargos": {},
            "errors": ["No document text available for LLM extraction"],
        }

    model_name = model or get_default_model()
    max_tokens_env = os.getenv("OPENAI_MAX_TOKENS") or os.getenv("LLM_EXTRACTION_MAX_TOKENS")
    timeout_env = os.getenv("OPENAI_TIMEOUT") or os.getenv("LLM_EXTRACTION_TIMEOUT")
    if max_tokens_env and str(max_tokens_env).strip().isdigit():
        max_tokens = int(max_tokens_env)
    if timeout_env and str(timeout_env).strip().isdigit():
        timeout = int(timeout_env)

    prepared_docs: List[str] = []
    for doc in document_texts:
        filename = str(doc.get("filename", "documento"))
        text = _truncate_text(str(doc.get("text", "")), max_chars=max_chars)
        if not text:
            continue
        prepared_docs.append(f"### Documento: {filename}\n{text}")

    if not prepared_docs:
        return {
            "enabled": False,
            "reason": "no_text_after_truncate",
            "holerites": [],
            "encargos": {},
            "errors": ["All document texts were empty after truncation"],
        }

    system_prompt = (
        "Voce e um analista contabila. Extraia dados apenas quando houver evidencias claras. "
        "Retorne JSON valido seguindo o esquema solicitado. "
        "Se algum campo nao puder ser extraido, use null. Nao invente dados."
    )
    user_prompt = (
        "Extraia informacoes de holerites e encargos (FGTS, INSS, IRRF, PIS, ISS) dos documentos.\n"
        "Responda no seguinte JSON:\n"
        "{\n"
        '  "holerites": [\n'
        '    {\n'
        '      "source_file": "nome do arquivo",\n'
        '      "periodo": "YYYY-MM ou texto do periodo",\n'
        '      "funcionario": "nome do funcionario",\n'
        '      "salario_bruto": 0.0,\n'
        '      "descontos": 0.0,\n'
        '      "salario_liquido": 0.0,\n'
        '      "observacoes": "texto curto",\n'
        '      "confidence": 0.0\n'
        "    }\n"
        "  ],\n"
        '  "encargos": {\n'
        '    "fgts": {"valor_pago": 0.0, "periodo": "YYYY-MM", "documento": "arquivo", "confidence": 0.0},\n'
        '    "inss": {"valor_pago": 0.0, "periodo": "YYYY-MM", "documento": "arquivo", "confidence": 0.0},\n'
        '    "irrf": {"valor_pago": 0.0, "periodo": "YYYY-MM", "documento": "arquivo", "confidence": 0.0},\n'
        '    "pis": {"valor_pago": 0.0, "periodo": "YYYY-MM", "documento": "arquivo", "confidence": 0.0},\n'
        '    "iss": {"valor_pago": 0.0, "periodo": "YYYY-MM", "documento": "arquivo", "confidence": 0.0}\n'
        "  },\n"
        '  "evidencias": [\n'
        '    {"source_file": "arquivo", "trecho": "trecho curto do documento"}\n'
        "  ]\n"
        "}\n\n"
        "Documentos:\n"
        + "\n\n".join(prepared_docs)
    )

    try:
        content = chat_completion(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            model=model_name,
            max_tokens=max_tokens,
            temperature=0,
            timeout=timeout,
            retries=2,
            response_format={"type": "json_object"},
        )
        data = _normalize_llm_response(json.loads(content or "{}"))
        data["enabled"] = True
        data["model"] = model_name
        data["errors"] = []
        return data
    except Exception as e:
        logger.warning(f"LLM extraction failed: {e}")
        return {
            "enabled": False,
            "reason": "llm_error",
            "holerites": [],
            "encargos": {},
            "errors": [str(e)],
        }


def should_trigger_llm(labor_analysis: Optional[Dict[str, Any]]) -> bool:
    if not labor_analysis:
        return True
    base = labor_analysis.get("base_calculo", {})
    folha_total = float(base.get("folha_pagamento_total", 0) or 0)
    transacoes_folha = base.get("transacoes_folha") or []
    missing_holerite = folha_total <= 0 and len(transacoes_folha) == 0

    encargos = labor_analysis.get("encargos", {})
    tributos = labor_analysis.get("tributos", {})

    def _missing_item(item: Dict[str, Any]) -> bool:
        try:
            valor_pago = float(item.get("valor_pago", 0) or 0)
        except Exception:
            valor_pago = 0.0
        status = str(item.get("status", "nao_identificado"))
        return valor_pago <= 0 and status in ("nao_identificado", "pendente", "verificar")

    missing_encargos = all(_missing_item(encargos.get(k, {})) for k in ("fgts", "inss", "irrf"))
    missing_tributos = all(_missing_item(tributos.get(k, {})) for k in ("pis", "iss"))

    return bool(missing_holerite or missing_encargos or missing_tributos)


def merge_labor_with_llm(labor_analysis: Dict[str, Any], llm_data: Dict[str, Any]) -> Dict[str, Any]:
    if not labor_analysis:
        labor_analysis = {}
    labor_analysis.setdefault("llm_extractions", {})
    labor_analysis["llm_extractions"] = {
        "enabled": llm_data.get("enabled", False),
        "model": llm_data.get("model"),
        "holerites": llm_data.get("holerites", []),
        "encargos": llm_data.get("encargos", {}),
        "evidencias": llm_data.get("evidencias", []),
    }

    holerites = llm_data.get("holerites", []) or []
    if holerites:
        base = labor_analysis.setdefault("base_calculo", {})
        folha_total = float(base.get("folha_pagamento_total", 0) or 0)
        if folha_total <= 0:
            total = 0.0
            for item in holerites:
                bruto = item.get("salario_bruto")
                liquido = item.get("salario_liquido")
                try:
                    total += float(bruto) if bruto is not None else float(liquido or 0)
                except Exception:
                    continue
            if total > 0:
                base["folha_pagamento_total"] = round(total, 2)
                base["inclui_adiantamento"] = base.get("inclui_adiantamento", False)
                base["transacoes_folha"] = [
                    {
                        "data": "",
                        "descricao": f"Holerite LLM - {item.get('funcionario') or 'Funcionario'}",
                        "valor": float(item.get("salario_bruto") or item.get("salario_liquido") or 0),
                        "tipo": "despesa",
                    }
                    for item in holerites[:20]
                ]

    encargos = labor_analysis.setdefault("encargos", {})
    tributos = labor_analysis.setdefault("tributos", {})
    llm_encargos = llm_data.get("encargos", {}) or {}

    def _merge_item(target: Dict[str, Any], source: Dict[str, Any], label: str):
        try:
            valor_pago = float(source.get("valor_pago", 0) or 0)
        except Exception:
            valor_pago = 0.0
        if valor_pago > 0 and float(target.get("valor_pago", 0) or 0) <= 0:
            target["valor_pago"] = round(valor_pago, 2)
            target["status"] = "identificado_llm"
            target["icon"] = "OK"
            doc = source.get("documento") or "documento"
            target["detalhes"] = f"Identificado via LLM no documento {doc}"

    for key in ("fgts", "inss", "irrf"):
        _merge_item(encargos.setdefault(key, {}), llm_encargos.get(key, {}), key.upper())
    for key in ("pis", "iss"):
        _merge_item(tributos.setdefault(key, {}), llm_encargos.get(key, {}), key.upper())

    if llm_data.get("enabled"):
        labor_analysis["resumo"] = labor_analysis.get(
            "resumo", "Analise de encargos trabalhistas concluida."
        ) + " Complemento por LLM aplicado quando necessario."

    return labor_analysis
