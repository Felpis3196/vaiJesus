"""
Cliente LLM agnóstico: suporta LLM local (Ollama, LM Studio, vLLM) ou OpenAI.
Quando LLM_BASE_URL está definido, usa servidor local sem necessidade de API key externa.
"""
import logging
import os
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Chave dummy aceita por servidores locais (Ollama, etc.)
LOCAL_API_KEY = "local"


def _get_client_config() -> Dict[str, Any]:
    """Retorna base_url e api_key para o cliente OpenAI-compatible."""
    base_url = (os.getenv("LLM_BASE_URL") or "").strip()
    if base_url:
        return {"base_url": base_url.rstrip("/"), "api_key": LOCAL_API_KEY}
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if api_key:
        return {"base_url": None, "api_key": api_key}
    return {}


def is_llm_available() -> bool:
    """True se houver LLM configurada (local ou OpenAI)."""
    return bool(_get_client_config())


# Modelos de fallback quando o configurado não está instalado no Ollama (ex.: 404)
OLLAMA_FALLBACK_MODELS = ["llama3.1", "llama3.1:8b", "llama3.1:70b", "llama2"]


def get_default_model() -> str:
    """Modelo padrão: LLM_MODEL para local, OPENAI_MODEL para OpenAI.
    Para extração estruturada com Ollama recomenda-se llama3.2 (ou variante ex.: llama3.2:3b).
    Se o modelo não estiver instalado, use: ollama pull llama3.2"""
    if os.getenv("LLM_BASE_URL"):
        return os.getenv("LLM_MODEL", "llama3.2")
    return os.getenv("OPENAI_MODEL", "gpt-4o-mini")


def chat_completion(
    messages: List[Dict[str, str]],
    *,
    model: Optional[str] = None,
    max_tokens: int = 8192,
    temperature: float = 0.0,
    timeout: int = 120,
    retries: int = 2,
    response_format: Optional[Dict[str, str]] = None,
) -> str:
    """
    Envia mensagens para a LLM (local ou OpenAI) e retorna o conteúdo da resposta.

    Args:
        messages: Lista de {"role": "system"|"user"|"assistant", "content": "..."}
        model: Nome do modelo (default: LLM_MODEL ou OPENAI_MODEL)
        max_tokens: Máximo de tokens na resposta
        temperature: 0 para respostas determinísticas
        timeout: Timeout em segundos
        retries: Número de tentativas com backoff
        response_format: Ex.: {"type": "json_object"} se suportado

    Returns:
        Conteúdo textual da resposta (content do primeiro choice).

    Raises:
        ValueError: Se nenhuma LLM estiver configurada (LLM_BASE_URL ou OPENAI_API_KEY).
        Exception: Erro da API após todas as retries.
    """
    config = _get_client_config()
    if not config:
        raise ValueError(
            "Nenhuma LLM configurada. Defina LLM_BASE_URL (para LLM local) ou OPENAI_API_KEY (para OpenAI)."
        )

    from openai import OpenAI

    client = OpenAI(
        api_key=config["api_key"],
        base_url=config.get("base_url") if config.get("base_url") else None,
        timeout=timeout,
    )
    model_name = model or get_default_model()

    kwargs: Dict[str, Any] = {
        "model": model_name,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if response_format is not None:
        kwargs["response_format"] = response_format

    last_error: Optional[Exception] = None
    models_to_try = [model_name]
    if config.get("base_url"):
        models_to_try = [model_name] + [m for m in OLLAMA_FALLBACK_MODELS if m != model_name]

    for try_model in models_to_try:
        kwargs["model"] = try_model
        for attempt in range(max(1, retries + 1)):
            try:
                logger.info(
                    "LLM request: model=%s, base=%s, attempt=%s",
                    try_model,
                    "local" if config.get("base_url") else "openai",
                    attempt + 1,
                )
                response = client.chat.completions.create(**kwargs)
                content = ""
                if response.choices:
                    content = (response.choices[0].message.content or "").strip()
                return content
            except Exception as e:
                last_error = e
                err_msg = str(e).lower()
                is_model_not_found = "404" in err_msg or "not found" in err_msg
                logger.warning("LLM request failed (attempt %s): %s", attempt + 1, e)
                if is_model_not_found and try_model != models_to_try[-1]:
                    logger.info(
                        "Modelo '%s' não encontrado no Ollama. Tentando fallback: %s. Para usar 3.2: ollama pull llama3.2",
                        try_model,
                        models_to_try[models_to_try.index(try_model) + 1],
                    )
                    break
                if attempt < retries and not is_model_not_found:
                    sleep_time = 2 ** attempt
                    time.sleep(sleep_time)
        else:
            continue
    if last_error is not None:
        raise last_error
    raise RuntimeError("LLM request failed after retries")
