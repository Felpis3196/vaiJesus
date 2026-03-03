"""
Estruturas padronizadas para errors e warnings da auditoria.
Implementa a estrutura expandida: code, message, details, timestamp, severity.
"""
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional


def _iso_timestamp() -> str:
    """Retorna timestamp ISO em UTC."""
    return datetime.now(timezone.utc).isoformat()


def make_error(
    code: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
    severity: str = "critical",
) -> Dict[str, Any]:
    """
    Cria um item estruturado para o array `errors`.

    Args:
        code: Código do erro (ex.: VALIDATION_ERROR, AUDIT_ERROR).
        message: Mensagem legível.
        details: Dados adicionais (serão serializáveis em JSON).
        severity: Severidade (critical por padrão).

    Returns:
        Dict com keys: code, message, details, timestamp, severity.
    """
    return {
        "code": code,
        "message": message,
        "details": details if details is not None else {},
        "timestamp": _iso_timestamp(),
        "severity": severity,
    }


def make_warning(
    code: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
    severity: str = "warning",
) -> Dict[str, Any]:
    """
    Cria um item estruturado para o array `warnings`.

    Args:
        code: Código do aviso (ex.: LARGE_FILE, VALIDATION_WARNING).
        message: Mensagem legível.
        details: Dados adicionais (serão serializáveis em JSON).
        severity: Severidade (warning por padrão).

    Returns:
        Dict com keys: code, message, details, timestamp, severity.
    """
    return {
        "code": code,
        "message": message,
        "details": details if details is not None else {},
        "timestamp": _iso_timestamp(),
        "severity": severity,
    }


# Códigos de erro (errors)
class ErrorCode:
    AUDIT_ERROR = "AUDIT_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    LOAD_ERROR = "LOAD_ERROR"
    PROCESSING_ERROR = "PROCESSING_ERROR"
    SERIALIZATION_ERROR = "SERIALIZATION_ERROR"
    CRITICAL_VALIDATION = "CRITICAL_VALIDATION"


# Códigos de aviso (warnings)
class WarningCode:
    VALIDATION_WARNING = "VALIDATION_WARNING"
    LARGE_FILE = "LARGE_FILE"
    LARGE_TRANSACTIONS = "LARGE_TRANSACTIONS"
    INVALID_TYPES = "INVALID_TYPES"
    INVALID_DATES = "INVALID_DATES"
    # Alertas do alert_generator usam seus próprios códigos (PAYSLIPS_PENDING, etc.)


def error_from_exception(e: BaseException, default_code: str = ErrorCode.AUDIT_ERROR) -> Dict[str, Any]:
    """
    Cria um erro estruturado a partir de uma exceção.

    Args:
        e: Exceção capturada.
        default_code: Código usado quando não há mapeamento por tipo.

    Returns:
        Item estruturado para errors[].
    """
    message = f"Erro durante auditoria avançada: {str(e)}"
    details = {"exception_type": type(e).__name__}
    code = default_code
    if isinstance(e, ValueError):
        code = ErrorCode.VALIDATION_ERROR
    elif isinstance(e, (FileNotFoundError, OSError)):
        code = ErrorCode.LOAD_ERROR
    return make_error(code, message, details=details, severity="critical")
