"""
Re-export from app.core for backward compatibility.
Prefer: from app.core import AuditLogger
"""
from app.core.logging_config import AuditLogger

__all__ = ["AuditLogger"]
