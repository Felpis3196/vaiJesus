"""Backward-compatibility wrapper. Prefer: from app.audit import AuditSystem."""
from app.audit import AuditSystem

__all__ = ["AuditSystem"]
