"""Backward-compatibility wrapper. Prefer: from app.reporting import ..."""
from app.reporting import (
    generate_report_pdf,
    generate_full_report,
    generate_conference_report,
    generate_summary_report,
    generate_anomaly_report,
    generate_alerts,
    add_alerts_to_audit_result,
)

__all__ = [
    "generate_report_pdf",
    "generate_full_report",
    "generate_conference_report",
    "generate_summary_report",
    "generate_anomaly_report",
    "generate_alerts",
    "add_alerts_to_audit_result",
]
