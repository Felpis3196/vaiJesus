"""
Services module - Serviços auxiliares do sistema
"""
try:
    from .document_analyzer import DocumentAnalyzer
    __all__ = ['DocumentAnalyzer']
except ImportError:
    __all__ = []

try:
    from .alert_generator import generate_alerts, add_alerts_to_audit_result
    __all__ = list(__all__) + ['generate_alerts', 'add_alerts_to_audit_result']
except ImportError:
    pass

try:
    from .audit_structures import make_error, make_warning, error_from_exception, ErrorCode, WarningCode
    __all__ = list(__all__) + ['make_error', 'make_warning', 'error_from_exception', 'ErrorCode', 'WarningCode']
except ImportError:
    pass

try:
    from .labor_analyzer import analyze_labor_charges, get_labor_summary
    __all__ = list(__all__) + ['analyze_labor_charges', 'get_labor_summary']
except ImportError:
    pass

try:
    from .report_formatter import (
        format_section_1, format_section_2, format_section_3, format_section_4,
        format_section_5, format_section_6, format_section_7, format_section_8,
        format_full_report, get_section_formatter
    )
    __all__ = list(__all__) + [
        'format_section_1', 'format_section_2', 'format_section_3', 'format_section_4',
        'format_section_5', 'format_section_6', 'format_section_7', 'format_section_8',
        'format_full_report', 'get_section_formatter'
    ]
except ImportError:
    pass
