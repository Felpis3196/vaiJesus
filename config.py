"""
Re-export from app.core for backward compatibility.
Prefer: from app.core import ConfigManager, SystemConfig, ...
"""
from app.core.config import (
    ConfigManager,
    SystemConfig,
    AnomalyDetectionConfig,
    DataProcessingConfig,
    ReportConfig,
    DEFAULT_CONFIG,
)

__all__ = [
    "ConfigManager",
    "SystemConfig",
    "AnomalyDetectionConfig",
    "DataProcessingConfig",
    "ReportConfig",
    "DEFAULT_CONFIG",
]


def __getattr__(name):
    # Allow any other symbol from app.core.config
    import app.core.config as m
    if hasattr(m, name):
        return getattr(m, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
