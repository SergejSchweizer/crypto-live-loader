"""Shared log formatting helpers used across CLI and scripts."""

from __future__ import annotations

import logging

UNIFIED_LOG_FORMAT = "%(asctime)s %(levelname)s %(module_scope)s %(name)s %(message)s"


class ModuleScopeFilter(logging.Filter):
    """Inject a stable module scope field for unified log formatting."""

    def filter(self, record: logging.LogRecord) -> bool:
        logger_name = record.name.lower()
        module_scope = "core"
        if "options" in logger_name:
            module_scope = "options"
        elif "l2" in logger_name or "bronze-builder" in logger_name:
            module_scope = "l2"
        record.module_scope = module_scope
        return True


def unified_formatter() -> logging.Formatter:
    """Return the shared formatter used by all project loggers."""

    return logging.Formatter(UNIFIED_LOG_FORMAT)
