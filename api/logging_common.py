"""Shared log formatting helpers used across CLI and scripts."""

from __future__ import annotations

import logging

UNIFIED_LOG_FORMAT = "%(asctime)s %(levelname)s %(module_scope)s %(name)s %(message)s"

COMMAND_LOG_SCOPES = {
    "futures-summary-bronze-builder": "futures_summary",
    "index-price-bronze-builder": "index_price",
    "instrument-metadata-bronze-builder": "instrument_metadata",
    "perp-l2-bronze-builder": "l2",
    "l2-bronze-builder": "l2",
    "bronze-builder": "l2",
    "option-l2-bronze-builder": "option_l2",
    "option-instrument-ticker-bronze-builder": "option_instrument_ticker",
    "options-bronze-builder": "options",
    "recent-trades-bronze-builder": "recent_trades",
    "volatility-index-bronze-builder": "volatility_index",
}


class ModuleScopeFilter(logging.Filter):
    """Inject a stable module scope field for unified log formatting."""

    def filter(self, record: logging.LogRecord) -> bool:
        logger_name = record.name.lower()
        module_scope = "core"
        for command, scope in sorted(COMMAND_LOG_SCOPES.items(), key=lambda item: len(item[0]), reverse=True):
            if command in logger_name:
                module_scope = scope
                break
        record.module_scope = module_scope
        return True


def unified_formatter() -> logging.Formatter:
    """Return the shared formatter used by all project loggers."""

    return logging.Formatter(UNIFIED_LOG_FORMAT)
