"""Shared log formatting helpers used across CLI and scripts."""

from __future__ import annotations

import logging

UNIFIED_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def unified_formatter() -> logging.Formatter:
    """Return the shared formatter used by all project loggers."""

    return logging.Formatter(UNIFIED_LOG_FORMAT)
