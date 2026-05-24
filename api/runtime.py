"""Runtime helpers for CLI logging and concurrency settings."""

from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler

from api.logging_common import unified_formatter
from ingestion.config import Config, config_int, config_section, configured_logfile_path, load_config

LOGGER_NAME = "crypto_live_loader"
DEFAULT_LOG_DIR = ".logs"
DEFAULT_LOG_ROTATION_DAYS = 7
DEFAULT_LOG_BACKUP_COUNT = 0
DEFAULT_FETCH_CONCURRENCY = 8


def _safe_log_module_name(module_name: str) -> str:
    """Return a filesystem-safe log module name."""

    normalized = module_name.strip().replace("/", "-").replace("\\", "-")
    return normalized or "crypto-live-loader"


def configure_logging(module_name: str = "crypto-live-loader", config: Config | None = None) -> logging.Logger:
    """Configure runtime logging with rotation to the shared configured logfile."""

    safe_module_name = _safe_log_module_name(module_name)
    logger = logging.getLogger(f"{LOGGER_NAME}.{safe_module_name}")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = unified_formatter()
    resolved_config = config or load_config()
    runtime_config = config_section(resolved_config, "runtime")
    logfile = configured_logfile_path(resolved_config)
    rotation_days = max(1, config_int(runtime_config, "log_rotation_days", DEFAULT_LOG_ROTATION_DAYS))
    backup_count = max(0, config_int(runtime_config, "log_backup_count", DEFAULT_LOG_BACKUP_COUNT))
    try:
        logfile.parent.mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            filename=logfile,
            when="D",
            interval=rotation_days,
            backupCount=backup_count,
            encoding="utf-8",
            utc=True,
        )
        file_handler.suffix = "%Y-%m-%d"
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError:
        logger.warning("Falling back to stderr logging; cannot create logfile '%s'", logfile)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def fetch_concurrency(config: Config | None = None) -> int:
    """Return bounded fetch concurrency from config."""

    runtime_config = config_section(config or load_config(), "runtime")
    value = config_int(runtime_config, "fetch_concurrency", DEFAULT_FETCH_CONCURRENCY)
    return max(1, value)
