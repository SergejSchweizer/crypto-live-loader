"""Source adapter registry and lookup helpers."""

from __future__ import annotations

from domain.contracts import SourceAdapter
from sources.deribit.adapter import DeribitAdapter


def source_adapter_for_exchange(exchange: str) -> SourceAdapter:
    """Return a source adapter instance for one configured exchange."""

    if exchange == "deribit":
        return DeribitAdapter()
    raise ValueError(f"Unsupported exchange '{exchange}'")
