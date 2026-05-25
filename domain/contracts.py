"""Source adapter contracts for multi-provider ingestion."""

from __future__ import annotations

from typing import Protocol

from domain.models import RawSnapshot


class SourceAdapter(Protocol):
    """Adapter protocol implemented by all market data sources."""

    @property
    def source_name(self) -> str:
        """Stable source identifier."""
        ...

    def normalize_symbol(self, symbol: str) -> str:
        """Normalize one user-provided symbol to source-native symbol."""
        ...

    def fetch_snapshot(self, symbol: str, depth: int) -> RawSnapshot:
        """Fetch one canonical snapshot from a source symbol."""
        ...
