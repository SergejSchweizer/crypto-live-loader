"""Tests for source adapter registry lookups."""

from __future__ import annotations

import pytest

from sources.deribit.adapter import DeribitAdapter
from sources.registry import source_adapter_for_exchange


def test_source_adapter_for_exchange_returns_deribit_adapter() -> None:
    """Known exchanges should resolve to the configured adapter implementation."""

    adapter = source_adapter_for_exchange("deribit")
    assert isinstance(adapter, DeribitAdapter)


def test_source_adapter_for_exchange_rejects_unknown_exchange() -> None:
    """Unknown exchanges should fail fast with a clear error."""

    with pytest.raises(ValueError, match="Unsupported exchange"):
        source_adapter_for_exchange("unknown")

