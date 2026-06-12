"""Tests for L2 snapshot ingestion."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from domain.models import OrderLevel, RawSnapshot
from ingestion.l2 import fetch_l2_snapshots_for_symbols


@dataclass(frozen=True)
class _StubAdapter:
    source_name: str = "deribit"

    def normalize_symbol(self, symbol: str) -> str:
        return symbol

    def fetch_snapshot(self, symbol: str, depth: int) -> RawSnapshot:
        del depth
        return RawSnapshot(
            exchange="deribit",
            symbol=f"{symbol}-PERPETUAL",
            timestamp_ms=1_700_000_000_000,
            bids=[OrderLevel(price=100.0, amount=1.0)],
            asks=[OrderLevel(price=101.0, amount=1.0)],
            mark_price=100.5,
            index_price=100.0,
            open_interest=1000.0,
            funding_8h=0.0001,
            current_funding=0.00001,
        )


def test_fetch_l2_snapshots_for_symbols_fetches_symbols_sequentially(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, str]] = []

    class Adapter(_StubAdapter):
        def fetch_snapshot(self, symbol: str, depth: int) -> RawSnapshot:
            del depth
            events.append(("start", symbol))
            events.append(("finish", symbol))
            event_count = len(events)
            return RawSnapshot(
                exchange="deribit",
                symbol=f"{symbol}-PERPETUAL",
                timestamp_ms=1_700_000_000_000 + event_count,
                bids=[OrderLevel(price=100.0, amount=1.0)],
                asks=[OrderLevel(price=101.0, amount=1.0)],
                mark_price=100.5,
                index_price=100.0,
                open_interest=1000.0,
                funding_8h=0.0001,
                current_funding=0.00001,
            )

    del monkeypatch

    snapshots = fetch_l2_snapshots_for_symbols(
        exchange="deribit",
        symbols=["BTC", "ETH"],
        depth=50,
        snapshot_count=1,
        poll_interval_s=0,
        concurrency=2,
        adapter=Adapter(),
    )

    assert events == [("start", "BTC"), ("finish", "BTC"), ("start", "ETH"), ("finish", "ETH")]
    assert len(snapshots["BTC"]) == 1
    assert len(snapshots["ETH"]) == 1


def test_fetch_l2_snapshots_for_symbols_logs_and_skips_failed_symbol(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify one failed symbol does not discard the whole polling tick."""

    @dataclass(frozen=True)
    class Adapter(_StubAdapter):
        def fetch_snapshot(self, symbol: str, depth: int) -> RawSnapshot:
            if symbol == "ETH":
                raise RuntimeError("exchange unavailable")
            return super().fetch_snapshot(symbol=symbol, depth=depth)

    with caplog.at_level("WARNING", logger="ingestion.l2"):
        snapshots = fetch_l2_snapshots_for_symbols(
            exchange="deribit",
            symbols=["BTC", "ETH"],
            depth=50,
            snapshot_count=1,
            poll_interval_s=0,
            concurrency=2,
            adapter=Adapter(),
        )

    assert len(snapshots["BTC"]) == 1
    assert snapshots["ETH"] == []
    assert "L2 snapshot fetch failed symbol=ETH" in caplog.text


def test_fetch_l2_snapshots_for_symbols_respects_expired_runtime_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an expired runtime budget stops polling before network calls."""

    calls: list[str] = []

    from ingestion import l2

    @dataclass(frozen=True)
    class Adapter(_StubAdapter):
        def fetch_snapshot(self, symbol: str, depth: int) -> RawSnapshot:
            calls.append(symbol)
            return super().fetch_snapshot(symbol=symbol, depth=depth)

    monkeypatch.setattr(l2, "_deadline_from_config", lambda config: 0.0)

    snapshots = fetch_l2_snapshots_for_symbols(
        exchange="deribit",
        symbols=["BTC"],
        depth=50,
        snapshot_count=1,
        poll_interval_s=0,
        max_runtime_s=1,
        adapter=Adapter(),
    )

    assert calls == []
    assert snapshots["BTC"] == []
