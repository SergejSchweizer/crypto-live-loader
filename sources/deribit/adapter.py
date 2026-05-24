"""Deribit source adapter for canonical L2 ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from domain.models import OrderLevel, RawSnapshot
from ingestion.exchanges.deribit_l2 import fetch_order_book_snapshot, normalize_l2_symbol


@dataclass(frozen=True)
class DeribitAdapter:
    """Deribit implementation of the source adapter contract."""

    source_name: str = "deribit"

    def normalize_symbol(self, symbol: str) -> str:
        return normalize_l2_symbol(symbol)

    def fetch_snapshot(self, symbol: str, depth: int) -> RawSnapshot:
        raw = fetch_order_book_snapshot(symbol=symbol, depth=depth)
        bids_payload = cast(list[tuple[float, float]], raw["bids"])
        asks_payload = cast(list[tuple[float, float]], raw["asks"])
        bids = [OrderLevel(price=float(price), amount=float(amount)) for price, amount in bids_payload]
        asks = [OrderLevel(price=float(price), amount=float(amount)) for price, amount in asks_payload]
        return RawSnapshot(
            exchange=str(raw["exchange"]),
            symbol=str(raw["symbol"]),
            timestamp_ms=int(cast(int | float, raw["timestamp_ms"])),
            bids=bids,
            asks=asks,
            mark_price=_to_optional_float(raw.get("mark_price")),
            index_price=_to_optional_float(raw.get("index_price")),
            open_interest=_to_optional_float(raw.get("open_interest")),
            funding_8h=_to_optional_float(raw.get("funding_8h")),
            current_funding=_to_optional_float(raw.get("current_funding")),
        )


def _to_optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(cast(int | float, value))
