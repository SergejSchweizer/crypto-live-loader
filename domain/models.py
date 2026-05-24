"""Canonical domain models shared across data sources."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OrderLevel:
    """One price/amount level from an order book side."""

    price: float
    amount: float


@dataclass(frozen=True)
class RawSnapshot:
    """Canonical raw L2 snapshot payload returned by source adapters."""

    exchange: str
    symbol: str
    timestamp_ms: int
    bids: list[OrderLevel]
    asks: list[OrderLevel]
    mark_price: float | None
    index_price: float | None
    open_interest: float | None
    funding_8h: float | None
    current_funding: float | None
