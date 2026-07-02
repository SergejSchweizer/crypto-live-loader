"""Tests for shared Deribit public REST response helpers."""

from __future__ import annotations

import pytest

from sources.deribit.public_api import (
    deribit_public_result_mapping,
    deribit_public_result_rows,
    deribit_result_from_payload,
)


def test_deribit_public_result_rows_filters_mapping_rows() -> None:
    """Verify list-result helpers keep only row mappings."""

    rows = deribit_public_result_rows(
        "https://example.test",
        params={"currency": "BTC"},
        context="example rows",
        json_getter=lambda _url, _params: {"result": [{"instrument_name": "BTC-PERPETUAL"}, ["bad"]]},
    )

    assert rows == [{"instrument_name": "BTC-PERPETUAL"}]


def test_deribit_public_result_mapping_rejects_non_mapping_result() -> None:
    """Verify mapping-result helpers fail with contextual errors."""

    with pytest.raises(ValueError, match="Unexpected Deribit example payload"):
        deribit_public_result_mapping(
            "https://example.test",
            params={},
            context="example",
            json_getter=lambda _url, _params: {"result": []},
        )


def test_deribit_result_from_payload_rejects_non_mapping_envelope() -> None:
    """Verify already-fetched payload validation uses the shared envelope contract."""

    with pytest.raises(ValueError, match="Unexpected Deribit trade response format"):
        deribit_result_from_payload([], context="trade")
