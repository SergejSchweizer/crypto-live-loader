"""Shared helpers for Deribit public REST response contracts."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TypeAlias, cast

from ingestion.http_client import get_json

JsonGetter: TypeAlias = Callable[[str, dict[str, object]], object]


def deribit_public_result(
    url: str,
    *,
    params: dict[str, object],
    context: str,
    json_getter: JsonGetter = get_json,
) -> object:
    """Fetch one Deribit public endpoint and return its ``result`` payload.

    Args:
        url (str): Deribit public REST endpoint URL.
        params (dict[str, object]): Query parameters sent to Deribit.
        context (str): Human-readable response context used in validation errors.
        json_getter (JsonGetter): Injectable JSON getter for tests and adapters.

    Returns:
        object: Raw Deribit ``result`` payload.
    """

    return deribit_result_from_payload(
        json_getter(url, params),
        context=context,
    )


def deribit_result_from_payload(payload: object, *, context: str) -> object:
    """Return ``result`` from an already-fetched Deribit public response envelope.

    Args:
        payload (object): Parsed Deribit JSON response.
        context (str): Human-readable response context used in validation errors.

    Returns:
        object: Raw Deribit ``result`` payload.

    Raises:
        ValueError: If the response envelope is not a mapping.
    """

    if not isinstance(payload, Mapping):
        raise ValueError(f"Unexpected Deribit {context} response format")
    response = cast(Mapping[str, object], payload)
    return response.get("result")


def deribit_public_result_mapping(
    url: str,
    *,
    params: dict[str, object],
    context: str,
    json_getter: JsonGetter = get_json,
) -> Mapping[str, object]:
    """Fetch one Deribit public endpoint whose ``result`` must be a mapping.

    Args:
        url (str): Deribit public REST endpoint URL.
        params (dict[str, object]): Query parameters sent to Deribit.
        context (str): Human-readable response context used in validation errors.
        json_getter (JsonGetter): Injectable JSON getter for tests and adapters.

    Returns:
        Mapping[str, object]: Deribit ``result`` mapping.

    Raises:
        ValueError: If the ``result`` payload is not a mapping.
    """

    result = deribit_public_result(url, params=params, context=context, json_getter=json_getter)
    if not isinstance(result, Mapping):
        raise ValueError(f"Unexpected Deribit {context} payload")
    return cast(Mapping[str, object], result)


def deribit_public_result_rows(
    url: str,
    *,
    params: dict[str, object],
    context: str,
    json_getter: JsonGetter = get_json,
) -> list[dict[str, object]]:
    """Fetch one Deribit public endpoint whose ``result`` must be a list of rows.

    Args:
        url (str): Deribit public REST endpoint URL.
        params (dict[str, object]): Query parameters sent to Deribit.
        context (str): Human-readable response context used in validation errors.
        json_getter (JsonGetter): Injectable JSON getter for tests and adapters.

    Returns:
        list[dict[str, object]]: Mapping rows from the Deribit ``result`` list.

    Raises:
        ValueError: If the ``result`` payload is not a list.
    """

    result = deribit_public_result(url, params=params, context=context, json_getter=json_getter)
    if not isinstance(result, list):
        raise ValueError(f"Unexpected Deribit {context} payload")
    return [cast(dict[str, object], row) for row in result if isinstance(row, dict)]
