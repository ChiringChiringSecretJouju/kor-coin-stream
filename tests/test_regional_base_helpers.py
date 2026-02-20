from __future__ import annotations

from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from src.core.connection.handlers.regional_base import BaseRegionalWebsocketHandler
from src.core.connection.services.realtime_collection import RealtimeBatchCollector
from tests.factory_builders import (
    build_preprocessed_message_payload,
    build_regional_handler_kwargs_payload,
    build_regional_normalize_message_payload,
    build_socket_params_payload,
)


class _FakeCollector:
    def __init__(self) -> None:
        self.items: list[tuple[str, dict[str, Any]]] = []

    async def add_message(self, kind: str, message: dict[str, Any]) -> None:
        self.items.append((kind, message))


class _DummyRegionalHandler(BaseRegionalWebsocketHandler):
    async def ticker_message(self, message: Any):
        return None

    async def trade_message(self, message: Any):
        return None


def _build_handler() -> _DummyRegionalHandler:
    return _DummyRegionalHandler(**build_regional_handler_kwargs_payload())


def test_normalize_message_merges_data_dict() -> None:
    handler = _build_handler()
    handler.projection = None

    normalized = handler._normalize_message(build_regional_normalize_message_payload())

    assert normalized["root"] == 1
    assert normalized["data"] == {"x": 10}
    assert normalized["x"] == 10


def test_scope_log_extra_contains_standard_keys() -> None:
    handler = _build_handler()

    payload = handler._scope_log_extra("parse", error="boom")

    assert payload["exchange"] == "upbit"
    assert payload["region"] == "korea"
    assert payload["request_type"] == "ticker"
    assert payload["phase"] == "parse"
    assert payload["error"] == "boom"


def test_normalize_message_applies_projection() -> None:
    handler = _build_handler()
    handler.projection = ["x", "y"]

    normalized = handler._normalize_message(build_regional_normalize_message_payload(y=20))

    assert normalized == {"x": 10, "y": 20}


def test_prepare_incoming_message_handles_preprocessed_dict() -> None:
    handler = _build_handler()
    prepared, is_preprocessed = handler._prepare_incoming_message(
        build_preprocessed_message_payload()
    )

    assert is_preprocessed is True
    assert prepared == {"symbol": "KRW-BTC"}


def test_prepare_incoming_message_empty_string_returns_none() -> None:
    handler = _build_handler()
    prepared, is_preprocessed = handler._prepare_incoming_message("   ")

    assert prepared is None
    assert is_preprocessed is False


def test_prepare_incoming_message_non_json_scalar_returns_none() -> None:
    handler = _build_handler()
    prepared, is_preprocessed = handler._prepare_incoming_message(12345)

    assert prepared is None
    assert is_preprocessed is False


@pytest.mark.asyncio
async def test_enqueue_batch_message_adds_to_collector() -> None:
    handler = _build_handler()
    collector = _FakeCollector()
    handler._batch_collector = cast(RealtimeBatchCollector, collector)
    handler._batch_enabled = True

    await handler._enqueue_batch_message("ticker", build_socket_params_payload(symbols=["KRW-BTC"]))

    assert collector.items == [("ticker", {"symbols": ["KRW-BTC"]})]


@pytest.mark.asyncio
async def test_enqueue_batch_message_noop_when_disabled() -> None:
    handler = _build_handler()
    collector = _FakeCollector()
    handler._batch_collector = cast(RealtimeBatchCollector, collector)
    handler._batch_enabled = False

    await handler._enqueue_batch_message("trade", build_socket_params_payload(symbols=["KRW-BTC"]))

    assert collector.items == []


@pytest.mark.asyncio
async def test_orderbook_message_emits_error_and_requests_disconnect() -> None:
    handler = _DummyRegionalHandler(
        **build_regional_handler_kwargs_payload(request_type="orderbook")
    )
    emit_ws_error = AsyncMock()
    request_disconnect = AsyncMock()
    handler._error_handler.emit_ws_error = emit_ws_error
    handler.request_disconnect = request_disconnect

    result = await handler.orderbook_message({"symbol": "KRW-BTC"})

    assert result is None
    emit_ws_error.assert_awaited_once()
    await_args = emit_ws_error.await_args
    assert await_args is not None
    assert isinstance(await_args.args[0], NotImplementedError)
    assert await_args.kwargs["observed_key"] == "orderbook_not_supported"
    request_disconnect.assert_awaited_once_with(reason="orderbook_not_supported")
