from __future__ import annotations

from src.core.dto.adapter.stream_context import adapter_stream_context
from src.core.dto.io.commands import CommandDTO
from tests.factory_builders import build_command_payload


def test_adapter_uses_socket_params_as_is() -> None:
    payload = build_command_payload(
        connection={
            "url": "wss://api.upbit.com/websocket/v1",
            "socket_params": [{"type": "ticker", "codes": ["KRW-BTC", "KRW-ETH"]}],
        },
    )
    dto = CommandDTO.model_validate(payload)

    context = adapter_stream_context(dto)

    assert context.url == "wss://api.upbit.com/websocket/v1"
    assert context.socket_params == [{"type": "ticker", "codes": ["KRW-BTC", "KRW-ETH"]}]


def test_adapter_keeps_empty_socket_params_without_regeneration() -> None:
    payload = build_command_payload(
        connection={
            "url": "wss://api.bithumb.com/public",
            "socket_params": {},
        },
        symbols=["BTC"],
        target={"exchange": "bithumb", "region": "korea", "request_type": "orderbook"},
    )
    dto = CommandDTO.model_validate(payload)

    context = adapter_stream_context(dto)

    assert context.url == "wss://api.bithumb.com/public"
    assert context.socket_params == {}
