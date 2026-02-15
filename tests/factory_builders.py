from __future__ import annotations

from typing import Any

from src.core.dto.internal.common import ConnectionPolicyDomain, ConnectionScopeDomain
from src.core.dto.internal.orchestrator import StreamContextDomain
from src.core.dto.io.commands import ConnectionTargetDTO


def build_connection_target(**overrides: str) -> dict[str, str]:
    payload: dict[str, str] = {
        "exchange": "upbit",
        "region": "korea",
        "request_type": "ticker",
    }
    payload.update(overrides)
    return payload


def build_command_payload(
    *,
    target: dict[str, str] | None = None,
    symbols: list[str] | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "type": "status",
        "action": "connect_and_subscribe",
        "schema_version": "1.0.0",
        "target": target or build_connection_target(),
        "symbols": symbols or ["KRW-BTC"],
        "connection": {
            "url": "wss://api.upbit.com/websocket/v1",
            "socket_params": [{"type": "ticker", "codes": ["KRW-BTC"]}],
        },
        "projection": ["symbol", "timestamp", "close", "high", "low", "volume"],
    }
    payload.update(overrides)
    return payload


def build_disconnect_payload(
    *,
    target: dict[str, str] | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "type": "status",
        "action": "disconnect",
        "target": target or build_connection_target(request_type="trade"),
    }
    payload.update(overrides)
    return payload


def build_connect_success_payload() -> dict[str, Any]:
    return {
        "ticket_id": "ticket-1234567890",
        "action": "connect_success",
        "ack": "clear",
        "target": build_connection_target(),
        "symbol": "KRW-BTC",
        "timestamp_utc": "2026-02-15T06:00:00Z",
        "meta": {
            "ticket_id": "meta-ticket-123",
            "schema_version": "1.0.0",
            "correlation_id": "123e4567-e89b-12d3-a456-426614174000",
            "observed_key": "upbit/korea/ticker",
        },
    }


def build_scope_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "exchange": "upbit",
        "region": "korea",
        "request_type": "ticker",
        "symbol": None,
    }
    payload.update(overrides)
    return payload


def build_scope_domain(**overrides: Any) -> ConnectionScopeDomain:
    payload = build_scope_payload(**overrides)
    if payload.get("symbol") is None:
        payload.pop("symbol", None)
    return ConnectionScopeDomain(**payload)


def build_socket_params_payload(
    *, symbols: list[str] | None = None, **overrides: Any
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "symbols": symbols or ["KRW-BTC"],
    }
    payload.update(overrides)
    return payload


def build_stream_context_payload(
    *,
    scope: dict[str, Any] | None = None,
    url: str = "wss://example.invalid/ws",
    socket_params: dict[str, Any] | None = None,
    symbols: tuple[str, ...] | None = None,
    projection: list[str] | None = None,
    correlation_id: str | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "scope": scope or build_scope_payload(),
        "url": url,
        "socket_params": socket_params or build_socket_params_payload(),
        "symbols": symbols or ("KRW-BTC",),
        "projection": projection,
        "correlation_id": correlation_id,
    }
    payload.update(overrides)
    return payload


def build_stream_context_domain(**overrides: Any) -> StreamContextDomain:
    payload = build_stream_context_payload(**overrides)
    scope_payload = payload.pop("scope")
    return StreamContextDomain(scope=build_scope_domain(**scope_payload), **payload)


def build_connection_target_dto(**overrides: str) -> ConnectionTargetDTO:
    return ConnectionTargetDTO.model_validate(build_connection_target(**overrides))


def build_connection_policy_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "initial_backoff": 1.0,
        "max_backoff": 30.0,
        "backoff_multiplier": 2.0,
        "jitter": 0.2,
        "heartbeat_kind": "frame",
        "heartbeat_message": None,
        "heartbeat_timeout": 10.0,
        "heartbeat_fail_limit": 3,
        "receive_idle_timeout": 120,
    }
    payload.update(overrides)
    return payload


def build_connection_policy_domain(**overrides: Any) -> ConnectionPolicyDomain:
    return ConnectionPolicyDomain(**build_connection_policy_payload(**overrides))


def build_subscription_ack_payload_korea(
    *,
    response_type: str = "SUBSCRIBED",
    **overrides: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "response_type": response_type,
    }
    payload.update(overrides)
    return payload


def build_subscription_ack_payload_global(
    *,
    result: str | None = None,
    event: str | None = None,
    data: dict[str, Any] | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if result is not None:
        payload["result"] = result
    if event is not None:
        payload["event"] = event
    if data is not None:
        payload["data"] = data
    payload.update(overrides)
    return payload


def build_regional_handler_kwargs_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "exchange_name": "upbit",
        "region": "korea",
        "request_type": "ticker",
    }
    payload.update(overrides)
    return payload


def build_korbit_command_payload(**overrides: Any) -> dict[str, Any]:
    payload = build_command_payload(
        target=build_connection_target(exchange="korbit"),
        symbols=["BTC"],
        connection={
            "url": "wss://ws-api.korbit.co.kr/v2/public",
            "socket_params": [
                {
                    "method": "subscribe",
                    "type": "ticker",
                    "symbols": ["btc_krw"],
                }
            ],
        },
        projection=[
            "symbol",
            "timestamp",
            "close",
            "high",
            "low",
            "volume",
            "quoteVolume",
        ],
    )
    payload.update(overrides)
    return payload


def build_target_missing_request_type_payload() -> dict[str, str]:
    return {
        "exchange": "upbit",
        "region": "korea",
    }


def build_target_with_unexpected_field_payload() -> dict[str, str]:
    return {
        "exchange": "upbit",
        "region": "korea",
        "request_type": "trade",
        "unexpected": "value",
    }


def build_ws_error_raw_context_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "url": "wss://example",
    }
    payload.update(overrides)
    return payload


def build_ticker_batch_item_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "target_currency": "BTCUSDT",
        "timestamp": 1700000000000,
        "first": 50000.0,
        "last": 51000.0,
        "high": 52000.0,
        "low": 49000.0,
        "target_volume": 1234.56,
    }
    payload.update(overrides)
    return payload


def build_ticker_batch_payload(**overrides: Any) -> list[dict[str, Any]]:
    return [build_ticker_batch_item_payload(**overrides)]


def build_counter_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {"count": 0, "reason": None}
    payload.update(overrides)
    return payload
