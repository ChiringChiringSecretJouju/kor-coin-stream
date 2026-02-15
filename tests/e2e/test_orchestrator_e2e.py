from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from src.application.connection_registry import ConnectionRegistry
from src.application.orchestrator import StreamOrchestrator
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.types import CONNECTION_STATUS_CONNECTED, CONNECTION_STATUS_DISCONNECTED
from tests.factory_builders import (
    build_connection_target_dto,
    build_scope_domain,
    build_scope_payload,
    build_socket_params_payload,
    build_stream_context_domain,
)


class _FakeErrorProducer:
    def __init__(self) -> None:
        self.stopped = False

    async def stop_producer(self) -> None:
        self.stopped = True


class _FakeHandler:
    def __init__(self, scope: ConnectionScopeDomain) -> None:
        self.scope = scope
        self.disconnect_reasons: list[str | None] = []

    async def request_disconnect(self, reason: str | None = None) -> None:
        self.disconnect_reasons.append(reason)


class _FakeConnector:
    def __init__(self) -> None:
        self.created_scopes: list[ConnectionScopeDomain] = []
        self.run_calls: list[dict[str, Any]] = []
        self._running_count = 0
        self._running_event = asyncio.Event()
        self._release_event = asyncio.Event()

    async def create_handler_with(
        self, scope: ConnectionScopeDomain, projection: list[str] | None
    ) -> _FakeHandler:
        self.created_scopes.append(scope)
        return _FakeHandler(scope)

    async def run_connection(
        self,
        handler: _FakeHandler,
        url: str,
        socket_params: dict[str, Any] | list[Any],
        correlation_id: str | None = None,
    ) -> None:
        self.run_calls.append(
            {
                "scope": handler.scope,
                "url": url,
                "socket_params": socket_params,
                "correlation_id": correlation_id,
            }
        )
        self._running_count += 1
        self._running_event.set()
        await self._release_event.wait()

    async def wait_until_running(self, expected_count: int, timeout: float = 1.0) -> None:
        end = asyncio.get_running_loop().time() + timeout
        while self._running_count < expected_count:
            if asyncio.get_running_loop().time() >= end:
                raise TimeoutError(f"Expected {expected_count} running tasks")
            self._running_event.clear()
            await self._running_event.wait()

    def release_all(self) -> None:
        self._release_event.set()


class _FakeWebsocketConnectionCache:
    lifecycle: list[tuple[str, str]] = []

    def __init__(self, spec: Any) -> None:
        self.scope = spec.scope

    async def update_connection_state(self, status: Any) -> bool:
        self.lifecycle.append((self.scope.to_key(), str(status)))
        return True

    async def replace_symbols(self, symbols: list[str]) -> None:
        self.lifecycle.append((self.scope.to_key(), f"symbols:{','.join(symbols)}"))

    async def remove_connection(self) -> bool:
        self.lifecycle.append((self.scope.to_key(), "removed"))
        return True


@pytest.mark.asyncio
async def test_orchestrator_connection_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.application import orchestrator as orchestrator_module

    _FakeWebsocketConnectionCache.lifecycle = []
    monkeypatch.setattr(
        orchestrator_module,
        "WebsocketConnectionCache",
        _FakeWebsocketConnectionCache,
    )

    registry = ConnectionRegistry()
    connector = _FakeConnector()
    error_producer = _FakeErrorProducer()
    orchestrator = StreamOrchestrator(
        error_producer=cast(Any, error_producer),
        registry=registry,
        connector=cast(Any, connector),
    )

    scope = build_scope_domain()
    ctx = build_stream_context_domain(
        scope=build_scope_payload(),
        socket_params=build_socket_params_payload(symbols=["KRW-BTC"]),
        symbols=("KRW-BTC",),
        correlation_id="cid-001",
    )

    await orchestrator.startup()
    await orchestrator.connect_from_context(ctx)
    await connector.wait_until_running(expected_count=1)
    assert registry.is_running(scope) is True

    await orchestrator.shutdown()

    active_connections = registry.get_active_connections()
    assert active_connections == []
    assert error_producer.stopped is True

    lifecycle_values = [entry[1] for entry in _FakeWebsocketConnectionCache.lifecycle]
    assert str(CONNECTION_STATUS_CONNECTED) in lifecycle_values
    assert str(CONNECTION_STATUS_DISCONNECTED) in lifecycle_values
    assert "removed" in lifecycle_values


@pytest.mark.asyncio
async def test_coinone_multi_symbol_creates_two_running_connections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.application import orchestrator as orchestrator_module

    _FakeWebsocketConnectionCache.lifecycle = []
    monkeypatch.setattr(
        orchestrator_module,
        "WebsocketConnectionCache",
        _FakeWebsocketConnectionCache,
    )

    registry = ConnectionRegistry()
    connector = _FakeConnector()
    orchestrator = StreamOrchestrator(
        error_producer=cast(Any, _FakeErrorProducer()),
        registry=registry,
        connector=cast(Any, connector),
    )

    ctx = build_stream_context_domain(
        scope=build_scope_payload(exchange="coinone"),
        socket_params=build_socket_params_payload(symbols=["KRW-BTC", "KRW-ETH"]),
        symbols=("KRW-BTC", "KRW-ETH"),
    )

    await orchestrator.connect_from_context(ctx)
    await connector.wait_until_running(expected_count=2)

    created_symbols = {created_scope.symbol for created_scope in connector.created_scopes}
    assert created_symbols == {"KRW-BTC", "KRW-ETH"}
    assert len(registry.get_active_connections()) == 2

    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_coinone_disconnect_by_base_scope_stops_all_symbol_connections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.application import orchestrator as orchestrator_module

    _FakeWebsocketConnectionCache.lifecycle = []
    monkeypatch.setattr(
        orchestrator_module,
        "WebsocketConnectionCache",
        _FakeWebsocketConnectionCache,
    )

    registry = ConnectionRegistry()
    connector = _FakeConnector()
    orchestrator = StreamOrchestrator(
        error_producer=cast(Any, _FakeErrorProducer()),
        registry=registry,
        connector=cast(Any, connector),
    )

    ctx = build_stream_context_domain(
        scope=build_scope_payload(exchange="coinone"),
        socket_params=build_socket_params_payload(symbols=["KRW-BTC", "KRW-ETH"]),
        symbols=("KRW-BTC", "KRW-ETH"),
    )

    await orchestrator.connect_from_context(ctx)
    await connector.wait_until_running(expected_count=2)
    assert len(registry.get_active_connections()) == 2

    disconnected = await orchestrator.disconnect(
        build_connection_target_dto(exchange="coinone"),
        reason="e2e-stop-all",
    )
    assert disconnected is True
    assert registry.get_active_connections() == []
