from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from src.core.connection.handlers.base import BaseWebsocketHandler
from src.core.connection.services.health_monitor import ConnectionHealthMonitor
from tests.factory_builders import (
    build_connection_policy_domain,
    build_scope_domain,
    build_socket_params_payload,
)


class _FakeBackpressureProducer:
    async def start_producer(self) -> bool:
        return True


class _FakeMetricsProducerInner:
    def __init__(self) -> None:
        self.backpressure_bound = False

    def set_backpressure_event_producer(
        self, producer: object, enable_periodic_monitoring: bool
    ) -> None:
        self.backpressure_bound = enable_periodic_monitoring and producer is not None


class _FakeMetricsProducer:
    def __init__(self) -> None:
        self.producer = _FakeMetricsProducerInner()

    async def start_producer(self) -> bool:
        return True

    async def stop_producer(self) -> None:
        return None


class _FakeErrorHandler:
    def __init__(self) -> None:
        self.connection_errors: list[dict[str, Any]] = []
        self.ws_errors: list[str] = []

    async def emit_connection_error(
        self,
        err: BaseException,
        url: str,
        attempt: int,
        backoff: float,
    ) -> None:
        self.connection_errors.append(
            {
                "error": str(err),
                "url": url,
                "attempt": attempt,
                "backoff": backoff,
            }
        )

    async def emit_ws_error(self, err: BaseException, **kwargs: Any) -> None:
        self.ws_errors.append(str(err))


class _FakeWebsocket:
    def __init__(self) -> None:
        self.closed = False
        self.sent: list[str | bytes] = []

    async def send(self, message: str | bytes) -> None:
        self.sent.append(message)

    async def close(self) -> None:
        self.closed = True

    async def ping(self) -> None:
        return None


class _FailThenSucceedContext:
    def __init__(self) -> None:
        self.attempt = 0
        self.websocket: _FakeWebsocket | None = None

    def __call__(self, uri: str, ping_interval: int | None = None) -> "_FailThenSucceedSession":
        return _FailThenSucceedSession(self)


class _FailThenSucceedSession:
    def __init__(self, owner: _FailThenSucceedContext) -> None:
        self.owner = owner

    async def __aenter__(self) -> _FakeWebsocket:
        self.owner.attempt += 1
        if self.owner.attempt == 1:
            raise OSError("temporary network error")
        self.owner.websocket = _FakeWebsocket()
        return self.owner.websocket

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


class _TestHandler(BaseWebsocketHandler):
    async def _handle_message_loop(self, websocket: Any, timeout: int) -> None:
        self._stop_requested = True


@pytest.mark.asyncio
async def test_websocket_connection_retries_and_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.core.connection.handlers import base as base_module

    connect_context = _FailThenSucceedContext()
    monkeypatch.setattr(base_module.websockets, "connect", connect_context)
    monkeypatch.setattr(base_module, "compute_next_backoff", lambda policy, attempt, last: 0.0)

    handler = _TestHandler(exchange_name="upbit", region="korea", request_type="ticker")
    fake_error_handler = _FakeErrorHandler()
    handler._error_handler = cast(Any, fake_error_handler)
    handler._backpressure_producer = cast(Any, _FakeBackpressureProducer())
    handler._metrics_producer = cast(Any, _FakeMetricsProducer())
    handler._batch_flush = lambda: asyncio.sleep(0)
    handler._health_monitor.start_monitoring = lambda websocket, ping_interval: asyncio.sleep(0)
    handler._health_monitor.stop_monitoring = lambda: asyncio.sleep(0)

    await handler.websocket_connection(
        url="wss://example.invalid/ws",
        parameter_info=build_socket_params_payload(symbols=["KRW-BTC"]),
        correlation_id="cid-e2e-retry",
    )

    assert connect_context.attempt == 2
    assert len(fake_error_handler.connection_errors) == 1
    assert fake_error_handler.connection_errors[0]["attempt"] == 1
    assert fake_error_handler.connection_errors[0]["backoff"] == 0.0

    websocket = connect_context.websocket
    assert websocket is not None
    assert len(websocket.sent) == 1


@pytest.mark.asyncio
async def test_watchdog_closes_idle_websocket(monkeypatch: pytest.MonkeyPatch) -> None:
    scope = build_scope_domain()
    policy = build_connection_policy_domain(receive_idle_timeout=1)
    monitor = ConnectionHealthMonitor(scope=scope, policy=policy)

    captured_phases: list[str] = []

    async def _fake_emit_error(
        err: BaseException, *, phase: str, extra: dict | None = None
    ) -> None:
        captured_phases.append(phase)

    monitor._emit_error = _fake_emit_error  # type: ignore[method-assign]

    websocket = _FakeWebsocket()
    await monitor.start_monitoring(websocket, ping_interval=100)
    await asyncio.sleep(1.2)
    await monitor.stop_monitoring()

    assert websocket.closed is True
    assert "watchdog_idle" in captured_phases
