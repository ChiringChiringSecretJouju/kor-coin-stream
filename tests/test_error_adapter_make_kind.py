from __future__ import annotations

import re
from typing import Tuple

import pytest

from core.dto.adapter.error_adapter import make_ws_error_event_from_kind
from core.dto.internal.common import ConnectionScopeDomain
from core.dto.io.error_event import WsErrorEventDTO
from core.types import ErrorDomain, ErrorCode, DEFAULT_SCHEMA_VERSION


@pytest.mark.asyncio
async def test_make_ws_error_event_from_kind_basic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: scope and a predictable classify_exception
    scope = ConnectionScopeDomain(
        region="korea", exchange="upbit", request_type="ticker"
    )

    def fake_classify(err: BaseException, kind: str) -> Tuple[ErrorDomain, ErrorCode, bool]:  # type: ignore[name-defined]
        assert kind == "ws"
        return (ErrorDomain.UNKNOWN, ErrorCode.UNKNOWN_ERROR, False)

    # Patch the symbol used inside the module under test
    import core.dto.adapter.error_adapter as mod

    monkeypatch.setattr(mod, "classify_exception", fake_classify, raising=True)

    # Act
    evt: WsErrorEventDTO = make_ws_error_event_from_kind(
        scope=scope,
        err=RuntimeError("socket closed"),
        kind="ws",
        observed_key="upbit/korea/ticker",
        raw_context={"url": "wss://example", "socket_params": {"symbols": ["KRW-BTC"]}},
    )

    # Assert basics
    assert evt.action == "error"
    assert evt.target.exchange == "upbit"
    assert evt.target.region == "korea"
    assert evt.target.request_type == "ticker"

    # Meta
    assert evt.meta.schema_version == DEFAULT_SCHEMA_VERSION
    assert isinstance(evt.meta.correlation_id, str) and len(evt.meta.correlation_id) > 0
    assert evt.meta.observed_key == "upbit/korea/ticker"
    assert evt.meta.raw_context == {
        "url": "wss://example",
        "socket_params": {"symbols": ["KRW-BTC"]},
    }

    # Error payload
    assert evt.error.error_message == {"message": "socket closed"}
    assert evt.error.error_domain == ErrorDomain.UNKNOWN
    assert evt.error.error_code == ErrorCode.UNKNOWN_ERROR


@pytest.mark.asyncio
async def test_make_ws_error_event_from_kind_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: return a different domain/code to ensure patch applied
    scope = ConnectionScopeDomain(
        region="korea", exchange="upbit", request_type="ticker"
    )

    def fake_classify(err: BaseException, kind: str):  # type: ignore[name-defined]
        return (ErrorDomain.ORCHESTRATOR, ErrorCode.ORCHESTRATOR_ERROR, True)

    import core.dto.adapter.error_adapter as mod

    monkeypatch.setattr(mod, "classify_exception", fake_classify, raising=True)

    # Act: do not pass observed_key/raw_context to exercise defaults
    evt = make_ws_error_event_from_kind(
        scope=scope,
        err=ValueError("bad"),
        kind="infra",
    )

    # Assert defaults
    assert evt.action == "error"
    assert evt.meta.observed_key == ""
    assert evt.meta.raw_context is None
    assert evt.error.error_message == {"message": "bad"}
    assert evt.error.error_domain == ErrorDomain.ORCHESTRATOR
    assert evt.error.error_code == ErrorCode.ORCHESTRATOR_ERROR
