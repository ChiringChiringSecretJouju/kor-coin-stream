from __future__ import annotations

from typing import Any

import pytest

from src.common.exceptions.error_dto_builder import make_ws_error_event_from_kind
from src.core.types import DEFAULT_SCHEMA_VERSION, ErrorCode, ErrorDomain
from tests.factory_builders import (
    build_connection_target_dto,
    build_ws_error_raw_context_payload,
)


class _FakeProducer:
    def __init__(self) -> None:
        self.sent: list[Any] = []

    async def send_error_event(self, event: Any) -> None:
        self.sent.append(event)


@pytest.mark.asyncio
async def test_make_ws_error_event_from_kind_basic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = build_connection_target_dto()
    producer = _FakeProducer()

    import src.common.exceptions.error_dto_builder as mod

    monkeypatch.setattr(
        mod,
        "classify_exception",
        lambda err, kind: (ErrorDomain.UNKNOWN, ErrorCode.UNKNOWN_ERROR, False),
        raising=True,
    )

    ok = await make_ws_error_event_from_kind(
        target=target,
        producer=producer,
        err=RuntimeError("socket closed"),
        kind="ws",
        observed_key="upbit/korea/ticker",
        raw_context=build_ws_error_raw_context_payload(),
    )

    assert ok is True
    assert len(producer.sent) == 1
    event = producer.sent[0]
    assert event.action == "error"
    assert event.target.exchange == "upbit"
    assert event.meta.schema_version == DEFAULT_SCHEMA_VERSION
    assert event.meta.observed_key == "upbit/korea/ticker"
    assert event.error.error_domain == ErrorDomain.UNKNOWN
    assert event.error.error_code == ErrorCode.UNKNOWN_ERROR
    assert event.error.error_message["message"] == "socket closed"


@pytest.mark.asyncio
async def test_make_ws_error_event_from_kind_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = build_connection_target_dto()
    producer = _FakeProducer()

    import src.common.exceptions.error_dto_builder as mod

    monkeypatch.setattr(
        mod,
        "classify_exception",
        lambda err, kind: (ErrorDomain.ORCHESTRATOR, ErrorCode.ORCHESTRATOR_ERROR, True),
        raising=True,
    )

    ok = await make_ws_error_event_from_kind(
        target=target,
        producer=producer,
        err=ValueError("bad"),
        kind="infra",
    )

    assert ok is True
    assert len(producer.sent) == 1
    event = producer.sent[0]
    assert event.meta.observed_key == ""
    assert event.error.error_domain == ErrorDomain.ORCHESTRATOR
    assert event.error.error_code == ErrorCode.ORCHESTRATOR_ERROR
    assert event.error.error_message["message"] == "bad"
