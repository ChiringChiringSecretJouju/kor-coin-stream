from __future__ import annotations

from typing import Any

import pytest

from infra.messaging.connect.services.command_validator import GenericValidator
from core.dto.io.commands import CommandDTO


# promting/event.json 형식과 CommandDTO 스키마를 반영한 유효 페이로드
VALID_PAYLOAD: dict[str, Any] = {
    "type": "status",
    "action": "connect_and_subscribe",
    "schema_version": "1.0.0",
    "target": {"region": "korea", "exchange": "korbit", "request_type": "ticker"},
    "symbols": ["BTC"],
    "connection": {
        "url": "wss://ws-api.korbit.co.kr/v2/public",
        "socket_params": [
            {"method": "subscribe", "type": "ticker", "symbols": ["btc_krw"]}
        ],
    },
    "projection": [
        "symbol",
        "timestamp",
        "close",
        "high",
        "low",
        "volume",
        "quoteVolume",
    ],
}


@pytest.mark.asyncio
async def test_validator_returns_dto_on_valid_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    v = GenericValidator(exchange_name="korbit", request_type="ticker")

    # DLQ가 호출되면 실패하도록 가드
    async def _fail_dlq(*args: Any, **kwargs: Any) -> None:  # pragma: no cover
        raise AssertionError("_send_to_dlq should not be called for valid payload")

    monkeypatch.setattr(v, "_send_to_dlq", _fail_dlq, raising=True)

    # Act
    dto = await v.validate_dto(
        payload=VALID_PAYLOAD, dto_class=CommandDTO, key="k|korbit|ticker"
    )

    # Assert
    assert dto is not None
    assert isinstance(dto, CommandDTO)
    assert dto.type == "status"
    assert dto.action == "connect_and_subscribe"
    assert dto.connection.url.startswith("wss://")
    assert dto.symbols == ["BTC"]


@pytest.mark.asyncio
async def test_validator_returns_none_and_calls_dlq_on_invalid_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange: connection 필드 누락으로 ValidationError 유도
    invalid = {k: v for k, v in VALID_PAYLOAD.items() if k != "connection"}

    v = GenericValidator(exchange_name="korbit", request_type="ticker")

    called: dict[str, Any] = {"count": 0, "reason": None}

    async def _spy_dlq(*, payload: dict, reason: str, key: str | None = None) -> None:
        called["count"] += 1
        called["reason"] = reason

    monkeypatch.setattr(v, "_send_to_dlq", _spy_dlq, raising=True)

    # Act
    dto = await v.validate_dto(
        payload=invalid,
        dto_class=CommandDTO,
        key="k|korbit|ticker",
    )

    # Assert
    assert dto is None
    assert called["count"] == 1
    assert isinstance(called["reason"], str) and "검증 실패" in called["reason"]
