from __future__ import annotations

import traceback
import uuid
from datetime import datetime
from typing import Any

from src.common.exceptions.exception_rule import classify_exception
from src.core.dto.io.error_event import (
    WsErrorEventDTO,
    WsEventErrorMetaDTO,
    WsEventErrorTypeDTO,
)
from src.core.dto.io.target import ConnectionTargetDTO
from src.core.types import DEFAULT_SCHEMA_VERSION, ErrorCode, ErrorDomain
from src.infra.messaging.connect.producer_client import ErrorEventProducer


def _make_json_serializable(obj: Any) -> Any:
    """datetime 객체를 JSON 직렬화 가능한 형태로 변환합니다."""
    match obj:
        case datetime():
            return obj.isoformat()
        case dict():
            return {k: _make_json_serializable(v) for k, v in obj.items()}
        case list():
            return [_make_json_serializable(item) for item in obj]
        case _:
            return obj


def build_error_meta(observed_key: str, raw_context: dict) -> WsEventErrorMetaDTO:
    return WsEventErrorMetaDTO(
        schema_version=DEFAULT_SCHEMA_VERSION,
        correlation_id=uuid.uuid4().hex,
        observed_key=observed_key,
        raw_context=raw_context,
    )


def build_error_type(
    error_message: dict[str, str],
    error_domain: ErrorDomain,
    error_code: ErrorCode,
) -> WsEventErrorTypeDTO:
    return WsEventErrorTypeDTO(
        error_message=error_message,
        error_domain=error_domain,
        error_code=error_code,
    )


async def make_ws_error_event_from_kind(
    target: ConnectionTargetDTO,
    err: BaseException,
    kind: str,
    observed_key: str = "",
    raw_context: dict | None = None,
    producer: ErrorEventProducer | None = None,
) -> bool:
    """규칙 기반 분류(classify_exception)로 DTO를 만들고, ErrorEventProducer로 전송합니다.

    - error_message에는 err 문자열을 담습니다.
    - observed_key/raw_context는 호출자가 넘겨준 값을 그대로 사용(필수는 아님)
    - 성공 시 True 반환
    """
    domain, code, _retryable = classify_exception(err, kind)
    meta: WsEventErrorMetaDTO = build_error_meta(
        observed_key=observed_key,
        raw_context=_make_json_serializable(raw_context or {}),
    )
    etype: WsEventErrorTypeDTO = build_error_type(
        error_message={
            "message": str(err),
            "detil_error": traceback.format_exc(),
        },
        error_domain=domain,
        error_code=code,
    )
    error_event = WsErrorEventDTO(
        action="error",
        target=target,
        meta=meta,
        error=etype,
    )
    producer = producer or ErrorEventProducer()
    await producer.send_error_event(error_event)
    return True
