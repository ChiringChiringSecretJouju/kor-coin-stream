from __future__ import annotations

import uuid
from core.dto.io.error_event import (
    WsErrorEventDTO,
    WsEventErrorMetaDTO,
    WsEventErrorTypeDTO,
)
from core.dto.io.target import ConnectionTargetDTO
from core.types import DEFAULT_SCHEMA_VERSION, ErrorCode, ErrorDomain
from common.exceptions.exception_rule import classify_exception
from infra.messaging.connect.producer_client import ErrorEventProducer


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
        raw_context=raw_context or {},
    )
    etype: WsEventErrorTypeDTO = build_error_type(
        error_message={"message": str(err)},
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
