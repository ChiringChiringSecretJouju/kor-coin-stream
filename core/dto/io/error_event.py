from typing import Literal
from datetime import datetime, timezone
from pydantic import Field
from core.dto.io._base import BaseIOModelDTO

from core.types import ErrorCode, ErrorDomain

from core.dto.io.target import ConnectionTargetDTO


class WsEventErrorTypeDTO(BaseIOModelDTO):
    """에러 타입 DTO (I/O 경계용 Pydantic v2 모델)."""

    error_message: dict
    error_domain: ErrorDomain
    error_code: ErrorCode


class WsEventErrorMetaDTO(BaseIOModelDTO):
    """에러 메타데이터 DTO (I/O 경계용 Pydantic v2 모델).

    - 외부로 내보내는 스키마 계약을 엄격히 보장합니다.
    - Enum 값은 직렬화 시 문자열 값으로 출력됩니다.
    """

    schema_version: str
    correlation_id: str | None = None
    observed_key: str | None = None
    raw_context: dict | None = None


class WsErrorEventDTO(BaseIOModelDTO):
    """웹소켓 에러 이벤트 DTO (I/O 경계용 Pydantic v2 모델).

    - action은 "error"로 고정합니다.
    - unknown/extra 필드는 금지합니다.
    """

    action: Literal["error"]
    # UTC 기준 시각을 추가 제공합니다(후방 호환). 기본값은 현재 UTC.
    error_timestamp_utc: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    target: ConnectionTargetDTO
    meta: WsEventErrorMetaDTO
    error: WsEventErrorTypeDTO
