from typing import Any, Literal

from core.dto.io._base import BaseIOModelDTO
from core.dto.io.error_event import WsEventErrorMetaDTO
from core.dto.io.target import ConnectionTargetDTO
from core.types import ErrorCode, ErrorDomain


class DlqEventDTO(BaseIOModelDTO):
    """DLQ 이벤트 DTO (I/O 경계용 Pydantic v2 모델).

    - action은 "dlq"로 고정합니다.
    - unknown/extra 필드는 금지합니다.
    """

    action: Literal["dlq"]
    reason: str
    original_message: Any
    raw_bytes_b64: str | None = None
    target: ConnectionTargetDTO | None = None
    meta: WsEventErrorMetaDTO


class DlqEventRequestDTO(BaseIOModelDTO):
    """DLQ 이벤트 요청 DTO.

    - 프로듀서 메서드의 장황한 인자를 단일 DTO로 캡슐화합니다.
    - 내부적으로 `make_dlq_event(...)` 호출로 최종 `DlqEventDTO`를 생성합니다.
    """

    reason: str
    original_message: Any
    correlation_id: str | None = None
    raw_bytes_b64: str | None = None
    target: ConnectionTargetDTO | None = None
    observed_key: str | None = None
    error_domain: ErrorDomain | None = None
    error_code: ErrorCode | None = None
    raw_context: dict[str, Any] | None = None
