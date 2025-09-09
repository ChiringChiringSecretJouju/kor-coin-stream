from typing import Any, Literal

from core.dto.io._base import BaseIOModelDTO

from core.types import ErrorCode, ErrorDomain

from core.dto.io.target import ConnectionTargetDTO


class WsEventErrorMetaDTO(BaseIOModelDTO):
    """에러 메타데이터 DTO (I/O 경계용 Pydantic v2 모델).

    - 외부로 내보내는 스키마 계약을 엄격히 보장합니다.
    - Enum 값은 직렬화 시 문자열 값으로 출력됩니다.
    """

    schema_version: str
    correlation_id: str | None = None
    observed_key: str | None = None
    raw_context: dict[str, Any] | None = None
    error_domain: ErrorDomain | None = None
    error_code: ErrorCode | None = None


class WsErrorEventDTO(BaseIOModelDTO):
    """웹소켓 에러 이벤트 DTO (I/O 경계용 Pydantic v2 모델).

    - action은 "error"로 고정합니다.
    - unknown/extra 필드는 금지합니다.
    """

    action: Literal["error"]
    message: str
    target: ConnectionTargetDTO
    meta: WsEventErrorMetaDTO


class ErrorEventRequestDTO(BaseIOModelDTO):
    """에러 이벤트 요청 DTO.

    - 프로듀서 메서드의 장황한 인자를 단일 DTO로 캡슐화합니다.
    - 내부적으로 `make_ws_error_event(...)` 호출로 최종 `WsErrorEventDTO`를 생성합니다.
    """

    error_domain: ErrorDomain
    error_code: ErrorCode
    message: str
    target: ConnectionTargetDTO
    correlation_id: str | None = None
    observed_key: str | None = None
    raw_context: dict[str, Any] | None = None
