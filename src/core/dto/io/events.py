"""이벤트 DTO 통합 모듈

에러, DLQ, 백프레셔 등 모든 이벤트 DTO를 포함합니다.
"""

from datetime import datetime, timezone
from typing import Literal

from pydantic import Field

from src.core.dto.io._base import BaseIOModelDTO
from src.core.dto.io.commands import ConnectionTargetDTO
from src.core.types import ErrorCode, ErrorDomain

# ========================================
# 에러 이벤트 (Error Event)
# ========================================


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
    error_timestamp_utc: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    target: ConnectionTargetDTO
    meta: WsEventErrorMetaDTO
    error: WsEventErrorTypeDTO


# ========================================
# DLQ 이벤트 (Dead Letter Queue)
# ========================================


class DlqEventDTO(BaseIOModelDTO):
    """DLQ 이벤트 DTO (I/O 경계용 Pydantic v2 모델).

    - action은 "dlq"로 고정합니다.
    - unknown/extra 필드는 금지합니다.
    """

    action: Literal["dlq"]
    reason: str
    error_timestamp_utc: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    target: ConnectionTargetDTO | None = None
    meta: WsEventErrorMetaDTO
    detail_error: dict[str, str]


# ========================================
# 백프레셔 이벤트 (Backpressure Event)
# ========================================


class BackpressureStatusDTO(BaseIOModelDTO):
    """백프레셔 상태 정보 DTO

    큐의 현재 상태 및 백프레셔 발생 여부를 담습니다.
    """

    queue_size: int = Field(..., description="현재 큐 크기")
    queue_max_size: int = Field(..., description="큐 최대 크기")
    usage_percent: float = Field(..., description="큐 사용률 (%)")
    is_throttled: bool = Field(..., description="백프레셔 활성화 여부")
    high_watermark: int = Field(..., description="High Watermark (throttle 시작)")
    low_watermark: int = Field(..., description="Low Watermark (throttle 해제)")


class BackpressureEventDTO(BaseIOModelDTO):
    """백프레셔 이벤트 DTO (I/O 경계용 Pydantic v2 모델)

    Producer 큐에서 백프레셔가 발생했을 때 Kafka로 전송되는 이벤트입니다.
    실시간 모니터링 및 알림에 사용됩니다.

    Example:
        >>> event = BackpressureEventDTO(
        ...     action="backpressure_activated",
        ...     producer_name="MetricsProducer",
        ...     status=BackpressureStatusDTO(...)
        ... )
    """

    action: Literal[
        "backpressure_activated", "backpressure_deactivated", "queue_status_report"
    ] = Field(..., description="백프레셔 상태 (활성화/비활성화/주기적 리포트)")
    event_timestamp_utc: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="이벤트 발생 시각 (UTC)",
    )
    producer_name: str = Field(..., description="Producer 이름 (클래스명)")
    producer_type: str = Field(default="AsyncProducerBase", description="Producer 타입")
    status: BackpressureStatusDTO = Field(..., description="백프레셔 상태 정보")
    message: str | None = Field(None, description="추가 메시지")
