"""이벤트 DTO 통합 모듈

에러, DLQ, 백프레셔 등 모든 이벤트 DTO를 포함합니다.
재사용 패턴: TimestampedModel 활용으로 코드 중복 제거
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from src.core.dto.io._base import BaseIOModelDTO, TimestampedModel
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


class WsErrorEventDTO(TimestampedModel):
    """웹소켓 에러 이벤트 DTO - 최적화됨 (TimestampedModel 재사용).

    특징:
    - action은 "error"로 고정
    - timestamp_utc 자동 생성 (TimestampedModel)
    - 불변 객체 (frozen=True)
    - extra 필드 금지
    """

    action: Literal["error"] = Field(default="error", description="액션 타입 (고정)")
    target: ConnectionTargetDTO = Field(..., description="에러 대상")
    meta: WsEventErrorMetaDTO = Field(..., description="에러 메타데이터")
    error: WsEventErrorTypeDTO = Field(..., description="에러 상세")


# ========================================
# DLQ 이벤트 (Dead Letter Queue)
# ========================================


class DlqEventDTO(TimestampedModel):
    """DLQ 이벤트 DTO - 최적화됨 (TimestampedModel 재사용).

    특징:
    - action은 "dlq"로 고정
    - timestamp_utc 자동 생성 (TimestampedModel)
    - 불변 객체 (frozen=True)
    - 처리 실패 메시지 복구를 위한 안전망
    """

    action: Literal["dlq"] = Field(default="dlq", description="액션 타입 (고정)")
    reason: str = Field(..., description="DLQ 전송 사유")
    target: ConnectionTargetDTO | None = Field(None, description="대상 정보 (선택)")
    meta: WsEventErrorMetaDTO = Field(..., description="메타데이터")
    detail_error: dict[str, str] = Field(..., description="상세 에러 정보")


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


class BackpressureEventDTO(TimestampedModel):
    """백프레셔 이벤트 DTO - 최적화됨 (TimestampedModel 재사용).

    Producer 큐 백프레셔 발생 시 Kafka로 전송되는 이벤트입니다.
    실시간 모니터링 및 알림에 사용됩니다.

    특징:
    - timestamp_utc 자동 생성 (TimestampedModel)
    - 불변 객체 (frozen=True)

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
    producer_name: str = Field(..., description="Producer 이름 (클래스명)")
    producer_type: str = Field(
        default="AsyncProducerBase", description="Producer 타입"
    )
    status: BackpressureStatusDTO = Field(..., description="백프레셔 상태 정보")
    message: str | None = Field(None, description="추가 메시지")
