"""I/O 경계 DTO 기반 클래스 및 Generic 재사용 모듈

Pydantic v2 Generic 패턴을 활용하여 코드 중복을 최소화합니다.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from src.core.types import ExchangeName, Region, RequestType

# ========================================
# ConfigDict 최적화 (전역 설정)
# ========================================

# Python 3.12 최적화된 ConfigDict
OPTIMIZED_CONFIG = ConfigDict(
    # 런타임 검증
    use_enum_values=True,  # Enum → 값 직렬화
    extra="forbid",  # 알 수 없는 필드 금지
    validate_default=True,  # 기본값도 검증
    str_strip_whitespace=True,  # 문자열 자동 트림
    # 불변성 (성능 향상)
    frozen=True,  # 불변 객체 (해시 가능, 안전)
    # 타입 안전성
    arbitrary_types_allowed=False,  # 검증 가능한 타입만 허용
    # JSON 스키마
    json_schema_extra={
        "$schema": "https://json-schema.org/draft/2020-12/schema",
    },
)

# 가변 모델용 ConfigDict (일부 DTO는 가변이 필요)
MUTABLE_CONFIG = ConfigDict(
    use_enum_values=True,
    extra="forbid",
    validate_default=True,
    str_strip_whitespace=True,
    frozen=False,  # 가변
    arbitrary_types_allowed=False,
)


# ========================================
# 베이스 클래스
# ========================================


class BaseIOModelDTO(BaseModel):
    """I/O 경계용 공통 Pydantic v2 베이스 모델.

    특징:
    - 불변 객체 (frozen=True)
    - Enum 직렬화를 값(value)로 고정
    - 알 수 없는 필드 금지 (extra="forbid")
    - ticket_id는 자동으로 UUID 생성
    - 문자열 자동 트림 (str_strip_whitespace=True)
    - 기본값 검증 (validate_default=True)
    """

    ticket_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="고유 티켓 ID (UUID)",
    )
    model_config = OPTIMIZED_CONFIG


class MutableBaseIOModelDTO(BaseModel):
    """가변 I/O 경계 DTO (불변성이 불필요한 경우).

    사용 사례:
    - 내부 상태 변경이 필요한 DTO
    - 캐시/임시 데이터 구조
    """

    ticket_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="고유 티켓 ID (UUID)",
    )
    model_config = MUTABLE_CONFIG


# ========================================
# Generic 재사용 패턴
# ========================================

T = TypeVar("T")
PayloadT = TypeVar("PayloadT", bound=BaseModel)


class TimestampedModel(BaseIOModelDTO):
    """자동 타임스탬프 생성 믹스인 (불변).

    UTC 타임스탬프를 자동으로 생성하는 모든 이벤트의 베이스입니다.

    Example:
        >>> class MyEvent(TimestampedModel):
        ...     action: str
        ...     data: dict
    """

    timestamp_utc: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="이벤트 발생 시각 (UTC ISO 8601)",
    )


class MarketContextModel(BaseIOModelDTO):
    """마켓 컨텍스트 재사용 믹스인 (region, exchange, request_type).

    거래소 관련 모든 이벤트의 공통 컨텍스트입니다.

    Example:
        >>> class MetricsEvent(MarketContextModel):
        ...     total_count: int
        ...     details: dict
    """

    region: Region = Field(..., description="지역 (korea, asia, etc)")
    exchange: ExchangeName = Field(..., description="거래소 (upbit, binance, etc)")
    request_type: RequestType = Field(
        ..., description="요청 타입 (ticker, orderbook, trade)"
    )


class EventEnvelope(BaseIOModelDTO, Generic[PayloadT]):
    """Generic 이벤트 봉투 (Envelope Pattern).

    공통 메타데이터와 함께 임의의 페이로드를 감싸는 범용 래퍼입니다.

    타입 안전성:
    - PayloadT: Pydantic BaseModel 서브클래스만 허용

    Example:
        >>> class MyPayload(BaseModel):
        ...     data: str
        >>>
        >>> event = EventEnvelope[MyPayload](
        ...     event_type="my_event",
        ...     payload=MyPayload(data="test")
        ... )
    """

    event_type: str = Field(..., description="이벤트 타입 (분류용)")
    timestamp_utc: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="이벤트 발생 시각 (UTC ISO 8601)",
    )
    correlation_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="상관관계 ID (분산 추적용)",
    )
    payload: PayloadT = Field(..., description="이벤트 페이로드 (타입 안전)")
    metadata: dict[str, str] | None = Field(
        None, description="추가 메타데이터 (선택사항)"
    )


class TargetedEvent(TimestampedModel, Generic[PayloadT]):
    """타겟팅된 이벤트 (target 필드 포함, Generic).

    특정 거래소/지역/요청 타입을 대상으로 하는 이벤트의 베이스입니다.

    자동 포함:
    - timestamp_utc (TimestampedModel)
    - ticket_id (BaseIOModelDTO)

    Example:
        >>> from src.core.dto.io.commands import ConnectionTargetDTO
        >>>
        >>> class ErrorPayload(BaseModel):
        ...     error_code: str
        ...     message: str
        >>>
        >>> event = TargetedEvent[ErrorPayload](
        ...     action="error",
        ...     target=ConnectionTargetDTO(...),
        ...     payload=ErrorPayload(error_code="E001", message="fail")
        ... )
    """

    action: str = Field(..., description="액션 타입")
    # NOTE: target은 순환 참조 방지를 위해 하위 클래스에서 정의
    payload: PayloadT = Field(..., description="이벤트 페이로드")
