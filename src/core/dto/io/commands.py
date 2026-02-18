"""명령 및 대상 DTO 통합 모듈

연결 명령, 연결 성공, 대상 정보 등 명령 관련 모든 DTO를 포함합니다.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, StrictStr, StringConstraints

from src.core.dto.io._base import OPTIMIZED_CONFIG, BaseIOModelDTO, TimestampedModel
from src.core.types import DEFAULT_SCHEMA_VERSION, ExchangeName, Region, RequestType, SocketParams

# ========================================
# 대상 (Target)
# ========================================


class ConnectionTargetDTO(BaseModel):
    """이벤트 대상(Target) Pydantic v2 모델 (불변, 최적화됨).

    거래소, 지역, 요청 타입을 식별하는 핵심 모델입니다.
    frozen=True로 불변성을 보장하여 해시 가능하고 안전합니다.
    """

    exchange: ExchangeName = Field(..., description="거래소 (upbit, binance, etc)")
    region: Region = Field(..., description="지역 (korea, asia, etc)")
    request_type: RequestType = Field(..., description="요청 타입 (ticker, orderbook, trade)")

    model_config = OPTIMIZED_CONFIG


# --- Strict and constrained DTOs for connect-success event ---
# Pydantic v2: 검증단 빡빡하게 (StrictStr + StringConstraints)

SymbolStr = Annotated[
    StrictStr,
    StringConstraints(
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9._-]+$",
        strip_whitespace=True,
    ),
]

# UUID (36) or hex (32), keep tight but flexible.
CorrelationIdStr = Annotated[
    StrictStr,
    StringConstraints(
        min_length=16,
        max_length=64,
        pattern=r"^[A-Fa-f0-9-]{16,64}$",
        strip_whitespace=True,
    ),
]

# exchange/region/request_type
ObservedKeyStr = Annotated[
    StrictStr,
    StringConstraints(
        min_length=5,
        max_length=128,
        pattern=r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$",
        strip_whitespace=True,
    ),
]

SchemaVersionStr = Annotated[
    StrictStr,
    StringConstraints(
        min_length=1,
        max_length=16,
        pattern=r"^\d+(?:\.\d+){1,2}$",
        strip_whitespace=True,
    ),
]


class ConnectionConfigDTO(BaseIOModelDTO):
    """연결 스키마(IO 모델)."""

    url: str
    socket_params: SocketParams


class CommandDTO(BaseIOModelDTO):
    """명령 스키마(IO 모델) - 최적화됨.

    ws.command 토픽에서 수신하는 연결 제어 명령입니다.
    """

    type: str = Field(..., description="메시지 타입")
    action: str = Field(..., description="액션 (connect_and_subscribe 등)")
    target: ConnectionTargetDTO = Field(..., description="대상 정보")
    symbols: list[str] = Field(..., description="심볼 목록")
    connection: ConnectionConfigDTO = Field(..., description="연결 설정")
    projection: list[str] | None = Field(None, description="프로젝션 필드")
    schema_version: str | None = Field(None, description="스키마 버전")
    ttl_ms: int | None = Field(None, description="TTL (밀리초)", gt=0)
    routing: dict[str, Any] | None = Field(None, description="라우팅 정보")
    ts_issue: int | None = Field(None, description="발행 타임스탬프")
    ts_ingest: int | None = Field(None, description="수신 타임스탬프")
    timestamp: str | None = Field(None, description="타임스탬프 (ISO 8601)")
    correlation_id: str | None = Field(
        None, description="상관관계 ID", validation_alias="ticket_id"
    )


class DisconnectCommandDTO(BaseIOModelDTO):
    """연결 해제 명령 이벤트 스키마 - 최적화됨.

    ws.disconnection 토픽에서 수신하는 연결 종료 명령입니다.
    """

    type: str = Field(..., description="메시지 타입")
    action: str = Field(..., description="액션 (disconnect 등)")
    target: ConnectionTargetDTO = Field(..., description="대상 정보")
    reason: str | None = Field(None, description="종료 사유")
    correlation_id: str | None = Field(None, description="상관관계 ID")
    meta: dict[str, Any] | None = Field(None, description="메타데이터")


class ConnectSuccessMetaDTO(BaseIOModelDTO):
    """connect-success 이벤트 메타.

    - schema_version은 `DEFAULT_SCHEMA_VERSION` 기본값 적용
    - observed_key는 "exchange/region/request_type" 형식 강제
    - correlation_id는 선택(엄격한 문자열 제약)
    """

    ticket_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    schema_version: SchemaVersionStr = DEFAULT_SCHEMA_VERSION
    correlation_id: CorrelationIdStr = Field(default_factory=lambda: str(uuid.uuid4()))
    observed_key: ObservedKeyStr


class ConnectSuccessEventDTO(TimestampedModel):
    """웹소켓 연결 성공(ACK) 이벤트 DTO - 최적화됨 (TimestampedModel 재사용).

    특징:
    - action은 "connect_success"로 고정
    - ack는 "clear"로 고정
    - symbol은 엄격한 문자열 제약 적용
    - timestamp_utc는 자동 생성 (TimestampedModel)
    - 불변 객체 (frozen=True)
    """

    action: Literal["connect_success"] = Field(
        default="connect_success", description="액션 타입 (고정)"
    )
    ack: Literal["clear"] = Field(default="clear", description="ACK 타입 (고정)")
    target: ConnectionTargetDTO = Field(..., description="연결 대상")
    symbol: SymbolStr = Field(..., description="심볼 (엄격한 검증)")
    meta: ConnectSuccessMetaDTO = Field(..., description="메타데이터")
