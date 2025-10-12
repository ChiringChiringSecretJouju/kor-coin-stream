"""명령 및 대상 DTO 통합 모듈

연결 명령, 연결 성공, 대상 정보 등 명령 관련 모든 DTO를 포함합니다.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictStr, StringConstraints

from src.core.dto.io._base import BaseIOModelDTO
from src.core.types import DEFAULT_SCHEMA_VERSION, ExchangeName, Region, RequestType, SocketParams

# ========================================
# 대상 (Target)
# ========================================


class ConnectionTargetDTO(BaseModel):
    """이벤트 대상(Target) Pydantic v2 모델."""

    exchange: ExchangeName
    region: Region
    request_type: RequestType

    model_config = ConfigDict(use_enum_values=True, extra="forbid")

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


class ConnectRequestDTO(BaseIOModelDTO):
    """연결 요청 이벤트 스키마(IO 모델)."""

    socket_mode: str
    symbols: str | list[str]
    orderbook_depth: int | None = None
    realtime_only: bool
    correlation_id: str | None = None


class ConnectionConfigDTO(BaseIOModelDTO):
    """연결 스키마(IO 모델)."""

    url: str
    socket_params: SocketParams


class CommandDTO(BaseIOModelDTO):
    """명령 스키마(IO 모델)."""

    type: str
    action: str
    target: dict[str, str]
    symbols: list[str]
    connection: ConnectionConfigDTO
    projection: list[str] | None = None
    schema_version: str | None = None
    ttl_ms: int | None = None
    routing: dict[str, Any] | None = None
    ts_issue: int | None = None
    ts_ingest: int | None = None
    timestamp: str | None = None
    correlation_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class DisconnectCommandDTO(BaseIOModelDTO):
    """연결 해제 명령 이벤트 스키마."""

    type: str
    action: str
    target: dict[str, str]
    reason: str | None = None
    correlation_id: str | None = None
    meta: dict[str, Any] | None = None

    model_config = ConfigDict(extra="forbid")


class ConnectSuccessMetaDTO(BaseIOModelDTO):
    """connect-success 이벤트 메타.

    - schema_version은 `DEFAULT_SCHEMA_VERSION` 기본값 적용
    - observed_key는 "exchange/region/request_type" 형식 강제
    - correlation_id는 선택(엄격한 문자열 제약)
    """

    schema_version: SchemaVersionStr = DEFAULT_SCHEMA_VERSION
    correlation_id: CorrelationIdStr = Field(default_factory=lambda: str(uuid.uuid4()))
    observed_key: ObservedKeyStr


class ConnectSuccessEventDTO(BaseIOModelDTO):
    """웹소켓 연결 성공(ACK) 이벤트 DTO.

    - action은 "connect_success"로 고정
    - ack는 "clear"로 고정
    - symbol은 엄격한 문자열 제약을 적용
    - timestamp_utc는 UTC aware 현재 시각 기본값
    - extra 필드 금지
    """

    action: Literal["connect_success"]
    ack: Literal["clear"]
    target: ConnectionTargetDTO
    symbol: SymbolStr
    timestamp_utc: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    meta: ConnectSuccessMetaDTO

    model_config = ConfigDict(extra="forbid")
