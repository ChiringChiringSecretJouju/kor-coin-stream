from __future__ import annotations

from dataclasses import dataclass

from core.dto.internal.common import ConnectionScope
from core.types import ConnectionMetaData, ConnectionStatus


@dataclass(slots=True, frozen=True, repr=False, match_args=False, kw_only=True)
class ConnectionMeta:
    """연결 메타데이터 DTO.

    Redis 해시에 저장될 필드 집합을 단일 객체로 캡슐화합니다.
    """

    status: ConnectionStatus
    connection_id: str
    created_at: int
    last_active: int
    scope: ConnectionScope

    def to_redis_mapping(self) -> ConnectionMetaData:
        # Redis에는 문자열 값만 저장하도록 Enum은 `.value`로 변환한다.
        return {
            "status": self.status.value,
            "created_at": self.created_at,
            "last_active": self.last_active,
            "connection_id": self.connection_id,
            "scope": self.scope,
        }


@dataclass(slots=True, frozen=True, repr=False, match_args=False, kw_only=True)
class ConnectionKeyBuilder:
    """연결 스코프에 대한 Redis 키를 생성합니다."""

    scope: ConnectionScope

    def meta(self) -> str:
        return f"ws:connection:{self.scope.exchange}:{self.scope.region}:{self.scope.request_type}"

    def symbols(self) -> str:
        return (
            f"ws:connection:{self.scope.exchange}:{self.scope.region}:"
            f"{self.scope.request_type}:symbols"
        )


@dataclass(slots=True, eq=False, repr=False, match_args=False, kw_only=True)
class WebsocketConnectionSpec:
    """WebsocketConnectionCache 생성 시 필요한 파라미터 묶음.

    - 다중 인자 전달을 DTO로 캡슐화하여 시그니처 단순화 및 유지보수성 향상
    """

    scope: ConnectionScope
    symbols: tuple[str, ...]
