from __future__ import annotations

from dataclasses import dataclass

from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.types import ConnectionStatus


@dataclass(slots=True, frozen=True, repr=False, match_args=False, kw_only=True)
class ConnectionMetaDomain:
    """연결 메타데이터(내부 불변 DTO).

    - 도메인 내부 표현을 담고, 외부 저장용 매핑은 `to_redis_mapping()`으로 제공합니다.
    - status는 Enum으로 유지하고, Redis 저장 시 문자열 값으로 변환합니다.
    """

    status: ConnectionStatus
    connection_id: str
    created_at: int
    last_active: int
    scope: ConnectionScopeDomain

    def to_redis_mapping(self) -> dict[str, str | int]:
        """Redis 해시로 쓰기 위한 매핑(dict)을 생성합니다.

        - Enum은 문자열 저장을 위해 `.value`로 변환합니다
        - scope는 개별 필드로 분해하여 저장합니다
        """
        return {
            "status": self.status.value,
            "created_at": self.created_at,
            "last_active": self.last_active,
            "connection_id": self.connection_id,
            "exchange": self.scope.exchange,
            "region": self.scope.region,
            "request_type": self.scope.request_type,
        }


@dataclass(slots=True, frozen=True, repr=False, match_args=False, kw_only=True)
class ConnectionKeyBuilderDomain:
    """연결 스코프별 Redis 키 빌더."""

    scope: ConnectionScopeDomain

    def meta(self) -> str:
        return f"ws:connection:{self.scope.exchange}:{self.scope.region}:{self.scope.request_type}"

    def symbols(self) -> str:
        return (
            f"ws:connection:{self.scope.exchange}:{self.scope.region}:"
            f"{self.scope.request_type}:symbols"
        )


@dataclass(slots=True, eq=False, repr=False, match_args=False, kw_only=True)
class WebsocketConnectionSpecDomain:
    """WebsocketConnectionCache 생성 시 필요한 파라미터 묶음.

    - 다중 인자 전달을 DTO로 캡슐화하여 시그니처 단순화 및 유지보수성 향상
    """

    scope: ConnectionScopeDomain
    symbols: tuple[str, ...]
