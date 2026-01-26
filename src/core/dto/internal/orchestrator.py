from __future__ import annotations

from dataclasses import dataclass

from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.types import SocketParams


@dataclass(slots=True, frozen=True, eq=False, repr=False, match_args=False, kw_only=True)
class StreamContextDomain:
    """스트림 실행 컨텍스트(도메인).
    
    Orchestrator가 연결을 생성/관리하는 데 필요한 모든 정보를 담습니다.
    """

    scope: ConnectionScopeDomain
    url: str
    socket_params: SocketParams
    symbols: tuple[str, ...]
    projection: list[str] | None = None
    correlation_id: str | None = None
