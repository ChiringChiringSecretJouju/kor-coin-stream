from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from core.types import (
    ErrorCategory,
    ExceptionGroup,
    ExchangeName,
    Region,
    RequestType,
    RuleKind,
)


@dataclass(slots=True, frozen=True, eq=True, repr=False, match_args=False, kw_only=True)
class ConnectionScopeDomain:
    """연결 스코프(내부 도메인 값 객체).

    - (exchange, region, request_type) 조합을 공통 타입으로 정의
    - Redis 키 생성, 캐시 스펙 등에서 재사용
    """

    region: Region
    exchange: ExchangeName
    request_type: RequestType


@dataclass(slots=True, repr=False, eq=False, match_args=False, kw_only=True)
class ConnectionPolicyDomain:
    """웹소켓 연결/백오프/하트비트/워치독 정책(도메인)."""

    # 백오프
    initial_backoff: float = 1.0
    max_backoff: float = 30.0
    backoff_multiplier: float = 2.0
    jitter: float = 0.2  # +/- 20%

    # 하트비트
    heartbeat_kind: Literal["frame", "text"] = "frame"
    heartbeat_message: str | None = None
    heartbeat_timeout: float = 10.0
    heartbeat_fail_limit: int = 3

    # 워치독
    receive_idle_timeout: int = 120


@dataclass(
    slots=True, frozen=True, eq=False, repr=False, match_args=False, kw_only=True
)
class RuleDomain:
    """예외 분류 규칙(도메인)

    kinds: 규칙이 적용될 경계 종류 ("kafka", "redis", "ws", "infra")
    exc:   매칭할 예외 타입(단일 타입 또는 타입 튜플)
    result: ErrorCategory
    """

    kinds: RuleKind
    exc: ExceptionGroup
    result: ErrorCategory


@dataclass(slots=True, frozen=True, eq=True, repr=False, match_args=False, kw_only=True)
class ConnectRequestDomain:
    """연결 요청(도메인 값 객체).

    - socket_mode: "ticker" | "orderbook" | "trade" 중 하나
    - symbols: 단일 심볼 문자열 또는 복수 심볼 리스트
    - orderbook_depth: 오더북일 때만 사용(선택)
    - realtime_only: 거래소가 지원하면 실시간만 사용
    - correlation_id: 요청 추적용(선택)
    """

    socket_mode: RequestType
    symbols: str | list[str]
    orderbook_depth: int | None = None
    realtime_only: bool = False
    correlation_id: str | None = None
