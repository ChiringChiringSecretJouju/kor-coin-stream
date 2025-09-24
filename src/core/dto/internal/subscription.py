from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.core.types import SocketParams


@dataclass(slots=True, frozen=True, eq=True, repr=False, match_args=False, kw_only=True)
class SubscriptionStateDomain:
    """구독 상태 도메인 객체 (내부용)"""

    current_params: SocketParams | None = None
    symbols: list[str] | None = None
    subscribe_type: str | None = None


@dataclass(slots=True, frozen=True, eq=True, repr=False, match_args=False, kw_only=True)
class SymbolMergeResultDomain:
    """심볼 병합 결과 도메인 객체 (내부용)"""

    merged_params: dict[str, Any] | None
    has_new_symbols: bool
    total_symbols: int
