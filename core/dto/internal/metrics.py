from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from typing import Any


@dataclass(slots=True, eq=False, repr=False, match_args=False, kw_only=True)
class MinuteItemDomain:
    """단일 분 집계 항목 DTO.

    - 카운팅/메트릭 전송 경계에서 사용되는 불변 데이터 객체입니다.
    - 외부 전송 이전 단계에서 표준 dict로 직렬화하여 사용합니다.
    """

    minute_start_ts_kst: int
    total: int
    details: dict[str, int]


@dataclass(slots=True, eq=False, repr=False, match_args=False, kw_only=True)
class MinuteStateDomain:
    """내부 상태 캡슐화 (슬롯으로 오버헤드 축소)."""

    kst: timezone
    current_minute_key: int
    total: int
    symbols: dict[str, int]
    # NOTE: buffer는 직렬화 경계 전 단계의 임시 저장소입니다.
    # - 내부 로직은 MinuteItem으로 다루지만, 프로듀서 경계에서 직렬화 용이성을 위해
    #   dict 형태로 보관합니다.
    # - dict 값 타입에 Any를 사용하는 이유: 세부 필드 확장(추가 메타) 가능성과
    #   직렬화 층(프로듀서)에서의 유연성을 허용하기 위함입니다.
    buffer: list[dict[str, Any]]
