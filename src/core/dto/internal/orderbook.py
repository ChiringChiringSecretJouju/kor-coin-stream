"""오더북 내부 도메인 모델.

내부 처리용 불변 도메인 객체 (dataclass 기반).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True, eq=True, repr=True, match_args=False, kw_only=True)
class OrderbookLevelDomain:
    """호가 레벨 도메인 (단일 호가).
    
    특징:
    - 불변 객체 (frozen=True)
    - 슬롯 최적화 (slots=True)
    - 문자열 타입으로 정밀도 보존
    """

    price: str
    size: str


@dataclass(slots=True, frozen=True, eq=True, repr=True, match_args=False, kw_only=True)
class OrderbookDataDomain:
    """오더북 데이터 도메인 (내부 처리용).
    
    특징:
    - 불변 객체 (frozen=True)
    - 슬롯 최적화 (slots=True)
    - 메모리 효율적
    - I/O DTO와 구조는 동일하나 Pydantic 검증 없음
    
    용도:
    - 내부 비즈니스 로직 처리
    - 배치 수집기 내부 저장
    - 변환 파이프라인 중간 객체
    """

    target_currency: str
    timestamp: int
    asks: list[OrderbookLevelDomain]
    bids: list[OrderbookLevelDomain]
    quote_currency: str | None = None
    orderbook_timestamp: int | None = None
    total_ask_size: str | None = None
    total_bid_size: str | None = None
    is_snapshot: bool | None = None
    level: int | None = None
    exchange_specific: dict[str, str] | None = None
