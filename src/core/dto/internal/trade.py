"""체결(Trade) 내부 도메인 모델.

내부 처리용 불변 도메인 객체 (dataclass 기반).
"""

from __future__ import annotations

from dataclasses import dataclass

from src.core.types import TradeSide


@dataclass(slots=True, frozen=True, eq=True, repr=True, match_args=False, kw_only=True)
class TradeDataDomain:
    """체결 데이터 도메인 (내부 처리용).
    
    특징:
    - 불변 객체 (frozen=True)
    - 슬롯 최적화 (slots=True)
    - 메모리 효율적
    - I/O DTO와 구조는 동일하나 Pydantic 검증 없음
    
    용도:
    - 내부 비즈니스 로직 처리
    - 배치 수집기 내부 저장
    - 변환 파이프라인 중간 객체
    
    거래소별 매핑:
    - Upbit/Bithumb: code → target_currency, trade_price, trade_volume, ask_bid → side
    - Korbit: symbol → target_currency, data[{price, qty, isBuyerTaker → side}]
    - Coinone: target_currency 직접 제공, is_seller_maker → side
    """

    target_currency: str
    trade_price: str
    trade_volume: str
    trade_timestamp: int
    trade_id: str
    side: TradeSide
    quote_currency: str | None = None
    server_timestamp: int | None = None
    is_buyer_maker: bool | None = None
    exchange_specific: dict[str, str] | None = None
