"""Upbit Orderbook 파서 (Upbit → StandardOrderbookDTO 완전 변환).

Upbit 메시지 포맷:
{
  "type": "orderbook",
  "code": "KRW-BTC",
  "orderbook_units": [
    {"ask_price": 137002000, "bid_price": 137001000, "ask_size": 0.10623869, "bid_size": 0.03656812}
  ]
}

최종 출력 포맷 (StandardOrderbookDTO → dict):
{
  "symbol": "BTC",
  "quote_currency": "KRW",
  "timestamp": 1760505707687,
  "asks": [{"price": "137002000", "size": "0.10623869"}],
  "bids": [{"price": "137001000", "size": "0.03656812"}]
}
"""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.number_format import normalize_number_string
from src.core.connection.utils.parsers.base import OrderbookParser, parse_symbol
from src.core.connection.utils.timestamp import get_regional_timestamp_ms
from src.core.dto.io.realtime import OrderbookItemDTO, StandardOrderbookDTO


class UpbitOrderbookParser(OrderbookParser):
    """Upbit Orderbook 파서 (완전 표준화).
    
    Upbit 특징:
    - orderbook_units 배열: ask/bid 쌍으로 묶여 있음
    - code: "KRW-BTC" 형식
    - 가격/수량: 숫자 타입 (과학적 표기법 가능)
    
    변환 과정:
    1. code → symbol + quote_currency 분리
    2. orderbook_units → asks/bids 분리
    3. 과학적 표기법 → 일반 숫자 문자열 변환
    4. StandardOrderbookDTO 생성 (Pydantic 검증)
    """
    
    def can_parse(self, message: dict[str, Any]) -> bool:
        """orderbook_units 필드로 판단.
        
        Args:
            message: 원본 메시지
        
        Returns:
            파싱 가능하면 True
        """
        return "orderbook_units" in message and isinstance(
            message.get("orderbook_units"), list
        )
    
    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """Upbit → 표준 포맷 (Pair → Separate, Pydantic DTO).
        
        Args:
            message: Upbit 원본 메시지
        
        Returns:
            표준화된 orderbook (Pydantic 검증 완료)
        """
        # 1. 심볼 추출
        code: str = message.get("code", "")
        symbol, quote = parse_symbol(code) if code else ("", None)
        
        # 2. Pair → Separate 변환 (Pydantic DTO 생성)
        units: list[dict[str, Any]] = message.get("orderbook_units", [])
        
        # 과학적 표기법 처리 (5.883e-05 → 0.00005883)
        asks = [
            OrderbookItemDTO(
                price=normalize_number_string(u["ask_price"]),
                size=normalize_number_string(u.get("ask_size", ""))
            )
            for u in units
            if isinstance(u, dict) and "ask_price" in u
        ]
        
        bids = [
            OrderbookItemDTO(
                price=normalize_number_string(u["bid_price"]),
                size=normalize_number_string(u.get("bid_size", ""))
            )
            for u in units
            if isinstance(u, dict) and "bid_price" in u
        ]
        
        return StandardOrderbookDTO(
            symbol=symbol,
            quote_currency=quote or "",
            timestamp=get_regional_timestamp_ms("korea"),
            asks=asks,
            bids=bids,
        )
