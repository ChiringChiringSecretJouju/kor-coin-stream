"""Coinone Orderbook 파서 (Coinone → StandardOrderbookDTO 완전 변환).

Coinone 메시지 포맷:
{
  "response_type": "DATA",
  "channel": "ORDERBOOK",
  "data": {
    "quote_currency": "KRW",
    "target_currency": "XRP",
    "asks": [{"price": "695.5", "qty": "5000"}],
    "bids": [{"price": "687.4", "qty": "6704.0994"}]
  }
}

최종 출력 포맷 (StandardOrderbookDTO → dict):
{
  "symbol": "XRP",
  "quote_currency": "KRW",
  "timestamp": 1760505707687,
  "asks": [{"price": "695.5", "size": "5000"}],
  "bids": [{"price": "687.4", "size": "6704.0994"}]
}
"""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.number_format import normalize_number_string
from src.core.connection.utils.parsers.base import OrderbookParser
from src.core.connection.utils.timestamp import get_regional_timestamp_ms
from src.core.dto.io.realtime import OrderbookItemDTO, StandardOrderbookDTO


class CoinoneOrderbookParser(OrderbookParser):
    """Coinone Orderbook 파서 (완전 표준화).
    
    Coinone 특징:
    - response_type: "DATA" 필터링 필요
    - data 내부에 asks/bids 이미 분리됨
    - quote_currency, target_currency 명시적 제공
    - 필드명: price, qty (문자열)
    
    변환 과정:
    1. response_type="DATA" 확인
    2. data에서 target_currency → symbol
    3. qty → size 변환
    4. StandardOrderbookDTO 생성
    """
    
    def can_parse(self, message: dict[str, Any]) -> bool:
        """data 내부에 asks/bids + currencies 존재 여부 + response_type='DATA' 확인.
        
        Args:
            message: 원본 메시지
        
        Returns:
            파싱 가능하면 True
        """
        # response_type이 DATA인 경우만 파싱
        if message.get("response_type") != "DATA":
            return False
        
        data = message.get("data")
        if not isinstance(data, dict):
            return False
        return (
            "asks" in data
            and "bids" in data
            and "quote_currency" in data
            and "target_currency" in data
        )
    
    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """Coinone → 표준 포맷 (qty → size, Pydantic DTO).
        
        Args:
            message: Coinone 원본 메시지
        
        Returns:
            표준화된 orderbook (Pydantic 검증 완료)
        """
        # 1. 심볼 (이미 제공됨)
        data: dict[str, Any] = message.get("data", {})
        symbol: str = data.get("target_currency", "")
        quote: str = data.get("quote_currency", "")
        
        # 2. asks/bids 변환 (qty → size, Pydantic DTO 생성)
        # 과학적 표기법 처리
        asks = [
            OrderbookItemDTO(
                price=normalize_number_string(item["price"]),
                size=normalize_number_string(item.get("qty", ""))
            )
            for item in data.get("asks", [])
            if isinstance(item, dict) and "price" in item
        ]
        
        bids = [
            OrderbookItemDTO(
                price=normalize_number_string(item["price"]),
                size=normalize_number_string(item.get("qty", ""))
            )
            for item in data.get("bids", [])
            if isinstance(item, dict) and "price" in item
        ]
        
        return StandardOrderbookDTO(
            symbol=symbol,
            quote_currency=quote,
            timestamp=get_regional_timestamp_ms("korea"),
            asks=asks,
            bids=bids,
        )
