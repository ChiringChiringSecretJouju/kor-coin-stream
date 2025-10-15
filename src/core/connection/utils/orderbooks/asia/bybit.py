"""Bybit Spot Orderbook 파서."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import OrderbookParser, parse_symbol
from src.core.connection.utils.timestamp import get_regional_timestamp_ms
from src.core.dto.io.realtime import OrderbookItemDTO, StandardOrderbookDTO


class BybitOrderbookParser(OrderbookParser):
    """Bybit Spot Orderbook 파서.
    
    특징:
    - v5 통합 API (Spot/Linear/Inverse 동일 구조)
    - {"topic": "orderbook.50.BTCUSDT", "data": {"s": "BTCUSDT", "b": [...], "a": [...]}}
    - 배열 구조: ["price", "qty"] (문자열)
    """
    
    def can_parse(self, message: dict[str, Any]) -> bool:
        """topic에 "orderbook"이 포함되고 data 필드가 있는지 확인.
        
        Args:
            message: 원본 메시지
        
        Returns:
            파싱 가능하면 True
        """
        topic = message.get("topic", "")
        data = message.get("data", {})
        
        return (
            isinstance(topic, str)
            and "orderbook" in topic
            and isinstance(data, dict)
            and "s" in data
            and "b" in data
            and "a" in data
        )
    
    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """Bybit → 표준 포맷.
        
        Args:
            message: Bybit 원본 메시지
        
        Returns:
            표준화된 orderbook (Pydantic 검증 완료)
        """
        data = message.get("data", {})
        
        # 심볼 추출: "BTCUSDT" → ("BTC", "USDT")
        symbol_raw: str = data.get("s", "")
        symbol, quote = parse_symbol(symbol_raw) if symbol_raw else ("", None)
        
        # 배열 → DTO 변환
        bids_raw = data.get("b", [])
        asks_raw = data.get("a", [])
        
        asks = [
            OrderbookItemDTO(price=item[0], size=item[1])
            for item in asks_raw
            if isinstance(item, list) and len(item) >= 2
        ]
        
        bids = [
            OrderbookItemDTO(price=item[0], size=item[1])
            for item in bids_raw
            if isinstance(item, list) and len(item) >= 2
        ]
        
        # 타임스탬프: Bybit의 ts 필드 사용 (밀리초)
        timestamp = message.get("ts", get_regional_timestamp_ms("korea"))
        
        return StandardOrderbookDTO(
            symbol=symbol,
            quote_currency=quote or "",
            timestamp=timestamp,
            asks=asks,
            bids=bids,
        )
