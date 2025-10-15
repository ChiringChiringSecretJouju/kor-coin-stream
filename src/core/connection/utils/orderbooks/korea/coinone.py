"""Coinone Orderbook 파서 (qty → size 변환)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.orderbooks.korea.base import OrderbookParser
from src.core.connection.utils.timestamp import get_regional_timestamp_ms
from src.core.dto.io.realtime import OrderbookItemDTO, StandardOrderbookDTO


class CoinoneOrderbookParser(OrderbookParser):
    """Coinone Orderbook 파서.
    
    특징:
    - data.asks/bids 이미 분리됨
    - quote_currency, target_currency 명시적 제공
    - 필드명: price, qty
    """
    
    def can_parse(self, message: dict[str, Any]) -> bool:
        """data 내부에 asks/bids + currencies 존재 여부.
        
        Args:
            message: 원본 메시지
        
        Returns:
            파싱 가능하면 True
        """
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
        asks = [
            OrderbookItemDTO(price=str(item["price"]), size=str(item.get("qty", "")))
            for item in data.get("asks", [])
            if isinstance(item, dict) and "price" in item
        ]
        
        bids = [
            OrderbookItemDTO(price=str(item["price"]), size=str(item.get("qty", "")))
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
