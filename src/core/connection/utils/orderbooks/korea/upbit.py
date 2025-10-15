"""Upbit Orderbook 파서 (Pair → Separate 변환)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.orderbooks.korea.base import (
    OrderbookParser,
    parse_symbol,
)
from src.core.connection.utils.timestamp import get_regional_timestamp_ms
from src.core.dto.io.realtime import OrderbookItemDTO, StandardOrderbookDTO


class UpbitOrderbookParser(OrderbookParser):
    """Upbit Orderbook 파서.
    
    특징:
    - orderbook_units 배열 (ask/bid 쌍으로 묶임)
    - code: "KRW-BTC"
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
        
        asks = [
            OrderbookItemDTO(price=str(u["ask_price"]), size=str(u.get("ask_size", "")))
            for u in units
            if isinstance(u, dict) and "ask_price" in u
        ]
        
        bids = [
            OrderbookItemDTO(price=str(u["bid_price"]), size=str(u.get("bid_size", "")))
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
