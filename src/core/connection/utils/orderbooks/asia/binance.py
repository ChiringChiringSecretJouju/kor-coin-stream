"""Binance Spot Orderbook 파서."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import OrderbookParser, parse_symbol
from src.core.connection.utils.timestamp import get_regional_timestamp_ms
from src.core.dto.io.realtime import OrderbookItemDTO, StandardOrderbookDTO


class BinanceOrderbookParser(OrderbookParser):
    """Binance Spot Orderbook 파서.
    
    특징:
    - Diff. Depth Stream: {"e": "depthUpdate", "s": "BNBBTC", "b": [...], "a": [...]}
    - 배열 구조: ["price", "quantity"] (문자열)
    """
    
    def can_parse(self, message: dict[str, Any]) -> bool:
        """depthUpdate 이벤트로 판단.
        
        Args:
            message: 원본 메시지
        
        Returns:
            파싱 가능하면 True
        """
        return (
            message.get("e") == "depthUpdate"
            and "s" in message
            and "b" in message
            and "a" in message
        )
    
    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """Binance → 표준 포맷.
        
        Args:
            message: Binance 원본 메시지
        
        Returns:
            표준화된 orderbook (Pydantic 검증 완료)
        """
        # 심볼 추출: "BNBBTC" → ("BNB", "BTC")
        symbol_raw: str = message.get("s", "")
        symbol, quote = parse_symbol(symbol_raw) if symbol_raw else ("", None)
        
        # 배열 → DTO 변환
        bids_raw = message.get("b", [])
        asks_raw = message.get("a", [])
        
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
        
        return StandardOrderbookDTO(
            symbol=symbol,
            quote_currency=quote or "",
            timestamp=get_regional_timestamp_ms("korea"),
            asks=asks,
            bids=bids,
        )
