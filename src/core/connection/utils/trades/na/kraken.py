"""Kraken v2 Trade 파서."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from src.core.connection.utils.parsers.base import TradeParser
from src.core.dto.io.realtime import StandardTradeDTO


class KrakenTradeParser(TradeParser):
    """Kraken v2 Trade 파서.
    
    특징:
    - {"channel": "trade", "type": "update", "data": [{"symbol": "MATIC/USD", "side": "sell", "price": 0.5117, "qty": 40.0, "trade_id": 4665906, "timestamp": "2023-09-25T07:49:37.708706Z"}]}
    - side: "buy" or "sell"
    - timestamp: ISO 8601 형식
    - price, qty: 숫자 타입
    """
    
    def can_parse(self, message: dict[str, Any]) -> bool:
        """channel == "trade"이고 data 필드가 있는지 확인.
        
        Args:
            message: 원본 메시지
        
        Returns:
            파싱 가능하면 True
        """
        channel = message.get("channel", "")
        data = message.get("data", [])
        
        return (
            isinstance(channel, str)
            and channel == "trade"
            and isinstance(data, list)
            and len(data) > 0
        )
    
    def parse(self, message: dict[str, Any]) -> StandardTradeDTO:
        """Kraken → 표준 포맷 (첫 번째 trade만 사용).
        
        Args:
            message: Kraken 원본 메시지
        
        Returns:
            표준화된 trade (Pydantic 검증 완료)
        """
        data = message.get("data", [])
        if not data:
            return self._create_empty_trade()
        
        trade = data[0]
        
        # symbol: "MATIC/USD" → "MATIC-USD" (하이픈으로 통일)
        symbol_raw = trade.get("symbol", "")
        code = symbol_raw.replace("/", "-") if symbol_raw else "UNKNOWN"
        
        # Side 변환: "buy" → BID, "sell" → ASK
        side_raw = trade.get("side", "buy")
        side = "BID" if side_raw.lower() == "buy" else "ASK"
        
        # ISO 8601 → Unix timestamp (밀리초)
        timestamp_str = trade.get("timestamp", "")
        timestamp = self._parse_iso_timestamp(timestamp_str)
        
        return StandardTradeDTO(
            code=code,
            trade_timestamp=timestamp,
            trade_price=float(trade.get("price", 0)),
            trade_volume=float(trade.get("qty", 0)),
            ask_bid=side,
            sequential_id=str(trade.get("trade_id", "")),
        )
    
    def _parse_iso_timestamp(self, time_str: str) -> int:
        """ISO 8601 문자열을 Unix timestamp (밀리초)로 변환.
        
        Args:
            time_str: "2023-09-25T07:49:37.708706Z"
        
        Returns:
            Unix timestamp (밀리초)
        """
        if not time_str:
            return 0
        
        try:
            dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except (ValueError, AttributeError):
            return 0
    
    def _create_empty_trade(self) -> StandardTradeDTO:
        """빈 Trade DTO 생성."""
        return StandardTradeDTO(
            code="UNKNOWN",
            trade_timestamp=0,
            trade_price=0.0,
            trade_volume=0.0,
            ask_bid="BID",
            sequential_id="0",
        )
