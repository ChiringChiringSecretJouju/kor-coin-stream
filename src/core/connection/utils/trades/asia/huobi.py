"""Huobi Spot Trade 파서."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import TradeParser
from src.core.dto.io.realtime import StandardTradeDTO


class HuobiTradeParser(TradeParser):
    """Huobi Spot Trade 파서.
    
    특징:
    - {
        "ch": "market.btcusdt.trade.detail",
        "tick": {
            "data": [{
                "price": ..., "amount": ..., "ts": ...,
                "tradeId": ..., "direction": "buy"
            }]
        }
    }
    - direction: "buy" or "sell"
    - tick.data는 배열
    """
    
    def can_parse(self, message: dict[str, Any]) -> bool:
        """ch 필드에 "trade"가 포함되고 tick.data 필드가 있는지 확인.
        
        Args:
            message: 원본 메시지
        
        Returns:
            파싱 가능하면 True
        """
        ch = message.get("ch", "")
        tick = message.get("tick", {})
        
        return (
            isinstance(ch, str)
            and "trade" in ch
            and isinstance(tick, dict)
            and "data" in tick
            and isinstance(tick.get("data"), list)
        )
    
    def parse(self, message: dict[str, Any]) -> StandardTradeDTO:
        """Huobi → 표준 포맷 (첫 번째 trade만 사용).
        
        Args:
            message: Huobi 원본 메시지
        
        Returns:
            표준화된 trade (Pydantic 검증 완료)
        """
        # ch에서 심볼 추출: "market.btcusdt.trade.detail" → "btcusdt"
        ch = message.get("ch", "")
        symbol_raw = ""
        if ch.startswith("market.") and ".trade." in ch:
            parts = ch.split(".")
            if len(parts) >= 3:
                symbol_raw = parts[1]
        
        code = self._format_code(symbol_raw)
        
        # tick.data 배열의 첫 번째 요소
        tick = message.get("tick", {})
        data = tick.get("data", [])
        
        if not data:
            return StandardTradeDTO(
                code=code or "UNKNOWN",
                trade_timestamp=0.0,
                trade_price=0.0,
                trade_volume=0.0,
                ask_bid=1,
                sequential_id="0",
            )
        
        trade = data[0]
        
        # Side 변환: "buy" → BID, "sell" → ASK
        direction = trade.get("direction", "buy")
        side = 1 if direction.lower() == "buy" else -1
        
        return StandardTradeDTO(
            code=code,
            trade_timestamp=float(trade.get("ts", 0)) / 1000.0,
            trade_price=float(trade.get("price", 0)),
            trade_volume=float(trade.get("amount", 0)),
            ask_bid=side,
            sequential_id=str(trade.get("tradeId", "")),
        )
    
    def _format_code(self, symbol: str) -> str:
        """심볼을 KRW-BTC 형식으로 변환."""
        if not symbol:
            return ""
        
        # btcusdt → BTC-USDT
        symbol_upper = symbol.upper()
        
        for quote in ["USDT", "USDC", "HUSD", "BTC", "ETH"]:
            if symbol_upper.endswith(quote):
                base = symbol_upper[:-len(quote)]
                return f"{base}-{quote}" if base else symbol_upper
        
        return symbol_upper
