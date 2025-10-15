"""Binance Spot Trade 파서."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import TradeParser
from src.core.dto.io.realtime import StandardTradeDTO


class BinanceTradeParser(TradeParser):
    """Binance Spot Trade 파서.
    
    특징:
    - Raw Trade: {"e": "trade", "s": "BNBBTC", "t": 12345, "p": "0.001", "q": "100", "T": ..., "m": true}
    - m: true=SELL (buyer is maker), false=BUY (buyer is taker)
    """
    
    def can_parse(self, message: dict[str, Any]) -> bool:
        """trade 이벤트로 판단.
        
        Args:
            message: 원본 메시지
        
        Returns:
            파싱 가능하면 True
        """
        return (
            message.get("e") == "trade"
            and "s" in message
            and "t" in message
            and "p" in message
            and "q" in message
            and "T" in message
        )
    
    def parse(self, message: dict[str, Any]) -> StandardTradeDTO:
        """Binance → 표준 포맷 (Upbit 형식).
        
        Args:
            message: Binance 원본 메시지
        
        Returns:
            표준화된 trade (Pydantic 검증 완료)
        """
        # 심볼: "BNBBTC" → "BNB-BTC" 형식으로 변환
        symbol_raw = message.get("s", "")
        # Binance는 보통 quote가 뒤에 (BTCUSDT → BTC-USDT)
        # 간단히 하이픈 추가 (실제론 parse_symbol 사용 가능)
        code = self._format_code(symbol_raw)
        
        # Side 변환: m=true → SELL, m=false → BUY
        is_maker = message.get("m", False)
        side = "ASK" if is_maker else "BID"  # maker=sell, taker=buy
        
        # 가격/수량은 문자열 → float
        price_str = message.get("p", "0")
        volume_str = message.get("q", "0")
        
        return StandardTradeDTO(
            code=code,
            trade_timestamp=message.get("T", 0),
            trade_price=float(price_str),
            trade_volume=float(volume_str),
            ask_bid=side,
            sequential_id=str(message.get("t", "")),
        )
    
    def _format_code(self, symbol: str) -> str:
        """심볼을 KRW-BTC 형식으로 변환.
        
        Args:
            symbol: "BTCUSDT", "BNBBTC" 등
        
        Returns:
            "BTC-USDT", "BNB-BTC" 등
        """
        if not symbol:
            return ""
        
        # USDT, USDC, BUSD 등 스테이블코인 감지
        for quote in ["USDT", "USDC", "BUSD", "BTC", "ETH", "BNB"]:
            if symbol.endswith(quote):
                base = symbol[:-len(quote)]
                return f"{base}-{quote}" if base else symbol
        
        # 감지 실패 시 원본 반환
        return symbol
