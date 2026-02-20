"""Bybit Spot Trade 파서."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.market_data.parsers.base import TradeParser
from src.core.dto.io.realtime import StandardTradeDTO


class BybitTradeParser(TradeParser):
    """Bybit Spot Trade 파서.

    특징:
    - v5 통합 API
    - {
        "topic": "publicTrade.BTCUSDT",
        "data": [{
            "T": ..., "s": "BTCUSDT", "S": "Buy",
            "v": "0.001", "p": "16578.50", "i": "..."
        }]
    }
    - S: "Buy" or "Sell"
    - data는 배열 (여러 거래 포함 가능)
    """

    def can_parse(self, message: dict[str, Any]) -> bool:
        """topic에 "publicTrade"가 포함되고 data 필드가 있는지 확인.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        topic = message.get("topic", "")
        data = message.get("data", [])

        return (
            isinstance(topic, str)
            and "publicTrade" in topic
            and isinstance(data, list)
            and len(data) > 0
        )

    def parse(self, message: dict[str, Any]) -> StandardTradeDTO:
        """Bybit → 표준 포맷 (첫 번째 trade만 사용).

        Args:
            message: Bybit 원본 메시지

        Returns:
            표준화된 trade (Pydantic 검증 완료)

        Note:
            data 배열에 여러 거래가 있을 수 있으나 첫 번째만 반환.
            실제로는 모든 거래를 처리해야 할 수 있음.
        """
        data = message.get("data", [])
        if not data:
            raise ValueError("Bybit trade data is empty")

        trade = data[0]

        # 심볼: "BTCUSDT" → "BTC-USDT"
        symbol_raw = trade.get("s", "")
        code = self._format_code(symbol_raw)

        # Side 변환: "Buy" → BID, "Sell" → ASK
        side_raw = trade.get("S", "Buy")
        side = 1 if side_raw.lower() == "buy" else -1

        # 가격/수량
        price = float(trade.get("p", "0"))
        volume = float(trade.get("v", "0"))

        return StandardTradeDTO.from_raw(code=code,
        trade_timestamp=float(trade.get("T", 0)) / 1000.0,
        trade_price=price,
        trade_volume=volume,
        ask_bid=side,
        sequential_id=str(trade.get("seq", trade.get("i", "0"))),
        trade_amount=price * volume,)

    def _format_code(self, symbol: str) -> str:
        """심볼을 KRW-BTC 형식으로 변환."""
        if not symbol:
            return ""

        for quote in ["USDT", "USDC", "BTC", "ETH"]:
            if symbol.endswith(quote):
                base = symbol[: -len(quote)]
                return f"{base}-{quote}" if base else symbol

        return symbol
