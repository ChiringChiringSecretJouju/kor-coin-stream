"""OKX Spot Trade 파서."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.market_data.parsers.base import TradeParser
from src.core.dto.io.realtime import StandardTradeDTO


class OKXTradeParser(TradeParser):
    """OKX Spot Trade 파서.

    특징:
    - {"arg": {"channel": "trades", "instId": "BTC-USDT"}, "data": [{"instId": "BTC-USDT", "tradeId": "...", "px": "42219.9", "sz": "0.12", "side": "buy", "ts": "..."}]}
    - side: "buy" or "sell"
    - px: price, sz: size
    """  # noqa: E501

    def can_parse(self, message: dict[str, Any]) -> bool:
        """arg.channel에 "trades"가 포함되고 data 필드가 있는지 확인.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        arg = message.get("arg", {})
        data = message.get("data", [])

        if not isinstance(arg, dict) or not isinstance(data, list):
            return False

        channel = arg.get("channel", "")
        return isinstance(channel, str) and channel == "trades" and len(data) > 0

    def parse(self, message: dict[str, Any]) -> StandardTradeDTO:
        """OKX → 표준 포맷 (첫 번째 trade만 사용).

        Args:
            message: OKX 원본 메시지

        Returns:
            표준화된 trade (Pydantic 검증 완료)
        """
        # data 배열의 첫 번째 요소
        data = message.get("data", [])
        if not data:
            raise ValueError("OKX trade data is empty")

        trade = data[0]

        # instId: "BTC-USDT" (이미 하이픈 포함)
        code = trade.get("instId", "UNKNOWN")

        # Side 변환: "buy" → BID, "sell" → ASK
        side_raw = trade.get("side", "buy")
        side = 1 if side_raw.lower() == "buy" else -1

        # px, sz는 문자열
        price = float(trade.get("px", "0"))
        volume = float(trade.get("sz", "0"))

        # ts는 문자열 (밀리초)
        ts_str = trade.get("ts", "0")
        timestamp = float(ts_str) if ts_str.isdigit() else 0.0

        return StandardTradeDTO(
            code=code,
            trade_timestamp=timestamp / 1000.0,
            trade_price=price,
            trade_volume=volume,
            ask_bid=side,
            sequential_id=str(trade.get("tradeId", "")),
            trade_amount=price * volume,
        )
