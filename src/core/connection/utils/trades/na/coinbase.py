"""Coinbase Advanced Trade Trade 파서."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from src.core.connection.utils.parsers.base import TradeParser
from src.core.dto.io.realtime import StandardTradeDTO


class CoinbaseTradeParser(TradeParser):
    """Coinbase Exchange API Trade 파서.

    메시지 형식:
    {
        "type": "match",
        "trade_id": 886925293,
        "product_id": "BTC-USD",
        "side": "sell",
        "size": "0.003",
        "price": "112451.99",
        "time": "2025-10-15T07:33:22.592488Z"
    }
    """

    def can_parse(self, message: dict[str, Any]) -> bool:
        """type == "match"이고 trade_id가 있는지 확인.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        return message.get("type") == "match" and "trade_id" in message

    def parse(self, message: dict[str, Any]) -> StandardTradeDTO:
        """Coinbase Exchange API → 표준 포맷.

        Args:
            message: Coinbase 원본 메시지

        Returns:
            표준화된 trade (Pydantic 검증 완료)
        """
        # product_id: "BTC-USD"
        code = message.get("product_id", "UNKNOWN")

        # Side 변환: "buy"/"sell" → BID/ASK
        side_raw = message.get("side", "buy").lower()
        side = "BID" if side_raw == "buy" else "ASK"

        # ISO 8601 → Unix timestamp (밀리초)
        time_str = message.get("time", "")
        timestamp = self._parse_iso_timestamp(time_str)

        return StandardTradeDTO(
            code=code,
            trade_timestamp=timestamp,
            trade_price=float(message.get("price", 0)),
            trade_volume=float(message.get("size", 0)),
            ask_bid=side,
            sequential_id=str(message.get("trade_id", "")),

        )

    def _parse_iso_timestamp(self, time_str: str) -> int:
        """ISO 8601 문자열을 Unix timestamp (밀리초)로 변환.

        Args:
            time_str: "2019-08-14T20:42:27.265Z"

        Returns:
            Unix timestamp (밀리초)
        """
        if not time_str:
            return 0

        try:
            # ISO 8601 파싱
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
