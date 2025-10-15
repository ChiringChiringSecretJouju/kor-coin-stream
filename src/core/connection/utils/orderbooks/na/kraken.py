"""Kraken v2 Orderbook 파서."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import OrderbookParser, parse_symbol
from src.core.connection.utils.timestamp import get_regional_timestamp_ms
from src.core.dto.io.realtime import OrderbookItemDTO, StandardOrderbookDTO


class KrakenOrderbookParser(OrderbookParser):
    """Kraken v2 Orderbook 파서.

    특징:
    - {"channel": "book", "type": "snapshot", "data": [{"symbol": "MATIC/USD", "bids": [...], "asks": [...]}]}
    - bids/asks: [{"price": 0.5666, "qty": 4831.75}] (객체 구조, 숫자 타입)
    """

    def can_parse(self, message: dict[str, Any]) -> bool:
        """channel == "book"이고 type == "update"인 메시지만 파싱.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True

        Note:
            snapshot 메시지는 무시 (update만 처리)
        """
        channel = message.get("channel", "")
        msg_type = message.get("type", "")
        data = message.get("data", [])

        # snapshot은 파싱하지 않음 (update만 허용)
        if msg_type == "snapshot":
            return False

        return (
            isinstance(channel, str)
            and channel == "book"
            and msg_type == "update"
            and isinstance(data, list)
            and len(data) > 0
            and isinstance(data[0], dict)
            and "bids" in data[0]
            and "asks" in data[0]
        )

    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """Kraken → 표준 포맷.

        Args:
            message: Kraken 원본 메시지

        Returns:
            표준화된 orderbook (Pydantic 검증 완료)
        """
        data = message.get("data", [])
        if not data:
            return StandardOrderbookDTO(
                symbol="",
                quote_currency="",
                timestamp=get_regional_timestamp_ms("korea"),
                asks=[],
                bids=[],
            )

        book_data = data[0]

        # symbol: "MATIC/USD" → ("MATIC", "USD")
        symbol_raw = book_data.get("symbol", "")
        symbol, quote = parse_symbol(symbol_raw) if symbol_raw else ("", None)

        # bids/asks 배열 → DTO 변환 (숫자 → 문자열)
        bids_raw = book_data.get("bids", [])
        asks_raw = book_data.get("asks", [])

        asks = [
            OrderbookItemDTO(price=str(item["price"]), size=str(item["qty"]))
            for item in asks_raw
            if isinstance(item, dict) and "price" in item and "qty" in item
        ]

        bids = [
            OrderbookItemDTO(price=str(item["price"]), size=str(item["qty"]))
            for item in bids_raw
            if isinstance(item, dict) and "price" in item and "qty" in item
        ]

        return StandardOrderbookDTO(
            symbol=symbol,
            quote_currency=quote or "",
            timestamp=get_regional_timestamp_ms("korea"),
            asks=asks,
            bids=bids,
        )
