"""Coinbase Advanced Trade Orderbook 파서."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import OrderbookParser, parse_symbol
from src.core.connection.utils.timestamp import get_regional_timestamp_ms
from src.core.dto.io.realtime import OrderbookItemDTO, StandardOrderbookDTO


class CoinbaseOrderbookParser(OrderbookParser):
    """Coinbase Exchange API Orderbook 파서.

    특징 (Exchange API - 인증 불필요):
    - snapshot: {"type": "snapshot", "product_id": "BTC-USD", "bids": [["10101.10", "0.45"]], "asks": [...]}
    - l2update: {"type": "l2update", "product_id": "BTC-USD", "changes": [["buy", "10101.80", "0.16"]]}
    - bids/asks/changes: 배열 구조 [price, size] 또는 [side, price, size]
    """  # noqa: E501

    def can_parse(self, message: dict[str, Any]) -> bool:
        """type == "snapshot" 또는 "l2update"인 Exchange API 메시지 확인.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        msg_type = message.get("type", "")
        product_id = message.get("product_id", "")

        return (
            isinstance(msg_type, str)
            and msg_type in ("snapshot", "l2update")
            and isinstance(product_id, str)
            and len(product_id) > 0
        )

    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """Coinbase Exchange API → 표준 포맷.

        Args:
            message: Coinbase 원본 메시지 (snapshot 또는 l2update)

        Returns:
            표준화된 orderbook (Pydantic 검증 완료)
        """
        msg_type = message.get("type", "")

        # product_id: "BTC-USD" → ("BTC", "USD")
        product_id = message.get("product_id", "")
        symbol, quote = parse_symbol(product_id) if product_id else ("", None)

        asks_list: list[OrderbookItemDTO] = []
        bids_list: list[OrderbookItemDTO] = []

        if msg_type == "snapshot":
            # snapshot: {"bids": [["10101.10", "0.45"]], "asks": [["10102.55", "0.57"]]}
            bids = message.get("bids", [])
            asks = message.get("asks", [])

            bids_list = [
                OrderbookItemDTO(price=str(bid[0]), size=str(bid[1]))
                for bid in bids
                if isinstance(bid, list) and len(bid) >= 2
            ]

            asks_list = [
                OrderbookItemDTO(price=str(ask[0]), size=str(ask[1]))
                for ask in asks
                if isinstance(ask, list) and len(ask) >= 2
            ]

        elif msg_type == "l2update":
            # l2update: {"changes": [["buy", "10101.80", "0.162567"], ["sell", "10102.55", "0.0"]]}
            changes = message.get("changes", [])

            bids_list = [
                OrderbookItemDTO(price=str(change[1]), size=str(change[2]))
                for change in changes
                if isinstance(change, list) and len(change) >= 3 and change[0] == "buy"
            ]

            asks_list = [
                OrderbookItemDTO(price=str(change[1]), size=str(change[2]))
                for change in changes
                if isinstance(change, list) and len(change) >= 3 and change[0] == "sell"
            ]

        return StandardOrderbookDTO(
            symbol=symbol,
            quote_currency=quote or "",
            timestamp=get_regional_timestamp_ms("korea"),
            asks=asks_list,
            bids=bids_list,
        )
