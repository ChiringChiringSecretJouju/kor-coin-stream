"""Korbit Orderbook 파서 (qty → size 변환)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.orderbooks.korea.base import (
    OrderbookParser,
    parse_symbol,
)
from src.core.connection.utils.timestamp import get_regional_timestamp_ms
from src.core.dto.io.realtime import OrderbookItemDTO, StandardOrderbookDTO


class KorbitOrderbookParser(OrderbookParser):
    """Korbit Orderbook 파서.

    특징:
    - data.asks/bids 이미 분리됨
    - symbol: "btc_krw"
    - 필드명: price, qty
    """

    def can_parse(self, message: dict[str, Any]) -> bool:
        """data 내부에 asks/bids 존재 여부.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        data = message.get("data")
        if not isinstance(data, dict):
            return False
        return "asks" in data and "bids" in data

    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """Korbit → 표준 포맷 (qty → size, Pydantic DTO).

        Args:
            message: Korbit 원본 메시지

        Returns:
            표준화된 orderbook (Pydantic 검증 완료)
        """
        # 1. 심볼 추출
        symbol_str: str = message.get("symbol", "")
        symbol, quote = parse_symbol(symbol_str) if symbol_str else ("", None)

        # 2. asks/bids 변환 (qty → size, Pydantic DTO 생성)
        data: dict[str, Any] = message.get("data", {})

        asks: list[OrderbookItemDTO] = [
            OrderbookItemDTO(price=str(item["price"]), size=str(item.get("qty", "")))
            for item in data.get("asks", [])
            if isinstance(item, dict) and "price" in item
        ]

        bids: list[OrderbookItemDTO] = [
            OrderbookItemDTO(price=str(item["price"]), size=str(item.get("qty", "")))
            for item in data.get("bids", [])
            if isinstance(item, dict) and "price" in item
        ]

        return StandardOrderbookDTO(
            symbol=symbol,
            quote_currency=quote or "",
            timestamp=get_regional_timestamp_ms("korea"),
            asks=asks,
            bids=bids,
        )
