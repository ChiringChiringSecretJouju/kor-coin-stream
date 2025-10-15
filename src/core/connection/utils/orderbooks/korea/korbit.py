"""Korbit Orderbook 파서 (Korbit → StandardOrderbookDTO 완전 변환).

Korbit 메시지 포맷:
{
  "type": "orderbook",
  "symbol": "btc_krw",
  "data": {
    "asks": [{"price": "99131000", "qty": "0.00456677"}],
    "bids": [{"price": "99120000", "qty": "0.00363422"}]
  }
}

최종 출력 포맷 (StandardOrderbookDTO → dict):
{
  "symbol": "BTC",
  "quote_currency": "KRW",
  "timestamp": 1760505707687,
  "asks": [{"price": "99131000", "size": "0.00456677"}],
  "bids": [{"price": "99120000", "size": "0.00363422"}]
}
"""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.number_format import normalize_number_string
from src.core.connection.utils.parsers.base import OrderbookParser, parse_symbol
from src.core.connection.utils.timestamp import get_regional_timestamp_ms
from src.core.dto.io.realtime import OrderbookItemDTO, StandardOrderbookDTO


class KorbitOrderbookParser(OrderbookParser):
    """Korbit Orderbook 파서 (완전 표준화).

    Korbit 특징:
    - data 내부에 asks/bids 이미 봠6리됨
    - symbol: "btc_krw" 형식 (언더스코어)
    - 필드명: price, qty (문자열)
    
    변환 과정:
    1. symbol → BTC, KRW 분리
    2. qty → size 변환
    3. StandardOrderbookDTO 생성
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

        # 과학적 표기법 처리
        asks: list[OrderbookItemDTO] = [
            OrderbookItemDTO(
                price=normalize_number_string(item["price"]),
                size=normalize_number_string(item.get("qty", ""))
            )
            for item in data.get("asks", [])
            if isinstance(item, dict) and "price" in item
        ]

        bids: list[OrderbookItemDTO] = [
            OrderbookItemDTO(
                price=normalize_number_string(item["price"]),
                size=normalize_number_string(item.get("qty", ""))
            )
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
