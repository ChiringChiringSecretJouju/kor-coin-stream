"""Korbit Trade 파서 (중첩 구조 → Upbit 포맷 변환)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.market_data.common.dict_utils import update_dict
from src.core.connection.utils.market_data.parsers.base import TradeParser
from src.core.dto.io.realtime import StandardTradeDTO, StreamType


class KorbitTradeParser(TradeParser):
    """Korbit Trade 파서.

    특징:
    - 중첩 구조: data 배열 안에 실제 체결 데이터
    - symbol: "btc_krw" → code: "KRW-BTC" 변환
    - isBuyerTaker: bool → ask_bid: "ASK" or "BID" 변환
    - update_dict 활용하여 data[0] 평탄화
    """

    def can_parse(self, message: dict[str, Any]) -> bool:
        """data 배열 및 필수 필드로 판단.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        if not isinstance(message.get("data"), list):
            return False

        data_list = message.get("data", [])
        if not data_list or not isinstance(data_list[0], dict):
            return False

        first_trade = data_list[0]
        return (
            "timestamp" in first_trade
            and "price" in first_trade
            and "qty" in first_trade
            and "isBuyerTaker" in first_trade
            and "tradeId" in first_trade
            and "symbol" in message
        )

    def parse(self, message: dict[str, Any]) -> StandardTradeDTO:
        """Korbit → Upbit 포맷 변환 (update_dict 활용).

        Args:
            message: Korbit 원본 메시지

        Returns:
            Upbit 형식의 표준화된 trade (Pydantic DTO)
        """
        # update_dict로 data[0] 평탄화
        flattened = update_dict(message, "data")

        # 1. symbol 변환: btc_krw → KRW-BTC
        symbol = message.get("symbol", "")
        code = self._convert_symbol_to_code(symbol)

        # 2. isBuyerTaker → ask_bid 변환
        # isBuyerTaker=True → 매수 주문이 Taker → 실제 체결은 매수(BID)
        # isBuyerTaker=False → 매도 주문이 Taker → 실제 체결은 매도(ASK)
        is_buyer_taker: bool = flattened.get("isBuyerTaker", False)
        ask_bid = 1 if is_buyer_taker else -1

        # snapshot → stream_type
        snapshot = message.get("snapshot")
        stream_type: StreamType | None = None
        if snapshot is True:
            stream_type = "SNAPSHOT"
        elif snapshot is False:
            stream_type = "REALTIME"

        price = float(flattened["price"])
        volume = float(flattened["qty"])

        return StandardTradeDTO.from_raw(code=code,
        trade_timestamp=float(flattened["timestamp"]) / 1000.0,
        trade_price=price,
        trade_volume=volume,
        ask_bid=ask_bid,
        sequential_id=str(flattened["tradeId"]),
        trade_amount=price * volume,
        stream_type=stream_type,)

    @staticmethod
    def _convert_symbol_to_code(symbol: str) -> str:
        """Korbit 심볼을 Upbit code로 변환.

        Args:
            symbol: "btc_krw", "eth_krw" 등

        Returns:
            "KRW-BTC", "KRW-ETH" 등

        Examples:
            >>> _convert_symbol_to_code("btc_krw")
            "KRW-BTC"
            >>> _convert_symbol_to_code("eth_krw")
            "KRW-ETH"
        """
        if not symbol or "_" not in symbol:
            return symbol.upper()

        parts = symbol.split("_")
        if len(parts) == 2:
            target, quote = parts[0].upper(), parts[1].upper()
            return f"{quote}-{target}"

        return symbol.upper()
