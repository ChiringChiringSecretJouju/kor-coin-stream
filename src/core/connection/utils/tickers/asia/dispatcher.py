"""아시아 거래소 Ticker 파서 자동 선택 (Strategy Pattern)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import TickerParser
from src.core.connection.utils.tickers.asia.binance import BinanceTickerParser
from src.core.connection.utils.tickers.asia.bybit import BybitTickerParser
from src.core.connection.utils.tickers.asia.okx import OKXTickerParser
from src.core.dto.io.realtime import StandardTickerDTO


class AsiaTickerDispatcher:
    """아시아 거래소 Ticker 파서 자동 선택.

    Strategy Pattern으로 메시지 구조를 자동 감지하여 적절한 파서를 선택합니다.
    모든 거래소의 Ticker 데이터를 표준 OHLCV 포맷(Pydantic DTO)으로 통일합니다.
    """

    def __init__(self) -> None:
        """파서 리스트 초기화 (우선순위 순서)."""
        self._parsers: list[TickerParser] = [
            BybitTickerParser(),    # topic + "tickers" + data(dict)
            OKXTickerParser(),      # arg.channel == "tickers" + data(array)
            BinanceTickerParser(),  # e == "24hrTicker"
        ]

    def parse(self, message: dict[str, Any]) -> StandardTickerDTO:
        """자동으로 파서를 선택하여 표준 OHLCV 포맷(Pydantic DTO)으로 변환.

        Args:
            message: 원본 메시지

        Returns:
            표준화된 ticker (Pydantic 검증 완료)

        Raises:
            ValueError: 지원하지 않는 메시지 포맷
        """
        for parser in self._parsers:
            if parser.can_parse(message):
                return parser.parse(message)

        raise ValueError(f"Unsupported Asia ticker format: {list(message.keys())}")
