"""아시아 거래소 Ticker 파서 자동 선택 (Strategy Pattern)."""

from __future__ import annotations

from src.core.connection.utils.market_data.dispatch.base import ParserDispatcher
from src.core.connection.utils.market_data.parsers.base import TickerParser
from src.core.connection.utils.market_data.tickers.asia.binance import BinanceTickerParser
from src.core.connection.utils.market_data.tickers.asia.bybit import BybitTickerParser
from src.core.connection.utils.market_data.tickers.asia.okx import OKXTickerParser


class AsiaTickerDispatcher(ParserDispatcher):
    """아시아 거래소 Ticker 파서 자동 선택.

    Strategy Pattern으로 메시지 구조를 자동 감지하여 적절한 파서를 선택합니다.
    모든 거래소의 Ticker 데이터를 표준 OHLCV 포맷(Pydantic DTO)으로 통일합니다.
    """

    def __init__(self) -> None:
        """파서 리스트 초기화 (우선순위 순서)."""
        parsers: list[TickerParser] = [
            BybitTickerParser(),
            OKXTickerParser(),
            BinanceTickerParser(),
        ]
        super().__init__(parsers, unsupported_message="Unsupported Asia ticker format")
