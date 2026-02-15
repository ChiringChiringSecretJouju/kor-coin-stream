"""한국 거래소 Ticker 파서 자동 선택 (Strategy Pattern)."""

from __future__ import annotations

from src.core.connection.utils.market_data.dispatch.base import ParserDispatcher
from src.core.connection.utils.market_data.parsers.base import TickerParser
from src.core.connection.utils.market_data.tickers.korea.bithumb import BithumbTickerParser
from src.core.connection.utils.market_data.tickers.korea.coinone import CoinoneTickerParser
from src.core.connection.utils.market_data.tickers.korea.korbit import KorbitTickerParser
from src.core.connection.utils.market_data.tickers.korea.upbit import UpbitTickerParser


class KoreaTickerDispatcher(ParserDispatcher):
    """한국 거래소 Ticker 파서 자동 선택.

    Strategy Pattern으로 메시지 구조를 자동 감지하여 적절한 파서를 선택합니다.
    모든 거래소의 Ticker 데이터를 표준 OHLCV 포맷(Pydantic DTO)으로 통일합니다.
    """

    def __init__(self) -> None:
        """파서 리스트 초기화 (우선순위 순서)."""
        parsers: list[TickerParser] = [
            CoinoneTickerParser(),
            KorbitTickerParser(),
            BithumbTickerParser(),
            UpbitTickerParser(),
        ]
        super().__init__(parsers, unsupported_message="Unsupported ticker format")
