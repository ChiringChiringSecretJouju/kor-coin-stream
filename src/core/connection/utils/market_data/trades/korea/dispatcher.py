"""한국 거래소 Trade 파서 자동 선택 (Strategy Pattern)."""

from __future__ import annotations

from src.core.connection.utils.market_data.dispatch.base import ParserDispatcher
from src.core.connection.utils.market_data.parsers.base import TradeParser
from src.core.connection.utils.market_data.trades.korea.bithumb import BithumbTradeParser
from src.core.connection.utils.market_data.trades.korea.coinone import CoinoneTradeParser
from src.core.connection.utils.market_data.trades.korea.korbit import KorbitTradeParser
from src.core.connection.utils.market_data.trades.korea.upbit import UpbitTradeParser


class KoreaTradeDispatcher(ParserDispatcher):
    """한국 거래소 Trade 파서 자동 선택.

    Strategy Pattern으로 메시지 구조를 자동 감지하여 적절한 파서를 선택합니다.
    모든 거래소의 Trade 데이터를 Upbit 포맷(Pydantic DTO)으로 통일합니다.
    """

    def __init__(self) -> None:
        """파서 리스트 초기화 (우선순위 순서)."""
        parsers: list[TradeParser] = [
            CoinoneTradeParser(),
            KorbitTradeParser(),
            BithumbTradeParser(),
            UpbitTradeParser(),
        ]
        super().__init__(parsers, unsupported_message="Unsupported trade format")
