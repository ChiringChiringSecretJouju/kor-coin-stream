"""아시아 거래소 Trade 파서 자동 선택 (Strategy Pattern)."""

from __future__ import annotations

from src.core.connection.utils.market_data.dispatch.base import (
    ParserDispatcher,
    ParserProtocol,
)
from src.core.connection.utils.market_data.trades.asia.binance import BinanceTradeParser
from src.core.connection.utils.market_data.trades.asia.bybit import BybitTradeParser
from src.core.connection.utils.market_data.trades.asia.okx import OKXTradeParser
from src.core.dto.io.realtime import StandardTradeDTO


class AsiaTradeDispatcher(ParserDispatcher[StandardTradeDTO]):
    """아시아 거래소 Trade 파서 자동 선택.

    Strategy Pattern으로 메시지 구조를 자동 감지하여 적절한 파서를 선택합니다.
    모든 거래소의 Trade 데이터를 표준 포맷(Pydantic DTO)으로 통일합니다.
    """

    def __init__(self) -> None:
        """파서 리스트 초기화 (우선순위 순서)."""
        parsers: list[ParserProtocol[StandardTradeDTO]] = [
            BybitTradeParser(),
            OKXTradeParser(),
            BinanceTradeParser(),
        ]
        super().__init__(parsers, unsupported_message="Unsupported Asia trade format")
