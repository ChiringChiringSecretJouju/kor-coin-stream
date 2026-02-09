"""한국 거래소 Ticker 파서 자동 선택 (Strategy Pattern)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import TickerParser
from src.core.connection.utils.tickers.korea.bithumb import BithumbTickerParser
from src.core.connection.utils.tickers.korea.coinone import CoinoneTickerParser
from src.core.connection.utils.tickers.korea.korbit import KorbitTickerParser
from src.core.connection.utils.tickers.korea.upbit import UpbitTickerParser
from src.core.dto.io.realtime import StandardTickerDTO


class KoreaTickerDispatcher:
    """한국 거래소 Ticker 파서 자동 선택.

    Strategy Pattern으로 메시지 구조를 자동 감지하여 적절한 파서를 선택합니다.
    모든 거래소의 Ticker 데이터를 표준 OHLCV 포맷(Pydantic DTO)으로 통일합니다.
    """

    def __init__(self) -> None:
        """파서 리스트 초기화 (우선순위 순서)."""
        self._parsers: list[TickerParser] = [
            CoinoneTickerParser(),  # 가장 구체적 (response_type + data.target_currency)
            KorbitTickerParser(),   # 중간 (symbol + data.open/close)
            BithumbTickerParser(),  # Upbit와 동일 구조
            UpbitTickerParser(),    # 기준 포맷 (최종 fallback)
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

        raise ValueError(f"Unsupported ticker format: {list(message.keys())}")
