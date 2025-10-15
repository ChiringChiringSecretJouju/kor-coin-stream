"""북미 거래소 Orderbook 파서 자동 선택 (Strategy Pattern)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.orderbooks.na.coinbase import CoinbaseOrderbookParser
from src.core.connection.utils.orderbooks.na.kraken import KrakenOrderbookParser
from src.core.connection.utils.parsers.base import OrderbookParser
from src.core.dto.io.realtime import StandardOrderbookDTO


class NaOrderbookDispatcher:
    """북미 거래소 Orderbook 파서 자동 선택.

    Strategy Pattern으로 메시지 구조를 자동 감지하여 적절한 파서를 선택합니다.
    """

    def __init__(self) -> None:
        """파서 리스트 초기화 (우선순위 순서)."""
        self._parsers: list[OrderbookParser] = [
            CoinbaseOrderbookParser(),  # channel == "l2_data"
            KrakenOrderbookParser(),  # channel == "book"
        ]

    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """자동으로 파서를 선택하여 파싱 (Pydantic DTO 반환).

        Args:
            message: 원본 메시지

        Returns:
            표준 포맷 orderbook (Pydantic 검증 완료)

        Raises:
            ValueError: 지원하지 않는 메시지 포맷
        """
        for parser in self._parsers:
            if parser.can_parse(message):
                return parser.parse(message)

        raise ValueError(f"Unsupported NA orderbook format: {list(message.keys())}")


# Module-level 싱글톤 인스턴스 (Thread-safe eager initialization)
_dispatcher = NaOrderbookDispatcher()


def get_na_orderbook_dispatcher() -> NaOrderbookDispatcher:
    """북미 거래소 Orderbook 디스패처 싱글톤 인스턴스 반환.

    Thread-safe한 pre-initialized singleton을 반환합니다.

    Returns:
        NaOrderbookDispatcher 인스턴스
    """
    return _dispatcher
