"""아시아 거래소 Orderbook 파서 자동 선택 (Strategy Pattern)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.orderbooks.asia.binance import BinanceOrderbookParser
from src.core.connection.utils.orderbooks.asia.bybit import BybitOrderbookParser
from src.core.connection.utils.orderbooks.asia.huobi import HuobiOrderbookParser
from src.core.connection.utils.orderbooks.asia.okx import OKXOrderbookParser
from src.core.connection.utils.parsers.base import OrderbookParser
from src.core.dto.io.realtime import StandardOrderbookDTO


class AsiaOrderbookDispatcher:
    """아시아 거래소 Orderbook 파서 자동 선택.

    Strategy Pattern으로 메시지 구조를 자동 감지하여 적절한 파서를 선택합니다.
    """

    def __init__(self) -> None:
        """파서 리스트 초기화 (우선순위 순서)."""
        self._parsers: list[OrderbookParser] = [
            HuobiOrderbookParser(),  # 가장 구체적 (ch + "depth")
            BybitOrderbookParser(),  # topic + "orderbook"
            OKXOrderbookParser(),  # arg.channel + "books"
            BinanceOrderbookParser(),  # e == "depthUpdate"
        ]

    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """메시지 형식을 자동 감지하여 적절한 파서로 전달.

        Args:
            message: 거래소로부터 받은 원본 메시지

        Returns:
            표준화된 orderbook (Pydantic DTO)

        Raises:
            ValueError: 지원하지 않는 메시지 형식
        """
        # Binance 감지: {'e': 'depthUpdate', ...}
        if "e" in message and message["e"] == "depthUpdate":
            return self._parsers[3].parse(message)

        # Bybit 감지: {'topic': 'orderbook.50.BTCUSDT', ...}
        if "topic" in message and "orderbook" in message.get("topic", ""):
            return self._parsers[1].parse(message)

        # Huobi 감지: {'ch': 'market.btcusdt.depth.step0', ...}
        if "ch" in message and "depth" in message.get("ch", ""):
            return self._parsers[0].parse(message)

        # OKX 감지: {'arg': {'channel': 'books', ...}, ...}
        if "arg" in message:
            arg = message.get("arg", {})
            if isinstance(arg, dict) and arg.get("channel") == "books":
                return self._parsers[2].parse(message)

        # 지원하지 않는 형식
        raise ValueError(
            f"Unsupported Asia orderbook format (AsiaOrderbookDispatcher): "
            f"keys={list(message.keys())}, "
            f"sample_data={str(message)[:200]}"
        )

# Module-level 싱글톤 인스턴스 (Thread-safe eager initialization)
_dispatcher = AsiaOrderbookDispatcher()


def get_asia_orderbook_dispatcher() -> AsiaOrderbookDispatcher:
    """아시아 거래소 Orderbook 디스패처 싱글톤 인스턴스 반환.
    
    Thread-safe한 pre-initialized singleton을 반환합니다.
    
    Returns:
        AsiaOrderbookDispatcher 인스턴스
    """
    return _dispatcher
