"""파서 공통 Base 클래스 및 유틸리티.

Strategy Pattern으로 각 거래소 메시지를 표준 포맷으로 변환합니다.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Any

from src.core.connection._utils import extract_base_currency
from src.core.dto.io.realtime import StandardOrderbookDTO, StandardTradeDTO


def parse_symbol(symbol_value: str) -> tuple[str, str | None]:
    """심볼에서 target_currency, quote_currency 추출.

    Args:
        symbol_value: "KRW-BTC", "BTC/USD", "btc_krw", "BTCUSDT" 등

    Returns:
        (target_currency, quote_currency)

    Examples:
        >>> parse_symbol("KRW-BTC")
        ("BTC", "KRW")
        >>> parse_symbol("BTC/USD")
        ("BTC", "USD")
        >>> parse_symbol("btc_krw")
        ("BTC", "KRW")
        >>> parse_symbol("BTCUSDT")
        ("BTC", "USDT")
    """
    if not symbol_value:
        return "", None

    upper_symbol = symbol_value.upper()

    # 패턴 1: 구분자가 있는 경우 (-, /, _)
    # 예: BTC-USD, BTC/USD, BTC_USD, KRW-BTC
    match = re.match(r"^([A-Z0-9]+)[-/_]([A-Z0-9]+)$", upper_symbol)
    if match:
        left, right = match.groups()
        # Fiat이 앞에 있으면 역순 (KRW-BTC → BTC, KRW)
        if left in ("KRW", "USD", "USDT", "USDC", "BUSD", "EUR", "GBP", "JPY"):
            return right, left
        return left, right

    # 패턴 2: 붙어있는 형태 - 알려진 quote 통화 추출
    # 예: BTCUSDT, ETHKRW, XRPUSD
    known_quotes = (
        "USDT",
        "USDC",
        "BUSD",
        "USD",
        "KRW",
        "EUR",
        "GBP",
        "JPY",
        "BTC",
        "ETH",
    )
    for quote in known_quotes:
        if upper_symbol.endswith(quote) and len(upper_symbol) > len(quote):
            target = upper_symbol[: -len(quote)]
            return target, quote

    # 패턴 3: extract_base_currency로 폴백
    target = extract_base_currency(symbol_value)
    if target and len(target) < len(upper_symbol):
        quote = upper_symbol.replace(target, "", 1)
        return target, quote if quote else None

    return target, None


class OrderbookParser(ABC):
    """Orderbook 파서 인터페이스.

    Strategy Pattern으로 각 거래소의 OrderBook 메시지를 표준 포맷으로 변환합니다.
    """

    @abstractmethod
    def can_parse(self, message: dict[str, Any]) -> bool:
        """파싱 가능 여부 판단.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        pass

    @abstractmethod
    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """메시지를 표준 포맷으로 변환.

        Args:
            message: 원본 메시지

        Returns:
            표준화된 orderbook (Pydantic DTO)
        """
        pass


class TradeParser(ABC):
    """Trade 파서 인터페이스.

    Strategy Pattern으로 각 거래소의 Trade 메시지를 표준 포맷으로 변환합니다.
    """

    @abstractmethod
    def can_parse(self, message: dict[str, Any]) -> bool:
        """파싱 가능 여부 판단.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        pass

    @abstractmethod
    def parse(self, message: dict[str, Any]) -> StandardTradeDTO:
        """메시지를 표준 포맷으로 변환.

        Args:
            message: 원본 메시지

        Returns:
            표준화된 trade (Pydantic DTO)
        """
        pass
