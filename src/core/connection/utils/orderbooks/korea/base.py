"""한국 거래소 Orderbook 파서 기본 인터페이스.

Strategy Pattern으로 각 거래소 메시지를 표준 포맷으로 변환합니다.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from src.core.connection._utils import extract_base_currency
from src.core.dto.io.realtime import StandardOrderbookDTO


def parse_symbol(symbol_value: str) -> tuple[str, str | None]:
    """심볼에서 target_currency, quote_currency 추출.

    Args:
        symbol_value: "KRW-BTC", "btc_krw", "BTCUSDT" 등

    Returns:
        (target_currency, quote_currency)

    Examples:
        >>> parse_symbol("KRW-BTC")
        ("BTC", "KRW")
        >>> parse_symbol("btc_krw")
        ("BTC", "KRW")
    """
    if not symbol_value:
        return "", None

    # _utils의 extract_base_currency 활용
    target = extract_base_currency(symbol_value)

    # Quote 추론
    upper_symbol = symbol_value.upper()

    # 하이픈: KRW-BTC → quote=KRW
    if "-" in symbol_value:
        parts = symbol_value.split("-")
        if len(parts) == 2:
            left, right = parts[0].upper(), parts[1].upper()
            if left in ("KRW", "USD", "USDT", "USDC"):
                return right, left  # Fiat이 앞이면 역순
            return left, right

    # 언더스코어: btc_krw → quote=KRW
    if "_" in symbol_value:
        parts = symbol_value.split("_")
        if len(parts) == 2:
            return parts[0].upper(), parts[1].upper()

    # 붙어있는 형태: BTCUSDT → quote=USDT
    if target and len(target) < len(upper_symbol):
        quote = upper_symbol.replace(target, "", 1)
        return target, quote if quote else None

    return target, None


class OrderbookParser(ABC):
    """Orderbook 파서 인터페이스."""

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
