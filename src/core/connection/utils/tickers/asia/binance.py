"""Binance Spot Ticker 파서.

공식 문서: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
스트림: <symbol>@ticker (Individual Symbol Ticker Streams - 24hr rolling window)
"""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import TickerParser
from src.core.dto.io.realtime import StandardTickerDTO


class BinanceTickerParser(TickerParser):
    """Binance Spot 24hr Ticker 파서.

    원본 응답 (플랫 구조):
    {
        "e": "24hrTicker", "E": 1672515782136, "s": "BTCUSDT",
        "o": "16500.00", "h": "17000.00", "l": "16000.00", "c": "16800.00",
        "v": "10000", "q": "168000000",
        "x": "16500.00",  // prev close
        "p": "300.00",    // price change
        "P": "1.82"       // price change percent
    }

    변환:
    - s → code: "BTCUSDT" → "BTC-USDT"
    - o → open, h → high, l → low, c → close
    - v → volume, q → quote_volume
    - x → prev_close
    - p → change_price, P → change_rate (/100)
    - E → timestamp (ms → s)
    """

    def can_parse(self, message: dict[str, Any]) -> bool:
        """e == "24hrTicker" 이벤트로 판단.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        return (
            message.get("e") == "24hrTicker"
            and "s" in message
            and "o" in message
            and "h" in message
            and "l" in message
            and "c" in message
            and "v" in message
        )

    def parse(self, message: dict[str, Any]) -> StandardTickerDTO:
        """Binance → StandardTickerDTO.

        Args:
            message: Binance 원본 메시지

        Returns:
            표준화된 ticker (Pydantic 검증 완료)
        """
        symbol_raw = message.get("s", "")
        code = _format_code(symbol_raw)

        close = float(message["c"])
        prev_close = _safe_float(message.get("x"))

        # Binance는 change_price(p), change_rate(P)를 직접 제공
        change_price = _safe_float(message.get("p"))
        change_rate_raw = _safe_float(message.get("P"))
        change_rate = change_rate_raw / 100.0 if change_rate_raw is not None else None

        return StandardTickerDTO.from_raw(code=code,
        timestamp=float(message.get("E", 0)) / 1000.0,
        open=float(message["o"]),
        high=float(message["h"]),
        low=float(message["l"]),
        close=close,
        volume=float(message["v"]),
        quote_volume=_safe_float(message.get("q")),
        prev_close=prev_close,
        change_price=change_price,
        change_rate=change_rate,
        stream_type=None,)


def _format_code(symbol: str) -> str:
    """Binance 심볼을 "BASE-QUOTE" 형식으로 변환."""
    if not symbol:
        return ""
    upper = symbol.upper()
    for quote in ("USDT", "USDC", "BUSD", "BTC", "ETH", "BNB"):
        if upper.endswith(quote) and len(upper) > len(quote):
            return f"{upper[:-len(quote)]}-{quote}"
    return upper


def _safe_float(value: Any) -> float | None:
    """None-safe float 변환."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
