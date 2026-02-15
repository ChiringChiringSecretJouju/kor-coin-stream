"""Bybit Spot Ticker 파서.

공식 문서: https://bybit-exchange.github.io/docs/v5/websocket/public/ticker
토픽: tickers.{symbol}
"""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.market_data.parsers.base import TickerParser
from src.core.dto.io.realtime import StandardTickerDTO


class BybitTickerParser(TickerParser):
    """Bybit Spot Ticker 파서.

    원본 응답 (data가 dict):
    {
        "topic": "tickers.BTCUSDT",
        "type": "snapshot",
        "data": {
            "symbol": "BTCUSDT",
            "lastPrice": "66666.60",
            "highPrice24h": "79266.30",
            "lowPrice24h": "65076.90",
            "prevPrice24h": "74902.40",
            "volume24h": "73191.3870",
            "turnover24h": "4999428047.9478",
            "price24hPcnt": "-0.1100"
        },
        "cs": 4614526570,
        "ts": 1720004424434
    }

    변환:
    - data.symbol → code: "BTCUSDT" → "BTC-USDT"
    - data.prevPrice24h → open (24시간 전 가격 = 시가 대용)
    - data.highPrice24h → high
    - data.lowPrice24h → low
    - data.lastPrice → close
    - data.volume24h → volume
    - data.turnover24h → quote_volume
    - data.prevPrice24h → prev_close
    - data.price24hPcnt → change_rate
    - ts → timestamp (ms → s)
    """

    def can_parse(self, message: dict[str, Any]) -> bool:
        """topic에 "tickers"가 포함되고 data가 dict인지 확인.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        topic = message.get("topic", "")
        data = message.get("data")

        return (
            isinstance(topic, str)
            and "tickers" in topic
            and isinstance(data, dict)
            and "lastPrice" in data
        )

    def parse(self, message: dict[str, Any]) -> StandardTickerDTO:
        """Bybit → StandardTickerDTO.

        Args:
            message: Bybit 원본 메시지

        Returns:
            표준화된 ticker (Pydantic 검증 완료)
        """
        data = message.get("data", {})

        symbol_raw = data.get("symbol", "")
        code = _format_code(symbol_raw)

        close = float(data["lastPrice"])
        prev_close = _safe_float(data.get("prevPrice24h"))

        # change 계산
        change_price: float | None = None
        change_rate: float | None = None
        if prev_close is not None and prev_close > 0.0:
            change_price = close - prev_close
            # Bybit는 price24hPcnt를 소수로 제공 (예: "-0.1100" = -11%)
            raw_pct = _safe_float(data.get("price24hPcnt"))
            change_rate = raw_pct if raw_pct is not None else change_price / prev_close

        return StandardTickerDTO(
            code=code,
            timestamp=float(message.get("ts", 0)) / 1000.0,
            open=float(data.get("prevPrice24h", "0")),
            high=float(data["highPrice24h"]),
            low=float(data["lowPrice24h"]),
            close=close,
            volume=float(data.get("volume24h", "0")),
            quote_volume=_safe_float(data.get("turnover24h")),
            prev_close=prev_close,
            change_price=change_price,
            change_rate=change_rate,
            stream_type=None,
        )


def _format_code(symbol: str) -> str:
    """Bybit 심볼을 "BASE-QUOTE" 형식으로 변환."""
    if not symbol:
        return ""
    upper = symbol.upper()
    for quote in ("USDT", "USDC", "BTC", "ETH"):
        if upper.endswith(quote) and len(upper) > len(quote):
            return f"{upper[: -len(quote)]}-{quote}"
    return upper


def _safe_float(value: Any) -> float | None:
    """None-safe float 변환."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
