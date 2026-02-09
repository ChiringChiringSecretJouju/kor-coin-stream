"""OKX Spot Ticker 파서.

공식 문서: https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-tickers-channel
채널: tickers
"""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import TickerParser
from src.core.dto.io.realtime import StandardTickerDTO


class OKXTickerParser(TickerParser):
    """OKX Spot Ticker 파서.

    원본 응답 (data가 array):
    {
        "arg": {"channel": "tickers", "instId": "BTC-USDT"},
        "data": [{
            "instType": "SPOT",
            "instId": "BTC-USDT",
            "last": "9999.99",
            "open24h": "9000",
            "high24h": "10000",
            "low24h": "8888.88",
            "vol24h": "2222",
            "volCcy24h": "22220000",
            "ts": "1597026383085",
            "sodUtc0": "...",
            "sodUtc8": "..."
        }]
    }

    변환:
    - data[0].instId → code: "BTC-USDT" (이미 하이픈 포함)
    - data[0].open24h → open
    - data[0].high24h → high
    - data[0].low24h → low
    - data[0].last → close
    - data[0].vol24h → volume (base currency)
    - data[0].volCcy24h → quote_volume (quote currency)
    - data[0].sodUtc0 → prev_close (당일 시가 UTC 기준)
    - data[0].ts → timestamp (ms → s)
    """

    def can_parse(self, message: dict[str, Any]) -> bool:
        """arg.channel == "tickers"이고 data가 비어있지 않은 배열인지 확인.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        arg = message.get("arg", {})
        data = message.get("data", [])

        if not isinstance(arg, dict) or not isinstance(data, list):
            return False

        channel = arg.get("channel", "")
        return isinstance(channel, str) and channel == "tickers" and len(data) > 0

    def parse(self, message: dict[str, Any]) -> StandardTickerDTO:
        """OKX → StandardTickerDTO.

        Args:
            message: OKX 원본 메시지

        Returns:
            표준화된 ticker (Pydantic 검증 완료)
        """
        data = message.get("data", [])
        if not data:
            raise ValueError("OKX ticker data is empty")

        ticker = data[0]

        # instId: "BTC-USDT" (이미 하이픈 포함 → 그대로 사용)
        code = ticker.get("instId", "UNKNOWN")

        close = float(ticker["last"])
        open_price = float(ticker.get("open24h", "0"))

        # prev_close: sodUtc0 (당일 UTC 0시 시가)
        prev_close = _safe_float(ticker.get("sodUtc0"))

        # change 파생
        change_price: float | None = None
        change_rate: float | None = None
        if prev_close is not None and prev_close > 0.0:
            change_price = close - prev_close
            change_rate = change_price / prev_close

        # ts는 문자열 (밀리초)
        ts_str = ticker.get("ts", "0")
        timestamp = float(ts_str) / 1000.0 if ts_str else 0.0

        return StandardTickerDTO(
            code=code,
            timestamp=timestamp,
            open=open_price,
            high=float(ticker["high24h"]),
            low=float(ticker["low24h"]),
            close=close,
            volume=float(ticker.get("vol24h", "0")),
            quote_volume=_safe_float(ticker.get("volCcy24h")),
            prev_close=prev_close,
            change_price=change_price,
            change_rate=change_rate,
            stream_type=None,
        )


def _safe_float(value: Any) -> float | None:
    """None-safe float 변환."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
