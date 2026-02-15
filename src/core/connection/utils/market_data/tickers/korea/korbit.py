"""Korbit Ticker 파서 (중첩 구조 → 표준 포맷 변환)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.market_data.parsers.base import TickerParser
from src.core.dto.io.realtime import StandardTickerDTO, StreamType


class KorbitTickerParser(TickerParser):
    """Korbit Ticker 파서.

    특징:
    - 중첩 구조: data 객체 안에 OHLCV 데이터
    - symbol: "btc_krw" → code: "KRW-BTC" 변환
    - data.open/high/low/close/volume (문자열 → float)
    - data.prevClose→prev_close, data.priceChange→change_price
    - data.priceChangePercent→change_rate (% → 소수)
    - data.quoteVolume→quote_volume
    - snapshot: true/false → stream_type: "SNAPSHOT"/"REALTIME"
    """

    def can_parse(self, message: dict[str, Any]) -> bool:
        """data 객체 + symbol + open/close 필드로 판단.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        if "symbol" not in message:
            return False

        data = message.get("data")
        if not isinstance(data, dict):
            return False

        return (
            "open" in data
            and "high" in data
            and "low" in data
            and "close" in data
            and "volume" in data
        )

    def parse(self, message: dict[str, Any]) -> StandardTickerDTO:
        """Korbit → StandardTickerDTO.

        Args:
            message: Korbit 원본 메시지

        Returns:
            표준화된 ticker (Pydantic 검증 완료)
        """
        data: dict[str, Any] = message["data"]

        # symbol 변환: btc_krw → KRW-BTC
        code = self._convert_symbol_to_code(message["symbol"])

        # prev_close 기반 파생 필드
        prev_close = _safe_float(data.get("prevClose"))
        close = float(data["close"])
        change_price = _safe_float(data.get("priceChange"))
        change_rate: float | None = None

        raw_percent = _safe_float(data.get("priceChangePercent"))
        if raw_percent is not None:
            change_rate = raw_percent / 100.0  # % → 소수

        # prev_close가 있고 직접 제공 안 된 경우 파생
        if prev_close is not None and prev_close > 0.0:
            if change_price is None:
                change_price = close - prev_close
            if change_rate is None:
                change_rate = (close - prev_close) / prev_close

        # snapshot → stream_type
        snapshot = message.get("snapshot")
        stream_type: StreamType | None = None
        if snapshot is True:
            stream_type = "SNAPSHOT"
        elif snapshot is False:
            stream_type = "REALTIME"

        return StandardTickerDTO(
            code=code,
            timestamp=float(message["timestamp"]) / 1000.0,
            open=float(data["open"]),
            high=float(data["high"]),
            low=float(data["low"]),
            close=close,
            volume=float(data["volume"]),
            quote_volume=_safe_float(data.get("quoteVolume")),
            prev_close=prev_close,
            change_price=change_price,
            change_rate=change_rate,
            stream_type=stream_type,
        )

    @staticmethod
    def _convert_symbol_to_code(symbol: str) -> str:
        """Korbit 심볼을 표준 code로 변환.

        Args:
            symbol: "btc_krw", "eth_krw" 등

        Returns:
            "KRW-BTC", "KRW-ETH" 등
        """
        if not symbol or "_" not in symbol:
            return symbol.upper()

        parts = symbol.split("_")
        if len(parts) == 2:
            target, quote = parts[0].upper(), parts[1].upper()
            return f"{quote}-{target}"

        return symbol.upper()


def _safe_float(value: Any) -> float | None:
    """None-safe float 변환."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
