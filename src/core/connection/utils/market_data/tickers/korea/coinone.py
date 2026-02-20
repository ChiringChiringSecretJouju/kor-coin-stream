"""Coinone Ticker 파서 (중첩 구조 → 표준 포맷 변환)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.market_data.parsers.base import TickerParser
from src.core.dto.io.realtime import StandardTickerDTO


class CoinoneTickerParser(TickerParser):
    """Coinone Ticker 파서.

    특징:
    - 중첩 구조: data 객체 안에 OHLCV 데이터
    - target_currency + quote_currency → code 조합
    - data.first→open, data.last→close
    - data.yesterday_last→prev_close (전일 종가)
    - data.quote_volume→quote_volume, data.target_volume→volume
    - response_type="DATA"인 경우만 처리 (SUBSCRIBED 제외)
    - stream_type 미제공 → None
    """

    def can_parse(self, message: dict[str, Any]) -> bool:
        """response_type + data 객체 + 필수 필드로 판단.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        if message.get("response_type") != "DATA":
            return False

        data = message.get("data")
        if not isinstance(data, dict):
            return False

        return (
            "target_currency" in data
            and "quote_currency" in data
            and "first" in data
            and "high" in data
            and "low" in data
            and "last" in data
        )

    def parse(self, message: dict[str, Any]) -> StandardTickerDTO:
        """Coinone → StandardTickerDTO.

        Args:
            message: Coinone 원본 메시지

        Returns:
            표준화된 ticker (Pydantic 검증 완료)
        """
        data: dict[str, Any] = message["data"]

        # code 조합: target_currency + quote_currency → KRW-BTC
        target = data.get("target_currency", "").upper()
        quote = data.get("quote_currency", "").upper()
        code = f"{quote}-{target}"

        close = float(data["last"])

        # prev_close: yesterday_last 사용
        prev_close = _safe_float(data.get("yesterday_last"))
        change_price: float | None = None
        change_rate: float | None = None

        if prev_close is not None and prev_close > 0.0:
            change_price = close - prev_close
            change_rate = (close - prev_close) / prev_close

        # volume: target_volume 우선, 없으면 volume
        volume = _safe_float(data.get("target_volume")) or _safe_float(data.get("volume"))
        if volume is None:
            volume = 0.0

        return StandardTickerDTO.from_raw(
            code=code,
            timestamp=float(data["timestamp"]) / 1000.0,
            open=float(data["first"]),
            high=float(data["high"]),
            low=float(data["low"]),
            close=close,
            volume=volume,
            quote_volume=_safe_float(data.get("quote_volume")),
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
