"""Bithumb Ticker 파서 (Upbit와 동일한 구조)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import TickerParser
from src.core.dto.io.realtime import StandardTickerDTO, StreamType


class BithumbTickerParser(TickerParser):
    """Bithumb Ticker 파서.

    특징:
    - Upbit와 동일한 필드 구조
    - opening_price→open, high_price→high, low_price→low,
      trade_price→close, acc_trade_volume→volume
    - prev_closing_price→prev_close, acc_trade_price→quote_volume
    - signed_change_price→change_price, signed_change_rate→change_rate
    - stream_type 직접 제공
    """

    def can_parse(self, message: dict[str, Any]) -> bool:
        """code + opening_price + trade_price 필드로 판단.

        Args:
            message: 원본 메시지

        Returns:
            파싱 가능하면 True
        """
        return (
            "code" in message
            and "opening_price" in message
            and "high_price" in message
            and "low_price" in message
            and "trade_price" in message
            and "acc_trade_volume" in message
        )

    def parse(self, message: dict[str, Any]) -> StandardTickerDTO:
        """Bithumb → StandardTickerDTO (Upbit와 동일 로직).

        Args:
            message: Bithumb 원본 메시지

        Returns:
            표준화된 ticker (Pydantic 검증 완료)
        """
        prev_close = _safe_float(message.get("prev_closing_price"))
        close = float(message["trade_price"])
        change_price: float | None = None
        change_rate: float | None = None

        if prev_close is not None and prev_close > 0.0:
            change_price = _safe_float(message.get("signed_change_price"))
            change_rate = _safe_float(message.get("signed_change_rate"))
            if change_price is None:
                change_price = close - prev_close
            if change_rate is None:
                change_rate = (close - prev_close) / prev_close

        raw_st = message.get("stream_type")
        stream_type: StreamType | None = (
            raw_st if raw_st in ("SNAPSHOT", "REALTIME") else None
        )

        return StandardTickerDTO.from_raw(code=message["code"],
        timestamp=float(message["timestamp"]) / 1000.0,
        open=float(message["opening_price"]),
        high=float(message["high_price"]),
        low=float(message["low_price"]),
        close=close,
        volume=float(message["acc_trade_volume"]),
        quote_volume=_safe_float(message.get("acc_trade_price")),
        prev_close=prev_close,
        change_price=change_price,
        change_rate=change_rate,
        stream_type=stream_type,)


def _safe_float(value: Any) -> float | None:
    """None-safe float 변환."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
