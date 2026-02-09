"""Upbit Ticker 파서 (기준 포맷)."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import TickerParser
from src.core.dto.io.realtime import StandardTickerDTO, StreamType


class UpbitTickerParser(TickerParser):
    """Upbit Ticker 파서.

    특징:
    - 플랫 구조 (변환 최소)
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
        """Upbit → StandardTickerDTO.

        Args:
            message: Upbit 원본 메시지

        Returns:
            표준화된 ticker (Pydantic 검증 완료)
        """
        # prev_close 기반 파생 필드
        prev_close = _safe_float(message.get("prev_closing_price"))
        close = float(message["trade_price"])
        change_price: float | None = None
        change_rate: float | None = None

        if prev_close is not None and prev_close > 0.0:
            # Upbit는 직접 제공하지만, 일관성을 위해 직접 계산도 가능
            change_price = _safe_float(message.get("signed_change_price"))
            change_rate = _safe_float(message.get("signed_change_rate"))
            # 직접 제공하지 않는 경우 파생
            if change_price is None:
                change_price = close - prev_close
            if change_rate is None:
                change_rate = (close - prev_close) / prev_close

        # stream_type 매핑
        raw_st = message.get("stream_type")
        stream_type: StreamType | None = (
            raw_st if raw_st in ("SNAPSHOT", "REALTIME") else None
        )

        return StandardTickerDTO(
            code=message["code"],
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
            stream_type=stream_type,
        )


def _safe_float(value: Any) -> float | None:
    """None-safe float 변환."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
