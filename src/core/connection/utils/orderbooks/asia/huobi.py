"""Huobi Spot Orderbook 파서."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.number_format import normalize_number_string
from src.core.connection.utils.parsers.base import OrderbookParser, parse_symbol
from src.core.connection.utils.timestamp import get_regional_timestamp_ms
from src.core.dto.io.realtime import OrderbookItemDTO, StandardOrderbookDTO


class HuobiOrderbookParser(OrderbookParser):
    """Huobi Spot Orderbook 파서.
    
    특징:
    - {"ch": "market.btcusdt.depth.step0", "tick": {"bids": [...], "asks": [...]}}
    - 배열 구조: [price, amount] (숫자 타입)
    - ch 필드에서 심볼 추출
    """
    
    def can_parse(self, message: dict[str, Any]) -> bool:
        """ch 필드에 "depth"가 포함되고 tick 필드가 있는지 확인.
        
        Args:
            message: 원본 메시지
        
        Returns:
            파싱 가능하면 True
        """
        ch = message.get("ch", "")
        tick = message.get("tick", {})
        
        return (
            isinstance(ch, str)
            and "depth" in ch
            and isinstance(tick, dict)
            and "bids" in tick
            and "asks" in tick
        )
    
    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """Huobi → 표준 포맷.
        
        Args:
            message: Huobi 원본 메시지
        
        Returns:
            표준화된 orderbook (Pydantic 검증 완료)
        """
        # ch 필드에서 심볼 추출: "market.btcusdt.depth.step0" → "btcusdt"
        ch: str = message.get("ch", "")
        symbol_raw = ""
        if ch.startswith("market.") and ".depth." in ch:
            parts = ch.split(".")
            if len(parts) >= 3:
                symbol_raw = parts[1]  # "btcusdt"
        
        symbol, quote = parse_symbol(symbol_raw) if symbol_raw else ("", None)
        
        # tick 데이터 추출
        tick = message.get("tick", {})
        bids_raw = tick.get("bids", [])
        asks_raw = tick.get("asks", [])
        
        # 숫자 → 문자열 변환 (과학적 표기법 처리)
        asks = [
            OrderbookItemDTO(
                price=normalize_number_string(item[0]), 
                size=normalize_number_string(item[1])
            )
            for item in asks_raw
            if isinstance(item, list) and len(item) >= 2
        ]
        
        bids = [
            OrderbookItemDTO(
                price=normalize_number_string(item[0]), 
                size=normalize_number_string(item[1])
            )
            for item in bids_raw
            if isinstance(item, list) and len(item) >= 2
        ]
        
        # 타임스탬프: tick.ts 또는 최상위 ts
        timestamp = tick.get("ts", message.get("ts", get_regional_timestamp_ms("korea")))
        
        return StandardOrderbookDTO(
            symbol=symbol,
            quote_currency=quote or "",
            timestamp=timestamp,
            asks=asks,
            bids=bids,
        )
