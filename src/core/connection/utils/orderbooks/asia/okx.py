"""OKX Spot Orderbook 파서."""

from __future__ import annotations

from typing import Any

from src.core.connection.utils.parsers.base import OrderbookParser, parse_symbol
from src.core.connection.utils.timestamp import get_regional_timestamp_ms
from src.core.dto.io.realtime import OrderbookItemDTO, StandardOrderbookDTO


class OKXOrderbookParser(OrderbookParser):
    """OKX Spot Orderbook 파서.
    
    특징:
    - {"arg": {"channel": "books", "instId": "BTC-USDT"}, "data": [{"asks": [...], "bids": [...]}]}
    - 배열 구조: ["price", "size", "0", "13"] (4개 요소, price/size만 사용)
    """
    
    def can_parse(self, message: dict[str, Any]) -> bool:
        """arg.channel에 "books"가 포함되고 data 필드가 있는지 확인.
        
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
        return (
            isinstance(channel, str)
            and "book" in channel
            and len(data) > 0
            and isinstance(data[0], dict)
            and "asks" in data[0]
            and "bids" in data[0]
        )
    
    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """OKX → 표준 포맷.
        
        Args:
            message: OKX 원본 메시지
        
        Returns:
            표준화된 orderbook (Pydantic 검증 완료)
        """
        # instId에서 심볼 추출: "BTC-USDT" → ("BTC", "USDT")
        arg = message.get("arg", {})
        inst_id: str = arg.get("instId", "")
        symbol, quote = parse_symbol(inst_id) if inst_id else ("", None)
        
        # data 배열의 첫 번째 요소
        data = message.get("data", [])
        if not data:
            return StandardOrderbookDTO(
                symbol=symbol,
                quote_currency=quote or "",
                timestamp=get_regional_timestamp_ms("korea"),
                asks=[],
                bids=[],
            )
        
        first_data = data[0]
        bids_raw = first_data.get("bids", [])
        asks_raw = first_data.get("asks", [])
        
        # 배열의 [0], [1]만 사용 (price, size)
        asks = [
            OrderbookItemDTO(price=item[0], size=item[1])
            for item in asks_raw
            if isinstance(item, list) and len(item) >= 2
        ]
        
        bids = [
            OrderbookItemDTO(price=item[0], size=item[1])
            for item in bids_raw
            if isinstance(item, list) and len(item) >= 2
        ]
        
        # 타임스탬프: data[0].ts
        timestamp_str = first_data.get("ts", "")
        timestamp = int(timestamp_str) if timestamp_str.isdigit() else get_regional_timestamp_ms("korea")
        
        return StandardOrderbookDTO(
            symbol=symbol,
            quote_currency=quote or "",
            timestamp=timestamp,
            asks=asks,
            bids=bids,
        )
