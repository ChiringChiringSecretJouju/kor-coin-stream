"""Bithumb Orderbook 파서 (Upbit과 동일 구조)."""

from __future__ import annotations

from src.core.connection.utils.orderbooks.korea.upbit import UpbitOrderbookParser


class BithumbOrderbookParser(UpbitOrderbookParser):
    """Bithumb Orderbook 파서 (Upbit 상속).
    
    Bithumb은 Upbit과 동일한 메시지 구조를 사용합니다.
    """
    
    pass
