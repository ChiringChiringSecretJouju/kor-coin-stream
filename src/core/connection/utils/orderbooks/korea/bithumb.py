"""Bithumb Orderbook 파서 (Upbit과 동일 구조).

Bithumb 메시지 포맷:
{
  "type": "orderbook",
  "code": "KRW-BTC",
  "orderbook_units": [
    {"ask_price": 478800, "bid_price": 478300, "ask_size": 4.3478, "bid_size": 5.6370}
  ]
}

최종 출력 포맷 (StandardOrderbookDTO → dict):
{
  "symbol": "BTC",
  "quote_currency": "KRW",
  "timestamp": 1760505707687,
  "asks": [{"price": "478800", "size": "4.3478"}],
  "bids": [{"price": "478300", "size": "5.6370"}]
}
"""

from __future__ import annotations

from src.core.connection.utils.orderbooks.korea.upbit import UpbitOrderbookParser


class BithumbOrderbookParser(UpbitOrderbookParser):
    """Bithumb Orderbook 파서 (Upbit 완전 동일).
    
    Bithumb은 Upbit과 동일한 메시지 구조를 사용합니다.
    orderbook_units, code, ask_price/bid_price/ask_size/bid_size 모두 동일.
    
    Upbit 파서를 그대로 상속하므로 별도 구현 불필요.
    """
    
    pass
