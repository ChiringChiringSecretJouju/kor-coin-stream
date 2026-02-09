"""거래소 메시지 전처리 유틸리티.

글로벌 거래소(아시아/북미) Ticker용 dict 기반 전처리 함수를 제공합니다.
Trade 및 한국 거래소 Ticker는 DI Container에서 관리되는 Dispatcher를 통해 처리됩니다.
"""

from typing import Any

from src.core.connection._utils import parse_symbol_pair
from src.core.connection.utils.dict_utils import update_dict
from src.core.connection.utils.timestamp import (
    get_regional_datetime,
    get_regional_timestamp_ms,
)
# Re-export for backward compatibility
__all__ = [
    "get_regional_timestamp_ms",
    "get_regional_datetime",
    "update_dict",
    "preprocess_ticker_message",
]


# 티커 필드 매핑 상수 (성능 최적화)
_TICKER_FIELD_MAPPING = {
    # 기존 필드 -> 새로운 표준 필드
    "symbol": "target_currency",
    "code": "target_currency",
    "market": "target_currency",
    "currency_pair": "target_currency",
    "s": "target_currency",  # Binance
    "instId": "target_currency",  # OKX
    "ch": "target_currency",  # ch 필드 (market.xxx.yyy 형식)
    "product_id": "target_currency",  # product_id 필드
    "ts": "timestamp",
    "timestamp": "timestamp",
    "time": "timestamp",
    "E": "timestamp",  # Binance
    "open": "first",
    "opening_price": "first",
    "prev_closing_price": "first",
    "o": "first",  # Binance
    "open24h": "first",  # OKX
    "prevPrice24h": "first",  # Bybit (24시간 전 가격을 시가 대용으로 사용)
    "high": "high",
    "high_price": "high",
    "max_price": "high",
    "h": "high",  # Binance
    "high24h": "high",  # OKX
    "highPrice24h": "high",  # Bybit
    "low": "low",
    "low_price": "low",
    "min_price": "low",
    "l": "low",  # Binance
    "low24h": "low",  # OKX
    "lowPrice24h": "low",  # Bybit
    "close": "last",
    "closing_price": "last",
    "last_price": "last",
    "trade_price": "last",
    "c": "last",  # Binance
    "last": "last",  # OKX
    "lastPrice": "last",  # Bybit
    "amount": "target_volume",
    "volume": "target_volume",  # 공통
    "acc_trade_volume": "target_volume",
    "base_volume": "target_volume",
    "v": "target_volume",  # Binance
    "vol24h": "target_volume",  # OKX
    "volume24h": "target_volume",  # Bybit
}


def preprocess_ticker_message(
    parsed_message: dict[str, Any], projection: list[str] | None
) -> dict[str, Any]:
    """공통 티커 메시지 전처리 (projection 필터링 + 표준 스키마 변환 + 심볼 정규화)

    처리 단계:
    1. projection 필터링
    2. 필드명 표준화
    3. 심볼 정규화 (symbol, quote_currency 추가)
    """

    # 1단계: projection이 지정되면 해당 필드만 추출
    if projection:
        # dict comprehension으로 필터링
        filtered_message = {
            field: parsed_message.get(field)
            for field in projection
            if field in parsed_message
        }
    else:
        filtered_message = parsed_message.copy()

    # 2단계: 추출된 필드들을 표준 스키마 키로 변경
    standardized_message: dict[str, Any] = {}
    for original_field, value in filtered_message.items():
        # 매핑 확인 (O(1) lookup failure is faster than large dict creation)
        new_field = _TICKER_FIELD_MAPPING.get(original_field)
        if new_field:
            standardized_message[new_field] = value
        else:
            # 매핑되지 않은 필드는 그대로 유지
            standardized_message[original_field] = value

    # 3단계: 심볼 정규화 (symbol, quote_currency 추가)
    # target_currency가 가장 우선순위가 높으며, 그 외에는 매핑되지 않은 원본 필드에서 찾음
    symbol_field = standardized_message.get("target_currency")

    # 매핑되지 않은 경우(예: 새로운 거래소)를 대비한 Fallback
    if not symbol_field:
        symbol_field = (
            standardized_message.get("code")
            or standardized_message.get("symbol")
            or standardized_message.get("market")
            or standardized_message.get("s")
        )

    if symbol_field and isinstance(symbol_field, str):
        base, quote = parse_symbol_pair(symbol_field)
        standardized_message["symbol"] = base  # "BTC"
        standardized_message["quote_currency"] = quote  # "KRW", "USDT", "USD" 등
        # 원본 필드(target_currency 등)는 유지됨

    standardized_message["_preprocessed"] = True
    return standardized_message
