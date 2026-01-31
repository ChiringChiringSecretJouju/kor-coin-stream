"""거래소 메시지 전처리 유틸리티.

새로운 파서 시스템(Strategy Pattern)을 사용하여 거래소별 메시지를 표준 포맷으로 변환합니다.
기존 API와의 하위 호환성을 유지합니다.
"""

from typing import Any

from src.core.connection._utils import parse_symbol_pair
from src.core.connection.utils.dict_utils import update_dict
from src.core.connection.utils.timestamp import (
    get_regional_datetime,
    get_regional_timestamp_ms,
)
from src.core.connection.utils.trades.asia import get_asia_trade_dispatcher
from src.core.connection.utils.trades.korea import get_korea_trade_dispatcher
from src.core.connection.utils.trades.na import get_na_trade_dispatcher
from src.core.dto.io.realtime import StandardTradeDTO

# Re-export for backward compatibility
__all__ = [
    "get_regional_timestamp_ms",
    "get_regional_datetime",
    "update_dict",
    "preprocess_ticker_message",
    "preprocess_trade_message",
    "preprocess_asia_trade_message",
    "preprocess_na_trade_message",
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
    "ch": "target_currency",  # Huobi (처리 후)
    "product_id": "target_currency",  # Coinbase
    # Kraken은 이미 "symbol" 필드를 사용하므로 추가 매핑 불필요
    "ts": "timestamp",
    "timestamp": "timestamp",
    "time": "timestamp",
    "E": "timestamp",  # Binance
    "open": "first",
    "opening_price": "first",
    "prev_closing_price": "first",
    "o": "first",  # Binance
    "open24h": "first",  # OKX
    "open_24h": "first",  # Coinbase
    "bid": "first",  # Kraken (매수 호가를 시가 대용으로 사용)
    "prevPrice24h": "first",  # Bybit (24시간 전 가격을 시가 대용으로 사용)
    "high": "high",
    "high_price": "high",
    "max_price": "high",
    "h": "high",  # Binance
    "high24h": "high",  # OKX
    "highPrice24h": "high",  # Bybit
    "high_24h": "high",  # Coinbase
    # Kraken은 이미 "high" 필드를 사용하므로 추가 매핑 불필요
    "low": "low",
    "low_price": "low",
    "min_price": "low",
    "l": "low",  # Binance
    "low24h": "low",  # OKX
    "lowPrice24h": "low",  # Bybit
    "low_24h": "low",  # Coinbase
    # Kraken은 이미 "low" 필드를 사용하므로 추가 매핑 불필요
    "close": "last",
    "closing_price": "last",
    "last_price": "last",
    "trade_price": "last",
    "c": "last",  # Binance
    "last": "last",  # OKX
    "lastPrice": "last",  # Bybit/Huobi
    "amount": "target_volume",
    "volume": "target_volume",  # 공통 (Kraken 포함)
    "acc_trade_volume": "target_volume",
    "base_volume": "target_volume",
    "v": "target_volume",  # Binance
    "vol24h": "target_volume",  # OKX
    "volume24h": "target_volume",  # Bybit
    "volume_24h": "target_volume",  # Coinbase
    "price": "last",  # Coinbase Ticker Price
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




def preprocess_trade_message(
    parsed_message: dict[str, Any], projection: list[str] | None
) -> StandardTradeDTO:
    """Trade 메시지 전처리 (새로운 파서 시스템 사용 - 한국 거래소).

    모든 한국 거래소의 Trade 데이터를 Upbit 표준 포맷(Pydantic DTO)으로 통일합니다.

    표준 포맷:
        StandardTradeDTO {
            code: str,                  # 마켓 코드 ("KRW-BTC" 형식)
            trade_timestamp: float,     # 체결 타임스탬프 (Unix Seconds, float)
            trade_price: float,         # 체결 가격
            trade_volume: float,        # 체결량
            ask_bid: 1 | -1 | 0,        # 매수/매도 구분 (1=BID, -1=ASK, 0=UNKNOWN)
            sequential_id: str          # 체결 고유 ID
        }

    Args:
        parsed_message: 원본 메시지
        projection: 추출할 필드 리스트 (사용하지 않음, 호환성 유지)

    Returns:
        표준화된 trade (Pydantic DTO, Upbit 형식)
    """
    # 디스패처로 자동 파싱
    dispatcher = get_korea_trade_dispatcher()
    return dispatcher.parse(parsed_message)






def preprocess_asia_trade_message(
    parsed_message: dict[str, Any], projection: list[str] | None
) -> StandardTradeDTO:
    """Trade 메시지 전처리 (새로운 파서 시스템 사용 - 아시아 거래소).

    아시아 거래소(Binance, Bybit, Huobi, OKX)의 Trade 데이터를 표준 포맷으로 통일합니다.

    Args:
        parsed_message: 원본 메시지
        projection: 추출할 필드 리스트 (사용하지 않음, 호환성 유지)

    Returns:
        표준화된 trade (Pydantic DTO)
    """
    dispatcher = get_asia_trade_dispatcher()
    return dispatcher.parse(parsed_message)


def preprocess_na_trade_message(
    parsed_message: dict[str, Any], projection: list[str] | None
) -> StandardTradeDTO:
    """Trade 메시지 전처리 (새로운 파서 시스템 사용 - 북미 거래소).

    북미 거래소(Coinbase, Kraken)의 Trade 데이터를 표준 포맷으로 통일합니다.

    Args:
        parsed_message: 원본 메시지
        projection: 추출할 필드 리스트 (사용하지 않음, 호환성 유지)

    Returns:
        표준화된 trade (Pydantic DTO)
    """
    dispatcher = get_na_trade_dispatcher()
    return dispatcher.parse(parsed_message)
