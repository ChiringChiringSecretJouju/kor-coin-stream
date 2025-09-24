from __future__ import annotations

from typing import Any


def update_dict(message: dict[str, Any], key: str) -> dict[str, Any]:
    """message[key]가 dict 또는 list[dict]인 경우 해당 내용을 message에 병합하여 반환.

    - 공용 파싱 유틸로 승격된 함수입니다.
    - 입력은 변경하지 않고 복사본을 기반으로 처리합니다.
    """
    merged = dict(message)
    data_sub = message.get(key)
    if isinstance(data_sub, dict):
        merged.update(data_sub)
    elif isinstance(data_sub, list) and data_sub and isinstance(data_sub[0], dict):
        merged.update(data_sub[0])
    return merged


def preprocess_ticker_message(
    parsed_message: dict[str, Any], projection: list[str] | None
) -> dict[str, Any]:
    """공통 티커 메시지 전처리 (projection 필터링 + 표준 스키마 변환)"""

    # 1단계: projection이 지정되면 해당 필드만 추출
    if projection:
        filtered_message = {field: parsed_message.get(field) for field in projection}  # type: ignore[misc]
    else:
        filtered_message = parsed_message.copy()

    # 2단계: 추출된 필드들을 표준 스키마 키로 변경
    FIELD_MAPPING = {
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
    }

    # 키 변경 적용
    standardized_message: dict[str, Any] = {}
    for original_field, value in filtered_message.items():
        if original_field in FIELD_MAPPING:
            new_field = FIELD_MAPPING[original_field]
            standardized_message[new_field] = value
        else:
            # 매핑되지 않은 필드는 그대로 유지
            standardized_message[original_field] = value

    standardized_message["_preprocessed"] = True
    return standardized_message
