from typing import Any

from src.common.logger import PipelineLogger

logger = PipelineLogger.get_logger("symbol_extractor", "utils")


BINANCE_QUOTES = (
    "USDT",
    "BUSD",
    "USDC",
    "FDUSD",
    "TUSD",
    "BTC",
    "ETH",
    "BNB",
    "ADA",
    "XRP",
    "DOT",
    "SOL",
)


SymbolFieldSpec = tuple[tuple[str, ...], bool]


FIELD_PRIORITIES: tuple[SymbolFieldSpec, ...] = (
    (("code",), True),
    (("symbol",), True),
    (("s",), True),
    (("market",), True),
    (("data", "target_currency"), False),
    (("data", "symbol"), True),
    (("data", "s"), True),
    (("data", "market"), True),
    (("data", "code"), True),
)


def _parse_binance_symbol(symbol: str) -> str:
    """Return the base asset for binance-style symbols."""

    for quote in BINANCE_QUOTES:
        if symbol.endswith(quote):
            base = symbol[: -len(quote)]
            if base:
                return base

    # Fallback: default to the first three characters if nothing matches
    return symbol[:3] if len(symbol) >= 3 else symbol


def _token_from(symbol: str) -> str:
    """Extract token from various symbol formats."""

    # e.g., Kraken style pairs like "BTC/USD" -> take base before '/'
    if "/" in symbol:
        left = symbol.split("/", 1)[0]
        if left:
            return left

    if "-" in symbol:
        return symbol.rsplit("-", 1)[-1]
    if "_" in symbol:
        return symbol.split("_", 1)[0]

    if symbol.isupper() and len(symbol) > 3:
        if any(symbol.endswith(quote) for quote in BINANCE_QUOTES):
            return _parse_binance_symbol(symbol)

    return symbol


def _format_counter_token(token: str | None) -> str | None:
    return f"{token.upper()}_COUNT" if isinstance(token, str) and token else None


def _resolve_path(message: dict[str, Any], path: tuple[str, ...]) -> Any:
    """Traverse nested dict/list structures following the provided path."""

    current: Any = message
    for key in path:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list):
            # prioritise the first dict-like element
            current = next((item for item in current if isinstance(item, dict)), None)
            if current is None:
                return None
            current = current.get(key)
        else:
            return None
    return current


def update_dict(message: dict, key: str) -> dict:
    """
    message[key]가 dict 또는 list[dict]인 경우 해당 내용을 message에 병합하여 반환
    """
    merged = message.copy()
    data_sub = message.get(key)
    if isinstance(data_sub, dict):
        merged.update(data_sub)
    elif isinstance(data_sub, list) and data_sub and isinstance(data_sub[0], dict):
        merged.update(data_sub[0])

    return merged


def extract_symbol(message: dict[str, Any]) -> str | None:
    """메시지에서 카운터 키를 추출합니다.

    반환 형식: `{TOKEN}_COUNT`
    우선순위:
    - 최상위: code → symbol → s(바이낸스) → market
    - data 블록: target_currency(coinone) → symbol → s → market → code
    정규화 규칙:
    - "KRW-BTC" 등 '-' 포함 시 마지막 세그먼트 → BTC
    - "btc_krw" 등 '_' 포함 시 첫 세그먼트 → btc
    - 바이낸스 "BTCUSDT" → BTC (quote currency 제거)
    - 그 외 전체 문자열을 토큰으로 간주
    """
    if not isinstance(message, dict):
        logger.debug(f"extract_symbol: message is not dict, type={type(message)}")
        return None

    logger.debug(f"extract_symbol: message keys = {list(message.keys())}")

    data_block = message.get("data")
    if isinstance(data_block, dict):
        logger.debug(f"extract_symbol: data block keys = {list(data_block.keys())}")
    elif isinstance(data_block, list) and data_block:
        first_dict = next((item for item in data_block if isinstance(item, dict)), None)
        if first_dict:
            logger.debug(
                f"extract_symbol: data block (list) first dict keys = {list(first_dict.keys())}"
            )

    for path, require_token in FIELD_PRIORITIES:
        raw_value = _resolve_path(message, path)
        if not (isinstance(raw_value, str) and raw_value):
            continue

        field_label = ".".join(path)
        token = _token_from(raw_value) if require_token else raw_value
        result = _format_counter_token(token)
        logger.debug(f"extract_symbol: found {field_label}={raw_value} -> {result}")

        if result:
            return result

    logger.debug("extract_symbol: no symbol found, returning None")
    return None


def normalize_coin_symbol(raw: str) -> str:
    """extract_symbol() 규칙과 100% 동일하게 토큰을 정규화.

    - extract_symbol({"symbol": raw})가 "BTC_COUNT" 형식이면 "BTC"를 반환
    - 폴백: 하이픈/언더스코어 분리 규칙 후 대문자화
    """
    try:
        extracted = extract_symbol({"symbol": raw})
        if isinstance(extracted, str) and extracted.endswith("_COUNT"):
            return extracted[:-6]
        # fallback rules
        token = raw
        if "-" in raw:
            token = raw.rsplit("-", 1)[-1]
        elif "_" in raw:
            token = raw.split("_", 1)[0]
        return token.upper()
    except Exception:
        return str(raw).upper()
