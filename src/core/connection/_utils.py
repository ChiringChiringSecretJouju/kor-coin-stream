import re
from typing import Any, Final

from src.common.logger import PipelineLogger

logger = PipelineLogger.get_logger("symbol_extractor", "utils")

# ============================================================================
# 성능 최적화: 사전 컴파일된 정규표현식 및 상수
# ============================================================================

# fmt: off
# Fiat/Stablecoin quote currencies (O(1) lookup을 위한 frozenset)
_FIAT_QUOTES: Final[frozenset[str]] = frozenset({
    "KRW", "USD", "USDT", "USDC", "BUSD", 
    "EUR", "GBP", "JPY", "CNY",
})

# Quote currency suffixes (길이 역순 정렬: 긴 것부터 매칭)
_QUOTE_SUFFIXES: Final[tuple[str, ...]] = (
    "FDUSD", "USDT", "BUSD", "USDC", "TUSD",  # 5자
    "USD", "EUR", "GBP", "JPY", "KRW", "BTC", 
    "ETH", "BNB", "SOL", "DOT", "ADA", "XRP",
)

# 사전 컴파일된 정규표현식 (성능 향상)
_HYPHEN_PATTERN: Final[re.Pattern] = re.compile(r"^([A-Za-z]{2,10})-(.+)$")
_ALPHA_ONLY_PATTERN: Final[re.Pattern] = re.compile(r"^([A-Za-z]+)")

# Symbol 필드 우선순위 (경로, 토큰 추출 필요 여부)
# - 경로: dict 중첩 경로 ("data", "symbol") → message["data"]["symbol"]
# - 토큰 추출 필요 여부: True면 extract_base_currency() 적용, False면 그대로 사용
SymbolFieldSpec = tuple[tuple[str, ...], bool]
FIELD_PRIORITIES: Final[tuple[SymbolFieldSpec, ...]] = (
    # 최상위 필드 (일반적인 거래소)
    (("code",), True),              # Upbit, Bithumb
    (("symbol",), True),            # Binance (병합 후)
    (("s",), True),                 # Binance
    (("market",), True),            # Coinone
    (("product_id",), True),        # Coinbase ⭐ NEW
    (("instId",), True),            # OKX (data 병합 후) ⭐ NEW
    (("ch",), True),                # Huobi ⭐ NEW (특수 처리)
    (("arg", "instId"), True),      # ✅ 추가

    # data 블록 내부 필드
    (("data", "target_currency"), False),
    (("data", "symbol"), True),
    (("data", "s"), True),
    (("data", "instId"), True),    # OKX (병합 전) ⭐ NEW
    (("data", "market"), True),
    (("data", "code"), True),
)


# ============================================================================
# 핵심 Symbol 추출 로직
# ============================================================================


def parse_symbol_pair(symbol: str) -> tuple[str, str]:
    """심볼을 (BASE, QUOTE) 페어로 한 번에 파싱합니다.
    
    기존 extract_base_currency() 로직을 확장하여 QUOTE도 함께 추출합니다.
    
    지원 포맷:
    - Kraken:    "BTC/USD"              → ("BTC", "USD")
    - OKX:       "BTC-USDT"             → ("BTC", "USDT")
    - Upbit:     "KRW-BTC"              → ("BTC", "KRW")
    - Coinone:   "btc_krw"              → ("BTC", "KRW")
    - Coinbase:  "BTC-USD"              → ("BTC", "USD")
    - Huobi:     "BTCUSDT"              → ("BTC", "USDT")
    - Huobi ch:  "market.btcusdt.ticker" → ("BTC", "USDT")
    - Binance:   "ETHBUSD"              → ("ETH", "BUSD")
    
    성능: O(1) ~ O(k), k는 quote suffix 개수 (최대 20개)
    
    Args:
        symbol: 거래소 원본 심볼 문자열
        
    Returns:
        (base, quote) 튜플. 파싱 실패 시 (symbol.upper(), "UNKNOWN")
    """
    if not isinstance(symbol, str) or not symbol:
        return (symbol.upper() if isinstance(symbol, str) else "", "UNKNOWN")
    
    # 0. Huobi 특수 처리: "market.btcusdt.ticker" → "btcusdt"
    if symbol.startswith("market.") and "." in symbol:
        parts = symbol.split(".")
        if len(parts) >= 2:
            symbol = parts[1]  # "btcusdt" 추출
    
    # 2. 슬래시 구분 - Kraken (O(1))
    if "/" in symbol:
        parts = symbol.split("/", 1)
        return (parts[0].strip().upper(), parts[1].strip().upper())
    
    # 3. 하이픈 구분 - OKX, Coinbase, Upbit (O(1) regex)
    match = _HYPHEN_PATTERN.match(symbol)
    if match:
        left, right = match.groups()
        # Fiat이 앞에 있으면 역순 (KRW-BTC → BTC, KRW)
        if left.upper() in _FIAT_QUOTES:
            # 뒤쪽에서 알파벳만 추출
            alpha_match = _ALPHA_ONLY_PATTERN.match(right)
            base = alpha_match.group(1).upper() if alpha_match else right.upper()
            return (base, left.upper())
        # 일반적인 경우 (BTC-USDT → BTC, USDT)
        return (left.upper(), right.upper())
    
    # 4. 언더스코어 구분 - Coinone (O(1))
    if "_" in symbol:
        parts = symbol.split("_", 1)
        return (parts[0].strip().upper(), parts[1].strip().upper())
    
    # 5. 붙어있는 형태 - Huobi, Binance (O(k), k=20)
    # Quote suffix 제거 (긴 것부터 매칭)
    if symbol.isupper() and len(symbol) >= 4:
        for suffix in _QUOTE_SUFFIXES:
            if symbol.endswith(suffix):
                base = symbol[: -len(suffix)]
                if base:  # 빈 문자열 방지
                    return (base, suffix)
    
    # 6. 소문자 변환 시도 (대소문자 혼합)
    upper_symbol = symbol.upper()
    if upper_symbol != symbol and len(upper_symbol) >= 4:
        for suffix in _QUOTE_SUFFIXES:
            if upper_symbol.endswith(suffix):
                base = upper_symbol[: -len(suffix)]
                if base:
                    return (base, suffix)
    
    # 7. 폴백: 전체 대문자 반환, QUOTE는 UNKNOWN
    return (upper_symbol, "UNKNOWN")


def extract_base_currency(symbol: str) -> str:
    """다양한 거래소 symbol 포맷에서 base currency를 추출합니다.
    
    Note:
        parse_symbol_pair()를 사용하여 BASE만 반환합니다.
        기존 코드와의 하위 호환성을 위해 유지됩니다.
    
    지원 포맷:
    - Kraken:    "BTC/USD"              → "BTC"
    - OKX:       "BTC-USDT"             → "BTC"
    - Upbit:     "KRW-BTC"              → "BTC"
    - Coinone:   "btc_krw"              → "BTC"
    - Coinbase:  "BTC-USD"              → "BTC"
    - Huobi:     "BTCUSDT"              → "BTC"
    - Huobi ch:  "market.btcusdt.ticker" → "BTC"
    - Binance:   "ETHBUSD"              → "ETH"
    
    Args:
        symbol: 거래소 원본 심볼 문자열
        
    Returns:
        Base currency (대문자)
    """
    base, _ = parse_symbol_pair(symbol)
    return base


def extract_quote_currency(symbol: str) -> str:
    """다양한 거래소 symbol 포맷에서 quote currency를 추출합니다.
    
    Note:
        parse_symbol_pair()를 사용하여 QUOTE만 반환합니다.
    
    지원 포맷:
    - Upbit:     "KRW-BTC"              → "KRW"
    - OKX:       "BTC-USDT"             → "USDT"
    - Coinbase:  "BTC-USD"              → "USD"
    - Huobi:     "BTCUSDT"              → "USDT"
    - Binance:   "ETHBUSD"              → "BUSD"
    - Kraken:    "BTC/USD"              → "USD"
    - Coinone:   "btc_krw"              → "KRW"
    
    Args:
        symbol: 거래소 원본 심볼 문자열
        
    Returns:
        Quote currency (대문자), 추출 실패 시 "UNKNOWN"
    """
    _, quote = parse_symbol_pair(symbol)
    return quote


def format_counter_key(base_currency: str | None) -> str | None:
    """Base currency를 카운터 키 형식으로 변환합니다.
    
    Args:
        base_currency: "BTC", "ETH" 등
        
    Returns:
        "BTC_COUNT", "ETH_COUNT" 등 (None이면 None 반환)
    """
    if not isinstance(base_currency, str) or not base_currency:
        return None
    return f"{base_currency.upper()}_COUNT"


def _resolve_path(message: dict[str, Any], path: tuple[str, ...]) -> Any:
    """중첩된 dict/list 구조에서 경로를 따라 값을 추출합니다.
    
    성능: O(n), n은 경로 깊이 (일반적으로 1~2)
    """
    current: Any = message
    for key in path:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list):
            # 첫 번째 dict 요소 선택
            current = next((item for item in current if isinstance(item, dict)), None)
            if current is None:
                return None
            current = current.get(key)
        else:
            return None
    return current


def update_dict(message: dict, key: str) -> dict:
    """message[key]의 내용을 최상위로 병합하여 반환합니다.
    
    성능: O(k), k는 병합할 키 개수
    """
    merged = message.copy()
    data_sub = message.get(key)
    if isinstance(data_sub, dict):
        merged.update(data_sub)
    elif isinstance(data_sub, list) and data_sub and isinstance(data_sub[0], dict):
        merged.update(data_sub[0])
    return merged


def extract_symbol(message: dict[str, Any]) -> str | None:
    """메시지에서 카운터 키를 추출합니다 (최종 형식: {TOKEN}_COUNT).
    
    알고리즘:
    1. 우선순위에 따라 필드 탐색 (최상위 → data 블록)
    2. Base currency 추출 (거래소별 포맷 자동 감지)
    3. {TOKEN}_COUNT 형식으로 변환
    
    성능: O(n), n은 필드 우선순위 개수 (최대 9개)
    
    Args:
        message: WebSocket 메시지 딕셔너리
        
    Returns:
        "BTC_COUNT", "ETH_COUNT" 등 (없으면 None)
        
    Examples:
        >>> extract_symbol({"symbol": "BTC-USDT"})   
        "BTC_COUNT"
        >>> extract_symbol({"s": "ETHUSDT"})
        "ETH_COUNT"
        >>> extract_symbol({"code": "KRW-BTC"})
        "BTC_COUNT"
    """
    
    if not isinstance(message, dict):
        logger.debug(f"extract_symbol: message is not dict, type={type(message)}")
        return None

    logger.debug(f"extract_symbol: message keys = {list(message.keys())}")

    # 우선순위에 따라 필드 탐색
    for path, need_extraction in FIELD_PRIORITIES:
        raw_value = _resolve_path(message, path)
        
        # 빈 값 스킵
        if not (isinstance(raw_value, str) and raw_value):
            continue

        # Base currency 추출 (거래소 포맷 자동 감지)
        base_currency = extract_base_currency(raw_value) if need_extraction else raw_value
        
        # {TOKEN}_COUNT 형식으로 변환
        counter_key = format_counter_key(base_currency)
        
        field_label = ".".join(path)
        logger.debug(
            f"extract_symbol: found {field_label}={raw_value} → {counter_key}"
        )

        if counter_key:
            return counter_key

    logger.debug("extract_symbol: no symbol found, returning None")
    return None
