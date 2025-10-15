"""실시간 데이터 DTO 통합 모듈

Ticker, Orderbook, Trade 등 모든 실시간 데이터 DTO를 포함합니다.
재사용 패턴: OPTIMIZED_CONFIG, MarketContextModel 활용으로 코드 중복 제거
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, StrictFloat, StrictInt, StrictStr, StringConstraints

from src.core.dto.io._base import OPTIMIZED_CONFIG, MarketContextModel

# ========================================
# 엄격한 타입 제약 - Orderbook
# ========================================

# 가격/수량 문자열 (소수점 표현)
PriceStr = Annotated[
    StrictStr,
    StringConstraints(
        min_length=1,
        max_length=32,
        pattern=r"^\d+(?:\.\d+)?$",
        strip_whitespace=True,
    ),
]

# 심볼 (BTC, ETH 등)
SymbolStr = Annotated[
    StrictStr,
    StringConstraints(
        min_length=1,
        max_length=16,
        pattern=r"^[A-Z]{2,10}$",
        strip_whitespace=True,
    ),
]

# Quote Currency (KRW, USDT 등)
QuoteCurrencyStr = Annotated[
    StrictStr,
    StringConstraints(
        min_length=2,
        max_length=10,
        pattern=r"^[A-Z]{2,10}$",
        strip_whitespace=True,
    ),
]

# ========================================
# 엄격한 타입 제약 - Trade
# ========================================

# 마켓 코드: "KRW-BTC" 형식 (하이픈 필수)
MarketCodeStr = Annotated[
    StrictStr,
    StringConstraints(
        min_length=5,
        max_length=32,
        pattern=r"^[A-Z]{2,10}-[A-Z]{2,10}$",
        strip_whitespace=True,
    ),
]

# 체결 고유 ID (숫자 문자열)
SequentialIdStr = Annotated[
    StrictStr,
    StringConstraints(
        min_length=1,
        max_length=32,
        pattern=r"^\d+$",
        strip_whitespace=True,
    ),
]


# ========================================
# Orderbook DTO
# ========================================


class OrderbookItemDTO(BaseModel):
    """호가 아이템 DTO (price, size).
    
    Orderbook의 개별 호가 데이터를 표현하는 불변 DTO입니다.
    모든 거래소에서 동일한 형식으로 변환됩니다.
    
    특징:
    - 불변 객체 (frozen=True)
    - 문자열 가격/수량 (정밀도 보존)
    - 엄격한 검증 (양수 소수점 형식)
    
    Example:
        >>> item = OrderbookItemDTO(
        ...     price="100473000.0",
        ...     size="0.00014208"
        ... )
    """
    
    price: PriceStr = Field(
        ..., 
        description="호가 가격 (문자열, 정밀도 보존)",
        examples=["100473000.0", "3700000.0"],
    )
    size: PriceStr = Field(
        ..., 
        description="호가 수량 (문자열, 정밀도 보존)",
        examples=["0.43139478", "0.02207517"],
    )
    
    model_config = OPTIMIZED_CONFIG


class StandardOrderbookDTO(BaseModel):
    """표준화된 Orderbook DTO.
    
    모든 한국 거래소의 Orderbook 데이터를 동일한 형식으로 통일한 불변 DTO입니다.
    
    거래소별 변환:
    - Upbit: orderbook_units 쌍 → asks/bids 분리
    - Bithumb: Upbit와 동일 (상속)
    - Korbit: data.asks/bids → 표준 포맷 (qty → size 변환)
    - Coinone: 이미 분리된 asks/bids 사용
    
    특징:
    - 불변 객체 (frozen=True)
    - 심볼/Quote 분리 저장
    - 타임스탬프 자동 생성 (파서에서 처리)
    
    Example:
        >>> orderbook = StandardOrderbookDTO(
        ...     symbol="BTC",
        ...     quote_currency="KRW",
        ...     timestamp=1730336862000,
        ...     asks=[
        ...         OrderbookItemDTO(price="100473000", size="0.43139478"),
        ...         OrderbookItemDTO(price="100474000", size="0.12345678")
        ...     ],
        ...     bids=[
        ...         OrderbookItemDTO(price="100465000", size="0.01990656"),
        ...         OrderbookItemDTO(price="100464000", size="0.98765432")
        ...     ]
        ... )
    """
    
    symbol: SymbolStr = Field(
        ..., 
        description="심볼 (BTC, ETH 등)",
        examples=["BTC", "ETH"],
    )
    quote_currency: QuoteCurrencyStr = Field(
        ..., 
        description="기준 통화 (KRW, USDT 등)",
        examples=["KRW", "USDT"],
    )
    timestamp: StrictInt = Field(
        ..., 
        description="타임스탬프 (Unix milliseconds)",
        gt=0,
        examples=[1730336862000],
    )
    asks: list[OrderbookItemDTO] = Field(
        ..., 
        description="매도 호가 리스트",
        min_length=0,
    )
    bids: list[OrderbookItemDTO] = Field(
        ..., 
        description="매수 호가 리스트",
        min_length=0,
    )
    
    model_config = OPTIMIZED_CONFIG


# ========================================
# Trade DTO
# ========================================


class StandardTradeDTO(BaseModel):
    """표준화된 Trade DTO (Upbit 포맷 기준).
    
    모든 한국 거래소의 Trade 데이터를 Upbit 형식으로 통일한 불변 DTO입니다.
    
    거래소별 변환:
    - Upbit: 변환 없음 (기준 포맷)
    - Bithumb: 변환 없음 (Upbit와 동일 구조)
    - Korbit: symbol "btc_krw" → code "KRW-BTC", data[0] 평탄화
    - Coinone: target_currency + quote_currency → code 조합, data 평탄화
    
    특징:
    - 불변 객체 (frozen=True)
    - 엄격한 타입 검증 (StrictStr, StrictFloat)
    - 필드 제약 (pattern, gt, ge 등)
    
    Example:
        >>> trade = StandardTradeDTO(
        ...     code="KRW-BTC",
        ...     trade_timestamp=1730336862047,
        ...     trade_price=100473000.0,
        ...     trade_volume=0.00014208,
        ...     ask_bid="BID",
        ...     sequential_id="17303368620470000"
        ... )
    """
    
    code: MarketCodeStr = Field(
        ..., 
        description='마켓 코드 ("KRW-BTC" 형식)',
        examples=["KRW-BTC", "KRW-ETH"],
    )
    trade_timestamp: StrictInt = Field(
        ..., 
        description="체결 타임스탬프 (Unix milliseconds)",
        gt=0,
        examples=[1730336862047],
    )
    trade_price: StrictFloat = Field(
        ..., 
        description="체결 가격",
        gt=0.0,
        examples=[100473000.0],
    )
    trade_volume: StrictFloat = Field(
        ..., 
        description="체결량",
        gt=0.0,
        examples=[0.00014208],
    )
    ask_bid: Literal["ASK", "BID"] = Field(
        ..., 
        description='매수/매도 구분 ("ASK" or "BID")',
        examples=["BID"],
    )
    sequential_id: SequentialIdStr = Field(
        ..., 
        description="체결 고유 ID (순서 보장 없음)",
        examples=["17303368620470000"],
    )
    
    model_config = OPTIMIZED_CONFIG


# ========================================
# 배치 DTO
# ========================================


class RealtimeDataBatchDTO(MarketContextModel):
    """실시간 데이터 배치 DTO - 최적화됨 (MarketContextModel 재사용).

    Ticker, Orderbook, Trade 데이터를 Kafka로 전송할 때 사용하는 통합 DTO입니다.

    특징:
    - exchange/region/request_type는 MarketContextModel에서 자동 제공
    - 불변 객체 (frozen=True)

    토픽 전략:
        - ticker-data.{region}
        - orderbook-data.{region}
        - trade-data.{region}
    """

    timestamp_ms: int = Field(
        ..., description="배치 생성 시각 (Unix timestamp milliseconds)", gt=0
    )
    batch_size: int = Field(..., description="배치 크기 (메시지 수)", ge=1)
    batch_id: str | None = Field(None, description="배치 ID (선택사항)")
    data: list[dict[str, Any]] = Field(
        ..., description="실시간 데이터 배열 (Ticker/Orderbook/Trade)"
    )
