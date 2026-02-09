"""실시간 데이터 DTO 통합 모듈

Ticker, Trade 등 모든 실시간 데이터 DTO를 포함합니다.
재사용 패턴: OPTIMIZED_CONFIG, MarketContextModel 활용으로 코드 중복 제거
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, StrictFloat, StrictInt, StrictStr, StringConstraints

from src.core.dto.io._base import OPTIMIZED_CONFIG, MarketContextModel

# ========================================
# 엄격한 타입 제약 - 공통
# ========================================

# 스트림 타입: SNAPSHOT 또는 REALTIME
StreamType = Literal["SNAPSHOT", "REALTIME"]

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
# Trade DTO
# ========================================


class StandardTradeDTO(BaseModel):
    """표준화된 Trade DTO.

    모든 거래소의 Trade 데이터를 통일한 불변 DTO입니다.

    거래소별 변환:
    - Upbit: 변환 없음 (기준 포맷)
    - Bithumb: 변환 없음 (Upbit와 동일 구조)
    - Korbit: symbol "btc_krw" → code "KRW-BTC", data[0] 평탄화
    - Coinone: target_currency + quote_currency → code 조합, data 평탄화
    - Binance: s "BTCUSDT" → code "BTC-USDT", m → ask_bid, p/q → price/volume
    - Bybit: data[0].s → code, S → ask_bid, p/v → price/volume, seq → sequential_id
    - OKX: data[0].instId → code (이미 하이픈 포함), px/sz → price/volume

    필수 필드:
    - code: 마켓 코드 ("KRW-BTC" 형식으로 통일)
    - trade_timestamp: 체결 타임스탬프 (Unix Seconds, float)
    - trade_price: 체결 가격
    - trade_volume: 체결량
    - ask_bid: 매수/매도 구분 (1=BID, -1=ASK, 0=UNKNOWN)
    - sequential_id: 체결 고유 ID

    파생 필드:
    - trade_amount: 체결대금 (trade_price × trade_volume)

    선택 필드 (백테스팅 지원):
    - stream_type: 스트림 유형 (SNAPSHOT/REALTIME)
    """

    # --- 필수 필드 ---
    code: MarketCodeStr = Field(
        ...,
        description='마켓 코드 ("KRW-BTC" 형식)',
        examples=["KRW-BTC", "KRW-ETH"],
    )
    trade_timestamp: StrictFloat = Field(
        ...,
        description="체결 타임스탬프 (Unix Seconds, float)",
        gt=0.0,
        examples=[1730336862.047],
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
    ask_bid: StrictInt = Field(
        ...,
        description='매수/매도 구분 (1=BID, -1=ASK, 0=UNKNOWN)',
        examples=[1],
    )
    sequential_id: SequentialIdStr = Field(
        ...,
        description="체결 고유 ID (순서 보장 없음)",
        examples=["17303368620470000"],
    )

    # --- 파생 필드 ---
    trade_amount: StrictFloat = Field(
        ...,
        description="체결대금 (trade_price × trade_volume, quote currency 기준)",
        gt=0.0,
        examples=[14291.18],
    )

    # --- 선택 필드 (백테스팅 지원) ---
    stream_type: StreamType | None = Field(
        None,
        description='스트림 유형 ("SNAPSHOT" 또는 "REALTIME")',
        examples=["REALTIME"],
    )

    model_config = OPTIMIZED_CONFIG



# ========================================
# Ticker DTO
# ========================================


class StandardTickerDTO(BaseModel):
    """표준화된 Ticker DTO (OHLCV + 부가 정보).

    모든 거래소의 Ticker 데이터를 통일한 불변 DTO입니다.

    거래소별 변환:
    - Upbit: opening_price→open, high_price→high, low_price→low,
             trade_price→close, acc_trade_volume→volume
    - Bithumb: Upbit와 동일 구조
    - Korbit: data.open/high/low/close/volume, symbol→code, prevClose→prev_close
    - Coinone: data.first→open, data.last→close, yesterday_last→prev_close
    - Binance: o→open, h→high, l→low, c→close, v→volume, x→prev_close
    - Bybit: data.prevPrice24h→open, data.lastPrice→close, data.volume24h→volume
    - OKX: data[0].open24h→open, data[0].last→close, data[0].vol24h→volume

    필수 필드 (모든 거래소 제공):
    - code: 마켓 코드 ("KRW-BTC" 형식)
    - timestamp: 타임스탬프 (Unix Seconds, float)
    - open: 시가 (24시간 기준)
    - high: 고가
    - low: 저가
    - close: 종가/현재가
    - volume: 거래량 (base currency 기준)

    선택 필드 (백테스팅 지원):
    - quote_volume: 거래대금 (KRW 기준, Korbit 미제공 시 None)
    - prev_close: 전일 종가
    - change_price: 변동가 (close - prev_close, 파생)
    - change_rate: 변동률 ((close - prev_close) / prev_close, 파생)
    - stream_type: 스트림 유형 (SNAPSHOT/REALTIME)
    """

    # --- 필수 필드 (OHLCV) ---
    code: MarketCodeStr = Field(
        ...,
        description='마켓 코드 ("KRW-BTC" 형식)',
        examples=["KRW-BTC", "KRW-ETH"],
    )
    timestamp: StrictFloat = Field(
        ...,
        description="타임스탬프 (Unix Seconds, float)",
        gt=0.0,
        examples=[1730336862.082],
    )
    open: StrictFloat = Field(
        ...,
        description="시가 (24시간 기준)",
        ge=0.0,
        examples=[100000000.0],
    )
    high: StrictFloat = Field(
        ...,
        description="고가",
        ge=0.0,
        examples=[101500000.0],
    )
    low: StrictFloat = Field(
        ...,
        description="저가",
        ge=0.0,
        examples=[99500000.0],
    )
    close: StrictFloat = Field(
        ...,
        description="종가/현재가",
        ge=0.0,
        examples=[100473000.0],
    )
    volume: StrictFloat = Field(
        ...,
        description="거래량 (base currency 기준)",
        ge=0.0,
        examples=[2429.588],
    )

    # --- 선택 필드 (백테스팅 지원) ---
    quote_volume: StrictFloat | None = Field(
        None,
        description="거래대금 (quote currency 기준, KRW)",
        ge=0.0,
        examples=[78039261076.512],
    )
    prev_close: StrictFloat | None = Field(
        None,
        description="전일 종가",
        gt=0.0,
        examples=[100000000.0],
    )
    change_price: StrictFloat | None = Field(
        None,
        description="변동가 (close - prev_close)",
        examples=[473000.0],
    )
    change_rate: StrictFloat | None = Field(
        None,
        description="변동률 ((close - prev_close) / prev_close)",
        examples=[0.00473],
    )
    stream_type: StreamType | None = Field(
        None,
        description='스트림 유형 ("SNAPSHOT" 또는 "REALTIME")',
        examples=["REALTIME"],
    )

    model_config = OPTIMIZED_CONFIG


# ========================================
# 배치 DTO
# ========================================


class RealtimeDataBatchDTO(MarketContextModel):
    """실시간 데이터 배치 DTO - 최적화됨 (MarketContextModel 재사용).

    Ticker, Trade 데이터를 Kafka로 전송할 때 사용하는 통합 DTO입니다.

    특징:
    - exchange/region/request_type는 MarketContextModel에서 자동 제공
    - 불변 객체 (frozen=True)

    토픽 전략:
        - ticker-data.{region}
        - trade-data.{region}
    """

    timestamp_ms: int = Field(
        ..., description="배치 생성 시각 (Unix timestamp milliseconds)", gt=0
    )
    batch_size: int = Field(..., description="배치 크기 (메시지 수)", ge=1)
    data: list[dict[str, Any]] = Field(
        ..., description="실시간 데이터 배열 (Ticker/Trade)"
    )
