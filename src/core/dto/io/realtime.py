"""실시간 데이터 DTO 통합 모듈

Ticker, Trade 등 모든 실시간 데이터 DTO를 포함합니다.
재사용 패턴: OPTIMIZED_CONFIG, MarketContextModel 활용으로 코드 중복 제거
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, Field, StrictFloat, StrictInt, StrictStr, StringConstraints

from src.core.dto.io._base import OPTIMIZED_CONFIG, MarketContextModel

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
    
    - code: 마켓 코드 ("KRW-BTC" 형식으로 통일)
    - trade_timestamp: 체결 타임스탬프 (Unix Seconds, float)
    - trade_price: 체결 가격 (float)
    - trade_volume: 체결량 (float)
    - ask_bid: 매수/매도 구분 (1=BID, -1=ASK, 0=UNKNOWN)
    - sequential_id: 체결 고유 ID (문자열)
    """
    
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
