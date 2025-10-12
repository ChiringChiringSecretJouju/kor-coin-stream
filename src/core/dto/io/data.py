"""실시간 데이터 DTO 모듈

Ticker, Orderbook, Trade 등 실시간 데이터 배치 DTO를 포함합니다.
"""

from typing import Any

from pydantic import BaseModel

from src.core.types import ExchangeName, Region, RequestType


class RealtimeDataBatchDTO(BaseModel):
    """실시간 데이터 배치 DTO (I/O 경계용 Pydantic v2 모델).

    Ticker, Orderbook, Trade 데이터를 Kafka로 전송할 때 사용하는 통합 DTO입니다.

    토픽 전략:
        - ticker-data.{region}
        - orderbook-data.{region}
        - trade-data.{region}

    Fields:
        exchange: 거래소 이름
        region: 지역
        request_type: 요청 타입 (ticker, orderbook, trade)
        timestamp_ms: 배치 생성 시각 (밀리초)
        batch_size: 배치 크기
        batch_id: 배치 ID (선택사항)
        data: 실시간 데이터 배열
    """

    exchange: ExchangeName
    region: Region
    request_type: RequestType
    timestamp_ms: int
    batch_size: int
    batch_id: str | None = None
    data: list[dict[str, Any]]  # Ticker/Orderbook/Trade 원본 데이터
