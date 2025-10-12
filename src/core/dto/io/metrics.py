"""메트릭 DTO 통합 모듈

카운팅, 분 단위 아이템, Producer 메트릭 등 모든 메트릭 DTO를 포함합니다.
"""

from pydantic import BaseModel, Field

from src.core.dto.io._base import BaseIOModelDTO
from src.core.types import ExchangeName, Region, RequestType

# ========================================
# 분 단위 메트릭 아이템 (Minute Item)
# ========================================


class MinuteItemDTO(BaseModel):
    """단일 분 집계 항목 DTO (I/O 경계용 Pydantic v2 모델).

    카운팅/메트릭 전송 시 사용되는 DTO입니다.

    Fields:
        minute_start_ts_kst: 분 시작 타임스탬프 (KST, Unix timestamp)
        total: 해당 분의 총 메시지 수
        details: 심볼별 카운트 (예: {"BTCUSDT_COUNT": 7, "ETHUSDT_COUNT": 3})
    """

    minute_start_ts_kst: int
    total: int
    details: dict[str, int]


# ========================================
# 카운팅 배치 (Counting Batch)
# ========================================


class CountingBatchDTO(BaseIOModelDTO):
    """카운팅 배치 페이로드 (Pydantic v2 모델).

    - 카운팅 아이템 목록과 범위 메타데이터를 포함합니다.
    - unknown/extra 필드는 금지합니다.
    - 타입 안정성: MinuteItemDTO 리스트 사용
    """

    ticket_id: str  # UUID
    range_start_ts_kst: int
    range_end_ts_kst: int
    bucket_size_sec: int
    items: list[MinuteItemDTO]  # 타입 안정성 보장
    version: int


class MarketCountingDTO(BaseIOModelDTO):
    """마켓 소켓 카운팅 메시지 페이로드 (Pydantic v2 모델).

    - region/exchange/request_type 메타와 배치 본문을 포함합니다.
    - unknown/extra 필드는 금지합니다.
    """

    ticket_id: str  # UUID
    region: Region
    exchange: ExchangeName
    request_type: RequestType
    batch: CountingBatchDTO


# ========================================
# Producer 메트릭 (Producer Metrics)
# ========================================


class ProducerMetricsEventDTO(BaseModel):
    """Producer 메트릭 이벤트 DTO (I/O 경계)

    Kafka 토픽으로 전송되는 Producer 성능 메트릭
    - 토픽: metrics.socket.producer
    - 스키마: producer_metrics.avsc와 매핑
    """

    timestamp_ms: int = Field(
        ..., description="메트릭 생성 시각 (Unix timestamp milliseconds)"
    )
    producer_class: str = Field(..., description="Producer 클래스명")
    event_type: str = Field(
        ...,
        description=(
            "메트릭 이벤트 타입 "
            "(DELIVERY_SUCCESS, DELIVERY_FAILURE, BUFFER_RETRY, QUEUE_FULL)"
        ),
    )
    topic: str | None = Field(None, description="대상 토픽명")
    partition: int | None = Field(None, description="파티션 번호")
    offset: int | None = Field(None, description="메시지 오프셋")
    message_key: str | None = Field(None, description="메시지 키 (최대 100자)")
    delivery_latency_ms: float | None = Field(
        None, description="배달 지연 시간 (produce 호출 ~ 배달 완료, milliseconds)"
    )
    queue_depth: int | None = Field(None, description="내부 메시지 큐 깊이")
    buffer_retries: int | None = Field(None, description="BufferError 재시도 횟수")
    error_code: str | None = Field(None, description="에러 코드")
    error_message: str | None = Field(None, description="에러 메시지")
    success: bool = Field(..., description="배달 성공 여부")
