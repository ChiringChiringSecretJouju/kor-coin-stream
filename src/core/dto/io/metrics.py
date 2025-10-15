"""메트릭 DTO 통합 모듈

카운팅, 분 단위 아이템, Producer 메트릭 등 모든 메트릭 DTO를 포함합니다.
재사용 패턴: MarketContextModel 활용으로 코드 중복 제거
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from src.core.dto.io._base import OPTIMIZED_CONFIG, BaseIOModelDTO, MarketContextModel

# ========================================
# 분 단위 메트릭 아이템 (Minute Item)
# ========================================


class ReceptionMetricsDTO(BaseModel):
    """Layer 1: 수신 메트릭 DTO (독립 전송용)."""

    minute_start_ts_kst: int = Field(..., description="분 시작 타임스탬프", gt=0)
    total_received: int = Field(..., description="수신한 모든 메시지 수", ge=0)
    total_parsed: int = Field(..., description="JSON 파싱 성공 수", ge=0)
    total_parse_failed: int = Field(..., description="JSON 파싱 실패 수", ge=0)
    bytes_received: int = Field(..., description="수신 바이트 수", ge=0)

    model_config = OPTIMIZED_CONFIG


class ProcessingMetricsDTO(BaseModel):
    """Layer 2: 처리 메트릭 DTO (독립 전송용)."""

    minute_start_ts_kst: int = Field(..., description="분 시작 타임스탬프", gt=0)
    total_processed: int = Field(..., description="정상 처리된 메시지 수", ge=0)
    total_failed: int = Field(..., description="처리 실패 메시지 수", ge=0)
    details: dict[str, int] = Field(..., description="심볼별 카운트")

    model_config = OPTIMIZED_CONFIG


class QualityMetricsDTO(BaseModel):
    """Layer 3: 품질 메트릭 DTO (독립 전송용)."""

    minute_start_ts_kst: int = Field(..., description="분 시작 타임스탬프", gt=0)
    data_completeness: float = Field(..., description="데이터 완전성", ge=0.0, le=1.0)
    symbol_coverage: int = Field(..., description="처리된 심볼 종류 수", ge=0)
    avg_latency_ms: float = Field(..., description="평균 처리 지연 (ms)", ge=0.0)
    p95_latency_ms: float = Field(..., description="95 백분위 지연 (ms)", ge=0.0)
    p99_latency_ms: float = Field(..., description="99 백분위 지연 (ms)", ge=0.0)
    health_score: float = Field(..., description="종합 건강도 점수", ge=0.0, le=100.0)

    model_config = OPTIMIZED_CONFIG


# ========================================
# Layer별 독립 배치 (Separate Batches per Layer)
# ========================================


class ReceptionBatchDTO(BaseIOModelDTO):
    """Layer 1: 수신 메트릭 배치 (독립 전송)."""

    ticket_id: str
    range_start_ts_kst: int
    range_end_ts_kst: int
    bucket_size_sec: int
    items: list[ReceptionMetricsDTO]
    version: int


class ProcessingBatchDTO(BaseIOModelDTO):
    """Layer 2: 처리 메트릭 배치 (독립 전송)."""

    ticket_id: str
    range_start_ts_kst: int
    range_end_ts_kst: int
    bucket_size_sec: int
    items: list[ProcessingMetricsDTO]
    version: int


class QualityBatchDTO(BaseIOModelDTO):
    """Layer 3: 품질 메트릭 배치 (독립 전송)."""

    ticket_id: str
    range_start_ts_kst: int
    range_end_ts_kst: int
    bucket_size_sec: int
    items: list[QualityMetricsDTO]
    version: int


# ========================================
# Layer별 Kafka 메시지 (Market Context 포함)
# ========================================


class ReceptionMetricsMessage(MarketContextModel):
    """Layer 1: 수신 메트릭 메시지 (Kafka 전송용)."""

    batch: ReceptionBatchDTO = Field(..., description="수신 메트릭 배치")


class ProcessingMetricsMessage(MarketContextModel):
    """Layer 2: 처리 메트릭 메시지 (Kafka 전송용)."""

    batch: ProcessingBatchDTO = Field(..., description="처리 메트릭 배치")


class QualityMetricsMessage(MarketContextModel):
    """Layer 3: 품질 메트릭 메시지 (Kafka 전송용)."""

    batch: QualityBatchDTO = Field(..., description="품질 메트릭 배치")


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
