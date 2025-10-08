"""Producer 메트릭 이벤트 DTO

Kafka Producer의 성능 및 배달 메트릭을 전송하기 위한 I/O 경계 DTO
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ProducerMetricsEventDTO(BaseModel):
    """Producer 메트릭 이벤트 DTO (I/O 경계)
    
    Kafka 토픽으로 전송되는 Producer 성능 메트릭
    - 토픽: metrics.socket.producer
    - 스키마: producer_metrics.avsc와 매핑
    """

    timestamp_ms: int = Field(..., description="메트릭 생성 시각 (Unix timestamp milliseconds)")
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
