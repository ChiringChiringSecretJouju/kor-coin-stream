"""
백프레셔 이벤트 DTO

Producer 큐의 백프레셔 상태를 모니터링하기 위한 이벤트 모델
"""

from datetime import datetime, timezone
from typing import Literal

from pydantic import Field

from src.core.dto.io._base import BaseIOModelDTO


class BackpressureStatusDTO(BaseIOModelDTO):
    """백프레셔 상태 정보 DTO
    
    큐의 현재 상태 및 백프레셔 발생 여부를 담습니다.
    """

    queue_size: int = Field(..., description="현재 큐 크기")
    queue_max_size: int = Field(..., description="큐 최대 크기")
    usage_percent: float = Field(..., description="큐 사용률 (%)")
    is_throttled: bool = Field(..., description="백프레셔 활성화 여부")
    high_watermark: int = Field(..., description="High Watermark (throttle 시작)")
    low_watermark: int = Field(..., description="Low Watermark (throttle 해제)")


class BackpressureEventDTO(BaseIOModelDTO):
    """백프레셔 이벤트 DTO (I/O 경계용 Pydantic v2 모델)
    
    Producer 큐에서 백프레셔가 발생했을 때 Kafka로 전송되는 이벤트입니다.
    실시간 모니터링 및 알림에 사용됩니다.
    
    Example:
        >>> event = BackpressureEventDTO(
        ...     action="backpressure_activated",
        ...     producer_name="MetricsProducer",
        ...     status=BackpressureStatusDTO(
        ...         queue_size=850,
        ...         queue_max_size=1000,
        ...         usage_percent=85.0,
        ...         is_throttled=True,
        ...         high_watermark=800,
        ...         low_watermark=200
        ...     )
        ... )
    """

    action: Literal[
        "backpressure_activated", "backpressure_deactivated", "queue_status_report"
    ] = Field(..., description="백프레셔 상태 (활성화/비활성화/주기적 리포트)")
    event_timestamp_utc: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="이벤트 발생 시각 (UTC)",
    )
    producer_name: str = Field(..., description="Producer 이름 (클래스명)")
    producer_type: str = Field(
        default="AsyncProducerBase", description="Producer 타입"
    )
    status: BackpressureStatusDTO = Field(..., description="백프레셔 상태 정보")
    message: str | None = Field(None, description="추가 메시지")
