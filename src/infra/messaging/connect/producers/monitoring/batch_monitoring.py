"""배치 성능 모니터링 이벤트 Producer.

토픽: monitoring.batch.performance
"""

import time

from src.common.logger import PipelineLogger
from src.core.dto.io.monitoring import (
    BatchMonitoringEventDTO,
    PerformanceSummaryEventDTO,
)
from src.infra.messaging.connect.producer_client import AvroProducer

logger = PipelineLogger.get_logger(__name__)


class BatchMonitoringProducer(AvroProducer):
    """배치 성능 모니터링 이벤트 Producer (Avro 직렬화).
    
    토픽: monitoring.batch.performance
    키: {region}:{exchange}:{request_type}
    스키마: batch-monitoring-value (Avro)
    
    성능 메트릭:
    - 배치 크기 및 처리 시간
    - 심볼 다양성 (캐시 효율성 지표)
    - 거래소/지역별 처리량
    - 주기적 성능 요약
    
    Example:
        >>> producer = BatchMonitoringProducer()
        >>> await producer.start_producer()
        >>> from src.core.dto.io.monitoring import (
        ...     BatchMonitoringEventDTO,
        ...     BatchPerformanceStatsDTO,
        ... )
        >>> stats = BatchPerformanceStatsDTO(
        ...     batch_size=100,
        ...     unique_symbols=10,
        ...     cache_efficiency=0.1,
        ...     collection_timestamp_ms=1234567890,
        ... )
        >>> event = BatchMonitoringEventDTO(
        ...     region="korea",
        ...     exchange="upbit",
        ...     request_type="ticker",
        ...     stats=stats,
        ... )
        >>> await producer.send_batch_event(event)
    """
    
    def __init__(self) -> None:
        """Avro 직렬화 사용."""
        super().__init__(use_avro=True)
        self.topic = "monitoring.batch.performance"
        # TODO: 스키마 등록 후 enable
        # self.enable_avro("batch-monitoring-value")
    
    async def send_batch_event(self, event: BatchMonitoringEventDTO) -> bool:
        """배치 수집 이벤트 전송.
        
        Args:
            event: BatchMonitoringEventDTO (배치 모니터링 이벤트 DTO)
            
        Returns:
            전송 성공 여부
        """
        if not self.producer_started or not self.producer:
            logger.warning("Producer not started, skipping batch monitoring event")
            return False
        
        try:
            # 메시지 키 생성
            key = f"{event.region}:{event.exchange}:{event.request_type}"
            
            # DTO를 dict로 변환 (Pydantic model_dump)
            payload = event.model_dump(mode="json")
            
            # event_timestamp_utc 추가
            payload["event_timestamp_utc"] = (
                time.strftime(
                    "%Y-%m-%dT%H:%M:%S.%f",
                    time.gmtime(event.stats.collection_timestamp_ms / 1000),
                )[:-3]
                + "Z"
            )
            
            # 메시지 전송
            await self.producer.send_and_wait(
                topic=self.topic,
                value=payload,
                key=key,
            )
            
            return True
        
        except Exception as e:
            logger.error(
                f"Failed to send batch monitoring event: {e}",
                extra={
                    "region": event.region,
                    "exchange": event.exchange,
                    "error": str(e),
                },
            )
            return False
    
    async def send_summary_event(self, event: PerformanceSummaryEventDTO) -> bool:
        """성능 요약 이벤트 전송.
        
        Args:
            event: PerformanceSummaryEventDTO (성능 요약 이벤트 DTO)
            
        Returns:
            전송 성공 여부
        """
        if not self.producer_started or not self.producer:
            logger.warning("Producer not started, skipping summary event")
            return False
        
        key = f"{event.region}:{event.exchange}:{event.request_type}"
        
        # DTO를 dict로 변환 (Pydantic model_dump)
        payload = event.model_dump(mode="json")
        
        # event_timestamp_utc 추가
        payload["event_timestamp_utc"] = time.strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ", time.gmtime()
        )
        
        await self.producer.send_and_wait(
            topic=self.topic,
            value=payload,
            key=key,
        )
        
        return True
