"""3-Tier 메트릭 Producer (기존 코드 유지).

토픽:
- ws.metrics.reception.{region}
- ws.metrics.processing.{region}
- ws.metrics.quality.{region}
"""

import uuid

from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.internal.metrics import (
    ProcessingMetricsDomain,
    QualityMetricsDomain,
    ReceptionMetricsDomain,
)
from src.core.dto.io.metrics import (
    ProcessingBatchDTO,
    ProcessingMetricsDTO,
    ProcessingMetricsMessage,
    QualityBatchDTO,
    QualityMetricsDTO,
    QualityMetricsMessage,
    ReceptionBatchDTO,
    ReceptionMetricsDTO,
    ReceptionMetricsMessage,
)
from src.infra.messaging.connect.producer_client import AvroProducer

KeyType = str | bytes | None


class MetricsProducer(AvroProducer):
    """3-Tier 메트릭 독립 전송 프로듀서 (완전 분리형).
    
    3개의 독립 토픽으로 Layer별 메트릭 전송:
    - Layer 1: ws.metrics.reception.{region}
    - Layer 2: ws.metrics.processing.{region}
    - Layer 3: ws.metrics.quality.{region}
    
    - use_avro=True: AvroProducerWrapper (스키마 기반) - 기본값
    - use_avro=False: JsonProducerWrapper (orjson 기반)
    """
    
    def __init__(self, use_avro: bool = False) -> None:
        """3-Tier는 JSON 사용."""
        super().__init__(use_avro=use_avro)
        # TODO: 3-Tier Avro 스키마 적용 필요
        # - reception-metrics-value
        # - processing-metrics-value
        # - quality-metrics-value
    
    def _generate_ticket_id(self) -> str:
        return str(uuid.uuid4())
    
    async def send_reception_batch(
        self,
        scope: ConnectionScopeDomain,
        items: list[ReceptionMetricsDomain],
        range_start_ts_kst: int,
        range_end_ts_kst: int,
        bucket_size_sec: int = 60,
        version: int = 1,
        key: KeyType = None,
    ) -> bool:
        """Layer 1: 수신 메트릭 전송 → ws.metrics.reception.{region}"""
        # Domain -> DTO 변환 (list comprehension)
        items_dtos: list[ReceptionMetricsDTO] = [
            ReceptionMetricsDTO(
                minute_start_ts_kst=it.minute_start_ts_kst,
                total_received=it.total_received,
                total_parsed=it.total_parsed,
                total_parse_failed=it.total_parse_failed,
                bytes_received=it.bytes_received,
            )
            for it in items
        ]
        
        payload = ReceptionMetricsMessage(
            ticket_id=self._generate_ticket_id(),
            region=scope.region,
            exchange=scope.exchange,
            request_type=scope.request_type,
            batch=ReceptionBatchDTO(
                ticket_id=self._generate_ticket_id(),
                range_start_ts_kst=range_start_ts_kst,
                range_end_ts_kst=range_end_ts_kst,
                bucket_size_sec=bucket_size_sec,
                items=items_dtos,
                version=version,
            ),
        )
        
        topic = f"ws.metrics.reception.{scope.region}"
        return await self.produce_sending(
            message=payload, topic=topic, key=key, stop_after_send=False
        )
    
    async def send_processing_batch(
        self,
        scope: ConnectionScopeDomain,
        items: list[ProcessingMetricsDomain],
        range_start_ts_kst: int,
        range_end_ts_kst: int,
        bucket_size_sec: int = 60,
        version: int = 1,
        key: KeyType = None,
    ) -> bool:
        """Layer 2: 처리 메트릭 전송 → ws.metrics.processing.{region}"""
        # Domain -> DTO 변환 (list comprehension)
        items_dtos: list[ProcessingMetricsDTO] = [
            ProcessingMetricsDTO(
                minute_start_ts_kst=it.minute_start_ts_kst,
                total_processed=it.total_processed,
                total_failed=it.total_failed,
                details=it.details,
            )
            for it in items
        ]
        
        payload = ProcessingMetricsMessage(
            ticket_id=self._generate_ticket_id(),
            region=scope.region,
            exchange=scope.exchange,
            request_type=scope.request_type,
            batch=ProcessingBatchDTO(
                ticket_id=self._generate_ticket_id(),
                range_start_ts_kst=range_start_ts_kst,
                range_end_ts_kst=range_end_ts_kst,
                bucket_size_sec=bucket_size_sec,
                items=items_dtos,
                version=version,
            ),
        )
        
        topic = f"ws.metrics.processing.{scope.region}"
        return await self.produce_sending(
            message=payload, topic=topic, key=key, stop_after_send=False
        )
    
    async def send_quality_batch(
        self,
        scope: ConnectionScopeDomain,
        items: list[QualityMetricsDomain],
        range_start_ts_kst: int,
        range_end_ts_kst: int,
        bucket_size_sec: int = 60,
        version: int = 1,
        key: KeyType = None,
    ) -> bool:
        """Layer 3: 품질 메트릭 전송 → ws.metrics.quality.{region}"""
        # Domain -> DTO 변환 (list comprehension)
        items_dtos: list[QualityMetricsDTO] = [
            QualityMetricsDTO(
                minute_start_ts_kst=it.minute_start_ts_kst,
                data_completeness=it.data_completeness,
                symbol_coverage=it.symbol_coverage,
                avg_latency_ms=it.avg_latency_ms,
                p95_latency_ms=it.p95_latency_ms,
                p99_latency_ms=it.p99_latency_ms,
                health_score=it.health_score,
            )
            for it in items
        ]
        
        payload = QualityMetricsMessage(
            ticket_id=self._generate_ticket_id(),
            region=scope.region,
            exchange=scope.exchange,
            request_type=scope.request_type,
            batch=QualityBatchDTO(
                ticket_id=self._generate_ticket_id(),
                range_start_ts_kst=range_start_ts_kst,
                range_end_ts_kst=range_end_ts_kst,
                bucket_size_sec=bucket_size_sec,
                items=items_dtos,
                version=version,
            ),
        )
        
        topic = f"ws.metrics.quality.{scope.region}"
        return await self.produce_sending(
            message=payload, topic=topic, key=key, stop_after_send=False
        )
