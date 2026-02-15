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
from src.infra.messaging.avro.subjects import (
    PROCESSING_METRICS_SUBJECT,
    QUALITY_METRICS_SUBJECT,
    RECEPTION_METRICS_SUBJECT,
)
from src.infra.messaging.connect.producer_client import AvroProducer
from src.infra.messaging.connect.serialization_policy import resolve_use_avro_for_topic

KeyType = str | bytes | None


class _LayerMetricsAvroProducer(AvroProducer):
    def __init__(self, subject: str) -> None:
        super().__init__(use_avro=True)
        self.enable_avro(subject)

    async def send_message(self, message: object, topic: str, key: KeyType) -> bool:
        return await self.produce_sending(
            message=message,
            topic=topic,
            key=key,
            stop_after_send=False,
        )


class MetricsProducer(AvroProducer):
    """3-Tier 메트릭 독립 전송 프로듀서 (완전 분리형).

    3개의 독립 토픽으로 Layer별 메트릭 전송:
    - Layer 1: ws.metrics.reception.{region}
    - Layer 2: ws.metrics.processing.{region}
    - Layer 3: ws.metrics.quality.{region}

    - use_avro=False: JsonProducerWrapper (orjson 기반, 기본값)
    - use_avro=True: AvroProducerWrapper (스키마 기반)
    """

    def __init__(self, use_avro: bool = False) -> None:
        """3-Tier 메트릭 Producer 초기화."""
        resolved_use_avro, _ = resolve_use_avro_for_topic("ws.metrics.reception.korea", use_avro)
        super().__init__(use_avro=resolved_use_avro)
        self._reception_avro = (
            _LayerMetricsAvroProducer(RECEPTION_METRICS_SUBJECT) if resolved_use_avro else None
        )
        self._processing_avro = (
            _LayerMetricsAvroProducer(PROCESSING_METRICS_SUBJECT) if resolved_use_avro else None
        )
        self._quality_avro = (
            _LayerMetricsAvroProducer(QUALITY_METRICS_SUBJECT) if resolved_use_avro else None
        )

    async def start_producer(self) -> bool:
        started = await super().start_producer()
        if not self._use_avro:
            return started
        if (
            self._reception_avro is None
            or self._processing_avro is None
            or self._quality_avro is None
        ):
            raise RuntimeError("Metrics Avro producers are not initialized")
        reception_avro = self._reception_avro
        processing_avro = self._processing_avro
        quality_avro = self._quality_avro
        reception_started = await reception_avro.start_producer()
        processing_started = await processing_avro.start_producer()
        quality_started = await quality_avro.start_producer()
        return started and reception_started and processing_started and quality_started

    async def stop_producer(self) -> None:
        await super().stop_producer()
        if not self._use_avro:
            return
        if (
            self._reception_avro is None
            or self._processing_avro is None
            or self._quality_avro is None
        ):
            return
        reception_avro = self._reception_avro
        processing_avro = self._processing_avro
        quality_avro = self._quality_avro
        await reception_avro.stop_producer()
        await processing_avro.stop_producer()
        await quality_avro.stop_producer()

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
        if self._use_avro:
            if self._reception_avro is None:
                raise RuntimeError("Reception Avro producer is not initialized")
            reception_avro = self._reception_avro
            return await reception_avro.send_message(
                message=payload,
                topic=topic,
                key=key,
            )
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
        if self._use_avro:
            if self._processing_avro is None:
                raise RuntimeError("Processing Avro producer is not initialized")
            processing_avro = self._processing_avro
            return await processing_avro.send_message(
                message=payload,
                topic=topic,
                key=key,
            )
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
        if self._use_avro:
            if self._quality_avro is None:
                raise RuntimeError("Quality Avro producer is not initialized")
            quality_avro = self._quality_avro
            return await quality_avro.send_message(
                message=payload,
                topic=topic,
                key=key,
            )
        return await self.produce_sending(
            message=payload, topic=topic, key=key, stop_after_send=False
        )
