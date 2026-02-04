from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, Callable

from pydantic import BaseModel

from src.common.logger import PipelineLogger
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.internal.metrics import (
    ProcessingMetricsDomain,
    QualityMetricsDomain,
    ReceptionMetricsDomain,
)
from src.core.dto.io.commands import ConnectRequestDTO, ConnectSuccessEventDTO
from src.core.dto.io.events import (
    BackpressureEventDTO,
    BackpressureStatusDTO,
    DlqEventDTO,
    WsErrorEventDTO,
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
from src.core.dto.io.monitoring import (
    BatchMonitoringEventDTO,
    PerformanceSummaryEventDTO,
)
from src.core.dto.io.realtime import RealtimeDataBatchDTO
from src.core.types import (
    TickerResponseData,
    TradeResponseData,
)
from src.infra.messaging.clients.avro_client import (
    AvroProducerWrapper,
    create_avro_producer,
)
from src.infra.messaging.clients.json_client import (
    AsyncProducerWrapper as JsonProducerWrapper,
)
from src.infra.messaging.clients.json_client import (
    create_producer as create_json_producer,
)

# NOTE: Any 사용 사유
# - 프로듀서 계층은 외부 전송 경계로, 스키마 확장(필드 추가)과 다양한 이벤트 페이로드를 수용해야 함
#   따라서 dict[str, Any] 형태의 payload/컨텍스트를 허용하여 직렬화 유연성을 확보합니다.


logger = PipelineLogger(__name__)
# NOTE: 직렬화는 Producer 타입에 따라 자동 처리
# - AvroProducerWrapper: Avro 스키마 기반 직렬화
# - JsonProducerWrapper: orjson 기반 JSON 직렬화
KeyType = str | bytes | None
BatchType = list[TickerResponseData | TradeResponseData]


class AvroProducer:
    """
    통합 Producer 클라이언트
    - use_avro=True: AvroProducerWrapper 사용 (Avro 직렬화)
    - use_avro=False: JsonProducerWrapper 사용 (orjson 직렬화)
    - 동일한 인터페이스로 두 가지 Producer 지원
    """

    def __init__(self, use_avro: bool = False) -> None:
        self._use_avro: bool = use_avro
        self.producer_started: bool = False

        # Producer 타입에 따른 인스턴스 (Union 타입)
        self.producer: AvroProducerWrapper | JsonProducerWrapper | None = None

        # Avro 전용 설정 (use_avro=True일 때만 사용)
        self._avro_subject: str | None = None

    # 실행할 비동기 함수, 예: self.producer.start 또는 self.producer.stop
    async def _execute_with_logging(self, action: Callable) -> bool:
        """지정된 action을 실행하며 로깅을 처리하는 헬퍼 비동기 메서드"""
        await action()
        return True

    async def start_producer(self) -> bool:
        """Producer 시작 - Avro/JSON 타입에 따라 적절한 Producer 사용

        - use_avro=True: AvroProducerWrapper (Avro 직렬화)
        - use_avro=False: JsonProducerWrapper (orjson 직렬화)
        - 동일한 인터페이스로 통일된 사용법 제공
        """
        # 이벤트 루프 가드
        try:
            loop = asyncio.get_running_loop()
            if loop.is_closed():
                logger.warning("Kafka Producer start skipped: event loop is closed")
                return False
        except RuntimeError:
            logger.warning("Kafka Producer start skipped: no running event loop")
            return False

        # 이미 시작되어 있으면 바로 True 반환(멱등성 보장)
        if self.producer_started and self.producer is not None:
            return True

        # Producer 타입에 따른 인스턴스 생성
        if self.producer is None:
            if self._use_avro:
                # Avro Producer: 스키마 기반 직렬화
                if not self._avro_subject:
                    raise ValueError("Avro subject must be set when use_avro=True")
                self.producer = create_avro_producer(value_subject=self._avro_subject)
                logger.info(
                    f"AvroProducerWrapper created with subject: {self._avro_subject}"
                )
            else:
                # JSON Producer: orjson 기반 고성능 직렬화
                self.producer = create_json_producer()
                logger.info("JsonProducerWrapper created with orjson serialization")

        # Producer 시작
        result: bool = await self._execute_with_logging(action=self.producer.start)
        if result:
            self.producer_started = True
            producer_type = "Avro" if self._use_avro else "JSON"
            logger.info(f"Kafka {producer_type} Producer started")
        return result

    async def stop_producer(self) -> None:
        """Producer 종료"""
        if self.producer_started and self.producer is not None:
            await self._execute_with_logging(action=self.producer.stop)
            self.producer_started = False
            logger.info("Kafka Producer stopped")

    def enable_avro(self, subject: str) -> None:
        """Avro 직렬화 활성화 - Producer 재생성 필요"""
        if self.producer_started:
            raise RuntimeError(
                "Cannot change producer type while running. Stop producer first."
            )
        self._use_avro = True
        self._avro_subject = subject
        self.producer = None  # 기존 Producer 무효화
        logger.info(f"Avro serialization enabled for subject: {subject}")

    def disable_avro(self) -> None:
        """Avro 직렬화 비활성화 - Producer 재생성 필요"""
        if self.producer_started:
            raise RuntimeError(
                "Cannot change producer type while running. Stop producer first."
            )
        self._use_avro = False
        self._avro_subject = None
        self.producer = None  # 기존 Producer 무효화
        logger.info("Avro serialization disabled, switching to JSON")

    def get_producer_type(self) -> str:
        """현재 Producer 타입 반환"""
        if self._use_avro:
            return f"AvroProducer(subject={self._avro_subject})"
        else:
            return "JsonProducer(orjson)"

    def get_producer_status(self) -> dict[str, Any]:
        """Producer 상태 조회 (모니터링용)"""
        return {
            "use_avro": self._use_avro,
            "avro_subject": self._avro_subject,
            "producer_started": self.producer_started,
            "producer_type": self.get_producer_type(),
            "producer_instance": (
                type(self.producer).__name__ if self.producer else None
            ),
        }

    async def produce_sending(
        self,
        message: Any,
        topic: str,
        key: KeyType,
        retries: int = 3,
        *,
        stop_after_send: bool = True,
    ) -> bool:
        """통합 메시지 전송 루틴 - Avro/JSON Producer 자동 선택.

        - use_avro=True: AvroProducerWrapper 사용 (스키마 기반)
        - use_avro=False: JsonProducerWrapper 사용 (orjson 기반)
        - Pydantic BaseModel → dict 변환 후 전송
        - 동일한 인터페이스로 통일된 사용법
        """
        try:
            # Pydantic 모델을 dict로 변환
            if isinstance(message, BaseModel):
                message = message.model_dump()

            # Producer 시작
            started = await self.start_producer()
            if not started or self.producer is None:
                logger.warning(
                    f"Kafka Producer not started; skipping send to topic '{topic}'"
                )
                return False

            # 재시도 로직
            attempt = 1
            while attempt <= retries:
                try:
                    await self.producer.send_and_wait(
                        topic=topic,
                        value=message,
                        key=key,
                    )
                    producer_type = "Avro" if self._use_avro else "JSON"
                    logger.debug(f"{producer_type} message sent to topic: {topic}")
                    return True  # 성공 시 즉시 반환

                except Exception as e:
                    logger.warning(f"logging warning attension!! : {e}")
                    if attempt < retries:
                        logger.warning(f"Send attempt {attempt} failed, retrying: {e}")
                        attempt += 1
                        await asyncio.sleep(0.1 * attempt)  # 지수 백오프
                    else:
                        logger.error(f"All {retries} send attempts failed: {e}")
                        return False  # 모든 재시도 실패

            return False  # while 루프 정상 종료 시 (도달 불가)

        finally:
            if stop_after_send:
                await self.stop_producer()


class ConnectRequestProducer(AvroProducer):
    """
    통합 연결 요청 프로듀서 (리팩토링됨).

    - ws.command 토픽으로 웹소켓 연결 요청 전송
    - use_avro=True: AvroProducerWrapper (스키마 기반)
    - use_avro=False: JsonProducerWrapper (orjson 기반) - 기본값
    - 부모 클래스 기반 고성능 비동기 처리
    """

    def __init__(self, topic: str = "ws.command") -> None:
        super().__init__()
        self.topic = topic

    async def send_event(self, event: ConnectRequestDTO, key: KeyType = None) -> bool:
        return await self.produce_sending(
            message=event,
            topic=self.topic,
            key=key,
        )


class ErrorEventProducer(AvroProducer):
    """
    통합 에러 이벤트 프로듀서 (리팩토링됨, DI 주입).

    - 토픽은 DI Container에서 주입받음
    - 실시간 에러 모니터링 및 디버깅 지원
    - use_avro=True: AvroProducerWrapper (error-events-value 스키마)
    - use_avro=False: JsonProducerWrapper (orjson 기반) - 기본값
    - 부모 클래스 기반 고성능 비동기 전송 및 배치 처리
    """

    def __init__(self, topic: str = "ws.error", use_avro: bool = False) -> None:
        super().__init__(use_avro=use_avro)
        self.topic = topic

    async def send_error_event(
        self, event: WsErrorEventDTO, key: KeyType = None
    ) -> bool:
        return await self.produce_sending(
            message=event,
            topic=self.topic,
            key=key,
        )


class DlqProducer(AvroProducer):
    """
    통합 Dead Letter Queue 프로듀서 (리팩토링됨, DI 주입).

    - 토픽은 DI Container에서 주입받음
    - 원본 메시지 + 실패 사유 포함
    - 장애 분석 및 데이터 복구를 위한 안전망
    - use_avro=True: AvroProducerWrapper (dlq-events-value 스키마)
    - use_avro=False: JsonProducerWrapper (orjson 기반) - 기본값
    - 부모 클래스 기반 고성능 비동기 처리
    """

    def __init__(self, topic: str = "ws.dlq", use_avro: bool = False) -> None:
        super().__init__(use_avro=use_avro)
        self.topic = topic

    async def send_dlq_event(self, event: DlqEventDTO, key: KeyType = None) -> None:
        """DTO 기반 DLQ 이벤트 전송.

        - Pydantic 모델을 그대로 전달하면 `produce_sending`에서 dict로 직렬화됩니다.
        """
        await self.produce_sending(
            message=event,
            topic=self.topic,
            key=key,
        )


class BackpressureEventProducer(AvroProducer):
    """
    통합 백프레셔 이벤트 프로듀서 (DI 주입)

    Producer 큐의 백프레셔 상태를 실시간으로 Kafka에 전송하여
    모니터링 및 알림을 지원합니다.

    - 토픽은 DI Container에서 주입받음
    - 백프레셔 활성화/비활성화 이벤트 추적
    - 큐 사용률 및 상태 정보 포함
    - use_avro=True: AvroProducerWrapper (backpressure-events-value 스키마)
    - use_avro=False: JsonProducerWrapper (orjson 기반) - 기본값

    Example:
        >>> producer = BackpressureEventProducer(topic="ws.backpressure.events")
        >>> await producer.start_producer()
        >>> await producer.send_backpressure_event(
        ...     action="backpressure_activated",
        ...     producer_name="MetricsProducer",
        ...     status=status_dict
        ... )
    """

    def __init__(
        self, topic: str = "ws.backpressure.events", use_avro: bool = False
    ) -> None:
        super().__init__(use_avro=use_avro)
        self.topic = topic

    async def send_backpressure_event(
        self,
        action: str,
        producer_name: str,
        status: dict[str, int | float | bool],
        producer_type: str = "AsyncProducerBase",
        message: str | None = None,
        key: KeyType = None,
    ) -> bool:
        """백프레셔 이벤트 전송

        Args:
            action: 백프레셔 상태 ("backpressure_activated" | "backpressure_deactivated")
            producer_name: Producer 이름 (클래스명)
            status: 백프레셔 상태 정보 dict
            producer_type: Producer 타입
            message: 추가 메시지
            key: Kafka 메시지 키

        Returns:
            전송 성공 여부
        """
        status_dto = BackpressureStatusDTO(**status)
        event = BackpressureEventDTO(
            action=action,  # type: ignore
            producer_name=producer_name,
            producer_type=producer_type,
            status=status_dto,
            message=message,
        )

        return await self.produce_sending(
            message=event,
            topic=self.topic,
            key=key,
            stop_after_send=False,
        )


class MetricsProducer(AvroProducer):
    """
    3-Tier 메트릭 독립 전송 프로듀서 (완전 분리형).

    3개의 독립 토픽으로 Layer별 메트릭 전송:
    - Layer 1: ws.metrics.reception.{region}
    - Layer 2: ws.metrics.processing.{region}
    - Layer 3: ws.metrics.quality.{region}

    - use_avro=True: AvroProducerWrapper (스키마 기반) - 기본값
    - use_avro=False: JsonProducerWrapper (orjson 기반)
    """

    def __init__(self, use_avro: bool = False):  # 3-Tier는 JSON 사용
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


class ConnectSuccessEventProducer(AvroProducer):
    """통합 연결 성공 ACK 이벤트 프로듀서 (리팩토링됨, Avro 직렬화 우선).

    - 지역별 토픽에 발행: ws.connect_success.{region}
    - 키 포맷: "{exchange}|{region}|{request_type}|{coin_symbol}"
    - use_avro=True: AvroProducerWrapper (connect-success-events 스키마) - 기본값
    - use_avro=False: JsonProducerWrapper (orjson 기반)
    - 부모 클래스 기반 고성능 비동기 처리
    """

    def __init__(self, use_avro: bool = True) -> None:
        super().__init__(use_avro=use_avro)
        # Avro 사용 시 연결 성공 스키마 설정
        if use_avro:
            self.enable_avro("connect_success")

    async def send_event(self, event: ConnectSuccessEventDTO, key: KeyType) -> bool:
        region = event.target.region
        topic = f"ws.connect_success.{region}"
        return await self.produce_sending(
            message=event,
            topic=topic,
            key=key,
            stop_after_send=False,
        )


class RealtimeDataProducer(AvroProducer):
    """통합 실시간 데이터 배치 프로듀서 (리팩토링됨, Avro 직렬화 우선)

    토픽 전략:
    - realtime_ticker.{region}
    - realtime_trade.{region}

    성능 최적화:
    - use_avro=True: AvroProducerWrapper (realtime_ticker/trade-data-value 스키마) - 기본값
    - use_avro=False: JsonProducerWrapper (orjson 기반, 3-5배 빠름)
    - 부모 클래스 기반 고성능 비동기 처리
    - Avro 직렬화로 20-40% 메시지 크기 감소
    - asyncio.to_thread() CPU 오프로드
    - 스키마 진화 자동 처리
    """

    def __init__(self, use_avro: bool = False) -> None:
        super().__init__(use_avro=use_avro)
        # Avro 사용 시 기본적으로 ticker 데이터 스키마 설정
        if use_avro:
            self.enable_avro("realtime_ticker")

    def _convert_to_dto(
        self, scope: ConnectionScopeDomain, batch: list[dict[str, Any]]
    ) -> RealtimeDataBatchDTO:
        """배치 데이터를 DTO로 변환 (타입 안전성 보장)"""
        return RealtimeDataBatchDTO(
            exchange=scope.exchange,
            region=scope.region,
            request_type=scope.request_type,
            timestamp_ms=int(time.time() * 1000),
            batch_size=len(batch),
            data=batch,  # Ticker/Trade 원본 데이터
        )

    async def send_batch(self, scope: ConnectionScopeDomain, batch: BatchType) -> bool:
        """타입별 배치 전송 통합 메서드 (DTO 기반, Avro 지원)"""
        topic = f"{scope.request_type}-data.{scope.region}"
        
        # Kafka Key 결정 (순서 보장): Coin Symbol 추출 시도
        key = None
        if batch and isinstance(batch[0], dict):
            first_msg = batch[0]
            # 우선순위: code > symbol > target_currency > s > market
            raw_symbol = (
                first_msg.get("code") or 
                first_msg.get("symbol") or 
                first_msg.get("target_currency") or
                first_msg.get("s") or
                first_msg.get("market")
            )
            if raw_symbol:
                key = str(raw_symbol).upper()

        if key is None:
            key = f"{scope.exchange}:{scope.region}:{scope.request_type}"

        # DTO로 변환 (타입 안전성 보장)
        message = self._convert_to_dto(scope, batch)
        return await self.produce_sending(
            message=message,
            topic=topic,
            key=key,
            stop_after_send=False,
        )


class BatchMonitoringProducer(AvroProducer):
    """배치 성능 모니터링 이벤트 Producer (Avro 직렬화)

    실시간 배치 수집 성능을 모니터링하고 이벤트를 Kafka로 전송합니다.

    토픽: monitoring.batch.performance
    키: {region}:{exchange}:{request_type}
    스키마: Avro (batch-monitoring-value)

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
        # Avro 직렬화 사용
        super().__init__(use_avro=False)
        self.topic = "monitoring.batch.performance"
        # TODO: 스키마 등록 후 enable
        # self.enable_avro("batch-monitoring-value")

    async def send_batch_event(self, event: BatchMonitoringEventDTO) -> bool:
        """배치 수집 이벤트 전송

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
        """성능 요약 이벤트 전송

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
