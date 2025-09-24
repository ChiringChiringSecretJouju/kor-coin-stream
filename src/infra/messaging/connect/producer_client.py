from __future__ import annotations

import asyncio
import time
from typing import Any, Callable
import uuid

from pydantic import BaseModel

from src.common.logger import PipelineLogger

# NOTE: confluent-kafka 기반으로 직렬화 처리 (기존 aiokafka 방식에서 마이그레이션)
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.internal.metrics import MinuteItemDomain
from src.core.dto.io.commands import ConnectRequestDTO, ConnectSuccessEventDTO
from src.core.dto.io.counting import CountingBatchDTO, MarketCountingDTO
from src.core.dto.io.dlq_event import DlqEventDTO
from src.core.dto.io.error_event import WsErrorEventDTO
from src.core.types import (
    TickerResponseData,
    OrderbookResponseData,
    TradeResponseData,
)
from src.infra.messaging.clients.clients import create_producer, AsyncProducerWrapper
from src.infra.messaging.avro import create_avro_serializer, SchemaRegistryClient

# NOTE: Any 사용 사유
# - 프로듀서 계층은 외부 전송 경계로, 스키마 확장(필드 추가)과 다양한 이벤트 페이로드를 수용해야 함
#   따라서 dict[str, Any] 형태의 payload/컨텍스트를 허용하여 직렬화 유연성을 확보합니다.


logger = PipelineLogger(__name__)
# NOTE: 직렬화는 AsyncProducerWrapper 내부의 default_value_serializer에서 처리
KeyType = str | bytes | None


class AvroProducer:
    """
    AvroProducer
    - 카프카 전송 전용 클라이언트
    - Avro 직렬화 지원 (선택적)
    """

    def __init__(self, use_avro: bool = False):
        # confluent-kafka 기반 AsyncProducerWrapper
        self.producer: AsyncProducerWrapper | None = None
        self.producer_started: bool = False

        # Avro 직렬화 관련 (선택적)
        self._avro_serializer: Any = None
        self._use_avro: bool = use_avro
        self._avro_initialized: bool = False
        self._avro_subject: str | None = None

    # 실행할 비동기 함수, 예: self.producer.start 또는 self.producer.stop
    async def _execute_with_logging(self, action: Callable) -> bool:
        """지정된 action을 실행하며 로깅을 처리하는 헬퍼 비동기 메서드"""
        await action()
        return True

    async def start_producer(self) -> bool:
        """confluent-kafka Producer 시작 및 재사용

        - confluent-kafka 기반 AsyncProducerWrapper 사용
        - 고성능 C 라이브러리 기반 처리
        - 기본 파티셔닝 정책 사용 (성능 최적화)
        """
        # 이벤트 루프 가드: 종료 중이거나 루프가 없으면 시작을 시도하지 않음
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

        # confluent-kafka 기반 프로듀서 생성
        if self.producer is None:
            # AsyncProducerWrapper: confluent-kafka Producer의 비동기 래퍼
            # - C 라이브러리 기반 고성능 처리
            # - 내장 직렬화: default_value_serializer (JSON)
            # - 배치 처리 및 압축 최적화 적용
            self.producer = create_producer()

        # 헬퍼 메서드를 통해 시작 시도
        result: bool = await self._execute_with_logging(action=self.producer.start)
        if result:
            self.producer_started = True
            logger.info("Kafka Producer started")
        return result

    async def stop_producer(self) -> None:
        """Producer 종료"""
        if self.producer_started and self.producer is not None:
            await self._execute_with_logging(action=self.producer.stop)
            self.producer_started = False
            logger.info("Kafka Producer stopped")

    def enable_avro(self, subject: str) -> None:
        """Avro 직렬화 활성화"""
        self._use_avro = True
        self._avro_subject = subject
        logger.info(f"Avro serialization enabled for subject: {subject}")

    def disable_avro(self) -> None:
        """Avro 직렬화 비활성화"""
        self._use_avro = False
        self._avro_subject = None
        logger.info("Avro serialization disabled")

    async def _ensure_avro_serializer(self) -> None:
        """Avro 직렬화기 초기화 (지연 로딩)"""
        if self._avro_initialized or not self._use_avro or not self._avro_subject:
            return

        try:
            # Schema Registry 클라이언트 생성
            schema_registry_client = SchemaRegistryClient(
                base_url="http://localhost:8082"
            )

            # Avro 직렬화기 생성 (자동 초기화 포함)
            self._avro_serializer = create_avro_serializer(
                schema_registry_client=schema_registry_client,
                subject=self._avro_subject,
            )

            # 스키마 초기화
            await self._avro_serializer.ensure_schema()

            self._avro_initialized = True
            logger.info(
                f"Avro serializer initialized for subject: {self._avro_subject}"
            )

        except Exception as e:
            logger.error(f"Failed to initialize Avro serializer: {e}")
            self._use_avro = False

    def get_avro_status(self) -> dict[str, Any]:
        """Avro 상태 조회 (모니터링용)"""
        return {
            "avro_enabled": self._use_avro,
            "avro_initialized": self._avro_initialized,
            "avro_subject": self._avro_subject,
            "serializer_available": self._avro_serializer is not None,
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
        """confluent-kafka 기반 메시지 전송 공통 루틴.

        - Pydantic BaseModel → dict 변환 후 전송
        - Avro 직렬화 지원 (선택적)
        - confluent-kafka의 고성능 C 라이브러리 활용
        - 배치 처리, 압축, 재시도 로직 내장
        """
        try:
            # Pydantic 모델을 dict로 변환
            if isinstance(message, BaseModel):
                message = message.model_dump()

            # Avro 직렬화 시도 (활성화된 경우)
            if self._use_avro:
                try:
                    await self._ensure_avro_serializer()
                    if self._avro_serializer:
                        # Avro로 직렬화 (CPU 오프로드)
                        serialized_data = await self._avro_serializer.serialize_async(
                            message
                        )

                        started = await self.start_producer()
                        if not started:
                            logger.warning(
                                f"Kafka Producer not started; skipping send to topic '{topic}'"
                            )
                            return False

                        # Avro 직렬화된 바이트 데이터 전송 (AsyncProducerWrapper가 바이트 감지)
                        await self.producer.send_and_wait(
                            topic=topic,
                            value=serialized_data,  # bytes 타입
                            key=key,
                        )
                        logger.debug(f"Avro message sent to topic: {topic}")
                        return True

                except Exception as e:
                    logger.warning(
                        f"Avro serialization failed, falling back to JSON: {e}"
                    )
                    # Avro 실패 시 JSON으로 폴백
                    self._use_avro = False

            # JSON 직렬화 (기본 방식 또는 Avro 실패 시 폴백)
            started = await self.start_producer()
            if not started:
                logger.warning(
                    f"Kafka Producer not started; skipping send to topic '{topic}'"
                )
                return False

            attempt = 1
            while attempt <= retries:
                await self.producer.send_and_wait(
                    topic=topic,
                    value=message,
                    key=key,
                )
                return True  # 성공 시 즉시 반환
        finally:
            if stop_after_send:
                await self.stop_producer()


class ConnectRequestProducer(AvroProducer):
    """
    confluent-kafka 기반 연결 요청 프로듀서.

    - ws.command 토픽으로 웹소켓 연결 요청 전송
    - 고성능 C 라이브러리 기반 처리
    - JSON 직렬화 및 배치 최적화 적용
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
    confluent-kafka 기반 에러 이벤트 프로듀서.

    - ws.error 토픽으로 웹소켓/데이터 에러 전송
    - 실시간 에러 모니터링 및 디버깅 지원
    - 고성능 비동기 전송 및 배치 처리
    """

    async def send_error_event(
        self, event: WsErrorEventDTO, key: KeyType = None
    ) -> bool:
        await self.produce_sending(
            message=event,
            topic="ws.error",
            key=key,
        )


class DlqProducer(AvroProducer):
    """
    confluent-kafka 기반 Dead Letter Queue 프로듀서.

    - ws.dlq 토픽으로 처리 실패 메시지 전송
    - 원본 메시지 + 실패 사유 포함
    - 장애 분석 및 데이터 복구를 위한 안전망
    """

    async def send_dlq_event(self, event: DlqEventDTO, key: KeyType = None) -> None:
        """DTO 기반 DLQ 이벤트 전송.

        - Pydantic 모델을 그대로 전달하면 `produce_sending`에서 dict로 직렬화됩니다.
        """
        await self.produce_sending(
            message=event,
            topic="ws.dlq",
            key=key,
        )


class MetricsProducer(AvroProducer):
    """
    confluent-kafka 기반 메트릭 배치 프로듀서 (Avro 직렬화 지원).

    - 지역별 토픽으로 실시간 메트릭 전송: ws.counting.message.{region}
    - 분 단위 메시지 카운팅 및 집계 데이터
    - 성능 모니터링 및 비즈니스 인사이트 제공
    - Avro 스키마: metrics-events-value
    """

    def __init__(self, use_avro: bool = True):
        super().__init__(use_avro=use_avro)
        # Avro 사용 시 메트릭 스키마 설정
        if use_avro:
            self.enable_avro("metrics-events-value")

    def _generate_ticket_id(self) -> str:
        return str(uuid.uuid4())

    async def send_counting_batch(
        self,
        scope: ConnectionScopeDomain,
        items: list[MinuteItemDomain],
        range_start_ts_kst: int,
        range_end_ts_kst: int,
        bucket_size_sec: int = 60,
        version: int = 1,
        key: KeyType = None,
    ) -> bool:
        # MinuteItem -> dict 직렬화 (프로토콜 경계에서 수행)
        # NOTE: dict[str, Any] 사용 사유
        # - MinuteItem을 프로듀서 경계에서 표준 dict로 직렬화하여 외부 시스템(Kafka/Schema) 호환성 확보
        items_dicts: list[dict[str, int | dict[str, int]]] = [
            {
                "minute_start_ts_kst": it.minute_start_ts_kst,
                "total": it.total,
                "details": it.details,
            }
            for it in items
        ]
        # Pydantic 모델로 구성하여 I/O 경계에서 스키마를 엄격히 보장
        payload = MarketCountingDTO(
            ticket_id=self._generate_ticket_id(),  # UUID 생성
            region=scope.region,
            exchange=scope.exchange,
            request_type=scope.request_type,
            batch=CountingBatchDTO(
                ticket_id=self._generate_ticket_id(),  # 배치에도 별도 UUID
                range_start_ts_kst=range_start_ts_kst,
                range_end_ts_kst=range_end_ts_kst,
                bucket_size_sec=bucket_size_sec,
                items=items_dicts,
                version=version,
            ),
        )

        # 지역별 토픽으로 라우팅 (부작용 최소화: 기존 기본값은 korea였으나, 여기서 동적으로 region 사용)
        topic_to_use = f"ws.counting.message.{scope.region}"

        return await self.produce_sending(
            message=payload,
            topic=topic_to_use,
            key=key,
            stop_after_send=False,
        )


class ConnectSuccessEventProducer(AvroProducer):
    """연결 성공 ACK 이벤트 프로듀서 (Avro 직렬화 지원).

    - 지역별 토픽에 발행: ws.connect_success.{region}
    - 키 포맷: "{exchange}|{region}|{request_type}|{coin_symbol}"
    - Avro 스키마: connect-success-events-value
    """

    def __init__(self, use_avro: bool = True):
        super().__init__(use_avro=use_avro)
        # Avro 사용 시 연결 성공 스키마 설정
        if use_avro:
            self.enable_avro("connect-success-events-value")

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
    """실시간 데이터 배치 프로듀서 (Avro 직렬화 지원)

    토픽 전략:
    - ticker-data.{region}
    - orderbook-data.{region}
    - trade-data.{region}

    성능 최적화:
    - Avro 직렬화로 20-40% 메시지 크기 감소 (선택적)
    - asyncio.to_thread() CPU 오프로드
    - 스키마 진화 자동 처리
    """

    def __init__(self, use_avro: bool = True):
        super().__init__(use_avro=use_avro)
        # Avro 사용 시 기본적으로 ticker 데이터 스키마 설정
        if use_avro:
            self.enable_avro("ticker-data-value")

    def _convert_to_avro_format(
        self, scope: ConnectionScopeDomain, batch: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """배치 데이터를 표준 Avro 스키마 형식으로 변환"""
        return {
            "exchange": scope.exchange,
            "region": scope.region,
            "request_type": scope.request_type,
            "timestamp_ms": int(time.time() * 1000),  # 표준 스키마 필드명
            "batch_size": len(batch),
            "batch_id": None,  # 선택적 필드
            "data": batch,  # 표준 스키마의 TickerMessage 배열
        }

    async def send_ticker_batch(
        self, scope: ConnectionScopeDomain, batch: list[TickerResponseData]
    ) -> bool:
        """티커 데이터 배치 전송 (Avro 직렬화 자동 지원)"""
        topic = f"ticker-data.{scope.region}"

        # Avro 형식으로 변환 (기본 클래스에서 자동 처리)
        message = self._convert_to_avro_format(scope, batch)

        return await self.produce_sending(
            message=message,
            topic=topic,
            key=f"{scope.exchange}:{scope.region}:ticker",
            stop_after_send=False,
        )

    async def send_orderbook_batch(
        self, scope: ConnectionScopeDomain, batch: list[OrderbookResponseData]
    ) -> bool:
        """오더북 데이터 배치 전송"""
        topic = f"orderbook-data.{scope.region}"

        # 표준 형식으로 변환
        message = self._convert_to_avro_format(scope, batch)

        return await self.produce_sending(
            message=message,
            topic=topic,
            key=f"{scope.exchange}:{scope.region}:orderbook",
            stop_after_send=False,
        )

    async def send_trade_batch(
        self, scope: ConnectionScopeDomain, batch: list[TradeResponseData]
    ) -> bool:
        """체결 데이터 배치 전송"""
        topic = f"trade-data.{scope.region}"

        # 표준 형식으로 변환
        message = self._convert_to_avro_format(scope, batch)

        return await self.produce_sending(
            message=message,
            topic=topic,
            key=f"{scope.exchange}:{scope.region}:trade",
            stop_after_send=False,
        )

    async def send_batch_by_type(
        self,
        scope: ConnectionScopeDomain,
        message_type: str,
        batch: list[dict[str, Any]],
    ) -> bool:
        """타입별 배치 전송 통합 메서드 (Avro 지원)"""
        if message_type == "ticker":
            return await self.send_ticker_batch(scope, batch)  # type: ignore[arg-type]
        elif message_type == "orderbook":
            return await self.send_orderbook_batch(scope, batch)  # type: ignore[arg-type]
        elif message_type == "trade":
            return await self.send_trade_batch(scope, batch)  # type: ignore[arg-type]
        else:
            logger.warning(f"Unknown message type: {message_type}")
            return False
