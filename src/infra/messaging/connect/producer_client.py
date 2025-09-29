from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, Callable

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
    OrderbookResponseData,
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
BatchType = list[TickerResponseData | OrderbookResponseData | TradeResponseData]


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
            if not started:
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
                    if attempt < retries:
                        logger.warning(f"Send attempt {attempt} failed, retrying: {e}")
                        attempt += 1
                        await asyncio.sleep(0.1 * attempt)  # 지수 백오프
                    else:
                        logger.error(f"All {retries} send attempts failed: {e}")
                        raise

            return False  # 모든 재시도 실패

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
    통합 에러 이벤트 프로듀서 (리팩토링됨).

    - ws.error 토픽으로 웹소켓/데이터 에러 전송
    - 실시간 에러 모니터링 및 디버깅 지원
    - use_avro=True: AvroProducerWrapper (error-events-value 스키마)
    - use_avro=False: JsonProducerWrapper (orjson 기반) - 기본값
    - 부모 클래스 기반 고성능 비동기 전송 및 배치 처리
    """

    async def send_error_event(
        self, event: WsErrorEventDTO, key: KeyType = None
    ) -> bool:
        return await self.produce_sending(
            message=event,
            topic="ws.error",
            key=key,
        )


class DlqProducer(AvroProducer):
    """
    통합 Dead Letter Queue 프로듀서 (리팩토링됨).

    - ws.dlq 토픽으로 처리 실패 메시지 전송
    - 원본 메시지 + 실패 사유 포함
    - 장애 분석 및 데이터 복구를 위한 안전망
    - use_avro=True: AvroProducerWrapper (dlq-events-value 스키마)
    - use_avro=False: JsonProducerWrapper (orjson 기반) - 기본값
    - 부모 클래스 기반 고성능 비동기 처리
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
    통합 메트릭 배치 프로듀서 (리팩토링됨, Avro 직렬화 우선).

    - 지역별 토픽으로 실시간 메트릭 전송: ws.counting.message.{region}
    - 분 단위 메시지 카운팅 및 집계 데이터
    - 성능 모니터링 및 비즈니스 인사이트 제공
    - use_avro=True: AvroProducerWrapper (metrics-events-value 스키마) - 기본값
    - use_avro=False: JsonProducerWrapper (orjson 기반)
    - 부모 클래스 기반 고성능 비동기 처리
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
        # - MinuteItem을 프로듀서 경계에서 표준 dict로 직렬화하여 외부 시스템(Kafka/Schema) 호환성
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

        # 지역별 토픽으로 라우팅 (부작용 최소화: 기존 기본값은 korea였으나, 여기서 동적으로 region)
        topic_to_use = f"ws.counting.message.{scope.region}"

        return await self.produce_sending(
            message=payload,
            topic=topic_to_use,
            key=key,
            stop_after_send=False,
        )


class ConnectSuccessEventProducer(AvroProducer):
    """통합 연결 성공 ACK 이벤트 프로듀서 (리팩토링됨, Avro 직렬화 우선).

    - 지역별 토픽에 발행: ws.connect_success.{region}
    - 키 포맷: "{exchange}|{region}|{request_type}|{coin_symbol}"
    - use_avro=True: AvroProducerWrapper (connect-success-events-value 스키마) - 기본값
    - use_avro=False: JsonProducerWrapper (orjson 기반)
    - 부모 클래스 기반 고성능 비동기 처리
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
    """통합 실시간 데이터 배치 프로듀서 (리팩토링됨, Avro 직렬화 우선)

    토픽 전략:
    - ticker-data.{region}
    - orderbook-data.{region}
    - trade-data.{region}

    성능 최적화:
    - use_avro=True: AvroProducerWrapper (ticker/orderbook/trade-data-value 스키마) - 기본값
    - use_avro=False: JsonProducerWrapper (orjson 기반, 3-5배 빠름)
    - 부모 클래스 기반 고성능 비동기 처리
    - Avro 직렬화로 20-40% 메시지 크기 감소
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

    async def send_batch(self, scope: ConnectionScopeDomain, batch: BatchType) -> bool:
        """타입별 배치 전송 통합 메서드 (Avro 지원)"""
        topic = f"{scope.request_type}-data.{scope.region}"
        key = f"{scope.exchange}:{scope.region}:{scope.request_type}"

        message = self._convert_to_avro_format(scope, batch)
        return await self.produce_sending(
            message=message,
            topic=topic,
            key=key,
            stop_after_send=False,
        )
