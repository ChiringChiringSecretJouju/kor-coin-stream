from __future__ import annotations

import asyncio
from dataclasses import dataclass
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
from src.infra.messaging.clients.clients import create_producer, AsyncProducerWrapper

# NOTE: Any 사용 사유
# - 프로듀서 계층은 외부 전송 경계로, 스키마 확장(필드 추가)과 다양한 이벤트 페이로드를 수용해야 함
#   따라서 dict[str, Any] 형태의 payload/컨텍스트를 허용하여 직렬화 유연성을 확보합니다.


logger = PipelineLogger(__name__)
# NOTE: 직렬화는 AsyncProducerWrapper 내부의 default_value_serializer에서 처리
KeyType = str | bytes | None


@dataclass(slots=True)
class KafkaProducerClient:
    """
    KafkaProducerClient
    - 카프카 전송 전용 클라이언트
    """

    producer: AsyncProducerWrapper | None = (
        None  # confluent-kafka 기반 AsyncProducerWrapper
    )
    producer_started: bool = False

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
        - confluent-kafka의 고성능 C 라이브러리 활용
        - 배치 처리, 압축, 재시도 로직 내장
        - JSON 직렬화는 default_value_serializer에서 처리
        """
        try:
            # Pydantic 모델을 dict로 변환
            if isinstance(message, BaseModel):
                message = message.model_dump()

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


class ConnectRequestProducer(KafkaProducerClient):
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


class ErrorEventProducer(KafkaProducerClient):
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


class DlqProducer(KafkaProducerClient):
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


class MetricsProducer(KafkaProducerClient):
    """
    confluent-kafka 기반 메트릭 배치 프로듀서.

    - 지역별 토픽으로 실시간 메트릭 전송: ws.counting.message.{region}
    - 분 단위 메시지 카운팅 및 집계 데이터
    - 성능 모니터링 및 비즈니스 인사이트 제공
    """

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


class ConnectSuccessEventProducer(KafkaProducerClient):
    """연결 성공 ACK 이벤트 프로듀서.

    - 지역별 토픽에 발행: ws.connect_success.{region}
    - 키 포맷: "{exchange}|{region}|{request_type}|{coin_symbol}"
    """

    async def send_event(self, event: ConnectSuccessEventDTO, key: KeyType) -> bool:
        region = event.target.region
        topic = f"ws.connect_success.{region}"
        return await self.produce_sending(
            message=event,
            topic=topic,
            key=key,
            stop_after_send=False,
        )
