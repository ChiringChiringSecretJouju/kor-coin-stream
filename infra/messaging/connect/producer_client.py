from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable

from pydantic import BaseModel

from common.logger import PipelineLogger

# from common.serde import Serializer, to_bytes  # confluent-kafka로 교체됨
from core.dto.internal.common import ConnectionScopeDomain
from core.dto.internal.metrics import MinuteItemDomain
from core.dto.io.commands import ConnectRequestDTO
from core.dto.io.counting import CountingBatchDTO, MarketCountingDTO
from core.dto.io.dlq_event import DlqEventDTO
from core.dto.io.error_event import WsErrorEventDTO
from infra.messaging.clients.clients import create_producer

# NOTE: Any 사용 사유
# - 프로듀서 계층은 외부 전송 경계로, 스키마 확장(필드 추가)과 다양한 이벤트 페이로드를 수용해야 함
#   따라서 dict[str, Any] 형태의 payload/컨텍스트를 허용하여 직렬화 유연성을 확보합니다.


logger = PipelineLogger(__name__)
# serializer: Serializer = lambda value: to_bytes(value)  # confluent-kafka로 교체됨
KeyType = str | bytes | None


@dataclass(slots=True)
class KafkaProducerClient:
    """
    KafkaProducerClient
    - 카프카 전송 전용 클라이언트
    """

    producer: Any | None = None  # AsyncProducerWrapper
    producer_started: bool = False

    # 실행할 비동기 함수, 예: self.producer.start 또는 self.producer.stop
    async def _execute_with_logging(self, action: Callable) -> bool:
        """지정된 action을 실행하며 로깅을 처리하는 헬퍼 비동기 메서드"""
        await action()
        return True

    async def start_producer(self) -> bool:
        """Producer 시작 및 재사용

        - 공용 설정 생성기를 사용하여 프로듀서를 생성합니다.
        - 커스텀 파티셔너는 제거했습니다(기본 정책 사용).
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

        # 프로듀서 미생성 시 생성
        if self.producer is None:
            # confluent-kafka 기반 AsyncProducerWrapper 생성
            # 직렬화는 내부적으로 default_value_serializer 사용
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
        """카프카 전송 공통 루틴.

        - Pydantic BaseModel 인스턴스가 들어오면 dict로 덤프하여 전송합니다.
        - dict 이외의 타입은 직렬화기가 처리 가능한지에 의존합니다.
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
    ConnectRequestEvent 전송용 특화 프로듀서.

    - 범용 베이스(KafkaProducerClient)를 상속
    - request_producer.py의 send_event 로직을 이식하여 topic 기본값과 로깅 의도를 유지
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
    ws.error 토픽으로 `WsErrorEvent`를 발행하는 프로듀서.
    """

    def __init__(self, topic: str = "ws.error") -> None:
        super().__init__()
        self.topic = topic

    async def send_error_event(
        self, event: WsErrorEventDTO, key: KeyType = None
    ) -> bool:
        await self.produce_sending(
            message=event,
            topic=self.topic,
            key=key,
        )


class DlqProducer(KafkaProducerClient):
    """
    처리하지 못한 이벤트를 ws.dlq 토픽으로 전달하는 프로듀서.
    페이로드는 원문과 사유(reason)를 포함합니다.
    """

    def __init__(self, topic: str = "ws.dlq") -> None:
        super().__init__()
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


class MetricsProducer(KafkaProducerClient):
    """수신 메시지 카운팅 배치를 ws.counting.message 토픽으로 발행하는 프로듀서."""

    def __init__(self, topic: str = "ws.counting.message.korea") -> None:
        super().__init__()
        self.topic = topic

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
            region=scope.region,
            exchange=scope.exchange,
            request_type=scope.request_type,
            batch=CountingBatchDTO(
                range_start_ts_kst=range_start_ts_kst,
                range_end_ts_kst=range_end_ts_kst,
                bucket_size_sec=bucket_size_sec,
                items=items_dicts,
                version=version,
            ),
        )

        return await self.produce_sending(
            message=payload,
            topic=self.topic,
            key=key,
            stop_after_send=False,
        )
