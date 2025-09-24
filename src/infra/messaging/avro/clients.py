"""
Avro 지원 Kafka Producer/Consumer 래퍼

기존 confluent-kafka 기반 클라이언트에 Avro 직렬화 기능을 추가합니다.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any
from dataclasses import dataclass

from confluent_kafka import Producer, Consumer
from src.common.logger import PipelineLogger
from src.infra.messaging.clients.clients import producer_config, consumer_config
from src.infra.messaging.avro.schema_registry import SchemaRegistryClient
from src.infra.messaging.avro.serializers import AvroSerializer, AvroDeserializer

logger = PipelineLogger.get_logger("avro_clients", "avro")


@dataclass(slots=True)
class AvroProducerWrapper:
    """
    Avro 지원 Producer 래퍼

    confluent-kafka Producer에 Avro 직렬화 기능을 추가합니다.
    """

    config: dict[str, Any]
    schema_registry_client: SchemaRegistryClient
    value_subject: str
    key_subject: str | None = None
    value_schema_str: str | None = None
    key_schema_str: str | None = None

    def __post_init__(self):
        # Any 사용 사유: confluent_kafka.Producer는 외부 라이브러리 타입으로 정확한 타입 정의가 복잡함
        self.producer: Producer | None = None
        self._started = False

        # Avro 직렬화기들
        self._value_serializer: AvroSerializer | None = None
        self._key_serializer: AvroSerializer | None = None

        # 비동기 메시지 큐 (기존 구조와 동일)
        self._message_queue: asyncio.Queue = asyncio.Queue()

        # 전용 poll 스레드 관리
        self._poll_thread: threading.Thread | None = None
        self._shutdown_event = threading.Event()

        # 코루틴 태스크 관리 - Any 사용 사유: asyncio.Task의 제네릭 타입이 복잡함
        self._producer_task: asyncio.Task[Any] | None = None

    async def start(self) -> None:
        """Producer 시작 - Avro 직렬화기 초기화 포함"""
        if self._started:
            return

        # Producer 인스턴스 생성
        self.producer = Producer(self.config)

        # Avro 직렬화기 초기화
        self._value_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            subject=self.value_subject,
            schema_str=self.value_schema_str,
        )
        await self._value_serializer.ensure_schema()

        if self.key_subject:
            self._key_serializer = AvroSerializer(
                schema_registry_client=self.schema_registry_client,
                subject=self.key_subject,
                schema_str=self.key_schema_str,
            )
            await self._key_serializer.ensure_schema()

        # 전용 poll 스레드 시작
        self._poll_thread = threading.Thread(
            target=self._poll_worker, name="avro-producer-poll", daemon=True
        )
        self._poll_thread.start()

        # 메시지 처리 코루틴 시작
        self._producer_task = asyncio.create_task(self._message_producer_worker())

        self._started = True
        logger.info(f"Avro Producer 시작: value_subject={self.value_subject}")

    async def stop(self) -> None:
        """Producer 종료 - graceful shutdown"""
        if not self._started or not self.producer:
            return

        # 1. 큐에 종료 신호 전송
        await self._message_queue.put(None)

        # 2. 메시지 처리 코루틴 종료 대기
        if self._producer_task:
            await self._producer_task

        # 3. 남은 메시지 flush
        await asyncio.to_thread(self.producer.flush, 10.0)

        # 4. poll 스레드 종료
        self._shutdown_event.set()
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5.0)

        # 5. Schema Registry 클라이언트 정리
        await self.schema_registry_client.close()

        self.producer = None
        self._started = False
        logger.info("Avro Producer 종료")

    async def send_and_wait(
        self,
        topic: str,
        value: dict[str, Any] | bytes,
        key: dict[str, Any] | bytes | str | None = None,
    ) -> None:
        """
        Avro 메시지 전송 - 큐에 추가

        Args:
            topic: 토픽명
            value: Value 객체 (딕셔너리 또는 이미 직렬화된 바이트)
            key: Key 객체 (딕셔너리, 바이트, 문자열, 선택사항)
        """
        if not self._started or not self.producer:
            raise RuntimeError("Avro Producer not started")

        # 메시지를 큐에 추가
        message = {"topic": topic, "value": value, "key": key}
        await self._message_queue.put(message)

    async def _message_producer_worker(self) -> None:
        """메시지 처리 코루틴 - Avro 직렬화 후 produce 호출"""
        while True:
            try:
                message = await self._message_queue.get()

                if message is None:
                    break

                # Avro 직렬화 (비동기 오프로드)
                serialized_value = None
                if message["value"] is not None and self._value_serializer:
                    serialized_value = await self._value_serializer.serialize_async(
                        message["value"]
                    )

                serialized_key = None
                if message["key"] is not None and self._key_serializer:
                    serialized_key = await self._key_serializer.serialize_async(
                        message["key"]
                    )

                # Producer.produce() 호출 (논블로킹)
                self.producer.produce(
                    topic=message["topic"],
                    value=serialized_value,
                    key=serialized_key,
                    callback=self._delivery_callback,
                )

                self._message_queue.task_done()

            except Exception as e:
                logger.error(f"Avro Producer worker error: {e}")

    def _poll_worker(self) -> None:
        """전용 poll 스레드 - delivery report 처리"""
        while not self._shutdown_event.is_set():
            try:
                if self.producer:
                    self.producer.poll(0.1)
                else:
                    threading.Event().wait(0.1)
            except Exception as e:
                logger.error(f"Avro Producer poll worker error: {e}")

    def _delivery_callback(self, err, msg) -> None:
        """Delivery report 콜백"""
        if err is not None:
            logger.error(f"Avro message delivery failed: {err}")


@dataclass(slots=True)
class AvroConsumerWrapper:
    """
    Avro 지원 Consumer 래퍼

    confluent-kafka Consumer에 Avro 역직렬화 기능을 추가합니다.
    """

    topics: list[str]
    config: dict[str, Any]
    schema_registry_client: SchemaRegistryClient
    reader_schema_str: str | None = None

    def __post_init__(self):
        # Any 사용 사유: confluent_kafka.Consumer는 외부 라이브러리 타입으로 정확한 타입 정의가 복잡함
        self.consumer: Consumer | None = None
        self._started = False

        # Avro 역직렬화기
        self._deserializer: AvroDeserializer | None = None

        # 비동기 메시지 큐
        self._message_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

        # 전용 poll 스레드 관리
        self._poll_thread: threading.Thread | None = None
        self._shutdown_event = threading.Event()

    async def start(self) -> None:
        """Consumer 시작 - Avro 역직렬화기 초기화 포함"""
        if self._started:
            return

        # Consumer 인스턴스 생성
        self.consumer = Consumer(self.config)
        self.consumer.subscribe(self.topics)

        # Avro 역직렬화기 초기화
        self._deserializer = AvroDeserializer(
            schema_registry_client=self.schema_registry_client,
            reader_schema_str=self.reader_schema_str,
        )

        # 전용 poll 스레드 시작
        self._poll_thread = threading.Thread(
            target=self._poll_worker, name="avro-consumer-poll", daemon=True
        )
        self._poll_thread.start()

        self._started = True
        logger.info(f"Avro Consumer 시작: topics={self.topics}")

    async def stop(self) -> None:
        """Consumer 종료 - graceful shutdown"""
        if not self._started or not self.consumer:
            return

        # 1. poll 스레드 종료 신호
        self._shutdown_event.set()

        # 2. 큐에 종료 신호 전송
        try:
            self._message_queue.put_nowait(None)
        except asyncio.QueueFull:
            pass

        # 3. poll 스레드 종료 대기
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5.0)

        # 4. Consumer 종료
        await asyncio.to_thread(self.consumer.close)

        # 5. Schema Registry 클라이언트 정리
        await self.schema_registry_client.close()

        self.consumer = None
        self._started = False
        logger.info("Avro Consumer 종료")

    def __aiter__(self):
        return self

    async def __anext__(self):
        """비동기 이터레이터 - Avro 역직렬화된 메시지 반환"""
        if not self._started:
            raise RuntimeError("Avro Consumer not started")

        # 큐에서 메시지 가져오기
        msg = await self._message_queue.get()

        if msg is None:
            raise StopAsyncIteration

        # Avro 역직렬화된 메시지 래퍼 반환
        return AvroMessage(msg, self._deserializer)

    def _poll_worker(self) -> None:
        """전용 poll 스레드 - 메시지 수신 및 큐에 전달"""
        while not self._shutdown_event.is_set():
            try:
                if self.consumer:
                    msg = self.consumer.poll(timeout=0.1)

                    if msg is None:
                        continue

                    if msg.error():
                        logger.error(f"Avro Consumer error: {msg.error()}")
                        continue

                    # 메시지를 큐에 전달
                    try:
                        loop = None
                        try:
                            loop = asyncio.get_event_loop()
                        except RuntimeError:
                            pass

                        if loop and not loop.is_closed():
                            loop.call_soon_threadsafe(self._add_message_to_queue, msg)

                    except Exception as e:
                        logger.error(f"Avro Consumer queue put error: {e}")

                else:
                    threading.Event().wait(0.1)

            except Exception as e:
                logger.error(f"Avro Consumer poll worker error: {e}")

    def _add_message_to_queue(self, msg) -> None:
        """메시지를 큐에 안전하게 추가"""
        try:
            self._message_queue.put_nowait(msg)
        except asyncio.QueueFull:
            try:
                self._message_queue.get_nowait()
                self._message_queue.put_nowait(msg)
            except asyncio.QueueEmpty:
                pass


@dataclass(slots=True)
class AvroMessage:
    """
    Avro 메시지 래퍼

    confluent-kafka Message를 래핑하여 Avro 역직렬화 기능을 제공합니다.
    """

    # Any 사용 사유: confluent_kafka.Message는 외부 라이브러리 타입으로 정확한 타입 정의가 복잡함
    _msg: Any  # confluent_kafka.Message
    _deserializer: AvroDeserializer

    def value(self) -> dict[str, Any] | None:
        """Value를 Avro 역직렬화하여 반환 - Any 사용 사유: Avro 메시지는 다양한 타입의 필드를 포함할 수 있음"""
        raw_value = self._msg.value()
        if raw_value is None:
            return None
        return self._deserializer(raw_value)

    async def value_async(self) -> dict[str, Any] | None:
        """Value를 비동기적으로 Avro 역직렬화하여 반환 (CPU 집약적인 작업을 오프로드)"""
        raw_value = self._msg.value()
        if raw_value is None:
            return None
        return await self._deserializer.deserialize_async(raw_value)

    def key(self) -> dict[str, Any] | str | None:
        """Key를 Avro 역직렬화하여 반환 (Key가 Avro인 경우)"""
        raw_key = self._msg.key()
        if raw_key is None:
            return None

        # Key가 Avro 형식인지 확인 (매직 바이트 체크)
        if len(raw_key) >= 5 and raw_key[0] == 0:
            return self._deserializer(raw_key)
        else:
            # 일반 문자열 키
            return raw_key.decode("utf-8")

    async def key_async(self) -> dict[str, Any] | str | None:
        """Key를 비동기적으로 Avro 역직렬화하여 반환 (CPU 집약적인 작업을 오프로드)"""
        raw_key = self._msg.key()
        if raw_key is None:
            return None

        # Key가 Avro 형식인지 확인 (매직 바이트 체크)
        if len(raw_key) >= 5 and raw_key[0] == 0:
            return await self._deserializer.deserialize_async(raw_key)
        else:
            # 일반 문자열 키
            return raw_key.decode("utf-8")

    def topic(self) -> str:
        """토픽명 반환"""
        return self._msg.topic()

    def partition(self) -> int:
        """파티션 번호 반환"""
        return self._msg.partition()

    def offset(self) -> int:
        """오프셋 반환"""
        return self._msg.offset()


def create_avro_producer(
    schema_registry_client: SchemaRegistryClient,
    value_subject: str,
    key_subject: str | None = None,
    value_schema_str: str | None = None,
    key_schema_str: str | None = None,
    **overrides: Any,  # Any 사용 사유: Producer 설정은 다양한 타입의 값을 받을 수 있음
) -> AvroProducerWrapper:
    """
    Avro Producer 래퍼 생성

    Args:
        schema_registry_client: Schema Registry 클라이언트
        value_subject: Value 스키마 주제명
        key_subject: Key 스키마 주제명 (선택사항)
        value_schema_str: Value 스키마 JSON 문자열 (선택사항)
        key_schema_str: Key 스키마 JSON 문자열 (선택사항)
        **overrides: Producer 설정 오버라이드

    Returns:
        설정된 AvroProducerWrapper 인스턴스
    """
    cfg = producer_config(**overrides)

    return AvroProducerWrapper(
        config=cfg,
        schema_registry_client=schema_registry_client,
        value_subject=value_subject,
        key_subject=key_subject,
        value_schema_str=value_schema_str,
        key_schema_str=key_schema_str,
    )


def create_avro_consumer(
    topics: str | list[str],
    schema_registry_client: SchemaRegistryClient,
    reader_schema_str: str | None = None,
    **overrides: Any,  # Any 사용 사유: Consumer 설정은 다양한 타입의 값을 받을 수 있음
) -> AvroConsumerWrapper:
    """
    Avro Consumer 래퍼 생성

    Args:
        topics: 구독할 토픽 (문자열 또는 리스트)
        schema_registry_client: Schema Registry 클라이언트
        reader_schema_str: Reader 스키마 JSON 문자열 (스키마 진화용)
        **overrides: Consumer 설정 오버라이드

    Returns:
        설정된 AvroConsumerWrapper 인스턴스
    """
    cfg = consumer_config(**overrides)
    topics_list = [topics] if isinstance(topics, str) else topics

    return AvroConsumerWrapper(
        topics=topics_list,
        config=cfg,
        schema_registry_client=schema_registry_client,
        reader_schema_str=reader_schema_str,
    )
