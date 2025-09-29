"""
Avro 기반 Kafka 클라이언트

BaseAvroHandler를 기반으로 한 고성능 Avro Producer/Consumer 래퍼입니다.
스키마 자동 관리, CPU 오프로드, 캐싱 최적화를 제공합니다.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, TypeAdapter

from src.infra.messaging.avro.serializers import (
    AsyncAvroDeserializer,
    AsyncAvroSerializer,
    create_avro_deserializer,
    create_avro_serializer,
)
from src.infra.messaging.clients.cb.base import AsyncConsumerBase, AsyncProducerBase
from src.infra.messaging.clients.cb.config import (
    avro_consumer_config,
    avro_producer_config,
)


@dataclass(slots=True)
class AvroProducerWrapper(AsyncProducerBase):
    """
    Avro 지원 Producer 래퍼

    BaseAvroHandler 기반으로 구현된 고성능 Avro Producer
    - 스키마 자동 관리 및 캐싱
    - CPU 오프로드를 통한 비동기 직렬화
    - 비동기 큐 기반 아키텍처 (부모 클래스에서 제공)
    """

    value_subject: str
    key_subject: str | None = None

    # Avro 전용 직렬화기 (init=False)
    _value_serializer: AsyncAvroSerializer | None = field(default=None, init=False)
    _key_serializer: AsyncAvroSerializer | None = field(default=None, init=False)

    async def _initialize_serializers(self) -> None:
        """Avro 직렬화기 초기화 (BaseAvroHandler 기반)"""
        self._value_serializer = create_avro_serializer(self.value_subject)
        await self._value_serializer.ensure_serializer()

        if self.key_subject:
            self._key_serializer = create_avro_serializer(self.key_subject)
            await self._key_serializer.ensure_serializer()

    async def _serialize_message(
        self, value: Any, key: Any = None
    ) -> tuple[bytes, bytes | None]:
        """Avro 메시지 직렬화 (CPU 오프로드)"""
        if not self._value_serializer:
            raise RuntimeError("Avro value serializer not initialized")

        # Avro 직렬화 (CPU 오프로드)
        serialized_value = await self._value_serializer.serialize_async(value)
        serialized_key = None

        if key is not None and self._key_serializer:
            serialized_key = await self._key_serializer.serialize_async(key)
        elif key is not None:
            # 키 직렬화기가 없으면 문자열로 처리
            serialized_key = str(key).encode("utf-8")

        return serialized_value, serialized_key


@dataclass(slots=True)
class AvroConsumerWrapper(AsyncConsumerBase):
    """
    Avro 지원 Consumer 래퍼

    BaseAvroHandler 기반으로 구현된 고성능 Avro Consumer
    - 스키마 자동 관리 및 캐싱
    - CPU 오프로드를 통한 비동기 역직렬화
    - 비동기 큐 기반 아키텍처 (부모 클래스에서 제공)
    """

    value_subject: str
    key_subject: str | None = None

    # Avro 전용 역직렬화기 (init=False)
    _value_deserializer: AsyncAvroDeserializer | None = field(default=None, init=False)
    _key_deserializer: AsyncAvroDeserializer | None = field(default=None, init=False)
    # 동시 역직렬화 제한(백프레셔)
    _max_concurrency: int = 16
    _sem: asyncio.Semaphore | None = field(default=None, init=False, repr=False)
    # 입력 경계 검증(선택): Pydantic v2 TypeAdapter
    value_model: type[BaseModel] | None = None
    _value_adapter: TypeAdapter | None = field(default=None, init=False, repr=False)
    offload_validation: bool = False
    # 스케줄된 태스크 관리(안전 종료)
    _tasks: set[asyncio.Task[Any]] = field(default_factory=set, init=False, repr=False)

    async def _initialize_deserializers(self) -> None:
        """Avro 역직렬화기 초기화 (BaseAvroHandler 기반)"""
        self._value_deserializer = create_avro_deserializer(self.value_subject)
        await self._value_deserializer.ensure_deserializer()

        if self.key_subject:
            self._key_deserializer = create_avro_deserializer(self.key_subject)
            await self._key_deserializer.ensure_deserializer()

        # 이벤트 루프 내 동시 역직렬화 제한 세마포어 준비
        self._sem = asyncio.Semaphore(self._max_concurrency)
        # 입력 경계 검증 어댑터 준비(선택)
        if self.value_model is not None:
            self._value_adapter = TypeAdapter(self.value_model)

    def _deserialize_message(self, raw_msg) -> dict[str, Any]:
        """동기 경로(호환용): 사용하지 않음. 비차단 파이프라인은 _handle_raw_message 사용."""
        raise NotImplementedError(
            "Use non-blocking _handle_raw_message pipeline instead"
        )

    def _handle_raw_message(self, raw_msg) -> None:
        """폴 스레드에서 호출됨: 비동기 역직렬화 작업을 이벤트 루프에 스케줄."""
        if not self._value_deserializer:
            raise RuntimeError("Avro value deserializer not initialized")
        if self._loop is None:
            raise RuntimeError("Event loop not initialized for consumer")

        value_bytes: bytes = raw_msg.value()
        key_bytes: bytes | None = raw_msg.key()
        topic: str = raw_msg.topic()

        async def _task() -> None:
            try:
                if self._sem is not None:
                    async with self._sem:
                        await self._deserialize_and_enqueue(
                            value_bytes, key_bytes, topic
                        )
                else:
                    await self._deserialize_and_enqueue(value_bytes, key_bytes, topic)
            except Exception as e:
                print(f"{self.__class__.__name__} 비동기 역직렬화 오류: {e}")

        # 이벤트 루프에 안전하게 태스크 스케줄 및 트래킹 등록
        def _schedule() -> None:
            t = asyncio.create_task(_task())
            self._register_task(t)

        self._loop.call_soon_threadsafe(_schedule)

    async def _deserialize_and_enqueue(
        self, value_bytes: bytes, key_bytes: bytes | None, topic: str
    ) -> None:
        """이벤트 루프 내에서 실행: 역직렬화 → (선택)키 처리 → 큐 삽입"""
        assert self._value_deserializer is not None

        deserialized_value = await self._value_deserializer.deserialize_async(
            value_bytes
        )

        # 입력 경계 검증/정규화(선택, 팀 룰: 1회 검증)
        if self._value_adapter is not None:
            if self.offload_validation:
                model = await asyncio.to_thread(
                    self._value_adapter.validate_python, deserialized_value
                )
            else:
                model = self._value_adapter.validate_python(deserialized_value)

            # BaseModel 인스턴스를 dict로 정규화
            try:
                # type: ignore[attr-defined]
                deserialized_value = model.model_dump()  # type: ignore[assignment]
            except Exception:
                # 모델이 이미 dict 타입을 반환하는 경우 대비
                deserialized_value = (
                    dict(model) if not isinstance(model, dict) else model
                )
            # Consumer 경로에서는 재-직렬화하지 않음

        deserialized_key: Any | None = None
        if key_bytes is not None:
            if self._key_deserializer:
                deserialized_key = await self._key_deserializer.deserialize_async(
                    key_bytes
                )
            else:
                try:
                    deserialized_key = key_bytes.decode("utf-8")
                except Exception:
                    deserialized_key = key_bytes

        message = {"key": deserialized_key, "value": deserialized_value, "topic": topic}
        self._add_message_to_queue(message)

    def _register_task(self, task: asyncio.Task[Any]) -> None:
        """스케줄된 태스크를 트래킹하고 완료 시 자동 제거."""
        self._tasks.add(task)

        def _on_done(t: asyncio.Task[Any]) -> None:
            self._tasks.discard(t)

        task.add_done_callback(_on_done)

    async def stop(self) -> None:
        """안전 종료: 새로운 스케줄 차단 → 태스크 취소/드레인 → 상위 정리.

        순서:
        1) 스케줄 차단(_shutdown_event.set)
        2) 루프 내에서 모든 태스크 cancel
        3) 태스크 드레인(gather)
        4) 상위 stop() 호출로 poll 스레드 join 및 consumer close
        """
        # 1) 추가 스케줄 차단
        self._shutdown_event.set()

        # 2) 태스크 취소(가능하면 동일 루프에서 직접, 아니면 스케줄)
        pending: list[asyncio.Task[Any]] = list(self._tasks)
        if pending:
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                current_loop = None  # 호출 컨텍스트에 루프 없음

            if self._loop is not None and self._loop is current_loop:
                for t in pending:
                    t.cancel()
            elif self._loop is not None:

                def _cancel_all() -> None:
                    for t in list(self._tasks):
                        t.cancel()

                self._loop.call_soon_threadsafe(_cancel_all)

            # 3) 태스크 드레인(가능한 경우에 한해)
            if self._loop is not None and self._loop is current_loop:
                try:
                    await asyncio.gather(*pending, return_exceptions=True)
                except Exception:
                    pass

        # 4) 상위 정리 호출
        await super().stop()


def create_avro_producer(
    value_subject: str, key_subject: str | None = None, **overrides: Any
) -> AvroProducerWrapper:
    """
    Avro 기반 비동기 Producer 래퍼 생성

    Args:
        value_subject: 값 스키마 주제명 (예: "ticker-data-value")
        key_subject: 키 스키마 주제명 (선택적)
        **overrides: 추가 Producer 설정

    Returns:
        AvroProducerWrapper 인스턴스

    Example:
        >>> producer = create_avro_producer("ticker-data-value")
        >>> await producer.start()
        >>> await producer.send_and_wait("ticker-data", {
        ...     "target_currency": "BTCUSDT",
        ...     "last": 50000.0,
        ...     "timestamp": 1640995200000
        ... })
        >>> await producer.stop()
    """
    cfg = avro_producer_config(**overrides)
    return AvroProducerWrapper(
        config=cfg,
        value_subject=value_subject,
        key_subject=key_subject,
    )


def create_avro_consumer(
    topic: list[str],
    value_subject: str,
    key_subject: str | None = None,
    **overrides: Any,
) -> AvroConsumerWrapper:
    """
    Avro 기반 비동기 Consumer 래퍼 생성

    Args:
        topic: 구독할 토픽 목록
        value_subject: 값 스키마 주제명 (예: "ticker-data-value")
        key_subject: 키 스키마 주제명 (선택적)
        **overrides: 추가 Consumer 설정

    Returns:
        AvroConsumerWrapper 인스턴스

    Example:
        >>> consumer = create_avro_consumer(["ticker-data"], "ticker-data-value")
        >>> await consumer.start()
        >>> async for message in consumer:
        ...     ticker_data = message["value"]  # 자동 Avro 역직렬화됨
        ...     print(f"Price: {ticker_data['last']}")
        >>> await consumer.stop()
    """
    overrides.setdefault("auto.offset.reset", "latest")
    cfg = avro_consumer_config(**overrides)
    return AvroConsumerWrapper(
        topic=topic,
        config=cfg,
        value_subject=value_subject,
        key_subject=key_subject,
    )
