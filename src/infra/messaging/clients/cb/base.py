"""
Kafka 클라이언트 기본 클래스

비동기 처리 옵션과 공통 기능을 제공하는 부모 클래스들을 정의합니다.
Producer/Consumer 래퍼들의 공통 비동기 처리 로직을 추상화합니다.
"""

import asyncio
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, Producer


@dataclass(slots=True)
class AsyncBaseClient:
    """비동기 클라이언트 기본 클래스"""

    # 비동기 처리 옵션
    _message_queue: asyncio.Queue = field(
        default_factory=lambda: asyncio.Queue(maxsize=1000), init=False
    )
    _poll_thread: threading.Thread | None = field(default=None, init=False)
    _shutdown_event: threading.Event = field(
        default_factory=lambda: threading.Event(), init=False
    )
    _producer_task: asyncio.Task[Any] | None = field(default=None, init=False)


@dataclass(slots=True)
class AsyncProducerBase(AsyncBaseClient, ABC):
    """
    비동기 Producer 기본 클래스

    공통 비동기 처리 옵션:
    - 비동기 메시지 큐 기반 아키텍처
    - 전용 poll 스레드로 delivery report 처리
    - graceful shutdown 지원
    """

    config: dict[str, Any]

    # 내부 상태 (init=False)
    producer: Producer | None = field(default=None, init=False)
    _started: bool = field(default=False, init=False)

    async def start(self) -> None:
        """Producer 시작 - 큐 처리 코루틴 및 poll 스레드 시작"""
        if self._started:
            return

        # Producer 인스턴스 생성
        self.producer = Producer(self.config)

        # 서브클래스별 초기화 수행
        await self._initialize_serializers()

        # 전용 poll 스레드 시작 (delivery report 처리)
        self._poll_thread = threading.Thread(
            target=self._poll_worker,
            name=f"{self.__class__.__name__}-poll",
            daemon=True,
        )
        self._poll_thread.start()

        # 메시지 처리 코루틴 시작
        self._producer_task = asyncio.create_task(self._message_producer_worker())

        self._started = True

    async def stop(self) -> None:
        """Producer 종료 - graceful shutdown"""
        if not self._started or not self.producer:
            return

        # 큐에 종료 신호 전송
        await self._message_queue.put(None)

        # 메시지 처리 코루틴 종료 대기
        if self._producer_task:
            await self._producer_task

        # 남은 메시지 flush (비동기)
        await asyncio.to_thread(self.producer.flush, 30.0)

        # poll 스레드 종료
        self._shutdown_event.set()
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5.0)

        self.producer = None
        self._started = False

    async def send_and_wait(self, topic: str, value: Any, key: Any = None) -> None:
        """메시지 전송 - 큐에 추가 (논블로킹)"""
        if not self._started or not self.producer:
            raise RuntimeError("Producer not started")

        # 서브클래스별 직렬화 수행
        serialized_value, serialized_key = await self._serialize_message(value, key)

        # 메시지를 큐에 추가 (비동기)
        message = {"topic": topic, "value": serialized_value, "key": serialized_key}
        await self._message_queue.put(message)

    async def _message_producer_worker(self) -> None:
        """메시지 처리 코루틴 - 큐에서 메시지를 가져와 produce() 호출"""
        while True:
            try:
                message = await self._message_queue.get()

                if message is None:
                    break

                self.producer.produce(
                    topic=message["topic"],
                    value=message["value"],
                    key=message["key"],
                    callback=self._delivery_callback,
                )

                self._message_queue.task_done()

            except Exception as e:
                print(f"{self.__class__.__name__} worker error: {e}")

    def _poll_worker(self) -> None:
        """전용 poll 스레드 - delivery report 처리"""
        while not self._shutdown_event.is_set():
            try:
                if self.producer:
                    self.producer.poll(0.1)  # 100ms 타임아웃
                else:
                    threading.Event().wait(0.1)
            except Exception as e:
                print(f"{self.__class__.__name__} poll worker error: {e}")

    def _delivery_callback(self, err, msg) -> None:
        """Delivery report 콜백 - poll 스레드에서 호출됨"""
        if err is not None:
            topic = msg.topic() if msg else "unknown"
            key = msg.key() if msg else "unknown"
            print(
                f"{self.__class__.__name__} delivery failed: {err} --> Topic: {topic}, Key: {key}"
            )

    @abstractmethod
    async def _initialize_serializers(self) -> None:
        """서브클래스별 직렬화기 초기화"""
        pass

    @abstractmethod
    async def _serialize_message(
        self, value: Any, key: Any = None
    ) -> tuple[bytes, bytes | None]:
        """서브클래스별 메시지 직렬화"""
        pass


@dataclass(slots=True)
class AsyncConsumerBase(AsyncBaseClient, ABC):
    """
    비동기 Consumer 기본 클래스

    공통 비동기 처리 옵션:
    - 전용 poll 스레드로 메시지 수신
    - 비동기 큐 기반 메시지 전달
    - 백프레셔 방지 (큐 크기 제한)
    - graceful shutdown 지원
    """

    topic: list[str]
    config: dict[str, Any]

    # 내부 상태 (init=False)
    consumer: Consumer | None = field(default=None, init=False)
    _started: bool = field(default=False, init=False)
    _loop: asyncio.AbstractEventLoop | None = field(default=None, init=False)

    async def start(self) -> None:
        """Consumer 시작 - 전용 poll 스레드 시작"""
        if self._started:
            return

        self._started = True

        # 현재 이벤트 루프 저장
        self._loop = asyncio.get_event_loop()

        # Consumer 인스턴스 생성
        self.consumer = Consumer(self.config)
        self.consumer.subscribe(self.topic)

        # 서브클래스별 초기화 수행
        await self._initialize_deserializers()

        # 전용 poll 스레드 시작 (메시지 수신 처리)
        self._poll_thread = threading.Thread(
            target=self._poll_worker,
            daemon=True,
            name=f"{self.__class__.__name__}-poll-{self.topic}",
        )
        self._poll_thread.start()

    async def stop(self) -> None:
        """Consumer 종료 - graceful shutdown"""
        if not self._started or not self.consumer:
            return

        # poll 스레드 종료 신호
        self._shutdown_event.set()

        # 큐에 종료 신호 전송
        try:
            self._message_queue.put_nowait(None)
        except asyncio.QueueFull:
            pass

        # poll 스레드 종료 대기
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5.0)

        # Consumer 종료
        await asyncio.to_thread(self.consumer.close)
        self.consumer = None
        self._started = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        """비동기 이터레이터 - 큐에서 메시지 가져오기"""
        if not self._started:
            raise RuntimeError("Consumer not started")

        msg = await self._message_queue.get()
        if msg is None:
            raise StopAsyncIteration

        return msg

    def _poll_worker(self) -> None:
        """전용 poll 스레드 - 메시지 수신 및 큐에 전달"""
        while not self._shutdown_event.is_set():
            try:
                if self.consumer:
                    raw_msg = self.consumer.poll(timeout=0.1)  # 100ms 타임아웃
                    if raw_msg is None:
                        continue

                    if raw_msg.error():
                        print(f"{self.__class__.__name__} error: {raw_msg.error()}")
                        continue

                    # 서브클래스별 raw 메시지 처리 (비차단 파이프라인 허용)
                    try:
                        self._handle_raw_message(raw_msg)
                    except Exception as e:
                        print(f"{self.__class__.__name__} 역직렬화 스케줄 오류: {e}")

                else:
                    threading.Event().wait(0.1)

            except Exception as e:
                print(f"{self.__class__.__name__} poll worker error: {e}")

    def _handle_raw_message(self, raw_msg) -> None:
        """기본 raw 메시지 처리: 동기 역직렬화 후 큐 삽입을 루프에 스케줄.

        서브클래스(예: Avro)는 이 메서드를 오버라이드하여 비동기 역직렬화 파이프라인을 구현할 수 있습니다.
        """
        # 기본 구현은 기존 동기 경로 유지
        message = self._deserialize_message(raw_msg)
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(lambda: self._add_message_to_queue(message))

    def _add_message_to_queue(self, msg: dict) -> None:
        """메시지를 큐에 안전하게 추가 (이벤트 루프에서 호출)"""
        try:
            self._message_queue.put_nowait(msg)
        except asyncio.QueueFull:
            # 큐가 가득 차면 가장 오래된 메시지 제거 후 새 메시지 추가
            try:
                self._message_queue.get_nowait()  # 오래된 메시지 제거
                self._message_queue.put_nowait(msg)  # 새 메시지 추가
            except asyncio.QueueEmpty:
                pass  # 큐가 비어있으면 무시

    @abstractmethod
    async def _initialize_deserializers(self) -> None:
        """서브클래스별 역직렬화기 초기화"""
        pass

    @abstractmethod
    def _deserialize_message(self, raw_msg) -> dict[str, Any]:
        """서브클래스별 메시지 역직렬화"""
        pass
