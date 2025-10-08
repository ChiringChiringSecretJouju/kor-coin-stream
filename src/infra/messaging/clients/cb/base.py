"""
Kafka 클라이언트 기본 클래스

비동기 처리 옵션과 공통 기능을 제공하는 부모 클래스들을 정의합니다.
Producer/Consumer 래퍼들의 공통 비동기 처리 로직을 추상화합니다.
"""

import asyncio
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, Producer

from src.core.dto.io.producer_metrics import ProducerMetricsEventDTO


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
    _closed: bool = field(default=False, init=False)


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
    _loop: asyncio.AbstractEventLoop | None = field(default=None, init=False)

    # 메트릭 전송 콜백 (외부에서 주입, 순환 참조 방지)
    _metrics_callback: Callable[..., Any] | None = field(default=None, init=False)

    # BufferError 재시도 설정
    _max_buffer_retries: int = field(default=100, init=False)
    _buffer_retry_base_sleep_s: float = field(default=0.001, init=False)  # 1ms
    _buffer_retry_max_sleep_s: float = field(default=0.050, init=False)  # cap at 50ms

    async def start(self) -> None:
        """Producer 시작 - 큐 처리 코루틴 및 poll 스레드 시작"""
        self._closed = False

        if self._started:
            return

        # 이벤트 루프 저장
        self._loop = asyncio.get_event_loop()

        # Producer 인스턴스 생성
        try:
            self.producer = Producer(self.config)
        except Exception as e:
            # start 에러 경로 견고화: 생성 실패 시 정리
            self.producer = None
            raise RuntimeError(f"Producer 생성 실패: {e}") from e

        # 서브클래스별 초기화 수행
        try:
            await self._initialize_serializers()
        except Exception as e:
            # 초기화 실패 시 정리
            if self.producer:
                self.producer.flush(5.0)
            self.producer = None
            raise RuntimeError(f"직렬화기 초기화 실패: {e}") from e

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
        """Producer 종료 - graceful shutdown (2. drain 보장)"""
        self._closed = True

        if not self._started or not self.producer:
            return

        # 2. 큐 drain: 남은 메시지 모두 처리 대기
        await self._message_queue.join()

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

    async def send_and_wait(
        self,
        topic: str,
        value: Any,
        key: Any = None,
        await_delivery: bool = False,
        timeout_ms: float | None = None,
    ) -> bool:
        """메시지 전송 - 큐에 추가 (1. ACK 가시성 지원)

        Args:
            topic: 대상 토픽
            value: 메시지 값
            key: 메시지 키
            await_delivery: True이면 브로커 ACK까지 대기, False이면 큐 삽입 후 즉시 반환
            timeout_ms: 메시지 전송 최대 대기 시간 (ms)
        Returns:
            bool: 전송 성공 여부
        """
        if not self._started or not self.producer:
            raise RuntimeError("Producer not started")
        if self._closed:
            raise RuntimeError("Producer is stopping/stopped; rejecting new messages")

        # 서브클래스별 직렬화 수행
        serialized_value, serialized_key = await self._serialize_message(value, key)

        # 1. delivery future 생성 (ACK 대기 시)
        delivery_future: asyncio.Future[bool] | None = None
        if await_delivery and self._loop:
            delivery_future = self._loop.create_future()

        # 메시지를 큐에 추가 (비동기) - opaque로 context 전달
        opaque_ctx = {
            "future": delivery_future,
            "enqueue_time": time.time(),  # 4. 메트릭용
        }
        message = {
            "topic": topic,
            "value": serialized_value,
            "key": serialized_key,
            "opaque": opaque_ctx,
        }
        await self._message_queue.put(message)

        # ACK 대기
        if delivery_future:
            if timeout_ms is not None:
                return await asyncio.wait_for(delivery_future, timeout_ms / 1000)
            return await delivery_future

        return True  # 큐 삽입 성공

    async def _message_producer_worker(self) -> None:
        """메시지 처리 코루틴 - 큐에서 메시지를 가져와 produce() 호출 (3. BufferError 재시도)"""
        while True:
            try:
                message = await self._message_queue.get()

                if message is None:
                    break

                # 3. BufferError 재시도 로직
                buffer_retries = 0
                opaque_ctx = message.get("opaque")
                while True:
                    try:
                        self.producer.produce(
                            topic=message["topic"],
                            value=message["value"],
                            key=message["key"],
                            callback=self._delivery_callback,
                            opaque=opaque_ctx,  # context 전달
                        )
                        break  # 성공 시 루프 탈출

                    except BufferError:
                        buffer_retries += 1
                        if buffer_retries > self._max_buffer_retries:
                            if opaque_ctx and "future" in opaque_ctx:
                                fut = opaque_ctx["future"]
                                if fut and not fut.done():
                                    err_msg = "Producer queue full after max retries"
                                    self._loop.call_soon_threadsafe(
                                        lambda f=fut, e=err_msg: f.set_exception(
                                            BufferError(e)
                                        )
                                    )
                            self._send_metric_event(
                                event_type="BUFFER_RETRY",
                                topic=message["topic"],
                                buffer_retries=buffer_retries,
                                success=False,
                            )
                            break

                        self.producer.poll(0)
                        await asyncio.sleep(self._buffer_retry_base_sleep_s)

                self._message_queue.task_done()

            except Exception:
                self._message_queue.task_done()

    def _poll_worker(self) -> None:
        """전용 poll 스레드 - delivery report 처리"""
        while not self._shutdown_event.is_set():
            try:
                if self.producer:
                    self.producer.poll(0.1)  # 100ms 타임아웃
                else:
                    threading.Event().wait(0.1)
            except Exception:
                pass

    def _delivery_callback(self, err, msg) -> None:
        """Delivery report 콜백 - poll 스레드에서 호출됨 (1. future resolve, 4. 메트릭)"""
        # opaque에서 context 추출
        if not msg:
            return
        ctx: dict | None = msg.opaque()

        # 메트릭 수집
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        key = msg.key().decode("utf-8") if msg.key() else None

        # 지연 시간 계산
        latency_ms = None
        if ctx and "enqueue_time" in ctx:
            latency_ms = (time.time() - ctx["enqueue_time"]) * 1000.0

        success = err is None
        error_code = None
        error_message = None

        if err is not None:
            error_code = err.code() if hasattr(err, "code") else str(err)
            error_message = err.str() if hasattr(err, "str") else str(err)

        # 1. future resolve (ACK 가시성)
        if ctx and "future" in ctx:
            fut = ctx["future"]
            if fut and not fut.done() and self._loop:
                if success:
                    self._loop.call_soon_threadsafe(lambda: fut.set_result(True))
                else:
                    err_msg = f"Delivery failed: {error_message}"
                    self._loop.call_soon_threadsafe(
                        lambda: fut.set_exception(RuntimeError(err_msg))
                    )

        # 4. 메트릭 전송 (Kafka로)
        event_type = "DELIVERY_SUCCESS" if success else "DELIVERY_FAILURE"
        self._send_metric_event(
            event_type=event_type,
            topic=topic,
            partition=partition,
            offset=offset,
            message_key=key[:100] if key else None,  # 최대 100자
            delivery_latency_ms=latency_ms,
            queue_depth=self._message_queue.qsize(),
            error_code=str(error_code) if error_code else None,
            error_message=error_message,
            success=success,
        )

    def set_metrics_callback(self, callback: Callable[..., Any] | None) -> None:
        """메트릭 전송 콜백 설정 (외부 ProducerMetricsProducer 주입)

        Args:
            callback: 메트릭 전송 함수 (비동기 또는 동기)
                     시그니처: (timestamp_ms, producer_class, event_type, ...)
        """
        self._metrics_callback = callback

    def _send_metric_event(
        self,
        event_type: str,
        topic: str | None = None,
        partition: int | None = None,
        offset: int | None = None,
        message_key: str | None = None,
        delivery_latency_ms: float | None = None,
        queue_depth: int | None = None,
        buffer_retries: int | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        success: bool = True,
    ) -> None:
        """4. 메트릭 이벤트 전송 (콜백 호출)"""
        if not self._metrics_callback:
            return

        # Pydantic DTO 생성
        metrics_dto = ProducerMetricsEventDTO(
            timestamp_ms=int(time.time() * 1000),
            producer_class=self.__class__.__name__,
            event_type=event_type,
            topic=topic,
            partition=partition,
            offset=offset,
            message_key=message_key,
            delivery_latency_ms=delivery_latency_ms,
            queue_depth=queue_depth,
            buffer_retries=buffer_retries,
            error_code=error_code,
            error_message=error_message,
            success=success,
        )

        # 외부 콜백 호출 (ProducerMetricsProducer.send_metric_event)
        # asyncio.create_task로 비동기 실행 (메인 로직 블로킹 방지)
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(
                lambda: asyncio.create_task(
                    self._metrics_callback(**metrics_dto.model_dump())
                )
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
        self._closed = False

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
        self._closed = True

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
        while not self._shutdown_event.is_set() and not self._closed:
            if self.consumer:
                raw_msg = self.consumer.poll(timeout=0.1)  # 100ms 타임아웃
                if raw_msg is None or raw_msg.error():
                    time.sleep(0.1)
                    continue

                self._handle_raw_message(raw_msg)
            else:
                threading.Event().wait(0.1)

    def _handle_raw_message(self, raw_msg) -> None:
        """기본 raw 메시지 처리: 동기 역직렬화 후 큐 삽입을 루프에 스케줄.

        서브클래스(예: Avro)는
        이 메서드를 오버라이드하여 비동기 역직렬화 파이프라인을 구현 가능
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
