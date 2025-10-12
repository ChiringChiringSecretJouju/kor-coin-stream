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

from src.common.logger import PipelineLogger

logger = PipelineLogger.get_logger("kafka_base", "infra")


@dataclass(slots=True, frozen=False)
class BackpressureConfig:
    """백프레셔 설정 (High/Low Watermark 패턴)

    큐 크기에 따른 동적 throttling으로 OOM 방지 및 안정적 처리
    주기적 모니터링 지원
    """

    queue_max_size: int = 1000  # 큐 최대 크기
    high_watermark: int = 800  # 80% - 이 이상이면 throttle 활성화
    low_watermark: int = 200  # 20% - 이 이하로 떨어지면 throttle 해제
    throttle_sleep_ms: int = 100  # throttle 시 대기 시간 (ms)
    # 주기적 모니터링 설정
    enable_periodic_monitoring: bool = False  # 주기적 모니터링 활성화 여부
    monitoring_interval_sec: int = 30  # 모니터링 주기 (초)


@dataclass(slots=True)
class AsyncBaseClient:
    """비동기 클라이언트 기본 클래스

    High/Low Watermark 기반 백프레셔 관리 지원
    백프레셔 이벤트 Kafka 전송 기능 포함
    """

    # 비동기 처리 옵션
    _message_queue: asyncio.Queue = field(
        default_factory=lambda: asyncio.Queue(maxsize=1000), init=False
    )
    _poll_thread: threading.Thread | None = field(default=None, init=False)
    _shutdown_event: threading.Event = field(
        default_factory=lambda: threading.Event(), init=False
    )
    _producer_task: asyncio.Task[Any] | None = field(default=None, init=False)
    # 백프레셔 설정
    _backpressure_config: BackpressureConfig = field(
        default_factory=BackpressureConfig, init=False
    )
    # 백프레셔 이벤트 전송 (선택적)
    _backpressure_event_producer: Any | None = field(default=None, init=False)
    _backpressure_throttled: bool = field(default=False, init=False)  # 중복 방지
    # 주기적 모니터링 태스크
    _monitoring_task: asyncio.Task[Any] | None = field(default=None, init=False)

    def _should_throttle(self) -> bool:
        """현재 throttle 필요 여부 확인

        Returns:
            True: High Watermark 초과 (백프레셔 활성화)
            False: 정상 범위
        """
        queue_size = self._message_queue.qsize()
        return queue_size >= self._backpressure_config.high_watermark

    async def _wait_for_capacity(self) -> None:
        """큐 여유 공간 대기 (Low Watermark까지)

        High Watermark 초과 시 호출되며, Low Watermark 이하로
        떨어질 때까지 대기합니다.
        """
        while self._message_queue.qsize() >= self._backpressure_config.low_watermark:
            await asyncio.sleep(self._backpressure_config.throttle_sleep_ms / 1000)

    def _get_queue_status(self) -> dict[str, Any]:
        """큐 상태 조회 (모니터링용)

        Returns:
            큐 크기, 사용률, throttle 상태 등
        """
        size = self._message_queue.qsize()
        max_size = self._backpressure_config.queue_max_size
        usage_pct = (size / max_size * 100) if max_size > 0 else 0

        return {
            "queue_size": size,
            "queue_max_size": max_size,
            "usage_percent": round(usage_pct, 1),
            "is_throttled": self._should_throttle(),
            "high_watermark": self._backpressure_config.high_watermark,
            "low_watermark": self._backpressure_config.low_watermark,
        }


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

        # 주기적 모니터링 태스크 종료
        if self._monitoring_task and not self._monitoring_task.done():
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

        # 큐에 종료 신호 전송
        await self._message_queue.put(None)

        # 메시지 처리 코루틴 종료 대기
        if self._producer_task:
            await self._producer_task

        # 남은 메시지 flush (비동기)
        if self.producer is not None:
            await asyncio.to_thread(self.producer.flush, 30.0)

        # poll 스레드 종료
        self._shutdown_event.set()
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5.0)

        self.producer = None
        self._started = False

    def set_backpressure_event_producer(
        self, producer: Any, enable_periodic_monitoring: bool = False
    ) -> None:
        """백프레셔 이벤트 Producer 설정 (선택적)

        백프레셔 발생 시 Kafka로 이벤트를 전송하려면 설정하세요.
        주기적 모니터링을 활성화하면 큐 상태를 주기적으로 Kafka로 전송합니다.

        Args:
            producer: BackpressureEventProducer 인스턴스
            enable_periodic_monitoring: 주기적 모니터링 활성화 여부 (기본: False)

        Example:
            >>> backpressure_producer = BackpressureEventProducer()
            >>> await backpressure_producer.start_producer()
            >>> # 백프레셔 이벤트만
            >>> producer.set_backpressure_event_producer(backpressure_producer)
            >>> # 백프레셔 + 주기적 모니터링
            >>> producer.set_backpressure_event_producer(
            ...     backpressure_producer, enable_periodic_monitoring=True
            ... )
        """
        self._backpressure_event_producer = producer

        # 주기적 모니터링 설정 업데이트
        if enable_periodic_monitoring:
            # frozen=False이므로 직접 수정 가능
            self._backpressure_config.enable_periodic_monitoring = True
            # 모니터링 태스크 시작
            self._start_periodic_monitoring()

    def _start_periodic_monitoring(self) -> None:
        """주기적 모니터링 태스크 시작 (내부 헬퍼)"""
        if self._monitoring_task is None or self._monitoring_task.done():
            self._monitoring_task = asyncio.create_task(
                self._periodic_monitoring_worker(),
                name=f"{self.__class__.__name__}-monitoring",
            )
            logger.info(
                "주기적 큐 모니터링 시작",
                extra={
                    "producer_name": self.__class__.__name__,
                    "interval_sec": self._backpressure_config.monitoring_interval_sec,
                },
            )

    async def _periodic_monitoring_worker(self) -> None:
        """주기적 큐 상태 모니터링 워커

        설정된 주기마다 큐 상태를 Kafka로 전송합니다.
        """
        interval = self._backpressure_config.monitoring_interval_sec

        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(interval)

                # 큐 상태 조회
                status = self._get_queue_status()

                # Kafka로 주기적 리포트 전송
                await self._send_backpressure_event("queue_status_report", status)

            except asyncio.CancelledError:
                logger.info(
                    "주기적 모니터링 종료",
                    extra={"producer_name": self.__class__.__name__},
                )
                break
            except Exception as e:
                logger.error(
                    f"주기적 모니터링 오류: {e}",
                    extra={"producer_name": self.__class__.__name__},
                    exc_info=True,
                )

    async def _send_backpressure_event(
        self, action: str, status: dict[str, Any]
    ) -> None:
        """백프레셔 이벤트 전송 (내부 헬퍼)

        설정된 경우에만 Kafka로 이벤트를 전송합니다.
        """
        if self._backpressure_event_producer is None:
            return

        try:
            producer_name = self.__class__.__name__
            await self._backpressure_event_producer.send_backpressure_event(
                action=action,
                producer_name=producer_name,
                status=status,
                message=f"{producer_name} {action}",
            )
        except Exception as e:
            # 이벤트 전송 실패는 주요 플로우를 방해하지 않음
            logger.error(
                f"백프레셔 이벤트 전송 실패: {e}",
                extra={"producer_name": self.__class__.__name__},
            )

    async def send_and_wait(self, topic: str, value: Any, key: Any = None) -> None:
        """메시지 전송 - 백프레셔 적용 플로우

        High Watermark 초과 시 자동으로 대기 (OOM 방지)
        백프레셔 이벤트를 Kafka로 전송 (설정된 경우)
        """
        if not self._started or not self.producer:
            raise RuntimeError("Producer not started")

        # 백프레셔 체크: High Watermark 초과 시 대기
        if self._should_throttle():
            status = self._get_queue_status()

            # 처음 백프레셔 활성화 시에만 이벤트 전송
            if not self._backpressure_throttled:
                self._backpressure_throttled = True
                logger.warning(
                    "백프레셔 활성화: 큐 여유 공간 대기 중",
                    extra={
                        "queue_size": status["queue_size"],
                        "usage_percent": status["usage_percent"],
                        "high_watermark": status["high_watermark"],
                    },
                )
                # Kafka로 백프레셔 활성화 이벤트 전송
                await self._send_backpressure_event("backpressure_activated", status)

            await self._wait_for_capacity()

            # 백프레셔 해제 시 이벤트 전송
            if self._backpressure_throttled:
                self._backpressure_throttled = False
                status_after = self._get_queue_status()
                logger.info(
                    "백프레셔 해제: 큐 여유 공간 확보",
                    extra={
                        "queue_size": status_after["queue_size"],
                        "usage_percent": status_after["usage_percent"],
                    },
                )
                # Kafka로 백프레셔 비활성화 이벤트 전송
                await self._send_backpressure_event(
                    "backpressure_deactivated", status_after
                )

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

                if self.producer is not None:
                    self.producer.produce(
                        topic=message["topic"],
                        value=message["value"],
                        key=message["key"],
                        callback=self._delivery_callback,
                    )

                self._message_queue.task_done()

            except Exception as e:
                logger.error(
                    f"{self.__class__.__name__} worker error: {e}", exc_info=True
                )

    def _poll_worker(self) -> None:
        """전용 poll 스레드 - delivery report 처리"""
        while not self._shutdown_event.is_set():
            try:
                if self.producer:
                    self.producer.poll(0.1)  # 100ms 타임아웃
                else:
                    threading.Event().wait(0.1)
            except Exception as e:
                logger.error(
                    f"{self.__class__.__name__} poll worker error: {e}", exc_info=True
                )

    def _delivery_callback(self, err, msg) -> None:
        """Delivery report 콜백 - poll 스레드에서 호출됨"""
        if err is not None:
            topic = msg.topic() if msg else "unknown"
            key = msg.key() if msg else "unknown"
            logger.error(
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
        if self.consumer is not None:
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
                        logger.error(
                            f"{self.__class__.__name__} Kafka error: {raw_msg.error()}"
                        )
                        continue

                    # 서브클래스별 raw 메시지 처리 (비차단 파이프라인 허용)
                    try:
                        self._handle_raw_message(raw_msg)
                    except Exception as e:
                        logger.error(
                            f"{self.__class__.__name__} 역직렬화 스케줄 오류: {e}",
                            exc_info=True,
                        )  # noqa: E501

                else:
                    threading.Event().wait(0.1)

            except Exception as e:
                logger.error(
                    f"{self.__class__.__name__} poll worker error: {e}", exc_info=True
                )

    def _handle_raw_message(self, raw_msg) -> None:
        """기본 raw 메시지 처리: 동기 역직렬화 후 큐 삽입을 루프에 스케줄.

        서브클래스(예: Avro)는 이 메서드를 오버라이드하여 비동기 역직렬화 파이프라인을 구현 가능
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
                dropped_msg = self._message_queue.get_nowait()  # 오래된 메시지 제거
                self._message_queue.put_nowait(msg)  # 새 메시지 추가
                logger.warning(
                    f"{self.__class__.__name__} queue full, dropped message",
                    extra={
                        "topic": dropped_msg.get("topic", "unknown"),
                        "component": "consumer_queue",
                    },
                )
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
