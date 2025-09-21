from __future__ import annotations

import asyncio
import json
import threading
from typing import Any

from confluent_kafka import Consumer, Producer
from config.settings import kafka_settings
from core.dto.internal.mq import ConsumerConfigDomain, ProducerConfigDomain


def default_value_serializer(value: Any) -> bytes:
    # 기본은 JSON 직렬화
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def default_key_serializer(value: Any) -> bytes:
    # 문자열 키 우선, 아니라면 그대로 bytes라고 가정
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    # 그 외는 문자열로 변환하여 사용
    return str(value).encode("utf-8")


def default_value_deserializer(raw: bytes) -> Any:
    # 기본은 JSON 역직렬화
    return json.loads(raw.decode("utf-8"))


def default_key_deserializer(raw: bytes | None) -> Any | None:
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except Exception:
        return raw


def producer_config(**overrides: Any) -> dict[str, Any]:
    """Producer 설정을 ProducerConfigDomain 기반으로 구성 후 confluent-kafka 형식으로 변환.

    - 성능 최적화: 처리량(throughput) 우선 설정
    - ProducerConfigDomain을 사용하여 타입 안전성 보장
    - confluent-kafka 매개변수 명으로 변환
    """
    # ProducerConfigDomain으로 성능 최적화 설정 구성
    cfg_domain = ProducerConfigDomain(
        # 기본 연결 설정
        bootstrap_servers=kafka_settings.BOOTSTRAP_SERVERS,
        security_protocol="PLAINTEXT",
        # 성능 최적화 설정 (처리량 우선)
        acks=1,  # 처리량 최적화 ("all"보다 빠름)
        batch_size=131072,  # 128KB (기본값 16KB보다 8배 크게)
        linger_ms=50,  # 50ms 대기 (배치 효율 극대화)
        compression_type="lz4",  # LZ4 압축 (빠른 압축/해제)
        buffer_memory=67108864,  # 64MB (기본값 32MB보다 2배)
        # 안정성 및 성능 설정
        enable_idempotence=True,  # 중복 방지
        retries=2147483647,  # 최대 재시도
        max_in_flight_requests_per_connection=5,  # 성능 최적화
        request_timeout_ms=30000,  # 30초 타임아웃
        delivery_timeout_ms=120000,  # 2분 전체 타임아웃
        # 직렬화 함수
        value_serializer=default_value_serializer,
        key_serializer=default_key_serializer,
    )

    # confluent-kafka 형식으로 변환 (점 표기법 사용)
    cfg = {
        "bootstrap.servers": cfg_domain.bootstrap_servers,
        "security.protocol": cfg_domain.security_protocol,
        "acks": cfg_domain.acks,
        "batch.size": cfg_domain.batch_size,
        "linger.ms": cfg_domain.linger_ms,
        "compression.type": cfg_domain.compression_type,
        "buffer.memory": cfg_domain.buffer_memory,
        "enable.idempotence": cfg_domain.enable_idempotence,
        "retries": cfg_domain.retries,
        "max.in.flight.requests.per.connection": cfg_domain.max_in_flight_requests_per_connection,
        "request.timeout.ms": cfg_domain.request_timeout_ms,
        "delivery.timeout.ms": cfg_domain.delivery_timeout_ms,
    }

    cfg.update(overrides)  # 사용자 지정 값으로 덮어쓰기
    return cfg


def consumer_config(**overrides: Any) -> dict[str, Any]:
    """Consumer 설정을 ConsumerConfigDomain 기반으로 구성 후 confluent-kafka 형식으로 변환.

    - 성능 최적화: 처리량(throughput) 우선 설정
    - ConsumerConfigDomain을 사용하여 타입 안전성 보장
    """
    # ConsumerConfigDomain으로 성능 최적화 설정 구성
    cfg_domain = ConsumerConfigDomain(
        # 기본 연결 설정
        bootstrap_servers=kafka_settings.BOOTSTRAP_SERVERS,
        security_protocol="PLAINTEXT",
        group_id=kafka_settings.CONSUMER_GROUP_ID,
        # 오프셋 관리
        enable_auto_commit=True,
        auto_offset_reset=kafka_settings.AUTO_OFFSET_RESET,
        auto_commit_interval_ms=5000,  # 5초 자동 커밋
        # 성능 최적화 설정 (처리량 우선)
        fetch_min_bytes=102400,  # 100KB (기본값 1보다 크게)
        fetch_max_wait_ms=500,  # 500ms 최대 대기
        max_poll_records=1000,  # 한 번에 1000개 레코드
        max_poll_interval_ms=300000,  # 5분 폴링 간격
        # 세션 관리
        session_timeout_ms=30000,  # 30초 세션 타임아웃
        heartbeat_interval_ms=10000,  # 10초 하트비트
        # 파티션 할당 전략
        partition_assignment_strategy="RoundRobin",  # 라운드로빈 방식
        # 역직렬화 함수
        value_deserializer=default_value_deserializer,
        key_deserializer=default_key_deserializer,
    )

    # confluent-kafka 형식으로 변환 (점 표기법 + 언더스코어 변환)
    cfg = {
        "bootstrap.servers": cfg_domain.bootstrap_servers,
        "security.protocol": cfg_domain.security_protocol,
        "group.id": cfg_domain.group_id,
        "enable.auto.commit": cfg_domain.enable_auto_commit,
        "auto.offset.reset": cfg_domain.auto_offset_reset,
        "auto.commit.interval.ms": cfg_domain.auto_commit_interval_ms,
        "fetch.min.bytes": cfg_domain.fetch_min_bytes,
        "fetch.max.wait.ms": cfg_domain.fetch_max_wait_ms,
        "max.poll.records": cfg_domain.max_poll_records,
        "max.poll.interval.ms": cfg_domain.max_poll_interval_ms,
        "session.timeout.ms": cfg_domain.session_timeout_ms,
        "heartbeat.interval.ms": cfg_domain.heartbeat_interval_ms,
        "partition.assignment.strategy": cfg_domain.partition_assignment_strategy,
    }

    cfg.update(overrides)
    return cfg


class AsyncProducerWrapper:
    """confluent-kafka Producer의 고성능 비동기 래퍼 - 큐 기반 아키텍처"""

    def __init__(self, config: dict, serializers: tuple) -> None:
        self.config = config
        self.value_serializer, self.key_serializer = serializers
        self.producer: Producer | None = None
        self._started = False

        # 비동기 메시지 큐
        self._message_queue: asyncio.Queue = asyncio.Queue()

        # 전용 poll 스레드 관리
        self._poll_thread: threading.Thread | None = None
        self._shutdown_event = threading.Event()

        # 코루틴 태스크 관리
        self._producer_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Producer 시작 - 큐 처리 코루틴 및 poll 스레드 시작"""
        if self._started:
            return

        # Producer 인스턴스 생성 (동기 호출이지만 빠름)
        self.producer = Producer(self.config)

        # 전용 poll 스레드 시작 (delivery report 처리)
        self._poll_thread = threading.Thread(
            target=self._poll_worker, name="kafka-producer-poll", daemon=True
        )
        self._poll_thread.start()

        # 메시지 처리 코루틴 시작
        self._producer_task = asyncio.create_task(self._message_producer_worker())

        self._started = True

    async def stop(self) -> None:
        """Producer 종료 - graceful shutdown"""
        if not self._started or not self.producer:
            return

        # 1. 큐에 종료 신호 전송
        await self._message_queue.put(None)  # 종료 신호

        # 2. 메시지 처리 코루틴 종료 대기
        if self._producer_task:
            await self._producer_task

        # 3. 남은 메시지 flush (비동기)
        await asyncio.to_thread(self.producer.flush, 10.0)

        # 4. poll 스레드 종료
        self._shutdown_event.set()
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5.0)

        self.producer = None
        self._started = False

    async def send_and_wait(self, topic: str, value: Any, key: Any = None) -> None:
        """메시지 전송 - 큐에 추가 (논블로킹)"""
        if not self._started or not self.producer:
            raise RuntimeError("Producer not started")

        # 직렬화 처리
        serialized_value = self.value_serializer(value) if value is not None else None
        serialized_key = self.key_serializer(key) if key is not None else None

        # 메시지를 큐에 추가 (비동기)
        message = {"topic": topic, "value": serialized_value, "key": serialized_key}
        await self._message_queue.put(message)

    async def _message_producer_worker(self) -> None:
        """메시지 처리 코루틴 - 큐에서 메시지를 가져와 produce() 호출"""
        while True:
            try:
                # 큐에서 메시지 가져오기 (비동기 대기)
                message = await self._message_queue.get()

                # 종료 신호 확인
                if message is None:
                    break

                # Producer.produce() 호출 (논블로킹)
                self.producer.produce(
                    topic=message["topic"],
                    value=message["value"],
                    key=message["key"],
                    callback=self._delivery_callback,
                )

                # 큐 작업 완료 표시
                self._message_queue.task_done()

            except Exception as e:
                # 에러 로깅 (실제 환경에서는 로거 사용)
                print(f"Producer worker error: {e}")

    def _poll_worker(self) -> None:
        """전용 poll 스레드 - delivery report 처리"""
        while not self._shutdown_event.is_set():
            try:
                if self.producer:
                    # 논블로킹 poll로 delivery report 처리
                    self.producer.poll(0.1)  # 100ms 타임아웃
                else:
                    threading.Event().wait(0.1)  # Producer 없으면 대기
            except Exception as e:
                # 에러 로깅 (실제 환경에서는 로거 사용)
                print(f"Poll worker error: {e}")

    def _delivery_callback(self, err, msg) -> None:
        """Delivery report 콜백 - poll 스레드에서 호출됨"""
        if err is not None:
            # 전송 실패 로깅 (실제 환경에서는 로거 + 메트릭 사용)
            print(f"Message delivery failed: {err}")
        # 성공한 경우는 조용히 처리 (필요시 메트릭 수집)


class AsyncConsumerWrapper:
    """confluent-kafka Consumer의 고성능 비동기 래퍼 - 전용 poll 스레드 기반"""

    def __init__(
        self,
        topics: list[str],
        config: dict[str, Any],
        deserializers: tuple[Any, Any],
    ):
        self.topics = topics
        self.config = config
        self.value_deserializer, self.key_deserializer = deserializers
        self.consumer: Consumer | None = None
        self._started = False

        # 비동기 메시지 큐
        self._message_queue: asyncio.Queue = asyncio.Queue(
            maxsize=1000
        )  # 백프레셔 방지

        # 전용 poll 스레드 관리
        self._poll_thread: threading.Thread | None = None
        self._shutdown_event = threading.Event()

    async def start(self) -> None:
        """Consumer 시작 - 전용 poll 스레드 시작"""
        if self._started:
            return

        # Consumer 인스턴스 생성 (동기 호출이지만 빠름)
        self.consumer = Consumer(self.config)
        self.consumer.subscribe(self.topics)

        # 전용 poll 스레드 시작 (메시지 수신 처리)
        self._poll_thread = threading.Thread(
            target=self._poll_worker, name="kafka-consumer-poll", daemon=True
        )
        self._poll_thread.start()

        self._started = True

    async def stop(self) -> None:
        """Consumer 종료 - graceful shutdown"""
        if not self._started or not self.consumer:
            return

        # 1. poll 스레드 종료 신호
        self._shutdown_event.set()

        # 2. 큐에 종료 신호 전송
        try:
            self._message_queue.put_nowait(None)  # 비동기 이터레이터 종료
        except asyncio.QueueFull:
            pass  # 큐가 가득 차면 무시

        # 3. poll 스레드 종료 대기
        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=5.0)

        # 4. Consumer 종료
        await asyncio.to_thread(self.consumer.close)
        self.consumer = None
        self._started = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        """비동기 이터레이터 - 큐에서 메시지 가져오기"""
        if not self._started:
            raise RuntimeError("Consumer not started")

        # 큐에서 메시지 가져오기 (비동기 대기)
        msg = await self._message_queue.get()

        # 종료 신호 확인
        if msg is None:
            raise StopAsyncIteration

        return msg

    def _poll_worker(self) -> None:
        """전용 poll 스레드 - 메시지 수신 및 큐에 전달"""
        while not self._shutdown_event.is_set():
            try:
                if self.consumer:
                    # 논블로킹 poll로 메시지 수신
                    msg = self.consumer.poll(timeout=0.1)  # 100ms 타임아웃

                    if msg is None:
                        continue  # 메시지 없음, 계속 대기

                    if msg.error():
                        # 에러 로깅 (실제 환경에서는 로거 사용)
                        print(f"Consumer error: {msg.error()}")
                        continue

                    # 정상 메시지를 큐에 전달 (비동기 코루틴에서 처리)
                    try:
                        # 비동기 플레그를 사용하여 스레드에서 비동기 큐에 접근
                        loop = None
                        try:
                            loop = asyncio.get_event_loop()
                        except RuntimeError:
                            pass  # 이벤트 루프가 없으면 무시

                        if loop and not loop.is_closed():
                            # call_soon_threadsafe로 안전하게 큐에 추가
                            loop.call_soon_threadsafe(self._add_message_to_queue, msg)

                    except Exception as e:
                        print(f"Queue put error: {e}")

                else:
                    # Consumer가 없으면 대기
                    threading.Event().wait(0.1)

            except Exception as e:
                # 에러 로깅 (실제 환경에서는 로거 사용)
                print(f"Poll worker error: {e}")

    def _add_message_to_queue(self, msg) -> None:
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


def create_producer(**overrides: Any) -> AsyncProducerWrapper:
    """비동기 Producer 래퍼 생성 - 성능 최적화 ProducerConfigDomain 기반"""
    # 성능 최적화된 confluent-kafka 설정 생성
    cfg = producer_config(**overrides)

    # 직렬화 함수는 별도로 전달 (성능 최적화된 기본 함수 사용)
    serializers = (default_value_serializer, default_key_serializer)

    return AsyncProducerWrapper(cfg, serializers)


def create_consumer(topic: str, **overrides: Any) -> AsyncConsumerWrapper:
    """비동기 Consumer 래퍼 생성 - 성능 최적화 ConsumerConfigDomain 기반"""
    # 성능 최적화된 confluent-kafka 설정 생성
    cfg = consumer_config(**overrides)

    # 역직렬화 함수는 별도로 전달 (성능 최적화된 기본 함수 사용)
    deserializers = (default_value_deserializer, default_key_deserializer)

    topics = [topic] if isinstance(topic, str) else topic
    return AsyncConsumerWrapper(topics, cfg, deserializers)
