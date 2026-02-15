from __future__ import annotations

import asyncio
from typing import Any, Callable

from pydantic import BaseModel

from src.common.logger import PipelineLogger
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
                logger.info(f"AvroProducerWrapper created with subject: {self._avro_subject}")
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
            raise RuntimeError("Cannot change producer type while running. Stop producer first.")
        self._use_avro = True
        self._avro_subject = subject
        self.producer = None  # 기존 Producer 무효화
        logger.info(f"Avro serialization enabled for subject: {subject}")

    def disable_avro(self) -> None:
        """Avro 직렬화 비활성화 - Producer 재생성 필요"""
        if self.producer_started:
            raise RuntimeError("Cannot change producer type while running. Stop producer first.")
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
            "producer_instance": (type(self.producer).__name__ if self.producer else None),
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
            if not started or self.producer is None:
                logger.warning(f"Kafka Producer not started; skipping send to topic '{topic}'")
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
                    logger.warning(f"logging warning attension!! : {e}")
                    if attempt < retries:
                        logger.warning(f"Send attempt {attempt} failed, retrying: {e}")
                        attempt += 1
                        await asyncio.sleep(0.1 * attempt)  # 지수 백오프
                    else:
                        logger.error(f"All {retries} send attempts failed: {e}")
                        return False  # 모든 재시도 실패

            return False  # while 루프 정상 종료 시 (도달 불가)

        finally:
            if stop_after_send:
                await self.stop_producer()


__all__ = [
    "AvroProducer",
]
