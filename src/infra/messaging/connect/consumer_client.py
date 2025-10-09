from __future__ import annotations

import asyncio
import random
from typing import Any, Final, cast

from src.application.orchestrator import StreamOrchestrator
from src.common.exceptions.error_dispatcher import dispatch_error
from src.common.logger import PipelineLogger
from src.core.dto.adapter.stream_context import adapter_stream_context
from src.core.dto.internal.orchestrator import StreamContextDomain
from src.core.dto.io.commands import CommandDTO
from src.core.dto.io.target import ConnectionTargetDTO
from src.core.types import ExchangeName, PayloadAction, PayloadType, Region, RequestType
from src.infra.messaging.clients.json_client import create_consumer
from src.infra.messaging.connect.services.cache_coordinator import CacheCoordinator
from src.infra.messaging.connect.services.command_validator import GenericValidator

logger: Final = PipelineLogger.get_logger("consumer", "app")


class KafkaConsumerClient:
    """
    connect_and_subscribe 명령을 처리하는 Kafka 컨슈머 (리팩토링됨).

    - 지정된 토픽에서 소비
    - payload 구조를 기본 검증
    - 오케스트레이터에 작업 위임
    - 부모 클래스 기반 고성능 비동기 처리
    - orjson 기반 JSON 역직렬화 (2-4배 빠름)
    """

    def __init__(self, topic: list[str], orchestrator: StreamOrchestrator) -> None:
        self.orchestrator = orchestrator
        self.topic = topic
        self.consumer = create_consumer(self.topic)
        self._tasks: set[asyncio.Task[None]] = set()
        self._cache_coord = CacheCoordinator()

    async def _start_consumer(self) -> None:
        if self.consumer is None:
            self.consumer = create_consumer(self.topic)
        await self.consumer.start()
        logger.info(f"Kafka 소비 시작 - topic: {self.topic}")

    async def _stop_consumer(self) -> None:
        if self.consumer is not None:
            await self.consumer.stop()
            logger.info("Kafka 소비 종료")

    def _should_process(self, payload: dict[str, Any]) -> bool:
        """처리 대상 여부 판단 (type/action 필터링)."""
        match (payload.get("type"), payload.get("action")):
            case (PayloadType.STATUS, PayloadAction.CONNECT_AND_SUBSCRIBE):
                return True
            case (PayloadType.STATUS, _):
                logger.debug(
                    f"무시: action!={PayloadAction.CONNECT_AND_SUBSCRIBE}, \n"
                    f"받음: {payload.get('action')}"
                )
                return False
            case (type_val, _):
                logger.debug(f"무시: type!={PayloadType.STATUS}, 받음: {type_val}")
                return False

    async def _enqueue_connect_task(
        self, validated: StreamContextDomain, record: dict
    ) -> None:
        """오케스트레이터 연결 작업을 백그라운드 태스크로 등록."""

        async def _task_with_offset_store() -> None:
            try:
                await self.orchestrator.connect_from_context(validated)

                # ✅ 성공 시 오프셋 저장 (메모리에만, 초고속)
                if raw_msg := record.get("_raw_msg"):
                    self.consumer.consumer.store_offsets(message=raw_msg)

            except Exception as e:
                logger.error(f"처리 실패, 재시도 예정: {e}", exc_info=True)
                # ❌ 실패 시 저장하지 않음 → 재처리됨

        task = asyncio.create_task(_task_with_offset_store())
        self._tasks.add(task)

        def _on_done(t: asyncio.Task[None]) -> None:
            self._tasks.discard(t)

        task.add_done_callback(_on_done)

    async def _consume_stream(self) -> None:
        """컨슈머 스트림을 순회하며 레코드를 처리."""
        async for record in self.consumer:
            # record는 dict 형태: {"key": str, "value": dict, "topic": str}
            payload: dict = record["value"]  # 이미 역직렬화된 dict
            message_key = record["key"]  # 이미 역직렬화된 str

            # 예외 발생 시 except 블록에서 안전하게 참조할 수 있도록 기본값 초기화
            exchange: str | None = "unknown"
            region: str | None = "unknown"
            request_type: str | None = "unknown"

            try:
                if not isinstance(payload, dict):
                    raise ValueError("invalid payload type: expected dict")
                tgt = payload["target"]
                exchange: str | None = tgt["exchange"]
                region: str | None = tgt["region"]
                request_type: str | None = tgt["request_type"]

                if not self._should_process(payload):
                    continue

                # CommandDTO로 검증 및 변환
                _validated = GenericValidator(
                    exchange_name=cast(ExchangeName, exchange),
                    region=cast(Region, region),
                    request_type=cast(RequestType, request_type),
                )
                validated_dto = await _validated.validate_dto(
                    key=message_key, payload=payload, dto_class=CommandDTO
                )
                if validated_dto is None:
                    continue

                skip: bool = await self._cache_coord.handle_and_maybe_skip(
                    validated_dto
                )

                if skip:
                    continue

                # Validator 성공 → 내부 도메인 컨텍스트로 어댑트
                ctx: StreamContextDomain = adapter_stream_context(validated_dto)

                await self._enqueue_connect_task(ctx, record)  # record 전달
            except Exception as e:
                # 통합 에러 처리 (분류 → 로깅 → ws.error 발행)
                target = ConnectionTargetDTO(
                    exchange=cast(ExchangeName, exchange),
                    region=cast(Region, region),
                    request_type=cast(RequestType, request_type),
                )
                await dispatch_error(
                    exc=e,
                    kind="kafka_consumer",
                    target=target,
                    context={"record": record, "payload": payload},
                )

    async def run(self) -> None:
        # 지수 백오프 + 지터 기반 재시도 루프
        attempt = 0
        max_sleep = 30  # 초
        try:
            while True:
                try:
                    await self._start_consumer()
                    attempt = 0  # 시작 성공 시 재시도 카운터 리셋
                    await self._consume_stream()
                except asyncio.CancelledError:
                    logger.info("Kafka 컨슈머 작업 취소 요청 처리: 종료합니다.")
                    break
                except Exception as e:
                    attempt += 1
                    # 2^attempt 기반, 상한 및 지터 적용
                    backoff = min(max_sleep, 2 ** min(attempt, 5))
                    sleep_s = backoff + random.uniform(0, 1)
                    logger.warning(
                        f"Kafka 컨슈머 오류 발생, {sleep_s:.1f}s 후 재시도 (시도 {attempt}): {e}"
                    )
                    await self._stop_consumer()
                    await asyncio.sleep(sleep_s)
                    continue
        finally:
            # 최종 정리 시에만 Consumer 종료
            await self._stop_consumer()
