from __future__ import annotations

import random
import asyncio
from typing import Any, Final

from aiokafka import AIOKafkaConsumer

from application.orchestrator import StreamOrchestrator
from common.logger import PipelineLogger
from core.dto.adapter.stream_context import adapter_stream_context
from core.dto.internal.orchestrator import StreamContextDomain
from core.dto.io.commands import CommandDTO
from core.dto.io.target import ConnectionTargetDTO
from core.types import PayloadAction, PayloadType
from infra.messaging.clients.clients import create_consumer
from infra.messaging.connect.services.cache_coordinator import CacheCoordinator
from infra.messaging.connect.services.command_validator import GenericValidator
from core.dto.adapter.error_adapter import make_ws_error_event_from_kind

logger: Final = PipelineLogger.get_logger("consumer", "app")


class KafkaConsumerClient:
    """
    connect_and_subscribe 명령을 처리하는 Kafka 컨슈머.

    - 지정된 토픽에서 소비
    - payload 구조를 기본 검증
    - 오케스트레이터에 작업 위임
    """

    def __init__(self, orchestrator: StreamOrchestrator, topic: str) -> None:
        """
        Args:
            orchestrator (StreamOrchestrator): 오케스트레이터
            topic (str): 소비할 토픽
        """
        self.orchestrator = orchestrator
        self.topic = topic
        self.consumer: AIOKafkaConsumer | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._cache_coord = CacheCoordinator()

    async def _start_consumer(self) -> None:
        self.consumer = create_consumer(self.topic)
        await self.consumer.start()
        logger.info(f"Kafka 소비 시작 - topic: {self.topic}")

    async def _stop_consumer(self) -> None:
        if self.consumer is not None:
            await self.consumer.stop()
            logger.info("Kafka 소비 종료")

    # ---------------- 내부 헬퍼들 ----------------
    def _should_process(self, payload: dict[str, Any]) -> bool:
        """처리 대상 여부 판단 (type/action 필터링)."""
        match (payload.get("type"), payload.get("action")):
            case (PayloadType.STATUS, PayloadAction.CONNECT_AND_SUBSCRIBE):
                return True
            case (PayloadType.STATUS, _):
                logger.debug(
                    f"무시: action!={PayloadAction.CONNECT_AND_SUBSCRIBE}, 받음: {payload.get('action')}"
                )
                return False
            case (type_val, _):
                logger.debug(f"무시: type!={PayloadType.STATUS}, 받음: {type_val}")
                return False

    async def _enqueue_connect_task(self, validated: StreamContextDomain) -> None:
        """오케스트레이터 연결 작업을 백그라운드 태스크로 등록."""
        task = asyncio.create_task(self.orchestrator.connect_from_context(validated))
        self._tasks.add(task)

        def _on_done(t: asyncio.Task[None]) -> None:
            self._tasks.discard(t)

        task.add_done_callback(_on_done)

    async def _consume_stream(self) -> None:
        """컨슈머 스트림을 순회하며 레코드를 처리."""
        async for record in self.consumer:
            payload: dict = record.value
            exchange: str = payload["exchange"]
            region: str = payload["region"]
            request_type: str = payload["request_type"]
            observed_key: str = f"{exchange}/{region}/{request_type}"

            try:
                if not self._should_process(payload):
                    continue

                # CommandDTO로 검증 및 변환
                _validated = GenericValidator(
                    exchange_name=exchange,
                    request_type=request_type,
                )
                validated_dto = await _validated.validate_dto(
                    key=record.key,
                    payload=payload,
                    dto_class=CommandDTO,
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

                await self._enqueue_connect_task(ctx)
            except Exception as e:
                # 가능한 경우에만 ws.error 발행 (필수 키가 모두 존재할 때)
                target = ConnectionTargetDTO(
                    exchange=exchange,
                    region=region,
                    request_type=request_type,
                )
                await make_ws_error_event_from_kind(
                    target=target,
                    err=e,
                    kind="kafka",
                    observed_key=observed_key,
                    raw_context=None,
                )

    async def run(self) -> None:
        # 지수 백오프 + 지터 기반 재시도 루프
        attempt = 0
        max_sleep = 30  # 초
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
                await self._stop_consumer()
