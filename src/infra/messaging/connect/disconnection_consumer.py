from __future__ import annotations

import asyncio
from typing import Any, Final, cast

from src.application.orchestrator import StreamOrchestrator
from src.common.exceptions.error_dispatcher import dispatch_error
from src.common.logger import PipelineLogger
from src.core.dto.io.commands import ConnectionTargetDTO, DisconnectCommandDTO
from src.core.types import ExchangeName, PayloadAction, PayloadType, Region, RequestType
from src.infra.messaging.clients.json_client import AsyncConsumerWrapper, create_consumer
from src.infra.messaging.connect.services.command_validator import GenericValidator

logger: Final = PipelineLogger.get_logger("consumer", "disconnect")


class KafkaDisconnectionConsumerClient:
    """ws.disconnection 토픽을 소비하여 연결 종료를 지시하는 컨슈머 (리팩토링됨).
    
    - 리팩토링된 JSON 클라이언트 사용 (orjson 기반 고성능 역직렬화)
    - 부모 클래스 기반 비동기 처리 아키텍처
    - 연결 종료 명령 처리 및 오케스트레이터 연동
    """

    def __init__(self, orchestrator: StreamOrchestrator, topic: list[str]) -> None:
        self.orchestrator = orchestrator
        self.topic = topic
        self.consumer: AsyncConsumerWrapper | None = None

    async def _start_consumer(self) -> None:
        self.consumer = create_consumer(self.topic)
        await self.consumer.start()
        logger.info(f"Kafka disconnect 소비 시작 - topic: {self.topic}")

    async def _stop_consumer(self) -> None:
        if self.consumer is not None:
            await self.consumer.stop()
            logger.info("Kafka disconnect 소비 종료")

    def _should_process(self, payload: dict[str, Any]) -> bool:
        match (payload.get("type"), payload.get("action")):
            case (PayloadType.STATUS, PayloadAction.DISCONNECT):
                return True
            case (PayloadType.STATUS, _):
                logger.debug("disconnect 컨슈머 무시: action!=disconnect")
                return False
            case _:
                logger.debug(
                    f"disconnect 컨슈머 무시: type!={PayloadType.STATUS}, "
                    f"받음: {payload.get('type')}"
                )
                return False

    async def _consume_stream(self) -> None:
        if self.consumer is None:
            logger.warning("Consumer not initialized")
            return
        
        async for record in self.consumer:
            payload: dict = record["value"]
            exchange = payload.get("target", {}).get("exchange", "")
            region = payload.get("target", {}).get("region", "")
            request_type = payload.get("target", {}).get("request_type", "")

            try:
                if not self._should_process(payload):
                    continue

                if not (exchange and region and request_type):
                    logger.warning(
                        f"disconnect 이벤트에 target 필드가 부족하여 무시합니다: {payload}"
                    )
                    continue

                validator = GenericValidator(
                    exchange_name=cast(ExchangeName, exchange),
                    request_type=cast(RequestType, request_type),
                    region=cast(Region, region),
                )
                dto = await validator.validate_dto(
                    payload=payload,
                    dto_class=DisconnectCommandDTO,
                    key=record["key"],
                )
                if dto is None:
                    continue

                target = ConnectionTargetDTO(
                    exchange=cast(ExchangeName, dto.target["exchange"]),
                    region=cast(Region, dto.target["region"]),
                    request_type=cast(RequestType, dto.target["request_type"]),
                )

                disconnected = await self.orchestrator.disconnect(
                    target=target,
                    reason=dto.reason,
                )

                if not disconnected:
                    logger.info(
                        f"disconnect 요청 처리: 활성 연결 없음 - {exchange}/{region}/{request_type}"
                    )
            except Exception as exc:
                logger.error(f"disconnect 이벤트 처리 실패: {exc}")
                try:
                    target = ConnectionTargetDTO(
                        exchange=cast(ExchangeName, exchange),
                        region=cast(Region, region),
                        request_type=cast(RequestType, request_type),
                    )
                except Exception:
                    continue

                context = {"phase": "disconnect", "payload": payload}
                await dispatch_error(
                    exc=exc if isinstance(exc, Exception) else Exception(str(exc)),
                    kind="consumer",
                    target=target,
                    context=context,
                )

    async def run(self) -> None:
        while True:
            try:
                await self._start_consumer()
                await self._consume_stream()
            except asyncio.CancelledError:
                logger.info("disconnect 컨슈머 작업 취소 요청 처리: 종료합니다.")
                break
            finally:
                await self._stop_consumer()
