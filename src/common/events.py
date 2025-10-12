"""이벤트 정의 및 Event Bus (EDA 패턴)

모든 레이어가 순환 import 없이 이벤트를 발행할 수 있도록 지원합니다.
이벤트는 순수 데이터 객체로, 의존성이 없습니다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from src.common.logger import PipelineLogger
from src.core.dto.io.commands import ConnectionTargetDTO


@dataclass(frozen=True, slots=True)
class ErrorEvent:
    """에러 이벤트 (순수 데이터)

    모든 레이어에서 발행 가능:
    - Infrastructure: Kafka, Redis, Avro 에러
    - Domain: WebSocket, 구독, 헬스체크 에러
    - Application: 비즈니스 로직 에러
    """

    exc: Exception
    kind: str  # "avro_deserialization", "kafka_consumer", "ws", etc.
    target: ConnectionTargetDTO
    context: dict[str, Any] | None = None
    timestamp: datetime = field(default_factory=datetime.now)


class EventBus:
    """전역 이벤트 버스 (의존성 없음)

    특징:
    - 완전한 비동기 처리
    - 타입 기반 핸들러 등록
    - 동시성 안전 (asyncio)
    - 순환 import 없음
    """

    _handlers: dict[type, list[Callable[[Any], Any]]] = {}

    @classmethod
    async def emit(cls, event: Any) -> None:
        """이벤트 발행 (비동기)

        Args:
            event: 발행할 이벤트 객체
        """
        event_type = type(event)
        handlers = cls._handlers.get(event_type, [])

        # 모든 핸들러를 비동기로 실행
        for handler in handlers:
            try:
                await handler(event)
            except Exception as e:

                logger = PipelineLogger.get_logger("event_bus", "common")
                logger.error(
                    f"Event handler failed: {e}",
                    exc_info=True,
                    extra={
                        "event_type": event_type.__name__,
                        "handler": handler.__name__,
                    },
                )

    @classmethod
    def on(cls, event_type: type, handler: Callable[[Any], Any]) -> None:
        """핸들러 등록

        Args:
            event_type: 이벤트 타입 (클래스)
            handler: 핸들러 함수 (async def)
        """
        if event_type not in cls._handlers:
            cls._handlers[event_type] = []
        cls._handlers[event_type].append(handler)

    @classmethod
    def clear(cls) -> None:
        """모든 핸들러 제거 (테스트용)"""
        cls._handlers.clear()


__all__ = ["ErrorEvent", "EventBus"]
