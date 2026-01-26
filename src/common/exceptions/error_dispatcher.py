"""통합 에러 디스패처 (EDA 환경용)

전략 기반 에러 처리:
- 예외 분류 (classify_exception)
- 전략 결정 (get_error_strategy)
- 이벤트 발행 (ws.error)
- DLQ 전송
- Circuit Breaker (Redis 기반 분산 서킷브레이커)
- 알람
"""

from __future__ import annotations

import traceback

from src.common.events import ErrorEvent, EventBus
from src.common.exceptions.exception_rule import (
    ErrorSeverity,
    classify_exception,
    get_error_strategy,
)
from src.common.logger import PipelineLogger
from src.core.dto.io.commands import ConnectionTargetDTO
from src.core.dto.io.events import DlqEventDTO
from src.infra.messaging.connect.producers.error.dlq import DlqProducer
from src.infra.messaging.connect.producers.error.error_event import (
    ErrorEventProducer,
)

from .error_dto_builder import (
    _make_json_serializable,
    build_error_meta,
    make_ws_error_event_from_kind,
)

logger = PipelineLogger.get_logger("error_dispatcher", "core")

__all__ = [
    "ErrorDispatcher",
    "dispatch_error",
]


class ErrorDispatcher:
    """통합 에러 처리 디스패처 (EDA 환경용)

    책임:
    1. 예외 분류 (classify_exception)
    2. 전략 결정 (get_error_strategy)
    3. 이벤트 발행 (ws.error)
    4. DLQ 전송
    5. 알람

    Note: ErrorEventProducer와 DlqProducer를 내부에서 관리하며,
          producer=None으로 호출 시 자동으로 생성합니다.
    """

    def __init__(
        self,
        error_producer: ErrorEventProducer | None = None,
        dlq_producer: DlqProducer | None = None,
    ):
        self._error_producer = error_producer
        self.dlq_producer = dlq_producer
        self._producer_created = False  # 내부 생성 여부 추적

    async def dispatch(
        self,
        exc: Exception,
        kind: str,
        target: ConnectionTargetDTO,
        context: dict | None = None,
        producer: ErrorEventProducer | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """전략 기반 통합 에러 디스패처"""
        # 1. 예외 분류
        domain, code, retryable = classify_exception(exc, kind)

        # 2. 전략 조회
        strategy = get_error_strategy(code)

        # 3. 로깅 (severity별)
        log_method = getattr(logger, strategy.log_level, logger.error)
        observed_key = f"{target.exchange}/{target.region}/{target.request_type}"

        # context에서 logging 예약 키워드 제거 (exc_info, stack_info 등)
        safe_context = {
            k: v
            for k, v in (context or {}).items()
            if k not in ("exc_info", "stack_info", "extra")
        }

        log_method(
            f"[{strategy.severity.value.upper()}] {kind} error: {exc}",
            exc_info=True,
            extra={
                "error_domain": (
                    domain.value if hasattr(domain, "value") else str(domain)
                ),
                "error_code": code.value if hasattr(code, "value") else str(code),
                "severity": strategy.severity.value,
                "retryable": retryable,
                "observed_key": observed_key,
                **safe_context,
            },
        )

        # 4. ws.error 발행 (전략에 따라)
        if strategy.send_to_ws_error:
            # Producer 결정: 파라미터 > 인스턴스 변수 > 새로 생성
            actual_producer = producer or self._error_producer
            if actual_producer is None:
                self._error_producer = ErrorEventProducer(use_avro=False)
                await self._error_producer.start_producer()
                self._producer_created = True
                actual_producer = self._error_producer

            try:
                await make_ws_error_event_from_kind(
                    target=target,
                    err=exc,
                    kind=kind,
                    observed_key=observed_key,
                    raw_context=context,
                    producer=actual_producer,
                    correlation_id=correlation_id,
                )
            except Exception as e:
                logger.error(f"Failed to send ws.error: {e}", exc_info=True)

        # 5. DLQ 전송 (전략에 따라)
        if strategy.send_to_dlq:
            try:
                await self._send_to_dlq(exc, kind, target, context)
            except Exception as e:
                logger.error(f"Failed to send DLQ: {e}", exc_info=True)

        # 6. 알람 (전략에 따라)
        if strategy.alert:
            await self._send_alert(exc, strategy.severity, target)

    async def _send_to_dlq(
        self,
        exc: Exception,
        kind: str,
        target: ConnectionTargetDTO,
        context: dict | None = None,
    ) -> None:
        """DLQ 전송 헬퍼"""
        dlq_producer = self.dlq_producer or DlqProducer()

        observed_key = f"{target.exchange}/{target.region}/{target.request_type}"
        meta = build_error_meta(
            observed_key=observed_key,
            raw_context=_make_json_serializable(context or {}),
        )

        dlq_event = DlqEventDTO(
            action="dlq",
            reason=f"{kind} failure: {type(exc).__name__}",
            target=target,
            meta=meta,
            detail_error={
                "exception_type": type(exc).__name__,
                "exception_message": str(exc),
                "traceback": traceback.format_exc(),
                "kind": kind,  # DLQ 원인 명시
            },
        )

        await dlq_producer.send_dlq_event(dlq_event)

    async def _send_alert(
        self,
        exc: Exception,
        severity: ErrorSeverity,
        target: ConnectionTargetDTO,
    ) -> None:
        """알람 발송 (Slack/PagerDuty 등) - TODO"""
        logger.info(
            f"Alert: {severity.value} - {exc}",
            extra={
                "target": f"{target.exchange}/{target.region}/{target.request_type}"
            },
        )

    async def cleanup(self) -> None:
        """리소스 정리 (내부 Producer)

        애플리케이션 종료 시 호출
        """
        # 내부에서 생성한 Producer 정리
        if self._producer_created and self._error_producer is not None:
            try:
                await self._error_producer.stop_producer()
                logger.info("Internal ErrorEventProducer stopped")
            except Exception as e:
                logger.warning(f"Failed to stop internal producer: {e}")
            finally:
                self._error_producer = None
                self._producer_created = False


# Event Bus 기반 에러 디스패처 (EDA 패턴)
async def dispatch_error(
    exc: Exception,
    kind: str,
    target: ConnectionTargetDTO,
    context: dict | None = None,
    correlation_id: str | None = None,
) -> None:
    """Event Bus 기반 에러 이벤트 발행 (EDA 패턴)

    모든 레이어에서 순환 import 없이 사용 가능:
    - Infrastructure: Kafka, Redis, Avro 에러
    - Domain: WebSocket, 구독, 헬스체크 에러
    - Application: 비즈니스 로직 에러

    Args:
        exc: 발생한 예외
        kind: 에러 종류 (분류용)
        target: 에러 발생 대상
        context: 추가 컨텍스트
    """

    await EventBus.emit(
        ErrorEvent(
            exc=exc,
            kind=kind,
            target=target,
            context=context,
            correlation_id=correlation_id,
        )
    )
