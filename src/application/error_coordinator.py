"""
에러 처리 통합 코디네이터

ErrorDispatcher의 강력한 기능(전략 패턴, Circuit Breaker, DLQ)과
StreamOrchestrator에 특화된 도메인 API를 결합합니다.
"""

from __future__ import annotations

from src.common.exceptions.error_dispatcher import ErrorDispatcher
from src.common.logger import PipelineLogger
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.io.commands import ConnectionTargetDTO
from src.infra.messaging.connect.producer_client import ErrorEventProducer

logger = PipelineLogger.get_logger("error_coordinator", "app")


class ErrorCoordinator:
    """에러 처리 통합 코디네이터

    ErrorDispatcher의 강력한 기능을 래핑하여 StreamOrchestrator에
    최적화된 도메인 API를 제공합니다.

    기능:
    - 도메인 특화 메서드 (emit_connection_error, emit_resubscribe_error 등)
    - ErrorDispatcher 통합 (전략 패턴, Circuit Breaker, DLQ)
    - Producer 라이프사이클 관리
    """

    def __init__(self, error_producer: ErrorEventProducer) -> None:
        """ErrorCoordinator 초기화

        Args:
            error_producer: ErrorEventProducer 인스턴스
        """
        self._error_producer = error_producer
        self._dispatcher = ErrorDispatcher()

    async def emit_connection_error(
        self,
        scope: ConnectionScopeDomain,
        error: BaseException,
        phase: str,
        additional_context: dict | None = None,
    ) -> bool:
        """연결 관련 에러 발행

        Args:
            scope: 연결 스코프
            error: 예외 객체
            phase: 에러 발생 단계 (예: "handler_creation", "connection", "run_with")
            additional_context: 추가 컨텍스트 정보

        Returns:
            발행 성공 여부
        """
        observed_key = self._format_scope(scope)
        raw_context = {"phase": phase, **(additional_context or {})}

        target = ConnectionTargetDTO(
            exchange=scope.exchange,
            region=scope.region,
            request_type=scope.request_type,
        )

        # ErrorDispatcher를 통해 전략 기반 처리
        try:
            await self._dispatcher.dispatch(
                exc=error,
                kind="ws",
                target=target,
                context=raw_context,
                producer=self._error_producer,
            )
            return True
        except Exception as dispatch_error:
            logger.critical(
                f"Failed to dispatch connection error: {dispatch_error}. "
                f"Original error: {error}. Scope: {observed_key}"
            )
            return False

    async def emit_resubscribe_error(
        self,
        scope: ConnectionScopeDomain,
        error: BaseException,
        socket_params: object,
    ) -> bool:
        """재구독 관련 에러 발행

        Args:
            scope: 연결 스코프
            error: 예외 객체
            socket_params: 소켓 파라미터

        Returns:
            발행 성공 여부
        """
        observed_key = f"{self._format_scope(scope)}|resubscribe"
        raw_context = {
            "phase": "resubscribe",
            "socket_params_type": type(socket_params).__name__,
            "socket_params": socket_params,
        }

        target = ConnectionTargetDTO(
            exchange=scope.exchange,
            region=scope.region,
            request_type=scope.request_type,
        )

        # ErrorDispatcher를 통해 전략 기반 처리
        try:
            await self._dispatcher.dispatch(
                exc=error,
                kind="ws",
                target=target,
                context=raw_context,
                producer=self._error_producer,
            )
            return True
        except Exception as dispatch_error:
            logger.critical(
                f"Failed to dispatch resubscribe error: {dispatch_error}. "
                f"Original error: {error}. Scope: {observed_key}"
            )
            return False

    async def emit_orchestrator_error(
        self,
        scope: ConnectionScopeDomain,
        error: BaseException,
        operation: str,
        additional_context: dict | None = None,
    ) -> bool:
        """오케스트레이터 관련 에러 발행

        Args:
            scope: 연결 스코프
            error: 예외 객체
            operation: 수행 중이던 작업 (예: "connect", "disconnect", "shutdown")
            additional_context: 추가 컨텍스트 정보

        Returns:
            발행 성공 여부
        """
        observed_key = f"{self._format_scope(scope)}|{operation}"
        raw_context = {
            "phase": "orchestrator",
            "operation": operation,
            **(additional_context or {}),
        }

        target = ConnectionTargetDTO(
            exchange=scope.exchange,
            region=scope.region,
            request_type=scope.request_type,
        )

        # ErrorDispatcher를 통해 전략 기반 처리
        try:
            await self._dispatcher.dispatch(
                exc=error,
                kind="orchestrator",
                target=target,
                context=raw_context,
                producer=self._error_producer,
            )
            return True
        except Exception as dispatch_error:
            logger.critical(
                f"Failed to dispatch orchestrator error: {dispatch_error}. "
                f"Original error: {error}. Scope: {observed_key}"
            )
            return False

    async def record_success(self, scope: ConnectionScopeDomain) -> None:
        """성공 기록 (Circuit Breaker용)

        연결 성공 시 호출하여 Circuit Breaker 상태를 CLOSED로 전환합니다.

        Args:
            scope: 연결 스코프
        """
        target = ConnectionTargetDTO(
            exchange=scope.exchange,
            region=scope.region,
            request_type=scope.request_type,
        )
        await self._dispatcher.record_success(target)

    async def cleanup(self) -> None:
        """리소스 정리 (Circuit Breaker 등)

        애플리케이션 종료 시 호출
        """
        await self._dispatcher.cleanup()

    def _format_scope(self, scope: ConnectionScopeDomain) -> str:
        """스코프를 읽기 쉬운 문자열로 변환"""
        return f"{scope.exchange}/{scope.region}/{scope.request_type}"
