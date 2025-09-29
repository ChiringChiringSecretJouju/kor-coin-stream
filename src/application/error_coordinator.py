"""
에러 처리 통합 코디네이터

시스템 전반의 에러 처리를 일관되게 관리합니다.
분산된 에러 처리 로직을 통합하고 표준화합니다.
"""

from __future__ import annotations

from src.common.logger import PipelineLogger
from src.core.dto.adapter.error_adapter import make_ws_error_event_from_kind
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.io.target import ConnectionTargetDTO

logger = PipelineLogger.get_logger("error_coordinator", "app")


class ErrorCoordinator:
    """에러 처리 통합 코디네이터

    시스템 전반의 에러를 일관된 방식으로 처리하고 발행합니다.
    에러 컨텍스트 생성, 분류, 발행을 담당합니다.
    """

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

        logger.error(f"Connection error in {phase}: {observed_key} - {error}")

        return await self._emit_scope_error(
            scope=scope,
            error=error,
            kind="ws",
            observed_key=observed_key,
            raw_context=raw_context,
        )

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

        logger.error(f"Resubscribe error: {observed_key} - {error}")

        return await self._emit_scope_error(
            scope=scope,
            error=error,
            kind="ws",
            observed_key=observed_key,
            raw_context=raw_context,
        )

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

        logger.error(f"Orchestrator error in {operation}: {observed_key} - {error}")

        return await self._emit_scope_error(
            scope=scope,
            error=error,
            kind="orchestrator",
            observed_key=observed_key,
            raw_context=raw_context,
        )

    async def _emit_scope_error(
        self,
        scope: ConnectionScopeDomain,
        error: BaseException,
        kind: str,
        observed_key: str,
        raw_context: dict,
    ) -> bool:
        """스코프 기반 에러 이벤트 발행 (내부 헬퍼)

        Args:
            scope: 연결 스코프
            error: 예외 객체
            kind: 에러 분류
            observed_key: 관측 키
            raw_context: 원시 컨텍스트

        Returns:
            발행 성공 여부
        """
        try:
            target = ConnectionTargetDTO(
                exchange=scope.exchange,
                region=scope.region,
                request_type=scope.request_type,
            )

            return await make_ws_error_event_from_kind(
                target=target,
                err=error,
                kind=kind,
                observed_key=observed_key,
                raw_context=raw_context,
            )
        except Exception as emit_error:
            # 에러 발행 자체가 실패한 경우 로깅만 수행
            logger.critical(
                f"Failed to emit error event: {emit_error}. "
                f"Original error: {error}. Scope: {self._format_scope(scope)}"
            )
            return False

    def _format_scope(self, scope: ConnectionScopeDomain) -> str:
        """스코프를 읽기 쉬운 문자열로 변환"""
        return f"{scope.exchange}/{scope.region}/{scope.request_type}"
