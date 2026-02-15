from __future__ import annotations

from typing import Any

from src.common.logger import PipelineLogger
from src.core.connection.utils.logging.pydantic_filter import PydanticFilter
from src.core.dto.internal.common import ConnectionScopeDomain


class ScopedConnectionLoggingMixin:
    """Connection scope-aware structured logging helpers."""

    _logger: PipelineLogger
    scope: ConnectionScopeDomain

    def _scope_log_extra(self, phase: str, **extra: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "exchange": self.scope.exchange,
            "region": self.scope.region,
            "request_type": self.scope.request_type,
            "phase": phase,
        }
        payload.update(extra)
        return PydanticFilter.filter_dict(payload)

    def _log_info(self, message: str, phase: str, **extra: Any) -> None:
        self._logger.info(message, extra=self._scope_log_extra(phase, **extra))

    def _log_debug(self, message: str, phase: str, **extra: Any) -> None:
        self._logger.debug(message, extra=self._scope_log_extra(phase, **extra))

    def _log_warning(self, message: str, phase: str, **extra: Any) -> None:
        self._logger.warning(message, extra=self._scope_log_extra(phase, **extra))

    def _log_error(self, message: str, phase: str, **extra: Any) -> None:
        self._logger.error(message, extra=self._scope_log_extra(phase, **extra))
