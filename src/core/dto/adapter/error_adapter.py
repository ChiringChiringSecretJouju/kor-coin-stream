"""
에러 어댑터 모듈 (하위 호환성 유지)

순환 참조 해결을 위해 파일이 분리되었습니다:
- error_dto_builder.py: DTO 빌더 함수들
- error_dispatcher.py: ErrorDispatcher 클래스

이 파일은 하위 호환성을 위해 모든 public API를 re-export합니다.
"""

# Re-export from error_dto_builder
# Re-export from error_dispatcher
from src.common.exceptions.error_dispatcher import ErrorDispatcher, dispatch_error
from src.common.exceptions.error_dto_builder import (
    build_error_meta,
    build_error_type,
    make_ws_error_event_from_kind,
)

__all__ = [
    # DTO builders
    "build_error_meta",
    "build_error_type",
    "make_ws_error_event_from_kind",
    # Dispatcher
    "ErrorDispatcher",
    "dispatch_error",
]
