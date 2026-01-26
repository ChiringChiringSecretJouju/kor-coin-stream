import functools
from datetime import datetime
from typing import Any, Callable, Type, TypeVar

from src.common.logger import PipelineLogger
from src.core.dto.io.events import WsErrorEventDTO
from src.core.types._exception_types import CONNECTION_EXCEPTIONS

logger = PipelineLogger.get_logger("stream_decorator", "core")

T = TypeVar("T")

def catch_exception(
    exceptions: tuple[Type[BaseException], ...] = CONNECTION_EXCEPTIONS,
    phase: str = "unknown",
    level: str = "error",
    fallback_return: Any = None,
):
    """지정된 예외를 포착하여 ErrorProducer로 이벤트를 발행하는 데코레이터.
    
    Args:
        exceptions: 포착할 예외 클래스 튜플 (기본: CONNECTION_EXCEPTIONS)
        phase: 에러 발생 단계 (Context)
        level: "error" or "warning"
        fallback_return: 예외 발생 시 반환할 기본값 (기본: None)
        
    Requirement:
        데코레이터가 적용되는 클래스는 반드시 `self._error_producer`와
        `self.scope`(옵션)를 가져야 합니다.
    """
    def decorator(func: Callable[..., Any]):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            try:
                return await func(self, *args, **kwargs)
            except exceptions as e:
                # 1. 로깅
                msg = f"Exception in {phase}: {str(e)}"
                if level == "error":
                    logger.error(msg)
                else:
                    logger.warning(msg)
                
                # 2. Context 구성
                context = {"phase": phase, "args": str(args), "kwargs": str(kwargs)}
                exchange_name = "unknown"
                
                # self.scope에서 정보 추출 시도
                if hasattr(self, "scope"):
                    exchange_name = getattr(self.scope, "exchange", "unknown")
                    context["exchange"] = exchange_name
                    context["region"] = getattr(self.scope, "region", "unknown")
                
                # 3. 에러 이벤트 발행 (Producer가 있는 경우)
                if hasattr(self, "_error_producer"):
                    try:
                        producer = self._error_producer
                        event = WsErrorEventDTO(
                            message=msg,
                            error_type=type(e).__name__,
                            error_code=f"{phase.upper()}_ERROR",
                            level=level,
                            service="core_decorator",
                            timestamp=int(datetime.now().timestamp() * 1000),
                            context=context,
                        )
                        # 비동기 전송 시도
                        await producer.send_error_event(event)
                    except Exception as producer_error:
                        logger.critical(f"Failed to emit error event: {producer_error}")
                
                return fallback_return
            # 지정되지 않은 예외는 상위로 전파 (Let it crash or handle upper layer)
        return wrapper
    return decorator
