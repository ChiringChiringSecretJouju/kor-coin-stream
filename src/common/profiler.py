from __future__ import annotations

import cProfile
import functools
import pstats
import time
from pathlib import Path
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


class PerformanceProfiler:
    """성능 프로파일링 유틸리티"""

    def __init__(self, output_dir: str = "profiles"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def profile_function(self, func_name: str | None = None) -> Callable[[F], F]:
        """함수 프로파일링 데코레이터"""

        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                name = func_name or f"{func.__module__}.{func.__name__}"
                timestamp = int(time.time())
                profile_file = self.output_dir / f"{name}_{timestamp}.prof"

                pr = cProfile.Profile()
                pr.enable()

                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    pr.disable()
                    pr.dump_stats(str(profile_file))
                    print(f"Profile saved: {profile_file}")

            return wrapper  # type: ignore

        return decorator

    def profile_async_function(self, func_name: str | None = None) -> Callable[[F], F]:
        """비동기 함수 프로파일링 데코레이터"""

        def decorator(func: F) -> F:
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                name = func_name or f"{func.__module__}.{func.__name__}"
                timestamp = int(time.time())
                profile_file = self.output_dir / f"{name}_{timestamp}.prof"

                pr = cProfile.Profile()
                pr.enable()

                try:
                    result = await func(*args, **kwargs)
                    return result
                finally:
                    pr.disable()
                    pr.dump_stats(str(profile_file))
                    print(f"Profile saved: {profile_file}")

            return wrapper  # type: ignore

        return decorator


# 전역 프로파일러 인스턴스
profiler = PerformanceProfiler()
