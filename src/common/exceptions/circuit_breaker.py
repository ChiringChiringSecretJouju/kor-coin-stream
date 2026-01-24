"""
Redis 기반 분산 서킷브레이커 구현 (Sliding Window 패턴)

3-State Finite State Machine:
- CLOSED: 정상 동작 (요청 허용)
- OPEN: 장애 감지 (요청 즉시 차단)
- HALF_OPEN: 회복 테스트 (제한된 요청만 허용)

특징:
- Redis Sorted Set 기반 슬라이딩 윈도우 구현
- 시간 기반 실패 추적 (기본 5분 윈도우)
- 분산 환경 상태 공유 (여러 인스턴스에서 일관성 보장)
- 자동 복구 (OPEN → HALF_OPEN → CLOSED)
- TTL 기반 자동 상태 만료
- 오래된 실패 자동 제거 (윈도우 밖 데이터)

슬라이딩 윈도우 동작 원리:
1. 실패 발생 시 타임스탬프와 함께 Redis Sorted Set에 기록
2. 매 실패마다 윈도우(기본 5분) 밖 오래된 실패 자동 제거
3. 윈도우 내 실패만 카운트하여 임계값 체크
4. 성공 시에도 오래된 실패 정리하여 메모리 효율성 유지

예시:
- 10:00 실패 → count=1
- 10:02 실패 → count=2
- 10:06 실패 → 10:00 실패 제거(윈도우 밖), count=2
"""

import time
from dataclasses import dataclass
from enum import Enum
from typing import Final

import orjson
from redis.asyncio import Redis

from src.common.logger import PipelineLogger
from src.infra.cache.cache_client import RedisConnectionManager

logger = PipelineLogger.get_logger("circuit_breaker", "core")


class CircuitState(str, Enum):
    """서킷브레이커 상태"""

    CLOSED = "CLOSED"  # 정상: 모든 요청 허용
    OPEN = "OPEN"  # 차단: 모든 요청 거부
    HALF_OPEN = "HALF_OPEN"  # 테스트: 제한된 요청만 허용


@dataclass(frozen=True, slots=True)
class CircuitBreakerConfig:
    """서킷브레이커 설정"""

    failure_threshold: int = 5  # 연속 실패 임계값
    success_threshold: int = 2  # HALF_OPEN에서 CLOSED로 전환 성공 횟수
    timeout_seconds: int = 60  # OPEN 상태 유지 시간 (초)
    half_open_max_calls: int = 3  # HALF_OPEN에서 허용할 최대 호출 수
    sliding_window_seconds: int = 300  # 슬라이딩 윈도우 시간 (5분)


class CircuitBreakerOpenError(Exception):
    """서킷브레이커가 OPEN 상태일 때 발생하는 예외"""

    pass


class RedisCircuitBreaker:
    """Redis 기반 분산 서킷브레이커

    Example:
        >>> breaker = RedisCircuitBreaker("upbit/kr/ticker")
        >>> await breaker.start()
        >>>
        >>> # 요청 전 체크
        >>> if not await breaker.is_request_allowed():
        >>>     raise CircuitBreakerOpenError("Circuit is OPEN")
        >>>
        >>> try:
        >>>     result = await some_operation()
        >>>     await breaker.record_success()
        >>> except Exception as e:
        >>>     await breaker.record_failure()
        >>>     raise
    """

    KEY_PREFIX: Final[str] = "cb"  # Circuit Breaker
    STATE_TTL: Final[int] = 3600  # 1시간 (상태 자동 만료)
    FAILURE_WINDOW_SUFFIX: Final[str] = ":failures"  # 슬라이딩 윈도우용 Sorted Set

    def __init__(
        self,
        resource_key: str,
        config: CircuitBreakerConfig | None = None,
        redis_client: Redis | None = None,
    ):
        """
        Args:
            resource_key: 서킷브레이커 식별 키 (예: "upbit/kr/ticker")
            config: 서킷브레이커 설정
            redis_client: Redis 클라이언트 (없으면 새로 생성)
        """
        self.resource_key = resource_key
        self.config = config or CircuitBreakerConfig()
        self._redis: Redis | None = redis_client
        self._redis_key = f"{self.KEY_PREFIX}:{resource_key}"
        self._failure_window_key = f"{self._redis_key}{self.FAILURE_WINDOW_SUFFIX}"

    async def start(self) -> None:
        """Redis 클라이언트 초기화 (RedisConnectionManager 사용)"""
        if self._redis is None:
            # 싱글톤 RedisConnectionManager 사용
            manager = RedisConnectionManager.get_instance()
            await manager.initialize()
            self._redis = manager.client
            logger.debug(f"Circuit breaker started for {self.resource_key}")

    async def stop(self) -> None:
        """Redis 클라이언트 참조 해제

        Note: RedisConnectionManager는 싱글톤이므로 직접 close 하지 않음.
              애플리케이션 종료 시 manager.close()로 일괄 종료.
        """
        if self._redis:
            self._redis = None
            logger.debug(f"Circuit breaker stopped for {self.resource_key}")

    async def is_request_allowed(self) -> bool:
        """요청 허용 여부 확인

        Returns:
            True: 요청 허용, False: 요청 차단
        """
        state_data = await self._get_state()

        # CLOSED: 모든 요청 허용
        if state_data["state"] == CircuitState.CLOSED:
            return True

        # OPEN: 타임아웃 체크 후 HALF_OPEN 전환 여부 확인
        if state_data["state"] == CircuitState.OPEN:
            elapsed = time.time() - state_data["opened_at"]
            if elapsed >= self.config.timeout_seconds:
                # HALF_OPEN으로 전환
                await self._transition_to_half_open()
                logger.info(
                    f"Circuit {self.resource_key}: OPEN → HALF_OPEN "
                    f"(timeout: {elapsed:.1f}s)"
                )
                return True  # HALF_OPEN으로 전환 후 요청 허용
            else:
                logger.warning(
                    f"Circuit {self.resource_key}: Request blocked (OPEN state, "
                    f"remaining: {self.config.timeout_seconds - elapsed:.1f}s)"
                )
                return False

        # HALF_OPEN: 제한된 요청만 허용
        if state_data["state"] == CircuitState.HALF_OPEN:
            if state_data["half_open_calls"] < self.config.half_open_max_calls:
                # 호출 카운트 증가
                await self._increment_half_open_calls()
                return True
            else:
                logger.warning(
                    f"Circuit {self.resource_key}: Request blocked "
                    f"(HALF_OPEN max calls reached)"
                )
                return False

        return False

    async def record_failure(self) -> None:
        """실패 기록 및 상태 전환 체크 (슬라이딩 윈도우 기반)"""
        state_data = await self._get_state()
        current_state = state_data["state"]

        if current_state == CircuitState.OPEN:
            # 이미 OPEN 상태면 무시
            return

        if current_state == CircuitState.HALF_OPEN:
            # HALF_OPEN에서 실패 → 즉시 OPEN으로 전환
            await self._transition_to_open()
            logger.warning(
                f"Circuit {self.resource_key}: HALF_OPEN → OPEN "
                f"(test request failed)"
            )
            return

        # CLOSED 상태: 슬라이딩 윈도우 기반 실패 기록
        if not self._redis:
            await self.start()

        current_time = time.time()

        # 1. 현재 실패를 Sorted Set에 추가 (score=timestamp, member=timestamp)
        await self._redis.zadd(  # type: ignore
            self._failure_window_key, {str(current_time): current_time}
        )

        # 2. 윈도우 밖 오래된 실패 제거
        await self._cleanup_old_failures()

        # 3. 윈도우 내 실패 카운트 조회
        failure_count = await self._get_failure_count_in_window()

        # 4. TTL 설정 (윈도우 크기 + 버퍼 60초)
        await self._redis.expire(  # type: ignore
            self._failure_window_key, self.config.sliding_window_seconds + 60
        )

        # 5. 임계값 초과 시 OPEN으로 전환
        if failure_count >= self.config.failure_threshold:
            await self._transition_to_open()
            logger.error(
                f"Circuit {self.resource_key}: CLOSED → OPEN "
                f"(failures: {failure_count}/{self.config.failure_threshold} "
                f"in {self.config.sliding_window_seconds}s window)"
            )

    async def record_success(self) -> None:
        """성공 기록 및 상태 전환 체크 (슬라이딩 윈도우 기반)"""
        state_data = await self._get_state()
        current_state = state_data["state"]

        if current_state == CircuitState.CLOSED:
            # CLOSED 상태에서 성공 → 오래된 실패만 정리 (윈도우 밖)
            await self._cleanup_old_failures()
            return

        if current_state == CircuitState.HALF_OPEN:
            # HALF_OPEN에서 성공 카운트 증가
            new_success_count = state_data["success_count"] + 1
            await self._increment_success_count()

            # 성공 임계값 도달 → CLOSED로 전환 및 윈도우 초기화
            if new_success_count >= self.config.success_threshold:
                await self._transition_to_closed()
                # 슬라이딩 윈도우 초기화 (정상 복구)
                if self._redis:
                    await self._redis.delete(self._failure_window_key)  # type: ignore
                logger.info(
                    f"Circuit {self.resource_key}: HALF_OPEN → CLOSED "
                    f"(successes: {new_success_count}/{self.config.success_threshold})"
                )

    async def get_state(self) -> CircuitState:
        """현재 상태 조회 (외부 API용)"""
        state_data = await self._get_state()
        return CircuitState(state_data["state"])

    async def force_open(self) -> None:
        """강제로 OPEN 상태로 전환 (운영 도구용)"""
        await self._transition_to_open()
        logger.warning(f"Circuit {self.resource_key}: Forced to OPEN state")

    async def force_close(self) -> None:
        """강제로 CLOSED 상태로 전환 (운영 도구용)"""
        await self._transition_to_closed()
        logger.info(f"Circuit {self.resource_key}: Forced to CLOSED state")

    # ========== Private Methods ==========

    async def _get_state(self) -> dict:
        """Redis에서 상태 조회"""
        if not self._redis:
            await self.start()

        data = await self._redis.get(self._redis_key)  # type: ignore

        if data is None:
            # 상태 없음 → CLOSED로 초기화
            return {
                "state": CircuitState.CLOSED,
                "success_count": 0,
                "opened_at": 0.0,
                "half_open_calls": 0,
            }

        return orjson.loads(data)

    async def _set_state(self, state_data: dict) -> None:
        """Redis에 상태 저장"""
        if not self._redis:
            await self.start()

        await self._redis.setex(  # type: ignore
            self._redis_key, self.STATE_TTL, orjson.dumps(state_data)
        )

    async def _transition_to_open(self) -> None:
        """OPEN 상태로 전환"""
        await self._set_state(
            {
                "state": CircuitState.OPEN,
                "success_count": 0,
                "opened_at": time.time(),
                "half_open_calls": 0,
            }
        )

    async def _transition_to_half_open(self) -> None:
        """HALF_OPEN 상태로 전환"""
        await self._set_state(
            {
                "state": CircuitState.HALF_OPEN,
                "success_count": 0,
                "opened_at": 0.0,
                "half_open_calls": 0,
            }
        )

    async def _transition_to_closed(self) -> None:
        """CLOSED 상태로 전환"""
        await self._set_state(
            {
                "state": CircuitState.CLOSED,
                "success_count": 0,
                "opened_at": 0.0,
                "half_open_calls": 0,
            }
        )

    async def _get_failure_count_in_window(self) -> int:
        """슬라이딩 윈도우 내 실패 카운트 조회

        Returns:
            윈도우 내 실패 횟수
        """
        if not self._redis:
            await self.start()

        # Sorted Set의 전체 카운트 반환 (이미 정리된 상태)
        count = await self._redis.zcard(self._failure_window_key)  # type: ignore
        return count or 0

    async def _cleanup_old_failures(self) -> None:
        """윈도우 밖 오래된 실패 제거

        현재 시간 기준 sliding_window_seconds 이전 실패 제거
        """
        if not self._redis:
            await self.start()

        current_time = time.time()
        window_start = current_time - self.config.sliding_window_seconds

        # score가 window_start보다 작은 (오래된) 항목 제거
        removed = await self._redis.zremrangebyscore(  # type: ignore
            self._failure_window_key, 0, window_start
        )

        if removed > 0:
            logger.debug(
                f"Circuit {self.resource_key}: Removed {removed} old failures "
                f"(older than {self.config.sliding_window_seconds}s)"
            )

    async def _increment_success_count(self) -> None:
        """성공 카운트 증가 (HALF_OPEN 상태용)"""
        state_data = await self._get_state()
        state_data["success_count"] = state_data.get("success_count", 0) + 1
        await self._set_state(state_data)

    async def _increment_half_open_calls(self) -> None:
        """HALF_OPEN 호출 카운트 증가"""
        state_data = await self._get_state()
        state_data["half_open_calls"] = state_data.get("half_open_calls", 0) + 1
        await self._set_state(state_data)


# ========== Factory Functions ==========


async def create_circuit_breaker(
    resource_key: str,
    config: CircuitBreakerConfig | None = None,
) -> RedisCircuitBreaker:
    """서킷브레이커 생성 및 시작

    Args:
        resource_key: 서킷브레이커 식별 키 (예: "upbit/kr/ticker")
        config: 서킷브레이커 설정

    Returns:
        시작된 RedisCircuitBreaker 인스턴스
    """
    breaker = RedisCircuitBreaker(resource_key, config)
    await breaker.start()
    return breaker
