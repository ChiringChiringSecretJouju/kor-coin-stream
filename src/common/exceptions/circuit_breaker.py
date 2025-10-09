"""
Redis 기반 분산 서킷브레이커 구현

3-State Finite State Machine:
- CLOSED: 정상 동작 (요청 허용)
- OPEN: 장애 감지 (요청 즉시 차단)
- HALF_OPEN: 회복 테스트 (제한된 요청만 허용)

특징:
- Redis를 통한 분산 환경 상태 공유
- 시간 기반 자동 복구 (OPEN → HALF_OPEN)
- 슬라이딩 윈도우 기반 장애 감지
- TTL 기반 자동 상태 만료
"""

from __future__ import annotations

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
        """실패 기록 및 상태 전환 체크"""
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

        # CLOSED 상태: 실패 카운트 증가
        new_count = state_data["failure_count"] + 1
        await self._set_failure_count(new_count)

        # 임계값 초과 시 OPEN으로 전환
        if new_count >= self.config.failure_threshold:
            await self._transition_to_open()
            logger.error(
                f"Circuit {self.resource_key}: CLOSED → OPEN "
                f"(failures: {new_count}/{self.config.failure_threshold})"
            )

    async def record_success(self) -> None:
        """성공 기록 및 상태 전환 체크"""
        state_data = await self._get_state()
        current_state = state_data["state"]

        if current_state == CircuitState.CLOSED:
            # CLOSED 상태에서 성공 → 실패 카운트 리셋
            await self._reset_failure_count()
            return

        if current_state == CircuitState.HALF_OPEN:
            # HALF_OPEN에서 성공 카운트 증가
            new_success_count = state_data["success_count"] + 1
            await self._increment_success_count()

            # 성공 임계값 도달 → CLOSED로 전환
            if new_success_count >= self.config.success_threshold:
                await self._transition_to_closed()
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
                "failure_count": 0,
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
                "failure_count": 0,
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
                "failure_count": 0,
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
                "failure_count": 0,
                "success_count": 0,
                "opened_at": 0.0,
                "half_open_calls": 0,
            }
        )

    async def _set_failure_count(self, count: int) -> None:
        """실패 카운트 설정"""
        state_data = await self._get_state()
        state_data["failure_count"] = count
        await self._set_state(state_data)

    async def _reset_failure_count(self) -> None:
        """실패 카운트 리셋"""
        await self._set_failure_count(0)

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
