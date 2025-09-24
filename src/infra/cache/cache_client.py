from __future__ import annotations

import redis.asyncio as redis
from redis.asyncio import Redis

from src.common.logger import PipelineLogger
from src.config.settings import redis_settings

logger = PipelineLogger.get_logger("redis", "cache_client")


class RedisConnectionManager:
    """Redis connection manager (singleton, async).

    Use initialize() before accessing client.
    """

    _instance: RedisConnectionManager | None = None
    _redis: Redis | None = None

    @classmethod
    def get_instance(cls) -> RedisConnectionManager:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def initialize(self, redis_url: str | None = None) -> None:
        if self._redis is None:
            url = redis_url or redis_settings.url
            logger.info(f"Redis 연결 시도: {url}")
            self._redis = redis.from_url(
                url=url,
                socket_timeout=redis_settings.CONNECTION_TIMEOUT,
            )
            await self._redis.ping()
            logger.info("Redis 연결 성공")

    async def close(self) -> None:
        if self._redis:
            await self._redis.close()
            self._redis = None
            logger.info("Redis 연결 종료")

    @property
    def client(self) -> Redis:
        return self._redis
