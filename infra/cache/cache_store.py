"""웹소켓 연결 상태 캐시와 관련된 Redis 접근 계층.

구성 요소
- WebsocketConnectionCache: 공개 API. 연결 상태/심볼 세트 관리의 퍼사드.
- _ConnectionKeyBuilder: (exchange, region, request_type)에 대한 키 빌더.
- _MetaRepository: 메타 해시(HSET/HGET) CRUD.
- _SymbolsRepository: 심볼 세트(SADD/SMEMBERS/DEL) CRUD.

설계 원칙
- I/O 경계에서만 직렬화/검증 수행, 내부는 정규화된 타입 유지.
- 예외는 상위 레벨 데코레이터(@handle_exchange_exceptions)로 표준화 처리.
- TTL 일관성 유지: 메타/심볼 키 모두 동일 TTL 적용.
"""

from __future__ import annotations

import time
import uuid


from redis.asyncio import Redis

from common.logger import PipelineLogger
from config.settings import redis_settings
from infra.cache.cache_client import RedisConnectionManager
from core.dto.internal.common import ConnectionScopeDomain
from core.dto.io.cache import ConnectionMetaHashDTO
from core.dto.internal.cache import (
    ConnectionKeyBuilderDomain,
    ConnectionMetaDomain,
    WebsocketConnectionSpecDomain,
)
from core.dto.io.target import ConnectionTargetDTO

from core.types import (
    ConnectionStatus,
    CONNECTION_STATUS_CONNECTED,
    CONNECTION_STATUS_CONNECTING,
    CONNECTION_STATUS_DISCONNECTED,
    connection_status_format,
)
from core.dto.adapter.error_adapter import make_ws_error_event_from_kind
import asyncio
from infra.messaging.connect.producer_client import ErrorEventProducer

logger = PipelineLogger.get_logger("redis", "cache_store")


class _MetaRepository:
    """메타 해시 조작(상태, 타임스탬프, 연결 ID, 라벨) 기능을 제공합니다."""

    def __init__(self, redis: Redis, keys: ConnectionKeyBuilderDomain) -> None:
        self.redis = redis
        self.keys = keys

    async def get(self) -> ConnectionMetaHashDTO | None:
        """해시 키의 모든 필드를 읽고, 경계에서 1회 검증해 반환한다.

        Returns:
            ConnectionMetaHashDTO | None: 필수 필드가 유효하면 정규화된 메타, 없거나 부정합이면 None
        """
        raw = await self.redis.hgetall(self.keys.meta())
        if not raw:
            return None

        def _decode(v: str | bytes) -> str:
            return v.decode() if isinstance(v, (bytes, bytearray)) else v

        decoded: dict[str, str] = {
            (k.decode() if isinstance(k, (bytes, bytearray)) else k): _decode(v)
            for k, v in raw.items()
        }

        # I/O 경계에서 Pydantic 모델로 1회 검증 및 정규화
        model = ConnectionMetaHashDTO.model_validate(decoded)
        return model.model_dump()

    async def exists(self) -> bool:
        """해시 키가 존재하는지 확인한다."""
        return bool(await self.redis.exists(self.keys.meta()))

    async def set_initial(self, meta: ConnectionMetaDomain, ttl: int) -> None:
        """초기 메타 해시를 설정한다.

        - status/created_at/last_active/connection_id/exchange/region/request_type 저장
        - 키 TTL 설정
        """
        await self.redis.hset(self.keys.meta(), mapping=meta.to_redis_mapping())
        await self.redis.expire(self.keys.meta(), ttl)

    async def update_status(self, status: ConnectionStatus, ttl: int) -> None:
        """상태와 마지막 활성 시간을 갱신한다.

        Enum은 `.value`로 직렬화하여 저장한다.
        """
        await self.redis.hset(
            self.keys.meta(),
            mapping={"status": status.value, "last_active": int(time.time())},
        )
        await self.redis.expire(self.keys.meta(), ttl)

    async def delete(self) -> int:
        return await self.redis.delete(self.keys.meta())


class _SymbolsRepository:
    """심볼 세트(Set) 조작 기능을 제공합니다."""

    def __init__(self, redis: Redis, keys: ConnectionKeyBuilderDomain) -> None:
        self.redis = redis
        self.keys = keys

    async def get_all(self) -> list[str]:
        """Set에서 모든 멤버를 읽어온다."""
        members = await self.redis.smembers(self.keys.symbols())

        def _decode(v: str | bytes) -> str:
            return v.decode() if isinstance(v, (bytes, bytearray)) else v

        return sorted([_decode(s) for s in members])

    async def replace(self, symbols: list[str], ttl: int) -> None:
        """Set을 교체한다."""
        lua = (
            "local key = KEYS[1] "
            "local ttl = tonumber(ARGV[1]) "
            "redis.call('DEL', key) "
            "for i=2,#ARGV do redis.call('SADD', key, ARGV[i]) end "
            "if ttl and ttl > 0 then redis.call('EXPIRE', key, ttl) end "
            "return 1"
        )
        await self.redis.eval(lua, 1, self.keys.symbols(), ttl, *symbols)

    async def delete(self) -> int:
        """Set 키를 삭제한다."""
        return await self.redis.delete(self.keys.symbols())


class WebsocketConnectionCache:
    """웹소켓 연결 상태 캐시 관리(위임 구성).

    - KeyBuilder: 키 생성 책임
    - MetaRepository: 메타 해시 책임
    - SymbolsRepository: 심볼 세트 책임
    """

    def __init__(self, spec: WebsocketConnectionSpecDomain) -> None:
        """초기화

        Args:
            spec: 연결 스펙 DTO (exchange/region/request_type/symbols/exchange_name)

            keys: 키 생성 책임
            meta: 메타 해시 책임
            symbols: 심볼 세트 책임
        """
        self._redis_manager = RedisConnectionManager.get_instance()
        self.exchange = spec.scope.exchange
        self.region = spec.scope.region
        # 내부 불변성 확보를 위해 tuple로 저장 (I/O 시에만 list로 변환)
        self.symbols: tuple[str, ...] = tuple(spec.symbols)
        self.request_type = spec.scope.request_type

        keys = ConnectionKeyBuilderDomain(
            scope=ConnectionScopeDomain(
                region=self.region,
                exchange=self.exchange,
                request_type=self.request_type,
            )
        )
        self._meta = _MetaRepository(self.redis, keys)
        self._symbols = _SymbolsRepository(self.redis, keys)

    @property
    def redis(self) -> Redis:
        """Redis 클라이언트."""
        return self._redis_manager.client

    async def check_connection_exists(self) -> dict[str, str | int | list[str]] | None:
        """현재 연결 상태(메타+심볼)를 조회합니다.

        존재하지 않거나 부정합이면 None을 반환합니다.
        """
        try:
            meta = await self._meta.get()
            if not meta:
                return None
            symbols = await self._symbols.get_all()
            # 검증된 메타 + 심볼로 단순 조립
            return {
                "status": meta["status"],
                "created_at": meta["created_at"],
                "last_active": meta["last_active"],
                "connection_id": meta["connection_id"],
                "exchange": meta["exchange"],
                "region": meta["region"],
                "request_type": meta["request_type"],
                "symbols": symbols,
            }
        except Exception as e:
            await self._emit_error(e)
            raise

    async def set_connection_state(
        self,
        status: ConnectionStatus,
        scope: ConnectionScopeDomain,
        connection_id: str | None = None,
        ttl: int | None = None,
    ) -> None:
        """연결 상태를 초기화하고 심볼 세트를 등록한다.

        - 메타 해시를 생성(초기 상태/타임스탬프/ID 포함)
        - 심볼 세트 대체 및 동일 TTL 부여
        """
        try:
            actual_ttl = ttl or redis_settings.DEFAULT_TTL
            actual_conn_id = connection_id or str(uuid.uuid4())
            now = int(time.time())

            # 메타 해시 생성
            meta = ConnectionMetaDomain(
                status=status,
                connection_id=actual_conn_id,
                created_at=now,
                last_active=now,
                scope=scope,
            )
            await self._meta.set_initial(meta=meta, ttl=actual_ttl)
            await self._symbols.replace(list(self.symbols), actual_ttl)
            logger.info(
                f"연결 상태 설정: {scope.exchange}/{scope.region}/{scope.request_type}, \n"
                f"상태={connection_status_format(status)}\t심볼={len(self.symbols)}개\tID={actual_conn_id}"
            )
        except Exception as e:
            await self._emit_error(e)
            raise

    async def update_connection_state(
        self, status: ConnectionStatus, ttl: int | None = None
    ) -> bool:
        """연결 상태만 갱신하고 TTL을 동기화합니다.

        - 메타 해시의 상태/last_active/TTL 갱신
        - 심볼 키 TTL도 동일하게 맞춤
        Returns: 성공 시 True, 대상 없음 시 False
        """
        try:
            if not await self._meta.exists():
                logger.warning(
                    f"업데이트할 연결이 없음: {self.exchange}/{self.region}/{self.request_type}"
                )
                return False
            actual_ttl = ttl or redis_settings.DEFAULT_TTL
            await self._meta.update_status(status, actual_ttl)
            # 심볼 키도 동일 TTL 유지
            await self.redis.expire(
                f"ws:connection:{self.exchange}:{self.region}:{self.request_type}:symbols",
                actual_ttl,
            )
            logger.info(
                f"연결 상태 업데이트: {self.exchange}/{self.region}/{self.request_type}, 상태={connection_status_format(status)}"
            )
            return True
        except Exception as e:
            await self._emit_error(e)
            raise

    async def remove_connection(self) -> bool:
        """메타/심볼 키를 모두 삭제합니다.

        Returns: 삭제된 키가 하나라도 있으면 True, 없으면 False
        """
        try:
            deleted = await self._meta.delete() + await self._symbols.delete()
            if deleted > 0:
                logger.info(
                    f"연결 상태 삭제 성공: {self.exchange}/{self.region}/{self.request_type}"
                )
                return True
            logger.warning(
                f"삭제할 연결 정보 없음: {self.exchange}/{self.region}/{self.request_type}"
            )
            return False
        except Exception as e:
            await self._emit_error(e)
            raise

    async def replace_symbols(self, symbols: list[str], ttl: int | None = None) -> None:
        """심볼 세트를 완전히 대체하고 TTL을 재설정합니다."""
        try:
            actual_ttl = ttl or redis_settings.DEFAULT_TTL
            await self._symbols.replace(symbols, actual_ttl)
        except Exception as e:
            await self._emit_error(e)
            raise

    async def set_connected(self, ttl: int | None = None) -> bool:
        """상태를 CONNECTED로 설정합니다."""
        return await self.update_connection_state(CONNECTION_STATUS_CONNECTED, ttl)

    async def set_connecting(self, ttl: int | None = None) -> bool:
        """상태를 CONNECTING으로 설정합니다."""
        return await self.update_connection_state(CONNECTION_STATUS_CONNECTING, ttl)

    async def set_disconnected(self, ttl: int | None = None) -> bool:
        """상태를 DISCONNECTED로 설정합니다."""
        return await self.update_connection_state(CONNECTION_STATUS_DISCONNECTED, ttl)

    async def _emit_error(self, err: BaseException) -> None:
        """Redis 계층 에러를 ws.error로 발행."""
        # 종료 중이거나 러닝 루프가 없으면 에러 발행을 건너뜁니다.
        try:
            loop = asyncio.get_running_loop()
            if loop.is_closed():
                logger.warning("skip emit_error: event loop is closed")
                return
        except RuntimeError:
            logger.warning("skip emit_error: no running event loop")
            return
        target = ConnectionTargetDTO(
            exchange=self.exchange,
            region=self.region,
            request_type=self.request_type,
        )
        observed = f"{self.exchange}/{self.region}/{self.request_type}"
        await make_ws_error_event_from_kind(
            target=target,
            err=err,
            kind="redis",
            observed_key=observed,
            raw_context=None,
        )
