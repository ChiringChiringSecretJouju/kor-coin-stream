from __future__ import annotations

from typing import Any

import pytest

from core.dto.internal.cache import (
    ConnectionKeyBuilderDomain,
    WebsocketConnectionSpecDomain,
)
from core.dto.internal.common import ConnectionScopeDomain
from core.types import CONNECTION_STATUS_CONNECTED, CONNECTION_STATUS_CONNECTING
from infra.cache.cache_client import RedisConnectionManager
from infra.cache.cache_store import WebsocketConnectionCache


class FakeRedis:
    def __init__(self) -> None:
        self._hashes: dict[str, dict[str, Any]] = {}
        self._sets: dict[str, set[str]] = {}
        self._ttls: dict[str, int] = {}

    # Hash ops
    async def hgetall(self, key: str) -> dict[str, Any]:
        return self._hashes.get(key, {}).copy()

    async def hset(self, key: str, mapping: dict[str, Any]) -> None:
        self._hashes.setdefault(key, {})
        self._hashes[key].update(mapping)

    # Set ops
    async def smembers(self, key: str) -> set[str]:
        return set(self._sets.get(key, set()))

    async def eval(
        self, lua: str, numkeys: int, key: str, ttl: int, *symbols: str
    ) -> int:
        # replace semantics: DEL + SADD all + EXPIRE
        self._sets[key] = set(symbols)
        if ttl and ttl > 0:
            self._ttls[key] = int(ttl)
        return 1

    # Common ops
    async def expire(self, key: str, ttl: int) -> None:
        self._ttls[key] = int(ttl)

    async def exists(self, key: str) -> int:
        return int(key in self._hashes or key in self._sets)

    async def delete(self, key: str) -> int:
        existed = 0
        if key in self._hashes:
            del self._hashes[key]
            existed = 1
        if key in self._sets:
            del self._sets[key]
            existed = 1
        if key in self._ttls:
            del self._ttls[key]
        return existed

    # Testing helper
    async def ttl(self, key: str) -> int | None:
        return self._ttls.get(key)


class _FakeRedisManager:
    def __init__(self, client: FakeRedis) -> None:
        self._client = client

    @property
    def client(self) -> FakeRedis:
        return self._client


@pytest.mark.asyncio
async def test_ttl_synced_between_meta_and_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    fake = FakeRedis()

    # Patch RedisConnectionManager.get_instance to return our fake manager
    monkeypatch.setattr(
        RedisConnectionManager, "get_instance", lambda: _FakeRedisManager(fake)
    )

    scope = ConnectionScopeDomain(
        region="korea", exchange="korbit", request_type="ticker"
    )
    spec = WebsocketConnectionSpecDomain(scope=scope, symbols=("BTC",))
    cache = WebsocketConnectionCache(spec)

    keys = ConnectionKeyBuilderDomain(scope=scope)

    # Act: set and then update with a new TTL
    await cache.set_connection_state(
        status=CONNECTION_STATUS_CONNECTING, scope=scope, connection_id="abc", ttl=7
    )
    await cache.update_connection_state(CONNECTION_STATUS_CONNECTED, ttl=11)

    # Assert: TTLs are equal and match the last update TTL
    meta_ttl = await fake.ttl(keys.meta())
    symbols_ttl = await fake.ttl(keys.symbols())
    assert meta_ttl == symbols_ttl == 11


@pytest.mark.asyncio
async def test_check_connection_exists_returns_none_on_missing_or_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    fake = FakeRedis()
    monkeypatch.setattr(
        RedisConnectionManager, "get_instance", lambda: _FakeRedisManager(fake)
    )

    scope = ConnectionScopeDomain(
        region="korea", exchange="korbit", request_type="ticker"
    )
    spec = WebsocketConnectionSpecDomain(scope=scope, symbols=("BTC",))
    cache = WebsocketConnectionCache(spec)

    # Case 1: missing keys
    res1 = await cache.check_connection_exists()
    assert res1 is None

    # Case 2: invalid meta (status invalid) -> validation should fail -> None
    keys = ConnectionKeyBuilderDomain(scope=scope)
    await fake.hset(
        keys.meta(),
        {  # minimal but invalid: status not in allowed values
            "status": "BAD",
            "created_at": 0,
            "last_active": 0,
            "connection_id": "x",
            "exchange": scope.exchange,
            "region": scope.region,
            "request_type": scope.request_type,
        },
    )
    res2 = await cache.check_connection_exists()
    assert res2 is None
