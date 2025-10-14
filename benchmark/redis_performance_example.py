"""Redis 성능 모니터링 사용 예제

cache_store.py에 성능 모니터링을 통합하는 방법을 보여줍니다.

실행:
    python examples/redis_performance_example.py
"""

from __future__ import annotations

import asyncio

from src.core.dto.internal.cache import WebsocketConnectionSpecDomain
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.types import CONNECTION_STATUS_CONNECTED, CONNECTION_STATUS_CONNECTING
from src.infra.cache.cache_client import RedisConnectionManager
from src.infra.cache.cache_store import WebsocketConnectionCache
from src.infra.cache.performance_monitor import get_redis_monitor


async def example_with_monitoring():
    """성능 모니터링을 사용한 Redis 연산 예제"""
    print("=" * 80)
    print("Redis 성능 모니터링 예제")
    print("=" * 80)

    # Redis 초기화
    manager = RedisConnectionManager.get_instance()
    await manager.initialize()
    print("✓ Redis 연결 완료\n")

    # 성능 모니터 가져오기 (싱글톤)
    monitor = get_redis_monitor(window_size=1000, log_interval=10)
    print("✓ 성능 모니터 초기화 완료\n")

    # 캐시 인스턴스 생성
    spec = WebsocketConnectionSpecDomain(
        scope=ConnectionScopeDomain(
            exchange="binance",
            region="asia",
            request_type="ticker",
        ),
        symbols=["BTC", "ETH", "XRP"],
    )
    cache = WebsocketConnectionCache(spec)

    print("시나리오 1: 상태 설정 (모니터링 적용)")
    print("-" * 80)

    # 1. 상태 설정 (모니터링 적용)
    async with monitor.track("set_connection_state"):
        await cache.set_connection_state(
            status=CONNECTION_STATUS_CONNECTING,
            scope=spec.scope,
            connection_id="test_conn_001",
            ttl=3600,
        )
    print("✓ 상태 설정 완료")

    # 2. 상태 갱신 (모니터링 적용)
    print("\n시나리오 2: 상태 갱신 (100회 반복)")
    print("-" * 80)

    for i in range(100):
        async with monitor.track("update_connection_state"):
            await cache.update_connection_state(CONNECTION_STATUS_CONNECTED, ttl=3600)

    print(f"✓ 상태 갱신 100회 완료")

    # 3. 심볼 교체 (Lua 스크립트, 모니터링 적용)
    print("\n시나리오 3: 심볼 교체 (100회 반복)")
    print("-" * 80)

    for i in range(100):
        symbols = [f"COIN{j}" for j in range(i % 10, i % 10 + 5)]
        async with monitor.track("replace_symbols"):
            await cache.replace_symbols(symbols, ttl=3600)

    print(f"✓ 심볼 교체 100회 완료")

    # 4. 상태 조회 (모니터링 적용)
    print("\n시나리오 4: 상태 조회 (50회 반복)")
    print("-" * 80)

    for i in range(50):
        async with monitor.track("check_connection_exists"):
            result = await cache.check_connection_exists()

    print(f"✓ 상태 조회 50회 완료")

    # 5. 통계 조회 및 출력
    print("\n" + "=" * 80)
    print("📊 성능 통계")
    print("=" * 80)

    all_stats = monitor.get_all_stats()

    for op_name, stats in sorted(all_stats.items()):
        print(f"\n{op_name}:")
        print(f"  총 호출:         {stats.total_count:,}회")
        print(f"  성공:            {stats.success_count:,}회")
        print(f"  에러:            {stats.error_count}회")
        print(f"  에러율:          {stats.error_rate:.2%}")
        print(f"  처리량:          {stats.ops_per_sec:.0f} ops/sec (최근 1분)")
        print(f"  지연시간:")
        print(f"    P50:           {stats.p50:.2f} ms")
        print(f"    P95:           {stats.p95:.2f} ms")
        print(f"    P99:           {stats.p99:.2f} ms")
        print(f"    Max:           {stats.max:.2f} ms")
        print(f"    Min:           {stats.min:.2f} ms")
        print(f"    Avg:           {stats.avg:.2f} ms")

        # 목표 달성 여부
        if "update" in op_name or "set" in op_name:
            if stats.p99 < 15:
                print(f"  ✅ P99 목표 달성 ({stats.p99:.2f}ms < 15ms)")
            else:
                print(f"  ⚠️  P99 목표 미달 ({stats.p99:.2f}ms >= 15ms)")

        if "replace" in op_name:
            if stats.ops_per_sec >= 100:  # 실제 환경에서는 5000+
                print(f"  ✅ 처리량 양호 ({stats.ops_per_sec:.0f} ops/sec)")
            else:
                print(f"  ⚠️  처리량 낮음 ({stats.ops_per_sec:.0f} ops/sec)")

    # 정리
    print("\n" + "=" * 80)
    print("✅ 예제 완료")
    print("=" * 80)

    # Redis 정리
    await cache.remove_connection()
    await manager.close()


async def example_error_tracking():
    """에러 추적 예제"""
    print("\n" + "=" * 80)
    print("에러 추적 예제")
    print("=" * 80)

    # Redis 초기화
    manager = RedisConnectionManager.get_instance()
    await manager.initialize()

    monitor = get_redis_monitor()

    # 의도적 에러 발생
    print("\n의도적 에러 10회 발생 중...")
    for i in range(10):
        try:
            async with monitor.track("intentional_error"):
                # 존재하지 않는 키 접근 (에러 유발)
                client = manager.client
                await client.get("nonexistent_key_" + "x" * 1000000)  # 메모리 에러 유발
        except Exception:
            pass  # 에러 무시

    # 정상 연산
    print("정상 연산 90회 실행 중...")
    for i in range(90):
        async with monitor.track("intentional_error"):
            client = manager.client
            await client.set("test_key", "test_value", ex=10)

    # 통계 확인
    stats = monitor.get_stats("intentional_error")
    if stats:
        print(f"\n📊 에러 추적 결과:")
        print(f"  총 호출:    {stats.total_count}회")
        print(f"  성공:       {stats.success_count}회")
        print(f"  에러:       {stats.error_count}회")
        print(f"  에러율:     {stats.error_rate:.2%}")

    # 정리
    await manager.close()


if __name__ == "__main__":
    # 기본 예제
    asyncio.run(example_with_monitoring())

    # 에러 추적 예제 (선택사항)
    # asyncio.run(example_error_tracking())
