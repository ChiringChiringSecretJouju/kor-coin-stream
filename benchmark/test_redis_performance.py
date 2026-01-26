"""Redis 성능 벤치마크 테스트

목표:
1. 상태 갱신 지연시간 P99 < 15ms (1,000 동시 연결)
2. Lua 스크립트 처리량 5,000+ ops/sec
3. TTL 정확성 ±1초 이내

실행:
    pytest tests/performance/test_redis_performance.py -v -s

또는:
    python tests/performance/test_redis_performance.py
"""

from __future__ import annotations

import asyncio
import statistics
import time
from dataclasses import dataclass

import pytest

from src.core.dto.internal.cache import WebsocketConnectionSpecDomain
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.types import CONNECTION_STATUS_CONNECTED, CONNECTION_STATUS_CONNECTING
from src.infra.cache.cache_client import RedisConnectionManager
from src.infra.cache.cache_store import WebsocketConnectionCache


@dataclass(slots=True)
class BenchmarkResult:
    """벤치마크 결과"""

    operation: str
    total_operations: int
    total_time: float
    ops_per_sec: float
    latencies_ms: list[float]
    p50: float
    p95: float
    p99: float
    max: float
    min: float
    avg: float


def calculate_percentiles(latencies: list[float]) -> dict[str, float]:
    """백분위수 계산"""
    if not latencies:
        return {"p50": 0, "p95": 0, "p99": 0, "max": 0, "min": 0, "avg": 0}

    sorted_lat = sorted(latencies)
    n = len(sorted_lat)

    return {
        "p50": sorted_lat[int(n * 0.50)],
        "p95": sorted_lat[int(n * 0.95)],
        "p99": sorted_lat[int(n * 0.99)],
        "max": sorted_lat[-1],
        "min": sorted_lat[0],
        "avg": statistics.mean(latencies),
    }


async def setup_redis() -> RedisConnectionManager:
    """Redis 초기화"""
    manager = RedisConnectionManager.get_instance()
    await manager.initialize()
    return manager


async def cleanup_redis(manager: RedisConnectionManager) -> None:
    """Redis 정리"""
    # 테스트 키 삭제
    client = manager.client
    keys = await client.keys("ws:*")
    if keys:
        await client.delete(*keys)


# ============================================================================
# 1. 상태 갱신 지연시간 벤치마크 (목표: P99 < 15ms)
# ============================================================================


async def benchmark_state_update_latency(
    num_connections: int = 1000,
) -> BenchmarkResult:
    """상태 갱신 지연시간 벤치마크

    Args:
        num_connections: 동시 연결 수 (기본: 1000)

    Returns:
        BenchmarkResult
    """
    print(f"\n{'='*80}")
    print("테스트 1: 상태 갱신 지연시간 (목표: P99 < 15ms)")
    print(f"{'='*80}")
    print(f"동시 연결 수: {num_connections:,}")

    manager = await setup_redis()
    latencies = []

    try:
        # 연결 생성 (사전 준비)
        caches = []
        for i in range(num_connections):
            spec = WebsocketConnectionSpecDomain(
                scope=ConnectionScopeDomain(
                    exchange=f"exchange_{i % 10}",  # 10개 거래소 순환
                    region="asia",
                    request_type="ticker",
                ),
                symbols=["BTC", "ETH"],
            )
            cache = WebsocketConnectionCache(spec)
            caches.append(cache)

            # 초기 상태 설정 (CONNECTING)
            await cache.set_connection_state(
                status=CONNECTION_STATUS_CONNECTING,
                scope=spec.scope,
                connection_id=f"conn_{i}",
                ttl=3600,
            )

        print(f"✓ {num_connections:,}개 연결 초기화 완료")

        # 상태 갱신 벤치마크 (CONNECTING → CONNECTED)
        print("⏱️  상태 갱신 중...")
        start_time = time.perf_counter()

        async def update_and_measure(cache: WebsocketConnectionCache) -> float:
            """단일 상태 갱신 및 지연시간 측정"""
            op_start = time.perf_counter()
            await cache.update_connection_state(CONNECTION_STATUS_CONNECTED, ttl=3600)
            op_end = time.perf_counter()
            return (op_end - op_start) * 1000  # ms

        # 병렬 실행
        latencies = await asyncio.gather(*[update_and_measure(c) for c in caches])

        total_time = time.perf_counter() - start_time

        # 통계 계산
        percentiles = calculate_percentiles(latencies)
        ops_per_sec = num_connections / total_time

        result = BenchmarkResult(
            operation="state_update",
            total_operations=num_connections,
            total_time=total_time,
            ops_per_sec=ops_per_sec,
            latencies_ms=latencies,
            **percentiles,
        )

        # 결과 출력
        print(f"\n{'─'*80}")
        print("📊 결과:")
        print(f"{'─'*80}")
        print(f"총 연산 수:      {result.total_operations:,}")
        print(f"총 소요 시간:    {result.total_time:.2f}초")
        print(f"처리량:          {result.ops_per_sec:,.0f} ops/sec")
        print("\n지연시간 (ms):")
        print(f"  P50:           {result.p50:.2f} ms")
        print(f"  P95:           {result.p95:.2f} ms")
        print(
            f"  P99:           {result.p99:.2f} ms "
            f"{'✅' if result.p99 < 15 else '❌ (목표: < 15ms)'}"
        )
        print(f"  Max:           {result.max:.2f} ms")
        print(f"  Min:           {result.min:.2f} ms")
        print(f"  Avg:           {result.avg:.2f} ms")

        # 목표 달성 여부
        if result.p99 < 15:
            print(f"\n✅ 목표 달성: P99 {result.p99:.2f}ms < 15ms")
        else:
            print(f"\n⚠️  목표 미달: P99 {result.p99:.2f}ms >= 15ms")

        return result

    finally:
        await cleanup_redis(manager)


# ============================================================================
# 2. Lua 스크립트 처리량 벤치마크 (목표: 5,000+ ops/sec)
# ============================================================================


async def benchmark_lua_script_concurrent(
    num_operations: int = 10000, concurrency: int = 100
) -> BenchmarkResult:
    """Lua 스크립트 실제 처리량 벤치마크 (병렬 실행)

    Args:
        num_operations: 연산 횟수 (기본: 10,000)
        concurrency: 동시 실행 수 (기본: 100)

    Returns:
        BenchmarkResult
    """
    print(f"\n{'='*80}")
    print("테스트 2-B: Lua 스크립트 실제 처리량 (병렬 실행, 목표: 5,000+ ops/sec)")
    print(f"{'='*80}")
    print(f"연산 횟수: {num_operations:,}")
    print(f"동시 실행 수: {concurrency}")

    manager = await setup_redis()
    latencies = []

    try:
        # 여러 캐시 인스턴스 생성 (동시성 테스트)
        caches = []
        num_caches = min(concurrency // 10, 10)  # 최대 10개
        for i in range(num_caches):
            spec = WebsocketConnectionSpecDomain(
                scope=ConnectionScopeDomain(
                    exchange=f"exchange_{i}",
                    region="asia",
                    request_type="ticker",
                ),
                symbols=["BTC", "ETH", "XRP"],
            )
            cache = WebsocketConnectionCache(spec)
            await cache.set_connection_state(
                status=CONNECTION_STATUS_CONNECTED,
                scope=spec.scope,
                connection_id=f"test_conn_{i}",
                ttl=3600,
            )
            caches.append(cache)

        print(f"✓ {len(caches)}개 캐시 인스턴스 준비 완료")
        print(f"⏱️  병렬 실행 중... (동시 {concurrency}개)\n")

        # 벤치마크 시작
        start_time = time.perf_counter()

        async def replace_and_measure(idx: int) -> float:
            """심볼 교체 및 지연시간 측정"""
            cache = caches[idx % len(caches)]
            symbols = [f"COIN{j}" for j in range(5)]

            op_start = time.perf_counter()
            await cache.replace_symbols(symbols, ttl=3600)
            op_end = time.perf_counter()
            return (op_end - op_start) * 1000  # ms

        # 세마포어로 동시성 제어
        semaphore = asyncio.Semaphore(concurrency)

        async def bounded_replace(idx: int) -> float:
            async with semaphore:
                return await replace_and_measure(idx)

        # 모든 연산을 비동기로 시작
        tasks = [bounded_replace(i) for i in range(num_operations)]

        # 진행률 추적
        completed = 0
        for coro in asyncio.as_completed(tasks):
            latency = await coro
            latencies.append(latency)
            completed += 1

            if completed % 1000 == 0:
                progress = (completed / num_operations) * 100
                elapsed = time.perf_counter() - start_time
                current_ops = completed / elapsed
                print(
                    f"  진행률: {progress:.0f}% ({completed:,}/{num_operations:,}) - "
                    f"현재 처리량: {current_ops:,.0f} ops/sec"
                )

        total_time = time.perf_counter() - start_time

        # 통계 계산
        percentiles = calculate_percentiles(latencies)
        ops_per_sec = num_operations / total_time

        result = BenchmarkResult(
            operation="lua_script_concurrent",
            total_operations=num_operations,
            total_time=total_time,
            ops_per_sec=ops_per_sec,
            latencies_ms=latencies,
            **percentiles,
        )

        # 결과 출력
        print(f"\n{'─'*80}")
        print("📊 결과 (병렬):")
        print(f"{'─'*80}")
        print(f"총 연산 수:      {result.total_operations:,}")
        print(f"총 소요 시간:    {result.total_time:.2f}초")
        print(f"동시 실행 수:    {concurrency}")
        print(
            f"처리량:          {result.ops_per_sec:,.0f} ops/sec "
        f"{'✅' if result.ops_per_sec >= 5000 else '❌ (목표: >= 5,000)'}"
        )
        print("\n지연시간 (ms):")
        print(f"  P50:           {result.p50:.2f} ms")
        print(f"  P95:           {result.p95:.2f} ms")
        print(f"  P99:           {result.p99:.2f} ms")
        print(f"  Max:           {result.max:.2f} ms")
        print(f"  Min:           {result.min:.2f} ms")
        print(f"  Avg:           {result.avg:.2f} ms")

        # 목표 달성 여부
        if result.ops_per_sec >= 5000:
            print(f"\n✅ 목표 달성: {result.ops_per_sec:,.0f} ops/sec >= 5,000")
            print("💡 병렬 실행으로 실제 처리량 검증 완료")
        else:
            print(f"\n⚠️  목표 미달: {result.ops_per_sec:,.0f} ops/sec < 5,000")
            print(
                f"💡 동시 실행 수를 늘리면 개선 가능: concurrency={concurrency*2} 권장"
            )

        # 정리
        for cache in caches:
            await cache.remove_connection()

        return result

    finally:
        await cleanup_redis(manager)


async def benchmark_lua_script_throughput(
    num_operations: int = 10000,
) -> BenchmarkResult:
    """Lua 스크립트 처리량 벤치마크

    Args:
        num_operations: 연산 횟수 (기본: 10,000)

    Returns:
        BenchmarkResult
    """
    print(f"\n{'='*80}")
    print("테스트 2: Lua 스크립트 처리량 (목표: 5,000+ ops/sec)")
    print(f"{'='*80}")
    print(f"연산 횟수: {num_operations:,}")

    manager = await setup_redis()
    latencies = []

    try:
        # 캐시 인스턴스 생성
        spec = WebsocketConnectionSpecDomain(
            scope=ConnectionScopeDomain(
                exchange="binance",
                region="asia",
                request_type="ticker",
            ),
            symbols=["BTC", "ETH", "XRP", "ADA", "SOL"],  # 5개 심볼
        )
        cache = WebsocketConnectionCache(spec)

        # 초기 상태 설정
        await cache.set_connection_state(
            status=CONNECTION_STATUS_CONNECTED,
            scope=spec.scope,
            connection_id="test_conn",
            ttl=3600,
        )

        print("✓ 초기 상태 설정 완료")
        print("⏱️  Lua 스크립트 실행 중...")

        # 벤치마크 시작
        start_time = time.perf_counter()

        async def replace_symbols_and_measure() -> float:
            """심볼 교체 및 지연시간 측정"""
            # 매번 다른 심볼로 교체 (실제 사용 시나리오)
            symbols = [f"COIN{i}" for i in range(5)]

            op_start = time.perf_counter()
            await cache.replace_symbols(symbols, ttl=3600)
            op_end = time.perf_counter()
            return (op_end - op_start) * 1000  # ms

        # 병렬 실행 (배치 단위로)
        batch_size = 100
        for batch_start in range(0, num_operations, batch_size):
            batch_end = min(batch_start + batch_size, num_operations)
            batch_latencies = await asyncio.gather(
                *[replace_symbols_and_measure() for _ in range(batch_end - batch_start)]
            )
            latencies.extend(batch_latencies)

            # 진행률 출력
            if (batch_end % 1000) == 0:
                progress = (batch_end / num_operations) * 100
                print(f"  진행률: {progress:.0f}% ({batch_end:,}/{num_operations:,})")

        total_time = time.perf_counter() - start_time

        # 통계 계산
        percentiles = calculate_percentiles(latencies)
        ops_per_sec = num_operations / total_time

        result = BenchmarkResult(
            operation="lua_script_replace",
            total_operations=num_operations,
            total_time=total_time,
            ops_per_sec=ops_per_sec,
            latencies_ms=latencies,
            **percentiles,
        )

        # 결과 출력
        print(f"\n{'─'*80}")
        print("📊 결과:")
        print(f"{'─'*80}")
        print(f"총 연산 수:      {result.total_operations:,}")
        print(f"총 소요 시간:    {result.total_time:.2f}초")
        print(
            f"처리량:          {result.ops_per_sec:,.0f} ops/sec "
        f"{'✅' if result.ops_per_sec >= 5000 else '❌ (목표: >= 5,000)'}"
        )
        print("\n지연시간 (ms):")
        print(f"  P50:           {result.p50:.2f} ms")
        print(f"  P95:           {result.p95:.2f} ms")
        print(f"  P99:           {result.p99:.2f} ms")
        print(f"  Max:           {result.max:.2f} ms")
        print(f"  Min:           {result.min:.2f} ms")
        print(f"  Avg:           {result.avg:.2f} ms")

        # 목표 달성 여부
        if result.ops_per_sec >= 5000:
            print(f"\n✅ 목표 달성: {result.ops_per_sec:,.0f} ops/sec >= 5,000")
        else:
            print(f"\n⚠️  목표 미달: {result.ops_per_sec:,.0f} ops/sec < 5,000")

        return result

    finally:
        await cleanup_redis(manager)


# ============================================================================
# 3. TTL 정확성 검증 (목표: ±1초 이내)
# ============================================================================


async def benchmark_ttl_accuracy(num_samples: int = 100) -> dict:
    """TTL 정확성 검증

    Args:
        num_samples: 샘플 수 (기본: 100)

    Returns:
        dict: 통계 정보
    """
    print(f"\n{'='*80}")
    print("테스트 3: TTL 정확성 (목표: ±1초 이내)")
    print(f"{'='*80}")
    print(f"샘플 수: {num_samples}")
    print("테스트 TTL: 5초 (빠른 테스트)")

    manager = await setup_redis()
    ttl_errors = []

    try:
        test_ttl = 5  # 5초 TTL (빠른 테스트)

        for i in range(num_samples):
            spec = WebsocketConnectionSpecDomain(
                scope=ConnectionScopeDomain(
                    exchange=f"exchange_{i}",
                    region="asia",
                    request_type="ticker",
                ),
                symbols=["BTC"],
            )
            cache = WebsocketConnectionCache(spec)

            # 상태 설정 (TTL 5초)

            await cache.set_connection_state(
                status=CONNECTION_STATUS_CONNECTED,
                scope=spec.scope,
                connection_id=f"conn_{i}",
                ttl=test_ttl,
            )

            # TTL 확인 (즉시)
            client = manager.client
            meta_key = cache._keys.meta()
            actual_ttl = await client.ttl(meta_key)

            # 오차 계산
            expected_ttl = test_ttl
            error = abs(actual_ttl - expected_ttl)
            ttl_errors.append(error)

            # 샘플 출력 (처음 10개)
            if i < 10:
                print(
                    f"  샘플 {i+1}: 예상={expected_ttl}s, 실제={actual_ttl}s, 오차={error}s"
                )

        # 통계 계산
        avg_error = statistics.mean(ttl_errors)
        max_error = max(ttl_errors)
        min_error = min(ttl_errors)
        within_1s = sum(1 for e in ttl_errors if e <= 1.0)
        within_1s_pct = (within_1s / num_samples) * 100

        result = {
            "num_samples": num_samples,
            "avg_error": avg_error,
            "max_error": max_error,
            "min_error": min_error,
            "within_1s": within_1s,
            "within_1s_pct": within_1s_pct,
        }

        # 결과 출력
        print(f"\n{'─'*80}")
        print("📊 결과:")
        print(f"{'─'*80}")
        print(f"샘플 수:         {result['num_samples']}")
        print(f"평균 오차:       {result['avg_error']:.3f}초")
        print(f"최대 오차:       {result['max_error']:.3f}초")
        print(f"최소 오차:       {result['min_error']:.3f}초")
        print(
            f"±1초 이내 비율:  {result['within_1s_pct']:.1f}% ({result['within_1s']}/{num_samples})"
        )

        # 목표 달성 여부
        if result["within_1s_pct"] >= 99.0:
            print(f"\n✅ 목표 달성: {result['within_1s_pct']:.1f}% >= 99%")
        else:
            print(f"\n⚠️  목표 미달: {result['within_1s_pct']:.1f}% < 99%")
        print("\n💡 참고: Redis TTL은 초 단위 정확도 (±1초 오차 정상)")

        return result

    finally:
        await cleanup_redis(manager)


# ============================================================================
# 4. 종합 성능 리포트
# ============================================================================


async def run_all_benchmarks():
    """모든 벤치마크 실행 및 종합 리포트"""
    print("\n" + "=" * 80)
    print("Redis 성능 벤치마크 시작")
    print("=" * 80)

    results = {}

    # 1. 상태 갱신 지연시간
    try:
        results["state_update"] = await benchmark_state_update_latency(
            num_connections=1000
        )
    except Exception as e:
        print(f"\n❌ 테스트 1 실패: {e}")
        results["state_update"] = None

    # 2. Lua 스크립트 처리량 (병렬 실행으로 실제 측정)
    try:
        results["lua_script"] = await benchmark_lua_script_concurrent(
            num_operations=10000, concurrency=100
        )
    except Exception as e:
        print(f"\n❌ 테스트 2 실패: {e}")
        results["lua_script"] = None

    # 3. TTL 정확성
    try:
        results["ttl_accuracy"] = await benchmark_ttl_accuracy(num_samples=100)
    except Exception as e:
        print(f"\n❌ 테스트 3 실패: {e}")
        results["ttl_accuracy"] = None

    # 종합 리포트
    print(f"\n{'='*80}")
    print("📋 종합 리포트")
    print(f"{'='*80}")

    # 테스트 1: 상태 갱신 지연시간
    if results["state_update"]:
        r = results["state_update"]
        status = "✅ PASS" if r.p99 < 15 else "❌ FAIL"
        print(f"\n1️⃣  상태 갱신 지연시간: {status}")
        print(f"   - P99: {r.p99:.2f}ms (목표: < 15ms)")
        print(f"   - 처리량: {r.ops_per_sec:,.0f} ops/sec")
    else:
        print("\n1️⃣  상태 갱신 지연시간: ⚠️  실패")

    # 테스트 2: Lua 스크립트 처리량
    if results["lua_script"]:
        r = results["lua_script"]
        status = "✅ PASS" if r.ops_per_sec >= 5000 else "❌ FAIL"
        print(f"\n2️⃣  Lua 스크립트 처리량: {status}")
        print(f"   - 처리량: {r.ops_per_sec:,.0f} ops/sec (목표: >= 5,000)")
        print(f"   - P99: {r.p99:.2f}ms")
    else:
        print("\n2️⃣  Lua 스크립트 처리량: ⚠️  실패")

    # 테스트 3: TTL 정확성
    if results["ttl_accuracy"]:
        r = results["ttl_accuracy"]
        status = "✅ PASS" if r["within_1s_pct"] >= 99.0 else "❌ FAIL"
        print(f"\n3️⃣  TTL 정확성: {status}")
        print(f"   - ±1초 이내: {r['within_1s_pct']:.1f}% (목표: >= 99%)")
        print(f"   - 평균 오차: {r['avg_error']:.3f}초")
    else:
        print("\n3️⃣  TTL 정확성: ⚠️  실패")

    print(f"\n{'='*80}")
    print("✅ 벤치마크 완료")
    print(f"{'='*80}\n")

    return results


# ============================================================================
# Pytest 테스트 케이스
# ============================================================================


@pytest.mark.asyncio
async def test_state_update_latency():
    """상태 갱신 지연시간 테스트 (P99 < 15ms)"""
    result = await benchmark_state_update_latency(num_connections=1000)
    assert (
        result.p99 < 15
    ), f"P99 지연시간이 목표를 초과했습니다: {result.p99:.2f}ms >= 15ms"


@pytest.mark.asyncio
async def test_lua_script_throughput():
    """Lua 스크립트 처리량 테스트 (>= 5,000 ops/sec) - 병렬 실행"""
    result = await benchmark_lua_script_concurrent(
        num_operations=10000, concurrency=100
    )
    assert (
        result.ops_per_sec >= 5000
    ), f"처리량이 목표에 미달했습니다: {result.ops_per_sec:,.0f} < 5,000 ops/sec"


@pytest.mark.asyncio
async def test_lua_script_throughput_high_concurrency():
    """Lua 스크립트 고동시성 테스트 (동시 200개)"""
    result = await benchmark_lua_script_concurrent(num_operations=5000, concurrency=200)
    print(f"\n고동시성 결과: {result.ops_per_sec:,.0f} ops/sec")
    assert result.ops_per_sec > 0, "처리량이 0입니다"


@pytest.mark.asyncio
async def test_ttl_accuracy():
    """TTL 정확성 테스트 (±1초 이내 99%)"""
    result = await benchmark_ttl_accuracy(num_samples=100)
    assert (
        result["within_1s_pct"] >= 99.0
    ), f"TTL 정확성이 목표에 미달했습니다: {result['within_1s_pct']:.1f}% < 99%"


# ============================================================================
# 메인 실행
# ============================================================================


if __name__ == "__main__":
    asyncio.run(run_all_benchmarks())
