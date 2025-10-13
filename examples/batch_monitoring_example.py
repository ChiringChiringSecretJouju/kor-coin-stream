"""
배치 모니터링 시스템 사용 예제

실시간 배치 성능을 모니터링하고 Kafka로 메트릭을 전송합니다.
"""

import asyncio

from src.infra.monitoring import BatchPerformanceMonitor


async def main() -> None:
    """배치 모니터링 실행 예제"""

    # 한국 거래소 모니터링
    monitor = BatchPerformanceMonitor(
        region="korea",
        exchange="upbit",
        request_type="ticker",
        summary_interval=60,  # 60초마다 요약 전송
        redis_url="redis://localhost:6379",  # 선택적 (없으면 None)
    )

    # 모니터링 시작
    await monitor.start()

    try:
        # 5분간 모니터링
        print("배치 모니터링 시작... (5분간 실행)")
        await asyncio.sleep(300)
    finally:
        # 중지
        await monitor.stop()
        print("배치 모니터링 중지")


async def multi_monitor_example() -> None:
    """여러 거래소 동시 모니터링 예제"""

    # 여러 모니터 생성
    monitors = [
        BatchPerformanceMonitor("korea", "upbit", "ticker", 60),
        BatchPerformanceMonitor("korea", "bithumb", "ticker", 60),
        BatchPerformanceMonitor("asia", "binance", "ticker", 60),
    ]

    # 모두 시작
    for monitor in monitors:
        await monitor.start()

    try:
        # 10분간 모니터링
        print(f"{len(monitors)}개 거래소 동시 모니터링 시작...")
        await asyncio.sleep(600)
    finally:
        # 모두 중지
        for monitor in monitors:
            await monitor.stop()
        print("모든 모니터링 중지")


if __name__ == "__main__":
    # 단일 모니터링
    asyncio.run(main())

    # 또는 여러 거래소 동시 모니터링
    # asyncio.run(multi_monitor_example())
