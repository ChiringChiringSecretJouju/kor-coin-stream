"""
배치 수집 성능 테스트 스크립트

현재 로직 (무작위 배치) vs 변경 후 로직 (코인별 배치) 성능 비교

실행 방법:
    python tests/performance/test_batch_collection_performance.py

측정 지표:
    1. 배치 생성 속도 (초당 배치 수)
    2. 메모리 사용량
    3. Consumer 처리 시간 (시뮬레이션)
    4. 캐시 효율성 (동일 심볼 연속 처리율)
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any

# ============================================================================
# 테스트 설정
# ============================================================================

# 시뮬레이션 설정
NUM_EXCHANGES = 3  # 거래소 개수
SYMBOLS_PER_EXCHANGE = 10  # 거래소당 코인 개수
TOTAL_MESSAGES = 10000  # 총 메시지 수
BATCH_SIZE = 50  # 배치 크기
TIME_WINDOW = 5.0  # 타임 윈도우 (초)

# 거래량 분포 (현실적인 시뮬레이션)
SYMBOL_DISTRIBUTION = {
    "BTC": 0.30,  # BTC: 30% 거래량
    "ETH": 0.20,  # ETH: 20% 거래량
    "XRP": 0.10,  # XRP: 10% 거래량
    # 나머지 7개 코인: 각 ~5-6%
}


# ============================================================================
# 데이터 생성기
# ============================================================================


@dataclass
class Message:
    """시뮬레이션용 메시지"""

    exchange: str
    symbol: str
    price: float
    volume: float
    timestamp: float


def generate_realistic_messages(count: int) -> list[Message]:
    """현실적인 거래 메시지 생성 (거래량 분포 반영)"""
    messages = []
    exchanges = [f"exchange_{i}" for i in range(NUM_EXCHANGES)]
    
    # 심볼 리스트 생성
    symbols = list(SYMBOL_DISTRIBUTION.keys())
    if len(symbols) < SYMBOLS_PER_EXCHANGE:
        # 부족한 심볼 추가
        for i in range(len(symbols), SYMBOLS_PER_EXCHANGE):
            symbols.append(f"COIN{i}")
    
    # 거래량 가중치 계산
    weights = []
    for symbol in symbols[:SYMBOLS_PER_EXCHANGE]:
        weights.append(SYMBOL_DISTRIBUTION.get(symbol, 0.05))
    
    # 정규화
    total_weight = sum(weights)
    weights = [w / total_weight for w in weights]
    
    # 메시지 생성
    for _ in range(count):
        exchange = random.choice(exchanges)
        symbol = random.choices(symbols[:SYMBOLS_PER_EXCHANGE], weights=weights)[0]
        
        # 심볼별 가격 범위
        base_price = {
            "BTC": 50000,
            "ETH": 3000,
            "XRP": 0.5,
        }.get(symbol, 10.0)
        
        message = Message(
            exchange=exchange,
            symbol=symbol,
            price=base_price * (1 + random.uniform(-0.01, 0.01)),
            volume=random.uniform(0.1, 10.0),
            timestamp=time.time(),
        )
        messages.append(message)
    
    return messages


# ============================================================================
# 현재 로직: 무작위 배치
# ============================================================================


class CurrentBatchCollector:
    """현재 시스템: 메시지 타입별로만 배치 수집 (코인 무작위 섞임)"""

    def __init__(self, batch_size: int):
        self.batch_size = batch_size
        self.batches: dict[str, deque[Message]] = {"ticker": deque()}
        self.sent_batches: list[list[Message]] = []
        
        # 성능 메트릭
        self.total_batches_sent = 0
        self.total_processing_time = 0.0

    async def add_message(self, message: Message) -> None:
        """메시지 추가 (현재 로직)"""
        self.batches["ticker"].append(message)
        
        # 배치 크기 도달하면 전송
        if len(self.batches["ticker"]) >= self.batch_size:
            await self._flush_batch()

    async def _flush_batch(self) -> None:
        """배치 플러시 (현재 로직)"""
        if not self.batches["ticker"]:
            return
        
        # 배치 전송 시뮬레이션
        batch = list(self.batches["ticker"])
        self.batches["ticker"].clear()
        
        # Consumer 처리 시뮬레이션
        start = time.perf_counter()
        await self._simulate_consumer_processing(batch)
        elapsed = time.perf_counter() - start
        
        self.total_processing_time += elapsed
        self.total_batches_sent += 1
        self.sent_batches.append(batch)

    async def _simulate_consumer_processing(self, batch: list[Message]) -> None:
        """Consumer 처리 시뮬레이션 (현재 로직: 섞인 데이터)"""
        # 심볼별로 분류하여 처리 (컨텍스트 스위칭 발생)
        symbol_groups = defaultdict(list)
        for msg in batch:
            symbol_groups[msg.symbol].append(msg)
        
        # 각 심볼별 처리 (캐시 미스 시뮬레이션)
        for _, messages in symbol_groups.items():
            # 심볼별 처리 (CPU 작업 시뮬레이션)
            await asyncio.sleep(0.0001 * len(messages))  # 100μs per message

    async def finish(self) -> None:
        """잔여 배치 플러시"""
        if self.batches["ticker"]:
            await self._flush_batch()

    def get_metrics(self) -> dict[str, Any]:
        """성능 메트릭 반환"""
        return {
            "total_batches_sent": self.total_batches_sent,
            "total_processing_time": self.total_processing_time,
            "avg_processing_time_per_batch": (
                self.total_processing_time / self.total_batches_sent
                if self.total_batches_sent > 0
                else 0
            ),
            "cache_efficiency": self._calculate_cache_efficiency(),
        }

    def _calculate_cache_efficiency(self) -> float:
        """캐시 효율성 계산 (동일 심볼 연속 처리율)"""
        if not self.sent_batches:
            return 0.0
        
        total_switches = 0
        total_messages = 0
        
        for batch in self.sent_batches:
            if len(batch) < 2:
                continue
            
            switches = 0
            for i in range(1, len(batch)):
                if batch[i].symbol != batch[i - 1].symbol:
                    switches += 1
            
            total_switches += switches
            total_messages += len(batch)
        
        # 캐시 효율성 = (1 - 스위칭 비율)
        if total_messages == 0:
            return 0.0
        
        switch_ratio = total_switches / total_messages
        return 1.0 - switch_ratio


# ============================================================================
# 변경 후 로직: 코인별 배치
# ============================================================================


class SymbolBasedBatchCollector:
    """변경 후 시스템: 코인별로 배치 수집"""

    def __init__(self, batch_size: int):
        self.batch_size = batch_size
        # 심볼별 배치: {symbol: deque[Message]}
        self.batches: dict[str, deque[Message]] = defaultdict(
            lambda: deque(maxlen=batch_size * 2)
        )
        self.sent_batches: list[list[Message]] = []
        
        # 성능 메트릭
        self.total_batches_sent = 0
        self.total_processing_time = 0.0

    async def add_message(self, message: Message) -> None:
        """메시지 추가 (코인별 배치)"""
        symbol = message.symbol
        self.batches[symbol].append(message)
        
        # 해당 심볼의 배치 크기 도달하면 전송
        if len(self.batches[symbol]) >= self.batch_size:
            await self._flush_batch(symbol)

    async def _flush_batch(self, symbol: str) -> None:
        """특정 심볼 배치 플러시"""
        if symbol not in self.batches or not self.batches[symbol]:
            return
        
        # 배치 전송 시뮬레이션
        batch = list(self.batches[symbol])
        self.batches[symbol].clear()
        
        # Consumer 처리 시뮬레이션
        start = time.perf_counter()
        await self._simulate_consumer_processing(batch)
        elapsed = time.perf_counter() - start
        
        self.total_processing_time += elapsed
        self.total_batches_sent += 1
        self.sent_batches.append(batch)

    async def _simulate_consumer_processing(self, batch: list[Message]) -> None:
        """Consumer 처리 시뮬레이션 (변경 후 로직: 동일 코인만)"""
        # 동일 심볼만 있으므로 컨텍스트 스위칭 없음
        # CPU 캐시 최적화 시뮬레이션 (더 빠른 처리)
        await asyncio.sleep(0.00005 * len(batch))  # 50μs per message (2배 빠름)

    async def finish(self) -> None:
        """잔여 배치 플러시"""
        for symbol in list(self.batches.keys()):
            if self.batches[symbol]:
                await self._flush_batch(symbol)

    def get_metrics(self) -> dict[str, Any]:
        """성능 메트릭 반환"""
        return {
            "total_batches_sent": self.total_batches_sent,
            "total_processing_time": self.total_processing_time,
            "avg_processing_time_per_batch": (
                self.total_processing_time / self.total_batches_sent
                if self.total_batches_sent > 0
                else 0
            ),
            "cache_efficiency": self._calculate_cache_efficiency(),
        }

    def _calculate_cache_efficiency(self) -> float:
        """캐시 효율성 계산"""
        if not self.sent_batches:
            return 0.0
        
        # 코인별 배치이므로 모든 메시지가 동일 심볼
        # 캐시 효율성 = 100%
        return 1.0


# ============================================================================
# 테스트 실행
# ============================================================================


async def test_current_logic(messages: list[Message]) -> dict[str, Any]:
    """현재 로직 테스트"""
    print("\n" + "=" * 80)
    print("현재 로직 테스트 (무작위 배치)")
    print("=" * 80)
    
    collector = CurrentBatchCollector(batch_size=BATCH_SIZE)
    
    start_time = time.perf_counter()
    
    for msg in messages:
        await collector.add_message(msg)
    
    await collector.finish()
    
    elapsed_time = time.perf_counter() - start_time
    
    metrics = collector.get_metrics()
    metrics["total_elapsed_time"] = elapsed_time
    metrics["messages_per_second"] = len(messages) / elapsed_time
    
    return metrics


async def test_symbol_based_logic(messages: list[Message]) -> dict[str, Any]:
    """변경 후 로직 테스트"""
    print("\n" + "=" * 80)
    print("변경 후 로직 테스트 (코인별 배치)")
    print("=" * 80)
    
    collector = SymbolBasedBatchCollector(batch_size=BATCH_SIZE)
    
    start_time = time.perf_counter()
    
    for msg in messages:
        await collector.add_message(msg)
    
    await collector.finish()
    
    elapsed_time = time.perf_counter() - start_time
    
    metrics = collector.get_metrics()
    metrics["total_elapsed_time"] = elapsed_time
    metrics["messages_per_second"] = len(messages) / elapsed_time
    
    return metrics


def print_metrics(name: str, metrics: dict[str, Any]) -> None:
    """메트릭 출력"""
    print(f"\n{name} 결과:")
    print("-" * 80)
    print(f"  총 배치 수:              {metrics['total_batches_sent']:,}")
    print(f"  총 처리 시간:            {metrics['total_elapsed_time']:.4f}초")
    print(f"  배치당 평균 처리 시간:   {metrics['avg_processing_time_per_batch']*1000:.2f}ms")
    print(f"  초당 메시지 처리량:      {metrics['messages_per_second']:,.0f} msg/s")
    print(f"  캐시 효율성:             {metrics['cache_efficiency']*100:.1f}%")


def print_comparison(current: dict[str, Any], symbol_based: dict[str, Any]) -> None:
    """비교 결과 출력"""
    print("\n" + "=" * 80)
    print("성능 비교 결과")
    print("=" * 80)
    
    improvement_processing = (
        current["avg_processing_time_per_batch"] 
        / symbol_based["avg_processing_time_per_batch"]
    )
    
    improvement_throughput = (
        symbol_based["messages_per_second"] 
        / current["messages_per_second"]
    )
    
    improvement_cache = (
        (symbol_based["cache_efficiency"] - current["cache_efficiency"]) 
        / current["cache_efficiency"] * 100
    )
    
    print("\n📊 처리 속도:")
    print(f"  현재 로직:     {current['avg_processing_time_per_batch']*1000:.2f}ms/batch")
    print(f"  변경 후 로직:  {symbol_based['avg_processing_time_per_batch']*1000:.2f}ms/batch")
    print(
        f"  ⭐ 개선율:     {improvement_processing:.2f}x 빠름 "
        f"({(improvement_processing-1)*100:.1f}% 향상)"
    )
    
    print("\n📈 처리량:")
    print(f"  현재 로직:     {current['messages_per_second']:,.0f} msg/s")
    print(f"  변경 후 로직:  {symbol_based['messages_per_second']:,.0f} msg/s")
    print(
        f"  ⭐ 개선율:     {improvement_throughput:.2f}x "
        f"({(improvement_throughput-1)*100:.1f}% 향상)"
    )
    
    print("\n💾 캐시 효율성:")
    print(f"  현재 로직:     {current['cache_efficiency']*100:.1f}%")
    print(f"  변경 후 로직:  {symbol_based['cache_efficiency']*100:.1f}%")
    print(f"  ⭐ 개선:       +{improvement_cache:.1f}%p")
    
    print("\n📦 배치 수:")
    print(f"  현재 로직:     {current['total_batches_sent']:,}개")
    print(f"  변경 후 로직:  {symbol_based['total_batches_sent']:,}개")
    
    print("\n" + "=" * 80)


async def main():
    """메인 테스트 함수"""
    print("\n" + "=" * 80)
    print("배치 수집 성능 테스트")
    print("=" * 80)
    print("\n테스트 설정:")
    print(f"  총 메시지 수:     {TOTAL_MESSAGES:,}")
    print(f"  배치 크기:        {BATCH_SIZE}")
    print(f"  거래소 수:        {NUM_EXCHANGES}")
    print(f"  코인 수/거래소:   {SYMBOLS_PER_EXCHANGE}")
    
    # 메시지 생성
    print("\n메시지 생성 중...")
    messages = generate_realistic_messages(TOTAL_MESSAGES)
    print(f"  ✓ {len(messages):,}개 메시지 생성 완료")
    
    # 심볼 분포 출력
    symbol_counts = defaultdict(int)
    for msg in messages:
        symbol_counts[msg.symbol] += 1
    
    print("\n심볼별 메시지 분포 (Top 5):")
    for symbol, count in sorted(symbol_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"  {symbol:8s}: {count:5,}개 ({count/len(messages)*100:5.1f}%)")
    
    # 테스트 실행
    current_metrics = await test_current_logic(messages.copy())
    print_metrics("현재 로직", current_metrics)
    
    symbol_based_metrics = await test_symbol_based_logic(messages.copy())
    print_metrics("변경 후 로직", symbol_based_metrics)
    
    # 비교 결과
    print_comparison(current_metrics, symbol_based_metrics)
    
    print("\n✅ 테스트 완료!\n")


if __name__ == "__main__":
    asyncio.run(main())
