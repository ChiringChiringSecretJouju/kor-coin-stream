# 배치 모니터링 시스템

실시간 배치 수집 성능을 모니터링하고 Kafka로 메트릭 이벤트를 전송하는 시스템입니다.

## 주요 기능

### 1. 실시간 배치 분석
- **배치 크기 추적**: 메시지 수, 처리 시간
- **심볼 다양성 분석**: 고유 심볼 수 추적
- **캐시 효율성 계산**: CPU 캐시 지역성 기반 효율성 측정

### 2. 캐시 효율성 (Cache Efficiency)

**계산 근거**: CPU 캐시 지역성(Cache Locality)
```python
cache_efficiency = 1.0 / unique_symbols
```

**의미**:
- **배치에 1개 심볼**: 효율 100% → 같은 심볼 연속 처리 → L1/L2 캐시 재사용률 ↑
- **배치에 10개 심볼**: 효율 10% → 심볼 전환 많음 → 컨텍스트 스위칭 ↑ 캐시 미스 ↑

**실제 영향**:
- 같은 심볼 연속 처리 시 메모리 접근 패턴이 예측 가능
- CPU prefetcher가 효율적으로 작동
- 직렬화/역직렬화 시 스키마 캐싱 효과
- 2-2.5배 성능 향상 가능

### 3. Redis 통합 (선택적)
- 심볼별 통계 추적
- 분산 환경에서 집계 가능
- 없어도 정상 동작

### 4. Kafka 이벤트 전송
- **토픽**: `monitoring.batch.performance`
- **키**: `{region}:{exchange}:{request_type}`
- **직렬화**: Avro

## 아키텍처

```
실시간 데이터 토픽 (ticker-data.korea)
         ↓
  BatchPerformanceMonitor
         ↓
   BatchAnalyzer (분석)
         ↓
BatchMonitoringProducer
         ↓
monitoring.batch.performance 토픽
```

## 구성 요소

### 1. BatchAnalyzer
배치 메시지를 분석하여 성능 메트릭을 계산합니다.

```python
from src.infra.monitoring import BatchAnalyzer

analyzer = BatchAnalyzer()

# Redis 사용 (선택적)
await analyzer.setup_redis("redis://localhost:6379")

# 배치 분석
result = analyzer.analyze_batch(
    batch_data=[{"target_currency": "KRW-BTC", ...}, ...],
    region="korea",
    exchange="upbit",
    request_type="ticker",
)

# result: (batch_size, unique_symbols, cache_efficiency, timestamp_ms)
```

### 2. BatchPerformanceMonitor
실시간 토픽을 구독하여 자동으로 분석 및 전송합니다.

```python
from src.infra.monitoring import BatchPerformanceMonitor

monitor = BatchPerformanceMonitor(
    region="korea",
    exchange="upbit",
    request_type="ticker",
    summary_interval=60,  # 60초마다 요약 전송
    redis_url="redis://localhost:6379",  # 선택적
)

await monitor.start()
# ... 모니터링 실행 ...
await monitor.stop()
```

### 3. BatchMonitoringProducer
모니터링 이벤트를 Kafka로 전송합니다.

```python
from src.infra.messaging.connect.producer_client import BatchMonitoringProducer

producer = BatchMonitoringProducer()
await producer.start_producer()

# 배치 이벤트 전송
await producer.send_batch_event(
    region="korea",
    exchange="upbit",
    request_type="ticker",
    batch_size=150,
    unique_symbols=25,
    cache_efficiency=0.04,
    timestamp_ms=1760031213693,
)

# 요약 이벤트 전송
await producer.send_summary_event(
    region="korea",
    exchange="upbit",
    request_type="ticker",
    total_batches=1000,
    total_messages=15000,
    avg_batch_size=15.0,
    avg_symbols_per_batch=10.5,
    avg_cache_efficiency=0.095,
    messages_per_second=250.0,
    batches_per_minute=16.7,
    elapsed_seconds=60.0,
    summary_period_seconds=60,
)

await producer.stop_producer()
```

## 메시지 구조

### 입력: 실시간 배치 메시지
```json
{
  "exchange": "bithumb",
  "region": "korea",
  "request_type": "ticker",
  "timestamp_ms": 1760031213693,
  "batch_size": 20,
  "data": [
    {
      "target_currency": "KRW-BTC",
      "timestamp": 1760031199778,
      "first": 177599000.0,
      "last": 176695000.0,
      "high": 178054000.0,
      "low": 176500000.0
    }
  ]
}
```

### 출력: 배치 수집 이벤트
```json
{
  "event_type": "batch_collected",
  "event_timestamp_utc": "2025-10-13T08:10:00.123Z",
  "region": "korea",
  "exchange": "upbit",
  "request_type": "ticker",
  "stats": {
    "batch_size": 150,
    "unique_symbols": 25,
    "cache_efficiency": 0.04,
    "collection_timestamp_ms": 1760031213693
  }
}
```

### 출력: 성능 요약 이벤트
```json
{
  "event_type": "performance_summary",
  "event_timestamp_utc": "2025-10-13T08:11:00.456Z",
  "region": "korea",
  "exchange": "upbit",
  "request_type": "ticker",
  "aggregated_stats": {
    "total_batches": 1000,
    "total_messages": 15000,
    "avg_batch_size": 15.0,
    "avg_symbols_per_batch": 10.5,
    "avg_cache_efficiency": 0.095,
    "messages_per_second": 250.0,
    "batches_per_minute": 16.7,
    "elapsed_seconds": 60.0
  },
  "summary_period_seconds": 60
}
```

## 에러 처리

모니터링 중 발생한 에러는 자동으로 `ErrorEventProducer`를 통해 전송됩니다:

```python
# BatchMonitoringProducer 내부에서 자동 처리
try:
    await producer.send_batch_event(...)
except Exception as e:
    # 자동으로 ws.error 토픽으로 에러 전송
    # error_domain="monitoring"
    # error_code="monitoring_error"
```

## 사용 예제

### 기본 사용
```python
import asyncio
from src.infra.monitoring import BatchPerformanceMonitor

async def main():
    monitor = BatchPerformanceMonitor(
        region="korea",
        exchange="upbit",
        request_type="ticker",
        summary_interval=60,
    )
    
    await monitor.start()
    await asyncio.sleep(300)  # 5분
    await monitor.stop()

asyncio.run(main())
```

### Redis 사용
```python
monitor = BatchPerformanceMonitor(
    region="korea",
    exchange="upbit",
    request_type="ticker",
    summary_interval=60,
    redis_url="redis://localhost:6379",  # Redis 활성화
)
```

### 여러 거래소 동시 모니터링
```python
monitors = [
    BatchPerformanceMonitor("korea", "upbit", "ticker", 60),
    BatchPerformanceMonitor("korea", "bithumb", "ticker", 60),
    BatchPerformanceMonitor("asia", "binance", "ticker", 60),
]

for monitor in monitors:
    await monitor.start()

# ... 실행 ...

for monitor in monitors:
    await monitor.stop()
```

## 성능 특성

- **처리량**: 1,000-10,000 msg/sec
- **오버헤드**: < 1% (비동기 처리)
- **메모리**: < 100MB per monitor
- **Redis**: 선택적, 없어도 동작

## 의존성

### 필수
- `confluent-kafka`: Kafka 클라이언트
- `orjson`: 고성능 JSON 직렬화

### 선택적
- `redis[asyncio]`: Redis 통합 (선택적)

```bash
# Redis 지원 설치
pip install redis[asyncio]
```

## 참고

- Producer는 `src/infra/messaging/connect/producer_client.py`에 위치
- Avro 직렬화 사용 (스키마는 TODO)
- 에러 시 자동으로 ErrorEventProducer 사용
