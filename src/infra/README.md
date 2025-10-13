# Infrastructure 모듈

외부 시스템 연동 및 기술 인프라 구현

## 개요

**Infrastructure 모듈**은 외부 시스템(Kafka, Redis, Avro)과의 연동을 담당하는 레이어입니다. 도메인 로직과 외부 기술을 분리하여 시스템의 유연성과 테스트 용이성을 확보합니다.

### 이 모듈의 역할

1. **외부 시스템 연동**: Kafka, Redis, Schema Registry
2. **직렬화/역직렬화**: JSON (orjson), Avro (fastavro)
3. **메시징 인프라**: Producer/Consumer 구현
4. **캐시 관리**: Redis 기반 분산 캐시

## 파일 구조

```
infra/
├─ messaging/              # Kafka 메시징
│  ├─ clients/            # Kafka 클라이언트
│  │  ├─ cb/             # Circuit Breaker
│  │  │  ├─ base.py     # 기본 CB Producer/Consumer
│  │  │  └─ config.py   # CB 설정
│  │  ├─ json_client.py  # JSON Producer/Consumer
│  │  └─ avro_client.py  # Avro Producer/Consumer
│  │
│  ├─ connect/           # 고수준 메시징 클라이언트
│  │  ├─ producer_client.py  # 비즈니스 Producer들
│  │  ├─ consumer_client.py  # Command Consumer
│  │  ├─ disconnection_consumer.py  # Disconnect Consumer
│  │  └─ services/       # 검증 서비스
│  │     └─ command_validator.py
│  │
│  ├─ avro/              # Avro 지원
│  │  ├─ serializers.py  # Avro 직렬화/역직렬화
│  │  ├─ schema_evolution.py  # 스키마 진화
│  │  └─ utils/          # Avro 유틸리티
│  │
│  ├─ schemas/           # Avro 스키마 관리
│  │  ├─ avro/          # .avsc 파일들
│  │  └─ schema_loader.py  # 스키마 로더
│  │
│  └─ serializers/       # 직렬화
│     └─ unified_serializer.py  # 통합 직렬화기
│
└─ cache/                # Redis 캐시
   ├─ cache_client.py    # Redis 연결 관리
   └─ cache_store.py     # 캐시 저장소
```

**파일 수**: 40+ 파일  
**총 코드 라인**: ~3,000줄  
**복잡도**: 중상급

## 핵심 컴포넌트

### 1. **Kafka 클라이언트** (messaging/clients/)

#### JSON 클라이언트

```python
# AsyncProducerWrapper - JSON 기반 고성능 Producer
class AsyncProducerWrapper:
    """
    confluent-kafka 기반 JSON Producer
    
    특징:
    - orjson으로 3-5배 빠른 직렬화
    - 비동기 큐 기반 아키텍처
    - 전용 poll 스레드로 delivery report 처리
    """
    
async def send_and_wait(
    self, 
    topic: str, 
    value: dict, 
    key: str | None = None
) -> bool:
    """메시지 발행 및 응답 대기"""
```

**성능 최적화:**
- `orjson`: Rust 기반 JSON (3-5배 빠름)
- `asyncio.to_thread()`: CPU 오프로드
- 배치 처리: `batch.size=128KB`, `linger.ms=50`

#### Avro 클라이언트

```python
# AvroProducerWrapper - Avro 기반 Producer
class AvroProducerWrapper:
    """
    confluent-kafka + fastavro 기반 Avro Producer
    
    특징:
    - 메시지 크기 20-40% 감소
    - Schema Registry 통합
    - 스키마 진화 자동 처리
    """
```

**장점:**
- 메시지 크기 20-40% 감소
- 스키마 버전 관리 자동화
- 타입 안전성 보장

### 2. **비즈니스 Producer** (messaging/connect/)

#### 통합 Producer 아키텍처

```python
class AvroProducer:
    """
    통합 Producer (JSON/Avro 자동 선택)
    
    - use_avro=True: AvroProducerWrapper
    - use_avro=False: JsonProducerWrapper
    """
    
    def __init__(self, use_avro: bool = False):
        if use_avro:
            self._producer = AvroProducerWrapper(...)
        else:
            self._producer = JsonProducerWrapper(...)
```

#### 주요 Producer들

```python
# 1. ErrorEventProducer - 에러 이벤트
class ErrorEventProducer(AvroProducer):
    """
    ws.error 토픽 발행
    기본값: JSON (빠른 전송)
    """
    
# 2. MetricsProducer - 메트릭 배치
class MetricsProducer(AvroProducer):
    """
    ws.metrics 토픽 발행
    기본값: Avro (압축, 스키마 진화)
    """
    
# 3. RealtimeDataProducer - 실시간 데이터
class RealtimeDataProducer(AvroProducer):
    """
    realtime.* 토픽 발행
    기본값: Avro (대용량 데이터 압축)
    """
```

### 3. **Consumer** (messaging/connect/)

#### KafkaConsumerClient

```python
class KafkaConsumerClient:
    """
    ws.command 토픽 소비
    
    책임:
    - Kafka 메시지 소비
    - CommandDTO 검증
    - StreamOrchestrator 호출
    """
    
    async def run(self) -> None:
        """
        메인 루프:
        1. 메시지 소비
        2. DTO 검증 (Pydantic)
        3. connect_and_subscribe 액션 처리
        4. 에러 처리 (DLQ)
        """
```

**특징:**
- confluent-kafka 기반 고성능
- Pydantic 런타임 검증
- 에러 시 DLQ 자동 발행

### 4. **Avro 시스템** (messaging/avro/)

#### 직렬화/역직렬화

```python
# AvroSerializer - CPU 오프로드
class AvroSerializer:
    """
    fastavro 기반 직렬화
    
    - asyncio.to_thread()로 CPU 오프로드
    - 이벤트 루프 블로킹 방지
    """
    
    async def serialize_async(
        self, 
        message: dict[str, Any]
    ) -> bytes:
        """비동기 직렬화 (CPU 오프로드)"""
        return await asyncio.to_thread(
            self._serialize_sync, 
            message
        )
```

#### 스키마 관리

```python
# Schema Registry 클라이언트
class SchemaRegistryClient:
    """
    Confluent Schema Registry 연동
    
    - 스키마 등록
    - 버전 관리
    - 호환성 검사 (BACKWARD, FORWARD, FULL)
    """
```

**스키마 매핑:**

| 파일명 | 주제명 | 호환성 |
|--------|--------|--------|
| `connect_request.avsc` | `connect-requests-value` | BACKWARD |
| `error_event.avsc` | `error-events-value` | BACKWARD |
| `metrics_event.avsc` | `metrics-events-value` | BACKWARD |
| `ticker_data.avsc` | `ticker-data-value` | FORWARD |

### 5. **Circuit Breaker** (messaging/clients/cb/)

```python
class CircuitBreakerProducer:
    """
    Circuit Breaker 통합 Producer
    
    상태:
    - CLOSED: 정상
    - OPEN: 차단 (재시도 중단)
    - HALF_OPEN: 복구 시도
    
    Redis 기반 분산 상태 관리
    """
```

**특징:**
- 장애 격리 (fail-fast)
- 분산 환경 지원 (Redis)
- 자동 복구 시도

### 6. **Redis 캐시** (cache/)

#### RedisConnectionManager

```python
class RedisConnectionManager:
    """
    Redis 연결 풀 관리
    
    - 싱글톤 패턴
    - Connection pooling
    - 자동 재연결
    """
    
    @staticmethod
    def get_instance() -> RedisConnectionManager:
        """싱글톤 인스턴스"""
```

#### WebsocketConnectionCache

```python
class WebsocketConnectionCache:
    """
    WebSocket 연결 상태 캐시
    
    저장 데이터:
    - 연결 URL
    - 구독 심볼 목록
    - 연결 시작 시간
    - 상태 (connected/disconnected)
    """
```

## 설계 원칙

### 1. **Adapter 패턴**

```python
# Domain은 Infrastructure를 모름
# Infrastructure가 Domain을 어댑트

# ✅ 올바른 방향
Core (StreamContextDomain)
  ↓
Application (StreamOrchestrator)
  ↓
Infrastructure (KafkaProducerWrapper)
```

### 2. **성능 최적화**

```python
# CPU 집약 작업: asyncio.to_thread()
async def serialize(data):
    return await asyncio.to_thread(
        expensive_serialization, 
        data
    )

# 배치 처리
producer_config = {
    "batch.size": 128 * 1024,  # 128KB
    "linger.ms": 50,            # 50ms 대기
}
```

### 3. **에러 처리**

```python
# 레이어별 에러 처리
try:
    await producer.send(topic, message)
except KafkaError as e:
    # Infrastructure 레이어 에러
    await error_producer.send_error_event(...)
except Exception as e:
    # 알 수 없는 에러
    logger.error(f"Unexpected: {e}")
```

## 사용 예시

### 1. JSON Producer

```python
# DI Container에서 주입
producer = await container.messaging.error_producer()

# 메시지 발행
await producer.send_error_event(
    target=ConnectionTargetDTO(...),
    error=exception,
    kind="ws_connection",
    context={"url": "wss://..."}
)
```

### 2. Avro Producer

```python
# Avro 활성화
producer = MetricsProducer(use_avro=True)
await producer.start_producer()

# 스키마 기반 발행
await producer.send_counting_batch(
    region="korea",
    exchange="upbit",
    request_type="ticker",
    batch=batch_data
)
```

### 3. Consumer

```python
# Consumer 생성
consumer = KafkaConsumerClient(
    topic=["ws.command"],
    orchestrator=orchestrator
)

# 실행 (무한 루프)
await consumer.run()
```

### 4. Redis 캐시

```python
# Connection Manager
manager = RedisConnectionManager.get_instance()
await manager.initialize()

# 캐시 저장
cache = WebsocketConnectionCache(manager)
spec = WebsocketConnectionSpecDomain(...)
await cache.set_connection_spec(scope, spec)

# 캐시 조회
cached = await cache.get_connection_spec(scope)
```

## 기술 비교

### JSON vs Avro

| 항목 | JSON (orjson) | Avro (fastavro) |
|------|---------------|-----------------|
| 직렬화 속도 | Baseline | 2-3배 |
| 역직렬화 속도 | Baseline | 2-4배 |
| 메시지 크기 | Baseline | 20-40% 감소 |
| 메모리 사용 | Baseline | 30% 감소 |

## 주요 흐름

### Producer 흐름

```
Application Layer
  ↓
ErrorEventProducer.send_error_event()
  ↓
AvroProducer.produce_sending()
  ↓
AsyncProducerWrapper.send_and_wait()
  ↓
[CPU 오프로드] orjson.dumps() / fastavro.schemaless_writer()
  ↓
confluent_kafka.Producer.produce()
  ↓
Poll 스레드 (delivery report)
  ↓
asyncio.Queue (결과 전달)
  ↓
Application Layer (성공/실패 확인)
```

### Consumer 흐름

```
confluent_kafka.Consumer.poll() (별도 스레드)
  ↓
[CPU 오프로드] orjson.loads() / fastavro.schemaless_reader()
  ↓
asyncio.Queue (메시지 전달)
  ↓
KafkaConsumerClient._consume_stream()
  ↓
CommandDTO 검증 (Pydantic)
  ↓
StreamOrchestrator.connect_from_context()
```

## 테스트

### 단위 테스트

```python
# tests/infra/test_json_client.py
async def test_producer_send():
    producer = create_producer()
    await producer.start()
    
    success = await producer.send_and_wait(
        "test-topic",
        {"key": "value"}
    )
    
    assert success is True
    await producer.stop()
```

### Mock 사용

```python
# Kafka Producer Mock
class MockProducer:
    async def send_and_wait(self, topic, value, key=None):
        return True

# DI Container Override
container.messaging.error_producer.override(MockProducer())
```

## 참고 자료

- [confluent-kafka 공식 문서](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/)
- [Avro 스펙](https://avro.apache.org/docs/current/spec.html)
- [orjson 벤치마크](https://github.com/ijl/orjson)
- [Redis 공식 문서](https://redis.io/docs/)

## 관련 모듈

- **Core**: DTO 정의
- **Application**: Producer/Consumer 사용
- **Config**: DI Container 설정
