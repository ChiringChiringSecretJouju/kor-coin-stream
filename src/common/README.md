# Common 모듈

크로스커팅 관심사 및 공통 유틸리티

## 개요

**Common 모듈**은 모든 레이어에서 사용되는 공통 기능을 제공하는 Cross-Cutting Concerns 레이어입니다. 로깅, 이벤트 버스, 예외 처리, 메트릭 수집 등 시스템 전반에 걸친 횡단 관심사를 담당합니다.

### 이 모듈의 역할

1. **구조적 로깅**: JSON 기반 구조화된 로그
2. **이벤트 버스**: 내부 이벤트 발행/구독 시스템
3. **예외 처리**: Circuit Breaker, 에러 디스패처
4. **메트릭 수집**: 분 단위 배치 카운팅
5. **프로파일링**: 성능 측정 유틸리티

## 파일 구조

```
common/
├─ events.py            # Event Bus 및 이벤트 정의
├─ logger.py            # 구조적 로깅
├─ metrics.py           # 메트릭 수집
├─ profiler.py          # 성능 프로파일링
└─ exceptions/          # 예외 처리
   ├─ circuit_breaker.py      # Circuit Breaker
   ├─ error_dispatcher.py     # 에러 디스패처
   ├─ error_dto_builder.py    # 에러 DTO 빌더
   └─ exception_rule.py       # 예외 규칙
```

**파일 수**: 8개  
**총 코드 라인**: ~1,500줄  
**복잡도**: 중급

## 핵심 컴포넌트

### 1. **EventBus** (events.py)

#### 전역 이벤트 버스

```python
class EventBus:
    """
    전역 이벤트 버스 (의존성 없음)
    
    특징:
    - 완전한 비동기 처리
    - 타입 기반 핸들러 등록
    - 동시성 안전 (asyncio)
    - 순환 import 없음
    """
    
    @classmethod
    async def emit(cls, event: Any) -> None:
        """이벤트 발행"""
        
    @classmethod
    def on(cls, event_type: type, handler: Callable) -> None:
        """핸들러 등록"""
```

#### 이벤트 정의

```python
@dataclass(frozen=True, slots=True)
class ErrorEvent:
    """
    에러 이벤트 (순수 데이터)
    
    모든 레이어에서 발행 가능:
    - Infrastructure: Kafka, Redis, Avro 에러
    - Domain: WebSocket, 구독, 헬스체크 에러
    - Application: 비즈니스 로직 에러
    """
    exc: Exception
    kind: str  # "avro_deserialization", "kafka_consumer", "ws"
    target: ConnectionTargetDTO
    context: dict[str, Any] | None = None
    timestamp: datetime = field(default_factory=datetime.now)
```

#### 사용 예시

```python
# 1. 핸들러 등록 (main.py)
async def handle_error_event(event: ErrorEvent):
    await error_dispatcher.dispatch(
        exc=event.exc,
        kind=event.kind,
        target=event.target,
        context=event.context
    )

EventBus.on(ErrorEvent, handle_error_event)

# 2. 이벤트 발행 (어디서든)
await EventBus.emit(ErrorEvent(
    exc=exception,
    kind="ws_connection",
    target=target,
    context={"url": "wss://..."}
))
```

**장점:**
- 순환 import 방지
- 느슨한 결합
- 테스트 용이 (핸들러 Mock)

### 2. **PipelineLogger** (logger.py)

#### 구조적 로깅

```python
class PipelineLogger:
    """
    구조적 JSON 로깅
    
    특징:
    - JSON 포맷 (ELK Stack 호환)
    - 컨텍스트 정보 자동 추가
    - 레벨별 색상 (개발 환경)
    - 파일 로테이션 지원
    """
    
    @staticmethod
    def get_logger(name: str, category: str = "general") -> Logger:
        """로거 인스턴스 가져오기"""
```

#### 로그 포맷

```json
{
  "timestamp": "2025-01-13T14:30:00.123456+09:00",
  "level": "INFO",
  "logger": "orchestrator",
  "category": "app",
  "message": "연결 생성 성공",
  "extra": {
    "exchange": "upbit",
    "symbols": ["BTC", "ETH"],
    "correlation_id": "uuid-1234"
  }
}
```

#### 사용 예시

```python
# 1. 로거 생성
logger = PipelineLogger.get_logger("my_module", "app")

# 2. 기본 로깅
logger.info("연결 성공")

# 3. 컨텍스트 정보 추가
logger.info(
    "메시지 처리 완료",
    extra={
        "topic": "ws.command",
        "partition": 0,
        "offset": 12345
    }
)

# 4. 예외 로깅
try:
    await risky_operation()
except Exception as e:
    logger.error(
        "작업 실패",
        exc_info=True,  # 스택 트레이스 포함
        extra={"context": "additional_info"}
    )
```

**특징:**
- 상관관계 ID 자동 추적
- PII 마스킹 (개인정보 보호)
- 샘플링 (고빈도 로그)

### 3. **MinuteBatchCounter** (metrics.py)

#### 메트릭 배치 수집

```python
class MinuteBatchCounter:
    """
    분 단위 메트릭 배치 카운팅
    
    특징:
    - 심볼별 카운트 집계
    - 분 단위 버킷팅
    - 자동 배치 전송 (Kafka)
    - 메모리 효율적 (슬라이딩 윈도우)
    """
    
    def increment(self, symbol: str | None) -> None:
        """카운트 증가"""
        
    async def flush_expired_batches(self) -> None:
        """만료된 배치 전송"""
```

#### 내부 구조

```python
# 분 단위 버킷
{
    1737619200: {  # 2025-01-23 14:00:00 KST
        "total": 180,
        "details": {
            "BTC_COUNT": 120,
            "ETH_COUNT": 60
        }
    },
    1737619260: {  # 2025-01-23 14:01:00 KST
        "total": 200,
        "details": {
            "BTC_COUNT": 140,
            "ETH_COUNT": 60
        }
    }
}
```

#### 사용 예시

```python
# 1. 카운터 생성
counter = MinuteBatchCounter(
    producer=metrics_producer,
    region="korea",
    exchange="upbit",
    request_type="ticker"
)

# 2. 카운트 증가
counter.increment("BTC")
counter.increment("ETH")

# 3. 주기적 플러시 (백그라운드 태스크)
async def flush_loop():
    while True:
        await asyncio.sleep(60)
        await counter.flush_expired_batches()
```

**배치 구조:**
```python
{
    "region": "korea",
    "exchange": "upbit",
    "request_type": "ticker",
    "batch": {
        "range_start_ts_kst": 1737619200,
        "range_end_ts_kst": 1737619259,
        "bucket_size_sec": 60,
        "items": [
            {
                "minute_start_ts_kst": 1737619200,
                "total": 180,
                "details": {"BTC_COUNT": 120, "ETH_COUNT": 60}
            }
        ],
        "version": 1
    }
}
```

### 4. **CircuitBreaker** (exceptions/circuit_breaker.py)

#### 분산 Circuit Breaker (Sliding Window 패턴)

```python
class RedisCircuitBreaker:
    """
    Redis 기반 분산 Circuit Breaker
    
    상태:
    - CLOSED: 정상 (요청 통과)
    - OPEN: 차단 (즉시 실패)
    - HALF_OPEN: 복구 시도
    
    특징:
    - Redis Sorted Set 기반 슬라이딩 윈도우
    - 시간 기반 실패 추적 (기본 5분)
    - 분산 환경 상태 공유
    - 자동 복구 (OPEN → HALF_OPEN → CLOSED)
    - 오래된 실패 자동 제거
    """
```

#### 슬라이딩 윈도우 동작 원리

```
타임라인:
10:00  실패 기록 (count=1)
10:02  실패 기록 (count=2)
10:04  실패 기록 (count=3)
10:06  실패 기록
       └─ 10:00 실패 제거 (5분 윈도우 밖)
       └─ count=3 (최근 5분 내 실패만 카운트)
```

**Redis Sorted Set 활용:**
- Score: 실패 발생 타임스탬프
- Member: 실패 ID (타임스탬프 문자열)
- ZREMRANGEBYSCORE로 윈도우 밖 데이터 자동 제거
- ZCARD로 윈도우 내 실패 카운트 조회

#### 상태 전이

```
CLOSED (정상)
  ↓ (윈도우 내 실패 ≥ threshold)
OPEN (차단)
  ↓ (timeout 후)
HALF_OPEN (복구 시도)
  ├─ 성공 × 2회 → CLOSED (윈도우 초기화)
  └─ 실패 × 1회 → OPEN
```

#### 사용 예시

```python
from src.common.exceptions.circuit_breaker import (
    create_circuit_breaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError
)

# 1. Circuit Breaker 생성 (설정 커스터마이징)
config = CircuitBreakerConfig(
    failure_threshold=5,           # 5회 실패 시 OPEN
    success_threshold=2,            # 2회 성공 시 CLOSED
    timeout_seconds=60,             # 60초 후 HALF_OPEN
    half_open_max_calls=3,          # HALF_OPEN 상태 최대 호출 수
    sliding_window_seconds=300      # 5분 슬라이딩 윈도우
)

breaker = await create_circuit_breaker(
    resource_key="upbit/kr/ticker",
    config=config
)

# 2. 요청 전 체크
if not await breaker.is_request_allowed():
    raise CircuitBreakerOpenError("Circuit is OPEN")

# 3. 보호된 호출
try:
    result = await some_operation()
    await breaker.record_success()  # 성공 기록
except Exception as e:
    await breaker.record_failure()  # 실패 기록 (자동 상태 전환)
    raise
```

**ErrorDispatcher 통합 사용:**
```python
from src.common.exceptions.error_dispatcher import ErrorDispatcher

dispatcher = ErrorDispatcher()

# 자동 Circuit Breaker 처리
try:
    if not await dispatcher.is_request_allowed(target):
        raise CircuitBreakerOpenError("Circuit is OPEN")
    
    result = await some_operation()
    await dispatcher.record_success(target)
except Exception as e:
    # dispatch 내부에서 Circuit Breaker 자동 처리
    await dispatcher.dispatch(e, "operation", target)
```

**장점:**
- **정확한 장애 감지**: 시간 기반 윈도우로 오래된 실패 무시
- **분산 환경 지원**: Redis 기반 상태 공유
- **자동 복구**: 시간 경과 후 자동 재시도
- **메모리 효율**: TTL 및 자동 정리로 메모리 누수 방지
- **fail-fast**: OPEN 상태에서 즉시 요청 차단

### 5. **ErrorDispatcher** (exceptions/error_dispatcher.py)

#### 에러 처리 통합

```python
class ErrorDispatcher:
    """
    에러 디스패처 (전략 기반)
    
    책임:
    - 에러 분류 (kind)
    - 에러 DTO 생성
    - ws.error 토픽 발행
    - Circuit Breaker 통합 (미래)
    - 알람 발송 (미래)
    """
    
    async def dispatch(
        self,
        exc: Exception,
        kind: str,
        target: ConnectionTargetDTO,
        context: dict[str, Any] | None = None
    ) -> None:
        """에러 디스패치"""
```

#### 에러 분류

```python
ERROR_KINDS = [
    "ws_connection",          # WebSocket 연결 실패
    "ws_subscription",        # 구독 실패
    "ws_heartbeat_timeout",   # 하트비트 타임아웃
    "kafka_producer",         # Kafka 발행 실패
    "kafka_consumer",         # Kafka 소비 실패
    "avro_serialization",     # Avro 직렬화 실패
    "redis_connection",       # Redis 연결 실패
    "unknown"                 # 알 수 없는 에러
]
```

#### 사용 예시

```python
# ErrorDispatcher 생성
dispatcher = ErrorDispatcher(
    error_producer=error_producer
)

# 에러 디스패치
await dispatcher.dispatch(
    exc=ConnectionError("연결 실패"),
    kind="ws_connection",
    target=ConnectionTargetDTO(
        exchange="upbit",
        region="korea",
        request_type="ticker"
    ),
    context={
        "url": "wss://api.upbit.com/websocket/v1",
        "attempt": 3
    }
)
```

### 6. **Profiler** (profiler.py)

#### 성능 프로파일링

```python
class Profiler:
    """
    간단한 성능 프로파일러
    
    특징:
    - 컨텍스트 매니저
    - 메모리 사용량 측정
    - 실행 시간 측정
    """
    
# 사용 예시
with Profiler("expensive_operation"):
    await expensive_operation()
# → "expensive_operation took 1.23s, memory: +5.4MB"
```

## 설계 원칙

### 1. **의존성 없음**

```python
# ✅ Common은 다른 레이어에 의존하지 않음
common/
  → (의존 없음)

# 다른 레이어가 Common을 사용
core/ → common/
application/ → common/
infrastructure/ → common/
```

### 2. **순수 유틸리티**

```python
# ✅ 상태 없는 함수
def mask_pii(text: str) -> str:
    """개인정보 마스킹 (순수 함수)"""
    return re.sub(r'\d{3}-\d{4}-\d{4}', '***-****-****', text)

# ❌ 전역 상태 지양
# global_state = {}  # 피하기
```

### 3. **재사용성**

```python
# 모든 레이어에서 재사용 가능
from src.common.logger import PipelineLogger
from src.common.events import EventBus
from src.common.metrics import MinuteBatchCounter
```

## 사용 패턴

### 1. 구조적 로깅

```python
# 모듈별 로거
logger = PipelineLogger.get_logger(__name__, "app")

# 상관관계 ID 추적
logger.info(
    "처리 시작",
    extra={"correlation_id": ctx.correlation_id}
)

# 예외 로깅 (스택 트레이스 포함)
try:
    await operation()
except Exception as e:
    logger.error("실패", exc_info=True)
```

### 2. 이벤트 기반 에러 처리

```python
# 어디서든 에러 발행
await EventBus.emit(ErrorEvent(
    exc=error,
    kind="ws_connection",
    target=target
))

# main.py에서 중앙 처리
EventBus.on(ErrorEvent, error_dispatcher.dispatch)
```

### 3. 메트릭 수집

```python
# 핸들러에서 카운트
async def ticker_message(self, message):
    symbol = self._extract_symbol(message)
    self._counter.increment(symbol)
    
# 백그라운드에서 자동 플러시
asyncio.create_task(counter.flush_loop())
```

## 테스트

### EventBus 테스트

```python
async def test_event_bus():
    received = []
    
    async def handler(event):
        received.append(event)
    
    EventBus.on(ErrorEvent, handler)
    await EventBus.emit(ErrorEvent(...))
    
    assert len(received) == 1
    EventBus.clear()  # 정리
```

### Logger 테스트

```python
def test_logger():
    logger = PipelineLogger.get_logger("test", "test")
    
    with patch('logging.Logger.info') as mock:
        logger.info("test message")
        mock.assert_called_once()
```

## 참고 자료

- [Python logging](https://docs.python.org/3/library/logging.html)
- [Circuit Breaker Pattern](https://martinfowler.com/bliki/CircuitBreaker.html)
- [Event-Driven Architecture](https://martinfowler.com/articles/201701-event-driven.html)

## 관련 모듈

- **모든 레이어**: Common 사용
- **Main**: EventBus 리스너 등록
- **Infrastructure**: ErrorDispatcher 통합
