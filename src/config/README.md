# Config 모듈

Dependency Injection Container 및 설정 관리

## 개요

**Config 모듈**은 애플리케이션의 의존성 주입(DI)과 설정 관리를 담당합니다. Dependency Injector 라이브러리를 활용하여 모든 컴포넌트의 라이프사이클을 자동으로 관리합니다.

### 이 모듈의 역할

1. **DI Container**: 모든 의존성 자동 주입 및 관리
2. **설정 관리**: Pydantic 기반 타입 안전 설정
3. **Resource 관리**: 비동기 초기화/정리 자동화
4. **Factory 패턴**: 거래소 핸들러 동적 생성

## 파일 구조

```
config/
├─ containers.py      # DI Container 정의
└─ settings.py        # Pydantic Settings
```

**파일 수**: 2개  
**총 코드 라인**: ~700줄  
**복잡도**: 중상급

## 핵심 컴포넌트

### 1. **ApplicationContainer** (containers.py)

#### 최상위 DI Container

```python
class ApplicationContainer(containers.DeclarativeContainer):
    """
    애플리케이션 최상위 컨테이너
    
    구조:
    - InfrastructureContainer (Redis)
    - MessagingContainer (Kafka Producers)
    - HandlerContainer (Factory)
    - ApplicationContainer (Orchestrator)
    
    특징:
    - Resource 자동 관리
    - 설정 외부화 (YAML)
    - Wiring 지원 (@inject)
    """
    
    # 설정 로드
    config = providers.Configuration()
    
    # 하위 컨테이너
    infra = providers.Container(InfrastructureContainer)
    messaging = providers.Container(MessagingContainer)
    handlers = providers.Container(HandlerContainer)
```

#### 컨테이너 계층 구조

```
ApplicationContainer (최상위)
├─ InfrastructureContainer
│  ├─ redis_config (설정)
│  ├─ websocket_config (설정)
│  ├─ redis_manager (Resource)
│  └─ cache_store (Singleton)
│
├─ MessagingContainer
│  ├─ kafka_config (설정)
│  ├─ metrics_config (설정)
│  ├─ error_producer (Resource)
│  ├─ metrics_producer (Resource)
│  └─ realtime_producer (Resource)
│
├─ HandlerContainer
│  └─ factory_aggregate (Factory)
│
└─ Application
   ├─ registry (Singleton)
   ├─ error_coordinator (Singleton)
   ├─ orchestrator (Singleton)
   ├─ status_consumer (Resource)
   └─ disconnection_consumer (Resource)
```

### 2. **Resource Provider**

#### 비동기 라이프사이클 자동 관리

```python
# Producer Resource
messaging.error_producer = providers.Resource(
    init_error_producer,
    use_avro=messaging.kafka_config.provided.use_avro_for_error
)

# 자동으로:
# 1. init_error_producer() 호출 → ErrorEventProducer 생성
# 2. await producer.start_producer() 호출
# 3. 사용
# 4. await producer.stop_producer() 호출 (정리 시)
```

#### Resource 정의 예시

```python
@asynccontextmanager
async def init_error_producer(use_avro: bool):
    """
    ErrorEventProducer 초기화 및 정리
    
    DI Container가 자동으로:
    1. 진입 시: producer 생성 및 시작
    2. 종료 시: producer 정리
    """
    producer = ErrorEventProducer(use_avro=use_avro)
    await producer.start_producer()
    try:
        yield producer
    finally:
        await producer.stop_producer()
```

**장점:**
- 메모리 누수 방지
- 리소스 정리 자동화
- 예외 안전성

### 3. **Settings** (settings.py)

#### Pydantic 기반 설정

```python
class KafkaSettings(BaseSettings):
    """
    Kafka 설정 (환경변수 우선)
    
    우선순위:
    1. 환경변수 (KAFKA_BOOTSTRAP_SERVERS)
    2. .env 파일
    3. 코드 기본값
    """
    
    bootstrap_servers: str = "localhost:9092"
    compression_type: str = "zstd"
    acks: str = "1"
    retries: int = 3
    
    model_config = SettingsConfigDict(
        env_prefix="KAFKA_",
        env_file=".env",
        case_sensitive=False
    )

# 싱글톤 인스턴스
kafka_settings = KafkaSettings()
```

#### 설정 카테고리

```python
# 1. Kafka 설정
kafka_settings = KafkaSettings()

# 2. Redis 설정
redis_settings = RedisSettings()

# 3. WebSocket 설정
websocket_settings = WebsocketSettings()

# 4. 메트릭 설정
metrics_settings = MetricsSettings()
```

#### 환경변수 오버라이드

```bash
# 개발 환경 (기본값)
python main.py

# 프로덕션 (환경변수 오버라이드)
export KAFKA_BOOTSTRAP_SERVERS=prod-kafka:9092
export KAFKA_COMPRESSION_TYPE=zstd
export REDIS_HOST=prod-redis
python main.py
```

### 4. **Factory Pattern**

#### HandlerContainer - 동적 핸들러 생성

```python
class HandlerContainer(containers.DeclarativeContainer):
    """거래소 핸들러 팩토리"""
    
    factory_aggregate = providers.Singleton(
        FactoryAggregate,
        handler_map=HANDLER_MAP
    )

class FactoryAggregate:
    """
    거래소 핸들러 동적 선택
    
    사용:
    handler = factory.create(region, exchange, scope, projection)
    """
    
    def __init__(self, handler_map: dict):
        self._handler_map = handler_map
    
    def create(
        self,
        region: str,
        exchange: str,
        scope: ConnectionScopeDomain,
        projection: list[str] | None
    ):
        HandlerClass = self._handler_map[region][exchange]
        return HandlerClass(scope, projection, orchestrator)
```

## 사용 예시

### 1. 컨테이너 초기화

```python
# main.py
async def main():
    # 1. 컨테이너 생성
    container = ApplicationContainer()
    
    # 2. 설정 로드 (YAML)
    config_path = Path("config/dev.yaml")
    container.config.from_yaml(str(config_path))
    
    # 3. Resource 초기화
    await container.init_resources()
    
    # 4. 컴포넌트 사용
    orchestrator = await container.orchestrator()
    consumer = await container.status_consumer()
    
    # 5. 실행
    await consumer.run()
    
    # 6. 정리 (자동)
    await container.shutdown_resources()
```

### 2. 의존성 주입

```python
# StreamOrchestrator는 DI로 모든 의존성 주입받음
orchestrator = await container.orchestrator()

# 내부적으로:
# StreamOrchestrator(
#     error_producer=await container.messaging.error_producer(),
#     registry=await container.application.registry(),
#     error_coordinator=await container.application.error_coordinator(),
#     connector=await container.application.connector(),
#     cache=await container.application.cache_coordinator(),
#     subs=await container.application.subscription_manager()
# )
```

### 3. 설정 접근

```python
# DI Container에서
container.messaging.kafka_config().bootstrap_servers

# 직접 접근
from src.config.settings import kafka_settings
print(kafka_settings.bootstrap_servers)
```

### 4. Mock 주입 (테스트)

```python
# 테스트 시 Mock으로 교체
container = ApplicationContainer()
container.messaging.error_producer.override(MockProducer())

# 이제 모든 곳에서 MockProducer 사용!
orchestrator = await container.orchestrator()
# → orchestrator._error_producer는 MockProducer
```

## 설계 원칙

### 1. **의존성 역전 (DIP)**

```python
# ❌ 직접 생성 (강한 결합)
class StreamOrchestrator:
    def __init__(self):
        self._producer = ErrorEventProducer()  # 직접 의존
        
# ✅ DI (느슨한 결합)
class StreamOrchestrator:
    def __init__(self, error_producer: ErrorEventProducer):
        self._producer = error_producer  # 주입받음
```

### 2. **단일 책임 (SRP)**

```python
# InfrastructureContainer → 인프라만
# MessagingContainer → 메시징만
# ApplicationContainer → 애플리케이션만

# 각 컨테이너가 하나의 책임
```

### 3. **설정 외부화**

```python
# ❌ 하드코딩
bootstrap_servers = "localhost:9092"

# ✅ 환경변수/YAML
bootstrap_servers = kafka_settings.bootstrap_servers
```

## 초기화 흐름

```
main.py 시작
  ↓
ApplicationContainer 생성
  ↓
config.from_yaml("config/dev.yaml")
  ↓
init_resources()
  ├─ RedisConnectionManager.initialize()
  ├─ ErrorEventProducer.start_producer()
  ├─ MetricsProducer.start_producer()
  └─ RealtimeDataProducer.start_producer()
  ↓
orchestrator() 호출
  ├─ ConnectionRegistry (Singleton)
  ├─ ErrorCoordinator (Singleton)
  ├─ WebsocketConnector (Singleton)
  ├─ RedisCacheCoordinator (Singleton)
  └─ SubscriptionManager (Singleton)
  ↓
status_consumer() 호출
  ├─ KafkaConsumerClient
  └─ orchestrator 주입
  ↓
애플리케이션 실행
  ↓
shutdown_resources()
  ├─ stop_producer() (모든 Producer)
  └─ RedisConnectionManager.close()
```

## Provider 타입

### 1. **Singleton**

```python
# 한 번만 생성, 재사용
providers.Singleton(ConnectionRegistry)

# 항상 같은 인스턴스
registry1 = await container.application.registry()
registry2 = await container.application.registry()
assert registry1 is registry2  # True
```

### 2. **Resource**

```python
# 비동기 초기화/정리 자동
providers.Resource(
    init_error_producer,
    use_avro=True
)

# init_resources() → start_producer()
# shutdown_resources() → stop_producer()
```

### 3. **Factory**

```python
# 호출마다 새 인스턴스
providers.Factory(SomeClass)

# 매번 다른 인스턴스
obj1 = container.some_factory()
obj2 = container.some_factory()
assert obj1 is not obj2  # True
```

### 4. **Configuration**

```python
# YAML/dict 설정
config = providers.Configuration()
config.from_yaml("config.yaml")

# 접근
config.kafka.bootstrap_servers()
```

## 테스트

### Mock 주입

```python
async def test_orchestrator():
    # 1. 컨테이너 생성
    container = ApplicationContainer()
    
    # 2. Mock 주입
    mock_producer = AsyncMock()
    container.messaging.error_producer.override(mock_producer)
    
    # 3. 테스트
    orchestrator = await container.orchestrator()
    await orchestrator.connect_from_context(ctx)
    
    # 4. 검증
    mock_producer.send_error_event.assert_called()
```

### 설정 오버라이드

```python
# 테스트용 설정
container.config.from_dict({
    "kafka": {
        "bootstrap_servers": "test-kafka:9092"
    }
})
```

## 참고 자료

- [Dependency Injector 공식 문서](https://python-dependency-injector.ets-labs.org/)
- [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
- [SOLID 원칙](https://en.wikipedia.org/wiki/SOLID)

## 관련 모듈

- **Main**: Container 초기화
- **모든 모듈**: DI로 의존성 주입

## Best Practices

### 1. 레이어별 컨테이너 분리

```python
# ✅ 관심사별 분리
InfrastructureContainer  # Redis 등
MessagingContainer       # Kafka
ApplicationContainer     # 비즈니스 로직
```

### 2. Resource 적극 활용

```python
# ✅ 자동 정리가 필요한 리소스
- Kafka Producer/Consumer
- Redis Connection
- WebSocket Connection
```

### 3. 설정 계층화

```python
# ✅ 환경별 설정
config/
├─ dev.yaml      # 개발
├─ staging.yaml  # 스테이징
└─ prod.yaml     # 프로덕션
```

### 4. 타입 힌트 활용

```python
# ✅ 명확한 타입
def __init__(
    self,
    error_producer: ErrorEventProducer,  # 구체적
    registry: ConnectionRegistry
):
```

