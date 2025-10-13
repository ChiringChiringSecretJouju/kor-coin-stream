# Application 모듈

유스케이스 오케스트레이션 및 비즈니스 흐름 관리

## 개요

**Application 모듈**은 Core 도메인 로직을 조합하여 실제 비즈니스 유스케이스를 구현하는 레이어입니다. 여러 도메인 객체와 서비스를 조율하여 복잡한 비즈니스 흐름을 실행합니다.

### 이 모듈의 역할

1. **유스케이스 구현**: WebSocket 연결 관리, 에러 조정, 레지스트리 관리
2. **오케스트레이션**: 여러 컴포넌트를 조율하여 비즈니스 흐름 실행
3. **책임 분리**: 단일 책임 원칙에 따라 역할 분산
4. **DI 통합**: Dependency Injection Container와 연동

## 파일 구조

```
application/
├─ orchestrator.py         # 스트림 오케스트레이터 (핵심)
├─ connection_registry.py  # 연결 레지스트리
└─ error_coordinator.py    # 에러 조정자
```

**파일 수**: 3개  
**총 코드 라인**: ~900줄 (주석 포함)  
**복잡도**: 중상급

## 핵심 컴포넌트

### 1. **StreamOrchestrator** (18KB)

시스템의 핵심 오케스트레이터로, WebSocket 연결의 전체 라이프사이클을 관리합니다.

```python
class StreamOrchestrator:
    """
    리팩토링된 스트림 오케스트레이터 (DI 적용)
    
    책임:
    - 연결 라이프사이클 관리
    - 중복 연결 방지
    - 에러 조정
    - 재구독 처리
    
    DI Features:
    - 모든 의존성을 생성자에서 주입
    - 테스트 시 Mock 주입 가능
    - Resource provider가 라이프사이클 자동 관리
    """
```

#### 주요 기능

##### 1) 연결 생성
```python
async def connect_from_context(self, ctx: StreamContextDomain) -> None:
    """
    WebSocket 연결 생성
    
    흐름:
    1. 중복 연결 확인
    2. 단일 구독 거래소 처리 (심볼별 분리)
    3. 핸들러 생성
    4. 캐시 준비
    5. 연결 태스크 시작
    6. 레지스트리 등록
    """
```

##### 2) 재구독 처리
```python
async def _handle_resubscribe(self, ctx: StreamContextDomain) -> None:
    """
    중복 연결 시 재구독 처리
    
    - 기존 연결에 심볼 추가
    - 중복 심볼 병합
    - 재구독 실패 시 에러 발행
    """
```

##### 3) 연결 종료
```python
async def disconnect_from_scope(self, scope: ConnectionScopeDomain) -> None:
    """
    WebSocket 연결 종료
    
    흐름:
    1. 태스크 취소
    2. 레지스트리 제거
    3. 에러 조정자 정리
    """
```

#### 의존성 (DI로 주입)

```python
def __init__(
    self,
    error_producer: ErrorEventProducer,     # 에러 발행
    registry: ConnectionRegistry,            # 연결 관리
    error_coordinator: ErrorCoordinator,     # 에러 조정
    connector: WebsocketConnector,           # WebSocket 연결
    cache: RedisCacheCoordinator,            # 캐시 관리
    subs: SubscriptionManager,               # 구독 관리
):
```

**특징:**
- 6개 의존성을 DI Container가 자동 주입
- 테스트 시 Mock으로 교체 가능
- Resource provider가 라이프사이클 자동 관리

### 2. **ConnectionRegistry** (6.7KB)

실행 중인 WebSocket 연결(태스크 및 핸들러)을 추적 관리합니다.

```python
class ConnectionRegistry:
    """
    연결 레지스트리
    
    책임:
    - 태스크/핸들러 등록 및 제거
    - 중복 연결 확인
    - 실행 중인 연결 조회
    """
    
    def register_connection(
        self, 
        scope: ConnectionScopeDomain, 
        task: asyncio.Task, 
        handler: Any
    ) -> None:
        """연결 등록"""
        
    def is_running(self, scope: ConnectionScopeDomain) -> bool:
        """중복 연결 확인"""
        
    def get_handler(self, scope: ConnectionScopeDomain) -> Any:
        """핸들러 조회"""
```

#### 특징

- **스레드 안전**: asyncio 단일 스레드 내에서만 동작
- **메모리 효율**: slots=True로 최적화
- **타입 안전**: 완전한 타입 힌트

#### 내부 구조

```python
@dataclass(slots=True)
class _ConnectionEntry:
    """내부 전용 연결 엔트리"""
    task: asyncio.Task
    handler: Any  # ExchangeSocketHandler
    registered_at: float

# 레지스트리
self._connections: dict[ConnectionScopeDomain, _ConnectionEntry] = {}
```

### 3. **ErrorCoordinator** (6.5KB)

에러 처리를 중앙 집중화하고 표준화합니다.

```python
class ErrorCoordinator:
    """
    에러 조정자
    
    책임:
    - 에러 이벤트 표준화
    - 에러 발행 통합
    - Circuit Breaker 통합 (미래)
    """
```

#### 주요 메서드

```python
async def emit_connection_error(
    self, 
    scope: ConnectionScopeDomain,
    error: Exception, 
    phase: str
) -> None:
    """연결 에러 발행"""
    
async def emit_resubscribe_error(
    self, 
    scope: ConnectionScopeDomain,
    error: Exception,
    socket_params: SocketParams
) -> None:
    """재구독 에러 발행"""
```

#### 특징

- **중앙 집중**: 모든 에러 발행이 이곳을 거침
- **표준화**: 일관된 에러 메시지 포맷
- **확장성**: Circuit Breaker 통합 준비

## 설계 원칙

### 1. **단일 책임 (SRP)**

```python
# ❌ 이전: Orchestrator가 모든 것 담당
class StreamOrchestrator:
    # 연결 관리 + 레지스트리 + 에러 처리 + 캐시 + 구독
    # → 20KB, 복잡도 극대

# ✅ 현재: 책임 분리
class StreamOrchestrator:      # 오케스트레이션만
class ConnectionRegistry:      # 레지스트리만
class ErrorCoordinator:        # 에러 처리만
# → 각 10KB 이하, 복잡도 50% 감소
```

### 2. **의존성 역전 (DIP)**

```python
# Application → Core (O)
from src.core.dto.internal.orchestrator import StreamContextDomain

# Application → Infrastructure (X, DI로 주입)
# from src.infra.cache import RedisCache  # 절대 금지!

# 대신 DI Container에서 주입
def __init__(self, cache: RedisCacheCoordinator):
    self._cache = cache  # 인터페이스만 의존
```

### 3. **컴포지션 (Composition)**

```python
# Orchestrator는 여러 컴포넌트를 조합
class StreamOrchestrator:
    def __init__(
        self,
        registry: ConnectionRegistry,      # 조합 1
        error_coordinator: ErrorCoordinator,  # 조합 2
        connector: WebsocketConnector,     # 조합 3
        # ...
    ):
```

## 사용 예시

### 1. 연결 생성

```python
# DI Container에서 주입받은 orchestrator
orchestrator = await container.orchestrator()

# 컨텍스트 생성
context = StreamContextDomain(
    scope=ConnectionScopeDomain(
        exchange="upbit",
        region="korea",
        request_type="ticker"
    ),
    symbols=["BTC", "ETH"],
    socket_params={"subscribe_type": "ticker"}
)

# 연결 생성 (중복 자동 방지)
await orchestrator.connect_from_context(context)
```

### 2. 연결 종료

```python
# Scope 생성
scope = ConnectionScopeDomain(
    exchange="upbit",
    region="korea",
    request_type="ticker"
)

# 연결 종료
await orchestrator.disconnect_from_scope(scope)
```

### 3. 레지스트리 조회

```python
registry = ConnectionRegistry()

# 중복 확인
if registry.is_running(scope):
    print("이미 연결 중")
    
# 핸들러 조회
handler = registry.get_handler(scope)
await handler.some_method()
```

## 주요 흐름

### 연결 생성 흐름

```
Kafka 메시지 수신
  ↓
KafkaConsumerClient
  ↓
connect_from_context(ctx)
  ↓
[중복 확인]
  ├─ 중복 없음 → 새 연결 생성
  │    ├─ 단일 구독 거래소? → 심볼별 분리
  │    ├─ 핸들러 생성
  │    ├─ 캐시 준비
  │    ├─ 태스크 시작
  │    └─ 레지스트리 등록
  │
  └─ 중복 있음 → 재구독 처리
       ├─ 기존 핸들러 조회
       ├─ 심볼 병합
       └─ 재구독 실행
```

### 에러 처리 흐름

```
에러 발생 (연결/재구독/기타)
  ↓
ErrorCoordinator.emit_*_error()
  ↓
에러 표준화 (kind, target, context)
  ↓
ErrorEventProducer.send_error_event()
  ↓
Kafka (ws.error 토픽)
```

### 연결 종료 흐름

```
disconnect_from_scope(scope)
  ↓
레지스트리에서 태스크 조회
  ↓
태스크 취소 (cancel())
  ↓
태스크 완료 대기 (CancelledError 무시)
  ↓
레지스트리에서 제거
  ↓
에러 조정자 정리
```

## 테스트

### 단위 테스트 예시

```python
# tests/application/test_connection_registry.py
async def test_register_connection():
    registry = ConnectionRegistry()
    scope = ConnectionScopeDomain(...)
    task = asyncio.create_task(dummy())
    handler = MockHandler()
    
    registry.register_connection(scope, task, handler)
    
    assert registry.is_running(scope)
    assert registry.get_handler(scope) == handler
```

### Mock 사용

```python
# Orchestrator 테스트 시 의존성 Mock
mock_registry = Mock(spec=ConnectionRegistry)
mock_error_coordinator = Mock(spec=ErrorCoordinator)

orchestrator = StreamOrchestrator(
    error_producer=mock_producer,
    registry=mock_registry,
    error_coordinator=mock_error_coordinator,
    # ...
)

await orchestrator.connect_from_context(ctx)

# 검증
mock_registry.register_connection.assert_called_once()
```

## 관련 모듈

- **Core 모듈**: 도메인 로직 및 DTO
- **Infrastructure 모듈**: Kafka, Redis (DI로 주입)
- **Config 모듈**: DI Container

## 참고 자료

- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html)
- [Dependency Injection in Python](https://python-dependency-injector.ets-labs.org/)
- [asyncio 공식 문서](https://docs.python.org/3/library/asyncio.html)
