# Core 모듈

도메인 로직 및 비즈니스 규칙 레이어

## 개요

**Core 모듈**은 시스템의 도메인 로직과 비즈니스 규칙을 포함하는 순수한 레이어입니다. 외부 인프라에 의존하지 않으며, WebSocket 연결 관리, 구독 관리, 헬스 모니터링 등의 핵심 비즈니스 로직을 구현합니다.

### 이 모듈의 역할

1. **도메인 로직 구현**: WebSocket 연결, 구독, 헬스 체크 등
2. **데이터 모델 정의**: DTO, Domain 객체, 타입 정의
3. **비즈니스 규칙 적용**: 재연결 정책, 에러 처리 규칙
4. **순수성 유지**: 인프라 의존성 없음

## 파일 구조

```
core/
├─ connection/          # WebSocket 연결 및 비즈니스 로직
│  ├─ handlers/        # 지역별 WebSocket 핸들러
│  │  ├─ base.py      # 기본 핸들러 추상 클래스
│  │  ├─ korea_handler.py      # 한국 거래소 핸들러
│  │  ├─ global_handler.py     # 글로벌 거래소 핸들러
│  │  └─ realtime_collection.py # 실시간 데이터 수집
│  ├─ services/        # 비즈니스 서비스
│  │  └─ backoff.py   # 지수 백오프 재연결 전략
│  ├─ emitters/        # 이벤트 발행
│  │  └─ connect_success_ack_emitter.py # 연결 성공 ACK
│  ├─ utils/           # 유틸리티
│  │  └─ parse.py     # 메시지 파싱
│  ├─ subscription_manager.py  # 구독 관리
│  ├─ health_monitor.py        # 헬스 체크
│  ├─ error_handler.py         # 에러 처리
│  └─ _utils.py                # 공통 유틸리티
│
├─ dto/                # 데이터 전송 객체
│  ├─ io/             # I/O 경계 (Pydantic 모델)
│  │  ├─ commands.py  # 외부 명령 (Kafka 메시지)
│  │  ├─ events.py    # 외부 이벤트
│  │  └─ metrics.py   # 메트릭 DTO
│  ├─ internal/       # 내부 도메인 (dataclass)
│  │  ├─ common.py    # 공통 도메인 객체
│  │  ├─ cache.py     # 캐시 도메인
│  │  ├─ metrics.py   # 메트릭 도메인
│  │  ├─ orchestrator.py # 오케스트레이터 도메인
│  │  └─ subscription.py # 구독 도메인
│  └─ adapter/        # DTO ↔ Domain 변환
│     ├─ stream_context.py # 컨텍스트 어댑터
│     └─ error_adapter.py  # 에러 어댑터
│
└─ types/              # 타입 정의
   ├─ _common_types.py # 공통 타입
   ├─ _payload_type.py # 페이로드 타입
   └─ _exception_types.py # 예외 타입
```

## 핵심 컴포넌트

### 1. **Connection 패키지** - WebSocket 연결 관리

#### BaseWebsocketHandler
```python
# 모든 거래소 핸들러의 기본 클래스
class BaseWebsocketHandler(ABC):
    """
    자동 제공 기능:
    - 지수 백오프 재연결
    - 하트비트 관리
    - 에러 처리
    - 메트릭 수집
    """
    @abstractmethod
    async def _get_subscribe_message(self, symbols): ...
```

**특징:**
- 추상 클래스로 공통 로직 제공
- 거래소별로 구독 메시지만 커스터마이징
- 90% 이상 기능 자동 제공

#### SubscriptionManager
```python
# 구독 관리 (추가/병합/재구독)
class SubscriptionManager:
    """
    책임:
    - 심볼 병합 (기존 구독 + 신규 구독)
    - 재구독 처리
    - 중복 방지
    """
```

#### ConnectionHealthMonitor
```python
# 연결 상태 모니터링
class ConnectionHealthMonitor:
    """
    책임:
    - 하트비트 주기 확인
    - 타임아웃 감지
    - 자동 재연결 트리거
    """
```

### 2. **DTO 패키지** - 데이터 모델

#### I/O 경계 (Pydantic)
```python
# 외부 입력 검증
class CommandDTO(BaseModel):
    """
    Kafka 메시지 → 시스템 입력
    - 런타임 검증
    - 타입 안전성
    """
    type: str
    action: str
    target: ConnectionTargetDTO
```

**특징:**
- Pydantic 기반 런타임 검증
- Kafka, HTTP 등 외부 입력 처리
- 자동 직렬화/역직렬화

#### Internal (dataclass)
```python
# 내부 도메인 객체
@dataclass(frozen=True, slots=True)
class StreamContextDomain:
    """
    내부 전용 불변 객체
    - 메모리 최적화 (slots)
    - 불변성 보장 (frozen)
    """
    scope: ConnectionScopeDomain
    symbols: list[str]
```

**특징:**
- 불변 객체 (frozen=True)
- 메모리 최적화 (slots=True)
- 타입 힌트 완전 적용

#### Adapter
```python
# DTO ↔ Domain 변환
class StreamContextAdapter:
    """
    IO DTO → Domain 변환
    - 입력 시 1회만 검증
    - 내부에서는 검증 생략
    """
    @staticmethod
    def from_command_dto(dto: CommandDTO) -> StreamContextDomain: ...
```

### 3. **Types 패키지** - 타입 정의

```python
# 공통 타입 정의
SocketParams: TypeAlias = dict[str, Any]
MessageHandler: TypeAlias = Callable[[dict], Awaitable[None]]

# 상수 정의
CONNECTION_STATUS_CONNECTED: Final[str] = "connected"
CONNECTION_STATUS_DISCONNECTED: Final[str] = "disconnected"
```

**특징:**
- TypeAlias로 의미 있는 별칭
- Final로 상수 보장
- 중앙 집중식 타입 관리

## 설계 원칙

### 1. **레이어 분리**
```
IO (Pydantic) → Adapter → Domain (dataclass) → Business Logic
```

### 2. **불변성**
```python
# ✅ 권장
@dataclass(frozen=True, slots=True)
class OrderDomain:
    id: str
    price: float
    
# ❌ 비권장
class Order:
    def __init__(self):
        self.id = None  # 가변 상태
```

### 3. **검증 최소화**
```python
# 입력 시 1회만 검증
dto = CommandDTO.model_validate(raw_data)  # Pydantic 검증

# 내부 전달 시 검증 생략
domain = StreamContextAdapter.from_command_dto(dto)
# → domain은 이미 검증된 데이터
```

### 4. **의존성 방향**
```
Core → (의존하지 않음)
Application → Core
Infrastructure → Core (DTO만)
```

## 사용 예시

### WebSocket 핸들러 사용

```python
# 1. 핸들러 생성
handler = UpbitWebsocketHandler(
    scope=scope,
    projection=projection,
    orchestrator=orchestrator
)

# 2. 연결 실행 (재연결 자동)
await handler.connect_and_run()

# 3. 메시지 자동 처리
# - ticker_message()
# - orderbook_message()
# - trade_message()
```

### DTO 사용

```python
# IO 경계: Pydantic 검증
raw_message = kafka_consumer.poll()
command = CommandDTO.model_validate(raw_message)

# Domain 변환
context = StreamContextAdapter.from_command_dto(command)

# 비즈니스 로직 실행
await orchestrator.connect_from_context(context)
```

### 구독 관리

```python
# 구독 병합
manager = SubscriptionManager()
merged = await manager.merge_symbols(
    existing=["BTC", "ETH"],
    new=["ETH", "XRP"]  # ETH 중복
)
# → ["BTC", "ETH", "XRP"] (중복 제거)

# 재구독
success = await manager.handle_resubscribe(
    running=handler,
    cache=cache,
    ctx=context
)
```

## 주요 흐름

### 연결 생성 흐름

```
CommandDTO (Kafka 메시지)
  ↓ (검증)
StreamContextAdapter
  ↓ (변환)
StreamContextDomain
  ↓ (전달)
StreamOrchestrator
  ↓ (핸들러 생성)
BaseWebsocketHandler
  ↓ (연결)
WebSocket 거래소
```

### 메시지 처리 흐름

```
WebSocket 메시지 수신
  ↓
_parse_message() (파싱)
  ↓
ticker_message() / orderbook_message() / trade_message()
  ↓
_extract_symbol() (심볼 추출)
  ↓
메트릭 집계
  ↓
Kafka Producer (실시간 데이터 발행)
```

## 테스트

### 단위 테스트 예시

```python
# tests/core/test_subscription_manager.py
async def test_merge_symbols():
    manager = SubscriptionManager()
    merged = await manager.merge_symbols(
        existing=["BTC"],
        new=["ETH", "BTC"]
    )
    assert merged == ["BTC", "ETH"]
```

### Mock 사용

```python
# DTO는 순수 데이터이므로 Mock 불필요
context = StreamContextDomain(
    scope=ConnectionScopeDomain(...),
    symbols=["BTC"]
)
# 직접 생성 가능!
```

## 참고 자료

- [Pydantic 공식 문서](https://docs.pydantic.dev/)
- [Python dataclasses](https://docs.python.org/3/library/dataclasses.html)
- [Python 3.12 타입 힌트](https://docs.python.org/3.12/library/typing.html)
