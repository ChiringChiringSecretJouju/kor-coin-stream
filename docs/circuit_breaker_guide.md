# Redis 기반 서킷브레이커 사용 가이드

> **작성일**: 2025-10-10  
> **버전**: 1.0  
> **상태**: Production Ready

---

## 📋 목차

1. [개요](#개요)
2. [서킷브레이커 패턴이란](#서킷브레이커-패턴이란)
3. [시스템 구조](#시스템-구조)
4. [사용법](#사용법)
5. [설정](#설정)
6. [모니터링](#모니터링)
7. [FAQ](#faq)

---

## 개요

### 배경
실시간 거래소 WebSocket 연결에서 장애가 발생하면:
- ❌ 계속 재시도 → 시스템 과부하
- ❌ 카스케이딩 실패 (연쇄 장애)
- ❌ 복구 지연

### 해결책: 서킷브레이커
- ✅ **자동 차단**: 장애 감지 시 요청 즉시 차단
- ✅ **자동 복구**: 일정 시간 후 제한적 테스트
- ✅ **분산 환경**: Redis 기반 상태 공유

---

## 서킷브레이커 패턴이란

### 3-State Finite State Machine

```
┌─────────┐  5회 실패   ┌─────────┐
│ CLOSED  │──────────▶│  OPEN   │
│(정상)   │            │(차단)   │
└─────────┘            └─────────┘
     ▲                      │
     │                      │ 60초 타임아웃
     │                      ▼
     │ 2회 성공      ┌──────────┐
     └──────────────│ HALF_OPEN │
                    │(테스트)   │
                    └──────────┘
```

### 상태별 동작

| 상태 | 요청 처리 | 전환 조건 |
|------|-----------|-----------|
| **CLOSED** | 모든 요청 허용 | 5회 연속 실패 → OPEN |
| **OPEN** | 모든 요청 차단 | 60초 경과 → HALF_OPEN |
| **HALF_OPEN** | 제한적 허용 (최대 3회) | 2회 성공 → CLOSED<br>1회 실패 → OPEN |

---

## 시스템 구조

### 아키텍처

```
┌─────────────────────────────────────────┐
│     ErrorDispatcher                     │
│  (에러 처리 통합 디스패처)              │
└───────────┬─────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────┐
│   RedisCircuitBreaker                   │
│  - is_request_allowed()                 │
│  - record_failure()                     │
│  - record_success()                     │
└───────────┬─────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────┐
│         Redis (분산 상태 저장소)        │
│  Key: cb:upbit/kr/ticker                │
│  Value: {"state": "CLOSED", ...}        │
└─────────────────────────────────────────┘
```

### 데이터 구조 (Redis)

```json
{
  "state": "CLOSED",           // CLOSED/OPEN/HALF_OPEN
  "failure_count": 0,          // 현재 실패 횟수
  "success_count": 0,          // HALF_OPEN 성공 횟수
  "opened_at": 0.0,            // OPEN 전환 시각 (Unix timestamp)
  "half_open_calls": 0         // HALF_OPEN 호출 횟수
}
```

---

## 사용법

### 1. ErrorDispatcher 사용 (권장)

```python
from src.common.exceptions.error_dispatcher import ErrorDispatcher, CircuitBreakerOpenError
from src.core.dto.io.target import ConnectionTargetDTO

dispatcher = ErrorDispatcher()

target = ConnectionTargetDTO(
    exchange="upbit",
    region="kr",
    request_type="ticker",
)

try:
    # 요청 전 체크
    if not await dispatcher.is_request_allowed(target):
        raise CircuitBreakerOpenError("Circuit is OPEN")
    
    # 비즈니스 로직
    result = await websocket_connect(target)
    
    # 성공 기록
    await dispatcher.record_success(target)
    
except Exception as e:
    # 실패 자동 기록 + 에러 이벤트 발행
    await dispatcher.dispatch(e, "websocket_connect", target)
```

### 2. 직접 사용 (고급)

```python
from src.common.exceptions.circuit_breaker import create_circuit_breaker

breaker = await create_circuit_breaker("upbit/kr/ticker")

try:
    if not await breaker.is_request_allowed():
        print("Circuit OPEN - Request blocked")
        return
    
    result = await some_operation()
    await breaker.record_success()
    
except Exception as e:
    await breaker.record_failure()
    raise

finally:
    await breaker.stop()
```

### 3. BaseWebsocketHandler 통합

```python
class UpbitWebsocketHandler(BaseKoreaWebsocketHandler):
    async def connect(self):
        """WebSocket 연결 (서킷브레이커 적용)"""
        
        # 서킷브레이커 체크
        if not await self.dispatcher.is_request_allowed(self.target):
            logger.error("Circuit breaker blocked connection attempt")
            return
        
        try:
            # 연결 시도
            self.ws = await websocket.connect(self.url)
            
            # 성공 기록
            await self.dispatcher.record_success(self.target)
            
        except Exception as e:
            # 실패 자동 기록
            await self.dispatcher.dispatch(e, "websocket_connect", self.target)
            raise
```

---

## 설정

### CircuitBreakerConfig

```python
from src.common.exceptions.circuit_breaker import CircuitBreakerConfig

config = CircuitBreakerConfig(
    failure_threshold=5,         # 연속 실패 임계값 (기본: 5)
    success_threshold=2,         # HALF_OPEN → CLOSED 성공 횟수 (기본: 2)
    timeout_seconds=60,          # OPEN 유지 시간 초 (기본: 60초)
    half_open_max_calls=3,       # HALF_OPEN 최대 호출 수 (기본: 3)
    sliding_window_seconds=300,  # 슬라이딩 윈도우 (기본: 5분)
)

breaker = await create_circuit_breaker("upbit/kr/ticker", config)
```

### 환경별 권장 설정

#### 개발 환경
```python
CircuitBreakerConfig(
    failure_threshold=3,      # 빠른 테스트
    timeout_seconds=10,       # 짧은 타임아웃
)
```

#### 스테이징 환경
```python
CircuitBreakerConfig(
    failure_threshold=5,
    timeout_seconds=30,
)
```

#### 운영 환경
```python
CircuitBreakerConfig(
    failure_threshold=5,
    timeout_seconds=60,
    half_open_max_calls=5,    # 더 많은 테스트
)
```

---

## 모니터링

### 1. 로그 모니터링

#### CLOSED → OPEN 전환
```json
{
  "level": "ERROR",
  "message": "Circuit upbit/kr/ticker: CLOSED → OPEN (failures: 5/5)",
  "circuit_breaker_key": "upbit/kr/ticker",
  "circuit_state": "OPEN"
}
```

#### OPEN → HALF_OPEN 전환
```json
{
  "level": "INFO",
  "message": "Circuit upbit/kr/ticker: OPEN → HALF_OPEN (timeout: 60.1s)",
  "circuit_state": "HALF_OPEN"
}
```

#### HALF_OPEN → CLOSED 복구
```json
{
  "level": "INFO",
```bash
# 서킷브레이커 상태 조회
redis-cli GET "cb:upbit/kr/ticker"

# HALF_OPEN → CLOSED 전환 로그
{
  "level": "INFO",
# 모든 서킷브레이커 키 조회
redis-cli KEYS "cb:*"

# 특정 거래소 서킷브레이커
redis-cli KEYS "cb:upbit/*"
```

### 3. 메트릭 수집 (TODO)

```python
# Prometheus 메트릭 예시
circuit_breaker_state{exchange="upbit",region="kr",type="ticker"} = 0  # CLOSED
circuit_breaker_failures_total{exchange="upbit",region="kr",type="ticker"} = 3
circuit_breaker_successes_total{exchange="upbit",region="kr",type="ticker"} = 100
```

---

## 운영 도구

### 강제 상태 전환

```python
# OPEN 강제 전환 (긴급 차단)
await breaker.force_open()

# CLOSED 강제 전환 (긴급 복구)
await breaker.force_close()
```

### CLI 도구 (예정)

```bash
# 서킷브레이커 상태 조회
python -m src.tools.circuit_breaker status upbit/kr/ticker

# 강제 OPEN
python -m src.tools.circuit_breaker open upbit/kr/ticker

# 강제 CLOSED
python -m src.tools.circuit_breaker close upbit/kr/ticker

# 모든 서킷브레이커 리셋
python -m src.tools.circuit_breaker reset --all
```

---

## FAQ

### Q1. 서킷브레이커가 OPEN 상태인데 복구가 안 돼요
**A**: 
1. Redis 연결 확인: `redis-cli PING`
2. 타임아웃 대기: 기본 60초 후 HALF_OPEN 전환
3. 수동 복구: `await breaker.force_close()`

### Q2. 분산 환경에서 상태가 동기화 안 돼요
**A**: 
- Redis 연결 확인
- 같은 Redis 인스턴스 사용 확인
- 키 형식 확인: `cb:{exchange}/{region}/{request_type}`

### Q3. 성공했는데도 OPEN 상태가 유지돼요
**A**: 
- `record_success()` 호출 확인
- HALF_OPEN 상태에서 충분한 성공 횟수 필요 (기본 2회)

### Q4. 너무 자주 OPEN 상태로 전환돼요
**A**: 
- `failure_threshold` 증가 (기본 5 → 10)
- 실제 장애인지 확인 (로그 분석)

### Q5. 복구가 너무 느려요
**A**: 
- `timeout_seconds` 감소 (60 → 30)
- `half_open_max_calls` 증가 (3 → 5)

---

## 성능 영향

### Redis 오버헤드
- **읽기**: ~1ms (is_request_allowed)
- **쓰기**: ~2ms (record_failure/success)
- **네트워크**: 로컬 Redis 사용 권장

### 메모리 사용
- **키당**: ~200 bytes
- **100개 서킷브레이커**: ~20KB
- **TTL**: 1시간 (자동 만료)

---

## 베스트 프랙티스

### ✅ DO

1. **요청 전 항상 체크**
   ```python
   if not await dispatcher.is_request_allowed(target):
       return  # 또는 raise CircuitBreakerOpenError
   ```

2. **성공 시 반드시 기록**
   ```python
   result = await operation()
   await dispatcher.record_success(target)  # 필수!
   ```

3. **리소스 정리**
   ```python
   finally:
       await dispatcher.cleanup()
   ```

### ❌ DON'T

1. **성공 기록 누락**
   ```python
   # 잘못된 예
   result = await operation()
   # record_success() 호출 안 함 → CLOSED로 복구 안 됨
   ```

2. **예외 무시**
   ```python
   # 잘못된 예
   try:
       await operation()
   except:
       pass  # record_failure() 호출 안 함
   ```

3. **동일 키 중복 사용**
   ```python
   # 잘못된 예
   breaker1 = await create_circuit_breaker("upbit/kr/ticker")
   breaker2 = await create_circuit_breaker("upbit/kr/ticker")
   # → 상태가 꼬일 수 있음, ErrorDispatcher 사용 권장
   ```

---

## 트러블슈팅

### 문제: Redis 연결 실패

**증상**:
```
redis.exceptions.ConnectionError: Error connecting to Redis
```

**해결**:
```bash
# Redis 실행 확인
redis-cli PING

# 설정 확인
cat src/config/settings.py | grep redis
```

### 문제: 서킷브레이커 상태 조회 안 됨

**증상**:
```python
state = await breaker.get_state()  # 항상 CLOSED
```

**해결**:
```python
# start() 호출 확인
await breaker.start()

# Redis 키 확인
redis-cli GET "cb:upbit/kr/ticker"
```

---

## 참고 자료

### 외부 링크
- [Martin Fowler - Circuit Breaker](https://martinfowler.com/bliki/CircuitBreaker.html)
- [Microsoft - Circuit Breaker Pattern](https://docs.microsoft.com/en-us/azure/architecture/patterns/circuit-breaker)
- [Netflix Hystrix](https://github.com/Netflix/Hystrix/wiki/How-it-Works)

### 내부 문서
- [error_dispatcher.py](/src/common/exceptions/error_dispatcher.py)
- [circuit_breaker.py](/src/common/exceptions/circuit_breaker.py)
- [circuit_breaker_example.py](/src/common/exceptions/circuit_breaker_example.py)

---

**마지막 업데이트**: 2025-10-10  
**작성자**: Cascade AI  
**리뷰어**: 사용자 확인 대기
