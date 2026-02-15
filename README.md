# kor-coin-stream

실시간 암호화폐 거래소 데이터 스트리밍 시스템

## 개요

다중 암호화폐 거래소로부터 실시간 시세 데이터를 수집하고 처리하는 스트리밍 파이프라인입니다.

### 기능

- 다중 거래소 지원: 한국(4개), 아시아(4개), 북미(2개), 유럽(1개) 총 11개
- WebSocket 기반 실시간 데이터 수집
- Kafka 메시징 시스템
- Avro 스키마 기반 직렬화
- Redis 기반 상태 관리
- Circuit Breaker 및 메트릭 수집
- Dependency Injection Container

## 아키텍처

레이어드 아키텍처를 기반으로 DI, 메시지 기반 통신, 플러그인 패턴을 조합한 구조입니다.

```
아키텍처 패턴:
- Layered Architecture
- Dependency Injection
- Pipes and Filters
- Plug-in Architecture
- Message-Driven Architecture
```

### 레이어 구조

```
src/
├─ core/          # Domain Layer - 비즈니스 로직, DTO, 타입
├─ application/   # Application Layer - 유스케이스 오케스트레이션
├─ infra/         # Infrastructure Layer - Kafka, Redis, Avro
├─ exchange/      # External Adapters - 거래소별 핸들러
├─ common/        # Cross-Cutting - 로깅, 이벤트, 예외
└─ config/        # Configuration - DI Container, Settings
```

## 시스템 요구사항

- Python 3.12.10
- Redis
- Kafka + Schema Registry
- uv (패키지 관리)

### 설치

```bash
# 저장소 클론
git clone <repository-url>
cd kor-coin-stream

# 의존성 설치 (uv 사용)
uv sync

# 또는 pip 사용
pip install -e .
```

### 설정

```bash
# 환경별 설정 파일
config/
├─ dev.yaml      # 개발 환경
├─ prod.yaml     # 프로덕션 환경
└─ settings.yaml # 기본 설정

# 환경 선택
export ENV=dev  # 또는 prod
```

### 실행

```bash
# 개발 환경
python main.py

# 프로덕션 환경
ENV=prod python main.py
```

## 데이터 흐름

```
Kafka (ws.command 토픽)
  ↓
KafkaConsumerClient (메시지 소비)
  ↓
StreamOrchestrator (연결 오케스트레이션)
  ↓
WebSocket 핸들러 (거래소별)
  ↓
실시간 데이터 수신 → 파싱 → 변환
  ↓
Kafka Producer (메트릭, 실시간 데이터 발행)
```

## 운영 정책 (현재 기준)

- 직렬화 정책은 토픽별 고정(Hybrid)입니다.
  - Avro required: `ticker-data.*`, `trade-data.*`, `ws.connect_success.*`, `ws.metrics.*`
  - JSON only: `ws.command`, `ws.error`, `ws.dlq`, `ws.backpressure.events`, `monitoring.batch.performance`
- `messaging.use_avro=true`일 때 startup preflight를 통해 Avro schema 파일 존재를 검증합니다.
- Connection 로깅은 structured key(`exchange`, `region`, `request_type`, `phase`)를 사용합니다.

## 지원 거래소

### 한국 (4개)
- Upbit, Bithumb, Coinone, Korbit

### 아시아 (4개)
- Binance, Bybit, OKX, Huobi

### 북미 (2개)
- Coinbase, Kraken

### 유럽 (1개)
- Bitfinex

## 프로젝트 구조

```
kor-coin-stream/
├─ src/
│  ├─ core/           # 도메인 로직 (자세한 내용은 src/core/README.md 참조)
│  ├─ application/    # 오케스트레이션 (src/application/README.md)
│  ├─ infra/          # 인프라 (src/infra/README.md)
│  ├─ exchange/       # 거래소 핸들러 (src/exchange/README.md)
│  ├─ common/         # 공통 기능 (src/common/README.md)
│  └─ config/         # 설정 (src/config/README.md)
├─ config/            # YAML 설정 파일
├─ tests/             # 테스트
├─ main.py            # 진입점
└─ pyproject.toml     # 프로젝트 설정
```

### Connection 모듈 구조 (리팩토링 반영)

```text
src/core/connection/
├─ handlers/   # 연결/수신 루프 오케스트레이션
├─ services/   # health/error/backoff/realtime batching 런타임 서비스
├─ emitters/   # connect_success 등 outbound emit
└─ utils/
   ├─ logging/       # log phase/constants, logging mixin, pydantic filter
   ├─ subscriptions/ # subscription merge/ack decision
   ├─ symbols/       # symbol parsing/extraction
   └─ market_data/
      ├─ common/     # parse/dict/time/format helpers
      ├─ parsers/    # parser base abstractions
      ├─ dispatch/   # common dispatcher base
      ├─ tickers/    # ticker parser/dispatcher
      └─ trades/     # trade parser/dispatcher
```

## 핵심 컴포넌트

### StreamOrchestrator (Application Layer)
- 연결 라이프사이클 관리
- 중복 연결 방지
- 에러 조정

### WebSocket 핸들러 (Exchange Layer)
- 거래소별 WebSocket 연결
- 자동 재연결
- 구독 관리, 헬스 모니터링

### Kafka 클라이언트 (Infrastructure Layer)
- Producer: JSON/Avro 직렬화
- Consumer: 메시지 소비 및 검증
- Circuit Breaker

### DI Container (Configuration)
- Resource 관리
- Factory Pattern
- Singleton Settings

## 개발

### 코드 품질

```bash
# Ruff 린팅
ruff check src/

# 타입 체크 (pyrefly)
pyrefly check

# 테스트
pytest tests/
```

### 거래소 추가

```python
# 1. 핸들러 생성 (src/exchange/)
class NewExchangeHandler(BaseGlobalWebsocketHandler):
    def _get_subscribe_message(self, symbols):
        return {"type": "subscribe", "symbols": symbols}

# 2. 등록 (src/exchange/__init__.py)
HANDLER_MAP["region"]["new_exchange"] = NewExchangeHandler
```

## 모듈 문서

- [Core 모듈](src/core/README.md)
- [Application 모듈](src/application/README.md)
- [Infrastructure 모듈](src/infra/README.md)
- [Exchange 모듈](src/exchange/README.md)
- [Common 모듈](src/common/README.md)
- [Config 모듈](src/config/README.md)

## Refactoring & Ops Docs

- `dataflow-backoffice/docs/kor-stream-coin-docs/operations_checklist.md`
- `dataflow-backoffice/docs/kor-stream-coin-docs/connection_logging_schema.md`
