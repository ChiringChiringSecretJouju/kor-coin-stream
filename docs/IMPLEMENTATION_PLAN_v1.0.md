# Implementation Plan: Engineering Manifesto v1.0 Compliance
> **Based on**: [Engineering Manifesto v1.0](./ENGINEERING_MANIFESTO_v1.0.md)  
> **Target**: `kor-coin-stream`  
> **Date**: 2026-01-26

본 문서는 Engineering Manifesto v1.0의 철학(Performance First, Quant Mindset)을 실제 코드 베이스에 적용하기 위한 구체적인 **기술적 실행 계획(Technical Execution Plan)**입니다.

---

## 📅 Roadmap Overview

| Phase | Title | Focus Area | Expected Gain |
| :--- | :--- | :--- | :--- |
| **Phase 1** | **Core Type Optimization** | Side(Int), Timestamp(Float) | Memory -30%, CPU (Compare) +50% |
| **Phase 2** | **Hot Path Bypass** | Pydantic `model_construct` | Object Creation Speed +3,000% |
| **Phase 3** | **Pipeline Streamlining** | SerDe Reduction, Inlining | E2E Latency < 1ms |
| **Phase 4** | **Benchmark & Verification** | Load Test, Profiling | Validation |

---

## 🚀 Phase 1: Core Data Structure Optimization

가장 기초가 되는 데이터 타입부터 "수학적/기계 친화적"으로 변경합니다. 문자열 처리는 CPU 사이클을 낭비하는 주범입니다.

### 1.1 Trade Side to Integer (`±1`)
*   **Current Specification**: `Literal["ASK", "BID"]` (String Storage & Comparison)
*   **New Design**:
    *   **Type**: `int` (Signed Integer using 8-bit logic)
    *   **Value Mapping**:
        *   `BID` (매수) → `1`
        *   `ASK` (매도) → `-1`
        *   `UNKNOWN` → `0`
*   **Implementation Steps**:
    1.  `src/core/types/_common_types.py`: `TradeSide` Enum을 `IntEnum`으로 변경하거나 상수로 대체.
    2.  `src/core/dto/io/realtime.py`: `StandardTradeDTO.ask_bid` 필드 타입을 `int`로 변경 (`Literal` 제거).
    3.  `src/core/connection/utils/parsers/`: 각 거래소 파서의 매수/매도 매핑 로직을 정수 반환으로 수정.

### 1.2 Normalized Float Timestamp
*   **Current Specification**: `int` (Milliseconds)
*   **New Design**:
    *   **Type**: `float` (Unix Epoch Seconds)
    *   **Precision**: 소수점 이하 6자리까지 허용 (Microsecond support)
    *   **Rationale**: `Pandas`/`Numpy` 등 퀀트 라이브러리와의 호환성 및 연산 편의성.
*   **Implementation Steps**:
    1.  `StandardTradeDTO.trade_timestamp` 타입을 `StrictFloat`로 변경.
    2.  파서 유틸리티에서 `datetime` 객체 생성을 금지하고, `timestamp / 1000.0` 연산으로 통일.

---

## ⚡ Phase 2: Hot Path Optimization (Pydantic Bypass)

데이터 검증(Validation)은 비쌉니다. "신뢰할 수 있는 구간"에서는 검증을 생략하고 객체를 직접 조립합니다.

### 2.1 `model_construct` Strategy
*   **Target**: `korea.py`, `asia.py`, `na.py`의 `trade_message` 및 `ticker_message` 핸들러.
*   **Change**:
    ```python
    # Before
    dto = StandardTradeDTO(**data)  # Full Validation (Slow)
    
    # After
    dto = StandardTradeDTO.model_construct(**data)  # Direct Memory Mapping (Fast)
    ```
*   **Safety Net**:
    *   필수 필드가 누락될 경우를 대비해 개발 환경(`ENV=dev`)에서는 검증을 수행하고, 상용 환경(`ENV=prod`)에서는 `model_construct`를 사용하는 **Hybrid Factory** 패턴 적용.

---

## ✂️ Phase 3: Pipeline Streamlining (SerDe Reduction)

데이터가 파이프라인을 통과하면서 `Bytes -> Dict -> DTO -> Dict -> Bytes`로 변환되는 횟수를 줄입니다.

### 3.1 DTO Direct Dumping
*   **Current**: `dto.model_dump()`로 Dict 변환 후 Kafka 전송.
*   **Optimization**: DTO 상태에서 `orjson`이 직접 직렬화할 수 있도록 `__dict__`나 `__slots__`를 활용하거나, Pydantic v2의 `model_dump_json()` (with Rust core)을 사용하여 Python 객체 변환 단계를 건너뜀.

### 3.2 Array-Format Response (with Protocol Versioning)
*   **Verification Result**: ⚠️ **주의 (Schema Rigidity vs Performance)**
    *   HFT 표준에 가까운 강력한 최적화이지만, 데이터의 자기 서술성(Self-describing)이 사라집니다.
*   **Proposal**: JSON Object(`{...}`) 대신 Array(`[...]`) 포맷으로 Kafka 전송.
    *   **Structure**: `[version, ts, price, qty, side, id]`
    *   **Protocol Versioning**: 스키마 변경에 대응하기 위해 헤더나 첫 번째 요소에 내부 프로토콜 버전(`v1`, `v2`...)을 명시합니다.
*   **Effect**: Payload 크기 40% 감소, 파싱 속도 2배 향상.
*   **Action**:
    *   초기 단계에서는 Consumer가 많지 않으므로 적극 도입.
    *   Schema Registry 대신 **Internal Protocol Header**로 버전 관리.

---

## 📊 Phase 4: Verification (The Benchmark)

변경 사항이 실제로 빨라졌는지 증명합니다.

### 4.1 Micro-Benchmark Scenarios
*   **Parse**: 10만 개 Trade 메시지 파싱 속도 비교 (`String` vs `Int` Side).
*   **Construct**: `BaseModel` vs `model_construct` 속도 비교.
*   **Total**: Websocket 수신부터 Kafka Ack까지의 Latency 분포(p50, p99) 측정.

### 4.2 Code Coverage & Safety
*   타입 변경으로 인한 회귀(Regression) 테스트.
*   잘못된 데이터(Dirty Data) 유입 시 시스템이 멈추지 않고 에러 로깅 후 무시(Skip)하는지 검증.

---

> **Note**: 이 계획은 즉시 실행 가능하며, `kor-coin-stream`을 단순 수집기가 아닌 **HFT급 데이터 공급 장치**로 탈바꿈시킬 것입니다.
