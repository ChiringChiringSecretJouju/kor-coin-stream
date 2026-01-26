# Engineering Manifesto v1.0
> **Version**: 1.0 (2026-01-26)  
> **Author**: Senior High-Performance Systems & Quant Engineer  
> **Target**: `kor-coin-stream` Project

---

## 1. Philosophy: The Quant Mindset
우리는 "대충 돌아가는 코드"를 작성하지 않습니다. 우리는 **1ms의 지연이 1억 원의 슬리피지(Slippage)를 유발한다**는 가정 하에 코드를 작성합니다.  
우리의 모든 결정은 벤치마크 데이터와 수학적 복잡도 분석에 기반하며, Python의 편리함 뒤에 숨겨진 C-Level의 비용을 집요하게 추적합니다.

### 1.1 Core Principles
1.  **Performance First**: 가독성이 성능을 저해한다면, 성능을 택합니다. 단, 그 성능 이득이 벤치마크로 입증되어야 합니다.
2.  **Zero-Tolerance for GC Pauses**: 고빈도 데이터 수집 구간(Hot Path)에서는 객체 생성(Allocation)을 최소화합니다.
3.  **Data Integrity**: 속도가 중요하지만, Trash In, Trash Out은 허용하지 않습니다. 엄격한 스키마(Schema)를 유지하되, 검증 비용은 스마트하게 지불합니다.

---

## 2. Implementation Standards (Strict Requirements)

### 2.1 Data Structure Optimization
모든 도메인 객체는 Python의 오버헤드를 극한으로 줄이는 구조를 따릅니다.

*   **Immutable Domains**:
    *   모든 내부 DTO는 `@dataclass(slots=True, frozen=True)`를 필수적으로 사용합니다.
    *   `__dict__` 생성을 억제하여 인스턴스 당 메모리 사용량을 60% 이상 절감합니다.

*   **Integer Arithmetic for Side**:
    *   **Current (Bad)**: `Enum(value="BUY")`, `Enum(value="SELL")` (String comparison cost)
    *   **Target (Good)**: `BUY = 1`, `SELL = -1`
    *   **Reasoning**: ML 모델, Numpy 연산, 전략 백테스팅 엔진과의 호환성을 위해 정수형을 사용합니다. 문자열 비교(`strcmp`)보다 정수 비교(`CMP`)가 CPU 사이클을 덜 소모합니다.

*   **Normalized Timestamps**:
    *   모든 타임스탬프는 `float` (Unix Epoch Seconds)로 통일합니다.
    *   Millisecond 정밀도가 필요하면 소수점 3자리까지 사용합니다 (e.g., `1702456789.123`).
    *   `datetime` 객체 변환은 디버깅/로그 목적 외에는 금지합니다.

### 2.2 Critical Path Optimization: Pydantic V2
`preprocess_message`와 같이 초당 수만 번 호출되는 함수에서는 Pydantic의 검증 로직조차 사치일 수 있습니다.

*   **Trusted Source Strategy**:
    *   Upstream(거래소) 데이터가 스키마를 준수한다고 확신할 수 있는 경우(또는 에러가 나도 다음 틱에서 복구 가능한 경우), `model_validate` 대신 `model_construct`를 사용합니다.
    *   **Code Example**:
        ```python
        # Slow (Validation Overhead)
        obj = MyModel(**data) 
        
        # Fast (Direct Construction - 30x faster)
        obj = MyModel.model_construct(**data)
        ```
    *   **Rule**: `Connector` 레벨에서는 `model_construct` 사용을 적극 권장하며, `Validator` 서비스에서만 엄격한 검증을 수행합니다.

### 2.3 Serialization Protocols
*   **JSON**: 무조건 `orjson`을 사용합니다. 표준 `json` 라이브러리는 import조차 하지 않습니다.
*   **Response Format**:
    *   **Trade**: `[price(float), qty(float), side(int), timestamp(float)]` (Array 구조 권장)
    *   **Orderbook**: `{'asks': [[p, q], ...], 'bids': [[p, q], ...], 'ts': float}`

---

## 3. Potential Risks & Audit Insights (The Critical View)

현재 `kor-coin-stream`의 아키텍처에서 식별된 리스크와 대응책입니다.

### 3.1 SerDe Overhead Tax
*   **진단**: `Bytes` → `Dict` → `Dataclass` → `Dict` → `Bytes(Kafka)` 변환 과정이 너무 많습니다.
*   **리스크**: 변환 비용이 실제 비즈니스 로직(알고리즘)보다 더 많은 CPU를 소모할 수 있습니다 (Tax > Profit).
*   **해결책**:
    *   가능하다면 `Dict` 단계를 생략하고 `Receive Bytes` → `Dataclass(via orjson)`로 바로 매핑합니다.
    *   Kafka 전송 시에는 `Dataclass`를 바로 직렬화 가능한 형태로 유지합니다.

### 3.2 Abstraction Latency vs Maintenance
*   **진단**: `Clean Architecture`는 유지보수에 좋지만, `Consumer -> Orchestrator -> Connector -> Handler -> Collector -> Producer`의 깊은 호출 스택은 레이턴시를 유발합니다.
*   **리스크**: 함수 호출 오버헤드(Python Function Call Overhead)가 누적되어 1ms 미만의 기회를 놓칠 수 있습니다.
*   **해결책**:
    *   **Hot Path Inlining**: 데이터 수집 루프 내부에서는 계층 간 호출을 최소화하거나, 중요 로직을 하나의 함수로 인라인(Inline) 처리하는 것을 고려해야 합니다.

### 3.3 GIL Bottleneck & Heartbeat
*   **진단**: `uvloop`는 싱글 스레드입니다. 복잡한 파싱 로직이 CPU를 점유하면 `Heartbeat` 전송 타이밍을 놓쳐 연결이 끊길 수 있습니다.
*   **리스크**: Self-DDoS (자신의 연산 부하로 인해 네트워크 연결이 끊김).
*   **해결책**:
    *   파싱 로직이 무거워질 경우, `ProcessPoolExecutor`를 사용하거나 Rust 바인딩으로 CPU 타임을 획기적으로 줄여야 합니다.

---

## 4. Benchmark Targets
본 프로젝트가 달성해야 할 최소 성능 지표입니다.

| Metric | Target | Note |
| :--- | :--- | :--- |
| **Parsing Latency** | < 50µs | Payload 1KB 기준 |
| **E2E Latency** | < 2ms | WebSocket 수신 -> Kafka Ack |
| **Throughput** | > 10,000 TPS | 단일 프로세스 기준 |
| **GC Overhead** | < 1% | 전체 CPU 타임 대비 |

---

> "Numbers don't lie. Measure everything."
