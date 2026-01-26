# Analysis Report: Subscription Logic Migration

## 1. 개요
`SubscriptionManager` 제거를 위해 기존 로직을 분석하고, 필수적인 부분과 폐기할 부분을 분류합니다. 목표는 `src/core/connection/_utils.py` 및 `BaseWebsocketHandler`로의 안전한 이관입니다.

## 2. SubscriptionManager 로직 분석

### 2.1. 필수 유지 로직 (Must Have)
재연결 및 상태 관리를 위해 반드시 보존해야 하는 로직입니다.

1.  **Symbol Extraction (`_extract_symbols_from_params`)**
    *   **기능**: 거래소별로 상이한 구독 요청 Payload(dict/list)에서 구독 대상 심볼 목록을 추출합니다.
    *   **사용처**: `ConnectSuccessAckEmitter`가 연결 성공 직후 어떤 심볼을 구독했는지 확인하여 초기 ACK를 날릴 때 사용됩니다.
    *   **복잡성**: 다양한 키(`symbols`, `codes`, `args`, `instId` 등)와 중첩 구조(`params` 내부)를 모두 탐색합니다.
    *   **이관 계획**: `src/core/connection/_utils.py` -> `extract_subscription_symbols` 함수로 이관.

2.  **Message Preparation (`prepare_subscription_message`)**
    *   **기능**: 파이썬 객체(dict/list)를 JSON 문자열로 직렬화합니다.
    *   **구현**: 단순 `orjson.dumps().decode()` 래퍼입니다.
    *   **이관 계획**: `BaseWebsocketHandler._sending_socket_parameter` 메서드 내부에 `orjson` 직접 호출로 단순화(Inlining).

### 2.2. 폐기 또는 단순화할 로직 (Discard or Simplify)
현재 구조의 복잡성을 유발하는 주범으로, 과감히 단순화합니다.

1.  **State Management (`SubscriptionStateDomain`)**
    *   **현황**: 별도 객체로 `current_params`, `symbols`, `subscribe_type`을 관리합니다.
    *   **단순화**: 핸들러의 인스턴스 변수 `self._current_subscription_params` (dict | list) 하나로 통합합니다. 심볼 목록은 필요할 때 유틸리티 함수로 즉시 추출합니다(Derived State).

2.  **Logic for Merge (`merge_symbols`)**
    *   **현황**: 기존 구독과 새 구독 요청 간의 집합 연산(Set Operation)을 수행하여 중복을 제거하고 병합합니다.
    *   **단순화**: **"Last Write Wins"** 정책 채택. 동적 재구독 요청이 오면 기존 파라미터를 덮어쓰고, 새 파라미터 전체를 전송합니다. 복잡한 병합 로직은 제거합니다.

3.  **Dynamic Update Checks (`update_subscription`)**
    *   **현황**: 웹소켓 상태 확인, 락(Lock) 관리, 에러 디스패치 등이 얽혀 있음.
    *   **단순화**: 핸들러의 `send()` 메서드를 호출하는 단순 래퍼로 변경하거나, 핸들러 내부 로직으로 흡수합니다.

## 3. 이관 대상 코드 상세 (Target Code)

### `src/core/connection/_utils.py`

```python
def extract_subscription_symbols(params: dict[str, Any] | list[Any]) -> list[str]:
    """구독 요청 파라미터에서 심볼 목록을 추출합니다.
    
    지원 키: symbols, codes, pair, pairs, symbol, product_ids, args, instId
    지원 구조: Flat Dict, List of Dicts, Nested Dict (params key)
    """
    # ... (기존 _extract_symbols_from_params 로직의 순수 함수 버전)
```

## 4. 결론
`SubscriptionManager`는 사실상 "구독 파라미터 저장소"와 "심볼 추출기" 두 가지 역할만 남기고 해체될 수 있습니다. 저장소 역할은 핸들러가 직접 수행하고, 추출기 역할은 유틸리티 모듈로 위임함으로써 구조적 복잡도를 90% 이상 줄일 수 있습니다.
