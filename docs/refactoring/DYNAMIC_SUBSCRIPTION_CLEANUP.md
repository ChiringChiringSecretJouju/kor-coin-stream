# Refactoring Plan: Dynamic Subscription Strategy (Revised)

## 1. 개요 (Overview)
기존 계획(기능 폐기)을 수정하여, **Zero-Downtime(무중단) 설정 변경**을 위해 **동적 구독 기능을 유지 및 단순화**합니다.
데이터 수신 중단 없이 새로운 심볼을 추가할 수 있어야 하며, 재연결 시에는 누적된 모든 심볼이 복구되어야 합니다.

## 2. 핵심 요구사항 (Requirements)
1.  **Zero-Downtime**: 기존 심볼(예: BTC)의 데이터 수신이 끊기지 않은 상태에서 새 심볼(예: ETH)을 추가해야 함.
2.  **State Consistency**: 재연결 시 발생할 때, 초기 심볼뿐만 아니라 동적으로 추가된 심볼까지 모두 포함하여 복구해야 함.
3.  **Simplicity**: 별도의 복잡한 Manager 클래스 없이, 유틸리티 함수와 핸들러 내부 로직만으로 구현.

## 3. 구현 전략 (Implementation Strategy)

### 3.1. 핸들러 (Handler)
- `BaseWebsocketHandler.update_subscription(new_symbols)` 메서드를 살려둡니다.
- **동작**:
    1.  현재 연결된 소켓으로 `new_symbols`에 대한 구독 메시지를 즉시 전송 (Incremental Subscribe).
    2.  `merge_subscription_params` 유틸리티를 호출하여 `self._last_socket_params`를 갱신 (State Merge).

### 3.2. 유틸리티 (Utility)
- 위치: `src/core/connection/utils/subscription.py`
- 추가 함수: `merge_subscription_params(current_params, new_symbols) -> merged_params`
- **구현 내용**:
    - `dict` 파라미터 내의 `symbols` 리스트에 새로운 심볼을 중복 없이 추가.
    - `params`나 `args` 키 내부의 리스트도 처리 가능한 범용성 확보.

### 3.3. 오케스트레이터 (Orchestrator)
- `StreamOrchestrator._handle_resubscribe()` 복원:
    - 중복 연결 요청 시 `handler.update_subscription()`을 호출하여 심볼 추가를 위임.
    - 내부 `SubscriptionManager` 클래스는 제거하고 핸들러에게 위임하는 방식으로 단순화.

## 4. 실행 계획 (Action Plan)
1.  **Step 1**: `src/core/connection/utils/subscription.py`에 `merge_subscription_params` 함수 구현.
2.  **Step 2**: `BaseWebsocketHandler.update_subscription` 메서드를 수정하여 신규 유틸리티 사용 및 상태 갱신 로직 적용.
3.  **Step 3**: `StreamOrchestrator`에서 내부 Manager 클래스 제거 후 핸들러 호출 방식으로 변경.

## 5. 결론
"기능은 유지하되 구현은 가볍게" 가져갑니다. Manager는 없지만 기능은 살아있습니다.
