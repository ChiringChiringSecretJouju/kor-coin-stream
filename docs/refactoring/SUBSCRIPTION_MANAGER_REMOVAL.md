# Refactoring Plan: SubscriptionManager Removal

## 1. 개요 (Overview)
`SubscriptionManager` 클래스는 재구독 및 동적 심볼 관리를 위해 도입되었으나, 현재 시스템 요구사항 대비 과도한 복잡성(Over-engineering)과 오버헤드를 유발하고 있습니다. 이를 `BaseWebsocketHandler`로 통합하여 구조를 단순화하고 유지보수성을 높이는 것이 목표입니다.

## 2. 현재 상태 (As-Is)
- **Layered Complexity**: `BaseWebsocketHandler` -> `SubscriptionManager` -> `SubscriptionStateDomain` 3단계 뎁스.
- **Complex Logic**: `merge_symbols` 등 잘 사용되지 않는 복잡한 심볼 병합 로직 존재.
- **State Duplication**: 핸들러와 매니저 간 상태 동기화 관리 필요.

## 3. 목표 상태 (To-Be)
- **Flat Structure**: `BaseWebsocketHandler`가 직접 `socket_parameters`를 관리.
- **Simplified Logic**: 복잡한 병합 로직 제거. 재연결 시 "마지막으로 성공한 파라미터"를 재전송하는 단순 정책 채택.
- **Pure Functions**: 심볼 추출 등 로직은 Stateless 유틸리티 함수로 분리.

## 4. 단계별 실행 계획 (Action Plan)

### Step 1: 유틸리티 로직 분리 (Extract Logic)
- `SubscriptionManager._extract_symbols_from_params` 메서드는 상태와 무관한 순수 로직입니다.
- 이를 `src/core/connection/_utils.py`의 `extract_subscription_symbols` 함수로 이동합니다.
- `BaseWebsocketHandler`에서 이 유틸리티를 직접 import 하여 사용하도록 준비합니다.

### Step 2: 상태 관리 핸들러로 이동 (Move State)
- `BaseWebsocketHandler`에 `self._last_socket_params` (또는 `_current_params`) 변수를 추가합니다.
- 연결 성공(`websocket_connection`) 시점에 이 변수에 파라미터를 저장합니다.
- 재연결 로직에서 `SubscriptionManager` 대신 이 변수를 참조하여 재전송하도록 수정합니다.

### Step 3: 전송 로직 내재화 (Inline Methods)
- `SubscriptionManager.prepare_subscription_message` -> `orjson.dumps`로 단순화하여 핸들러 내부 `_sending_socket_parameter`에 구현.
- `update_subscription` 등의 메서드를 핸들러 내부 메서드로 흡수.
- 복잡한 `merge_symbols` 로직은 폐기하고, 새로운 파라미터가 오면 `_last_socket_params`를 덮어쓰는(Replace) 방식으로 단순화.

### Step 4: SubscriptionManager 삭제 (Delete)
- `BaseWebsocketHandler`에서 `SubscriptionManager` 참조 제거.
- `src/core/connection/subscription_manager.py` 파일 삭제.
- `SubscriptionStateDomain` 등 관련 DTO 삭제.

## 5. 기대 효과
- **코드 라인 수 감소**: 약 300라인 이상의 복잡한 코드 제거.
- **직관성 향상**: "핸들러가 파라미터를 기억하고 재연결 시 다시 보낸다"는 명확한 흐름.
- **디버깅 용이성**: 호출 스택 깊이 감소.
