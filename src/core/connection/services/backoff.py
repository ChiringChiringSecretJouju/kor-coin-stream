from __future__ import annotations

import random

from src.core.dto.internal.common import ConnectionPolicyDomain


def compute_next_backoff(
    policy: ConnectionPolicyDomain, 
    attempt: int, 
    last_backoff: float = 0.0
) -> float:
    """고도화된 Decorrelated Jitter + Circuit Breaker 전략
    
    특징:
    1. Decorrelated Jitter: 이전 백오프 값을 기반으로 3배 범위 내 랜덤 선택
       -> 클러스터링 및 스파이크 방지
    2. Dual-Cap: 10회 이상 실패 시 장기 장애 모드(5분)
    3. Early Bird: 장기 장애 모드에서도 5% 확률로 일찍 깨어나(Wait 50%) 장애 해소 확인
    """
    
    LONG_TERM_THRESHOLD = 10
    LONG_TERM_CAP = 300.0
    
    # 1. 캡 결정 (Dual-Cap)
    current_cap = LONG_TERM_CAP if attempt >= LONG_TERM_THRESHOLD else policy.max_backoff
    
    # 2. Decorrelated Jitter 핵심 로직
    # 이전 백오프 값의 3배 범위 내에서 결정하여 무작위성 극대화
    if attempt == 0:
        next_backoff = policy.initial_backoff
    else:
        # random.uniform(하한선, 상한선)
        # 하한선을 initial_backoff로 설정하여 최소한의 대기 시간 보장
        # last_backoff가 0이면(초기 상태) initial_backoff 사용
        prev = last_backoff if last_backoff > 0 else policy.initial_backoff
        next_backoff = random.uniform(policy.initial_backoff, prev * 3)
    
    # 3. 상한선 적용
    next_backoff = min(current_cap, next_backoff)
    
    # 4. 확률적 Early Bird (장기 장애 시 5% 확률로 대기 시간 50% 단축)
    # 서버가 예상보다 빨리 복구되었을 때 감지 확률을 높임
    if attempt >= LONG_TERM_THRESHOLD and random.random() < 0.05:
        next_backoff *= 0.5
        
    return round(next_backoff, 4) # 소수점 4자리까지 정규화

