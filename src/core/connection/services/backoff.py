from __future__ import annotations

import random

from src.core.dto.internal.common import ConnectionPolicyDomain


def compute_next_backoff(policy: ConnectionPolicyDomain, attempt: int) -> float:
    """지수 백오프(+지터) 계산.

    Args:
        policy: 백오프 파라미터가 담긴 정책 객체
        attempt: 0부터 시작하는 시도 인덱스

    Returns:
        다음 대기 시간(초)
    """
    base = min(
        policy.initial_backoff * (policy.backoff_multiplier**attempt),
        policy.max_backoff,
    )
    jitter_range = base * policy.jitter
    jitter = random.uniform(-jitter_range, jitter_range)
    return max(0.0, base + jitter)
