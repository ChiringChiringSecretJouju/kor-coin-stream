"""Layer 1: Raw Reception Metrics Collector.

WebSocket 수신 단계의 원시 메트릭을 수집합니다.
- 수신 메시지 수
- 파싱 성공/실패
- 수신 바이트 수
"""

from dataclasses import dataclass


@dataclass(slots=True)
class ReceptionMetrics:
    """Layer 1: 원천 수신 메트릭 (불변).

    Attributes:
        total_received: WebSocket에서 받은 모든 메시지 수
        total_parsed: JSON 파싱 성공 수
        total_parse_failed: JSON 파싱 실패 수
        bytes_received: 수신한 총 바이트 수
    """

    total_received: int = 0
    total_parsed: int = 0
    total_parse_failed: int = 0
    bytes_received: int = 0


class ReceptionMetricsCollector:
    """Layer 1: 원천 수신 메트릭 수집기.

    WebSocket 메시지 수신 단계에서의 메트릭을 추적합니다.
    - 초경량 설계 (메시지마다 호출됨)
    - 불변 메트릭 스냅샷 반환
    """

    __slots__ = (
        "_total_received",
        "_total_parsed",
        "_total_parse_failed",
        "_bytes_received",
    )

    def __init__(self) -> None:
        self._total_received: int = 0
        self._total_parsed: int = 0
        self._total_parse_failed: int = 0
        self._bytes_received: int = 0

    def record_received(self, byte_size: int = 0) -> None:
        """메시지 수신 기록.

        Args:
            byte_size: 수신한 바이트 크기 (선택)
        """
        self._total_received += 1
        if byte_size > 0:
            self._bytes_received += byte_size

    def record_parse_success(self) -> None:
        """JSON 파싱 성공 기록."""
        self._total_parsed += 1

    def record_parse_failed(self) -> None:
        """JSON 파싱 실패 기록."""
        self._total_parse_failed += 1

    def snapshot(self) -> ReceptionMetrics:
        """현재 메트릭 스냅샷 반환 (불변)."""
        return ReceptionMetrics(
            total_received=self._total_received,
            total_parsed=self._total_parsed,
            total_parse_failed=self._total_parse_failed,
            bytes_received=self._bytes_received,
        )

    def reset(self) -> None:
        """메트릭 초기화 (분 롤오버 시)."""
        self._total_received = 0
        self._total_parsed = 0
        self._total_parse_failed = 0
        self._bytes_received = 0
