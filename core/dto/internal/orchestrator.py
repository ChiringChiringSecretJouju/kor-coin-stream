from dataclasses import dataclass

from core.dto.internal.common import ConnectionScopeDomain
from core.types import SocketParams


@dataclass(slots=True)
class StreamContextDomain:
    """연결 처리에 공통으로 사용되는 컨텍스트.

    책임과 규약(내부 DTO 규격)
    - 파라미터 전달 최적화 위해 만든 **internal 전용 dataclass**임
    - 이 객체는 **검증/직렬화 책임 없음**. 검증은 I/O 경계에서 Pydantic으로 1회만 함

    경계/불변 조건
    - scope: `ConnectionScopeDomain`는 생성 후 불변임
    - socket_params: I/O 경계에서 **검증 완료된 값** 받아야 함
    - url: 실행 전 **None 아님** 보장되어야 함
    - symbols: `SubscriptionManager.fill_symbols()`에서만 갱신하고 tuple로 저장해 불변성 유지함

    금지
    - IO(Pydantic) 모델 의존 금지
    - 외부 직렬화 로직 포함 금지
    """

    scope: ConnectionScopeDomain
    socket_params: SocketParams
    url: str | None = None
    symbols: tuple[str, ...] = ()
    projection: list[str] | None = None
