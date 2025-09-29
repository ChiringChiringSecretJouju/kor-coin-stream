from __future__ import annotations

from typing import cast

from pydantic import BaseModel

from src.common.logger import PipelineLogger
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.internal.orchestrator import StreamContextDomain
from src.core.dto.io.commands import CommandDTO
from src.core.types import ExchangeName, Region, RequestType, SocketParams

logger = PipelineLogger(__name__)


def to_connection_scope(target: dict[str, str]) -> ConnectionScopeDomain:
    """target(dict)에서 ConnectionScopeDomain을 생성하는 공용 헬퍼.

    기대 키: "region", "exchange", "request_type"
    """
    return ConnectionScopeDomain(
        region=cast(Region, target["region"]),
        exchange=cast(ExchangeName, target["exchange"]),
        request_type=cast(RequestType, target["request_type"]),
    )


def adapter_stream_context(dto: CommandDTO) -> StreamContextDomain:
    """Validated CommandDTO -> StreamContextDomain 변환 어댑터.

    전제:
    - 입력은 이미 I/O 경계에서 Pydantic 검증이 끝난 CommandDTO 입니다.
    - 따라서 여기서는 추가 검증/직렬화 없이 내부 도메인으로만 매핑합니다.
    """
    # 방어 로직 (Pydantic 케이스):
    # - 만약 다른 Pydantic 모델이 넘어오면 혼동을 막기 위해 명시적으로 거부합니다.
    if isinstance(dto, BaseModel) and not isinstance(dto, CommandDTO):
        raise TypeError(
            "adapter_stream_context expects CommandDTO (validated Pydantic model)"
        )

    # scope 구성
    target: dict[str, str] = dto.target
    scope: ConnectionScopeDomain = to_connection_scope(target)

    # 나머지 필드 매핑
    socket_params: SocketParams = dto.connection.socket_params

    logger.debug(
        "adapted to StreamContextDomain",
        extra={
            "exchange": scope.exchange,
            "region": scope.region,
            "request_type": scope.request_type,
        },
    )
    return StreamContextDomain(
        scope=scope,
        socket_params=socket_params,
        url=dto.connection.url,
        symbols=tuple(dto.symbols),
        projection=dto.projection,
    )
