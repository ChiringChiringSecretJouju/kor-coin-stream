from __future__ import annotations

from typing import Final

from redis.exceptions import RedisError

from src.common.logger import PipelineLogger
from src.core.dto.internal.cache import WebsocketConnectionSpecDomain
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.io.commands import CommandDTO
from src.core.types import (
    CONNECTION_STATUS_CONNECTED,
    CONNECTION_STATUS_CONNECTING,
)
from src.infra.cache.cache_store import WebsocketConnectionCache

logger: Final = PipelineLogger.get_logger("consumer", "cache_coordinator")


class CacheCoordinator:
    """웹소켓 연결 캐시(메타/심볼) 조정 책임.

    - 중복 연결 방지
    - 심볼 변경 시 원자적 교체 및 TTL 갱신
    - 최초 요청 시 CONNECTING 상태 세팅
    """

    async def handle_and_maybe_skip(self, command: CommandDTO) -> bool:
        """중복 연결 및 심볼 업데이트 처리.

        Args:
            command: 검증된 CommandDTO 객체
        Returns:
            True 이면 추가 처리(연결 생성)를 스킵, False 이면 계속 진행
        """
        exchange_name: str = command.target["exchange"]
        region: str = command.target["region"]
        request_type: str = command.target["request_type"]
        symbols: list[str] = command.symbols

        if not symbols:
            return False

        spec = WebsocketConnectionSpecDomain(
            scope=ConnectionScopeDomain(
                region=region,
                exchange=exchange_name,
                request_type=request_type,
            ),
            symbols=tuple(symbols),
        )
        cache = WebsocketConnectionCache(spec)
        existing = await cache.check_connection_exists()
        if existing and existing["status"] in (
            CONNECTION_STATUS_CONNECTING,
            CONNECTION_STATUS_CONNECTED,
        ):
            try:
                symbols_data = existing.get("symbols", [])
                current_symbols = (
                    set(symbols_data) if isinstance(symbols_data, list) else set()
                )
                requested_symbols = set(symbols)
                if current_symbols != requested_symbols:
                    await cache.replace_symbols(symbols)
                    await cache.set_connection_state(
                        status=CONNECTION_STATUS_CONNECTED,
                        scope=spec.scope,
                    )
                    logger.info(
                        msg=(
                            f"심볼 업데이트 반영: {exchange_name}/{region}/{request_type} "
                            f"{sorted(list(current_symbols))} -> {sorted(list(requested_symbols))}"
                        ),
                    )
                else:
                    logger.info(
                        msg=f"중복 연결 요청 스킵: {exchange_name}/{region}/{symbols}",
                    )
            except RedisError:
                # 업데이트 중 문제가 있어도 중복 연결 생성은 하지 않음
                logger.info(
                    msg=(
                        "중복 연결 요청 처리 중 오류(심볼 업데이트 시도, RedisError): "
                        f"{exchange_name}/{region}/{symbols}"
                    ),
                )
            return True

        # 경합 방지: 우선 CONNECTING 표시
        await cache.set_connection_state(
            status=CONNECTION_STATUS_CONNECTING,
            scope=spec.scope,
        )
        return False
