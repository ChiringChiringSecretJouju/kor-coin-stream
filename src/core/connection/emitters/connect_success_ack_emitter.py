from __future__ import annotations

from typing import Iterable

from src.common.exceptions.error_dispatcher import dispatch_error
from src.common.logger import PipelineLogger
from src.core.connection._utils import extract_symbol as _extract_symbol_impl
from src.core.dto.internal.common import ConnectionScopeDomain
from src.core.dto.io.commands import ConnectSuccessEventDTO, ConnectSuccessMetaDTO
from src.core.dto.io.target import ConnectionTargetDTO
from src.infra.messaging.connect.producer_client import ConnectSuccessEventProducer

logger = PipelineLogger.get_logger("ack_emitter", "connection")


def _normalize_coin_symbol(raw: str) -> str:
    """extract_symbol() 규칙과 동일한 토큰 정규화.

    - extract_symbol({"symbol": raw})가 "BTC_COUNT" 형식이면 "BTC" 반환
    - 폴백: 하이픈/언더스코어 기준 분리 후 대문자화
    """
    try:
        extracted = _extract_symbol_impl({"symbol": raw})
        if isinstance(extracted, str) and extracted.endswith("_COUNT"):
            return extracted[:-6]
        token = raw
        if "-" in raw:
            token = raw.rsplit("-", 1)[-1]
        elif "_" in raw:
            token = raw.split("_", 1)[0]
        elif "/" in raw:
            token = raw.split("/", 1)[0]
        elif "@" in raw:
            token = raw.split("@", 1)[0]
        return token.upper()
    except Exception:
        return str(raw).upper()


class ConnectSuccessAckEmitter:
    """연결 성공(ACK) 이벤트 전담 방출기.

    - topic: ws.connect_success_{region}
    - key: "{exchange}|{region}|{request_type}|{coin_symbol}"
    - DTO: ConnectSuccessEventDTO
    """

    def __init__(
        self,
        scope: ConnectionScopeDomain,
        producer: ConnectSuccessEventProducer | None = None,
    ) -> None:
        self.scope = scope
        self._producer = producer or ConnectSuccessEventProducer()
        self._target = ConnectionTargetDTO(
            exchange=self.scope.exchange,
            region=self.scope.region,
            request_type=self.scope.request_type,
        )
        self._observed_key = (
            f"{self.scope.exchange}/{self.scope.region}/{self.scope.request_type}"
        )

    async def emit_for_symbols(self, symbols: Iterable[str]) -> bool:
        """심볼 목록에 대해 ACK 이벤트를 발행.

        Returns: 모든 전송이 성공적으로 호출되면 True (중간 실패는 warning 로그)
        """
        ok = True
        for s in symbols:
            coin = _normalize_coin_symbol(s)
            event = ConnectSuccessEventDTO(
                action="connect_success",
                ack="clear",
                target=self._target,
                symbol=coin,
                meta=ConnectSuccessMetaDTO(
                    observed_key=self._observed_key,
                ),
            )
            key = f"{self._target.exchange}|{self._target.region}|{self._target.request_type}|{coin}"  # noqa: E501
            try:
                await self._producer.send_event(event, key=key)
            except Exception as e:
                ok = False
                logger.warning(
                    f"ACK emit failed: {self._observed_key} symbol={coin} error={e}"
                )
                # ACK 전송 실패를 ws.error로도 발행하여 관측 가능성 확보
                try:
                    context = {
                        "phase": "ack_emit",
                        "symbol": coin,
                        "key": key,
                        "error_message": str(e),
                    }
                    await dispatch_error(
                        exc=e if isinstance(e, Exception) else Exception(str(e)),
                        kind="ack",
                        target=self._target,
                        context=context,
                    )
                except Exception:
                    # 에러 발행 자체 실패는 무시(로그만 남김)
                    logger.warning(
                        f"ACK error emit failed: {self._observed_key} symbol={coin}"
                    )
        return ok

    async def aclose(self) -> None:
        """내부 프로듀서 자원 정리"""
        try:
            await self._producer.stop_producer()
        except Exception:
            pass
