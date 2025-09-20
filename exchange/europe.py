from core.connection._base_handler import BaseGlobalWebsocketHandler
from typing import Any


class BitfinexWebsocketHandler(BaseGlobalWebsocketHandler):
    """비트파이넥스 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Bitfinex는 ping/pong 프레임 사용
        self.set_heartbeat(kind="frame")

    async def prepare_subscription_message(
        self, params: dict[str, Any] | list[Any]
    ) -> str:
        """Bitfinex 구독 메시지 형식으로 변환"""
        # Bitfinex WebSocket API 형식에 맞게 구독 메시지 생성
        # 예: {"event": "subscribe", "channel": "ticker", "symbol": "BTCUSD"}
        return await super()._sending_socket_parameter(params)
