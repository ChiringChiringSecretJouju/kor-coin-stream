from core.connection._base_handler import BaseGlobalWebsocketHandler
from typing import Any


class CoinbaseWebsocketHandler(BaseGlobalWebsocketHandler):
    """코인베이스 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Coinbase는 ping/pong 프레임 사용
        self.set_heartbeat(kind="frame")

    async def prepare_subscription_message(
        self, params: dict[str, Any] | list[Any]
    ) -> str:
        """Coinbase 구독 메시지 형식으로 변환"""
        # Coinbase WebSocket API 형식에 맞게 구독 메시지 생성
        # 예: {"type": "subscribe", "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]}
        return await super()._sending_socket_parameter(params)


class KrakenWebsocketHandler(BaseGlobalWebsocketHandler):
    """크라켄 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Kraken은 ping/pong 프레임 사용
        self.set_heartbeat(kind="frame")

    async def prepare_subscription_message(
        self, params: dict[str, Any] | list[Any]
    ) -> str:
        """Kraken 구독 메시지 형식으로 변환"""
        # Kraken WebSocket API 형식에 맞게 구독 메시지 생성
        # 예: {"event": "subscribe", "pair": ["XBT/USD"], "subscription": {"name": "ticker"}}
        return await super()._sending_socket_parameter(params)
