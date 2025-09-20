from core.connection._base_handler import BaseGlobalWebsocketHandler
from typing import Any


class BinanceWebsocketHandler(BaseGlobalWebsocketHandler):
    """바이낸스 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Binance는 일반적으로 ping/pong 프레임 사용
        self.set_heartbeat(kind="frame")

    async def prepare_subscription_message(
        self, params: dict[str, Any] | list[Any]
    ) -> str:
        """Binance 구독 메시지 형식으로 변환"""
        # Binance WebSocket API 형식에 맞게 구독 메시지 생성
        # 예: {"method": "SUBSCRIBE", "params": ["btcusdt@ticker"], "id": 1}
        return await super()._sending_socket_parameter(params)


class BybitWebsocketHandler(BaseGlobalWebsocketHandler):
    """바이비트 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Bybit은 ping 메시지 전송 방식 사용
        self.set_heartbeat(kind="text", message='{"op":"ping"}')

    async def prepare_subscription_message(
        self, params: dict[str, Any] | list[Any]
    ) -> str:
        """Bybit 구독 메시지 형식으로 변환"""
        # Bybit WebSocket API 형식에 맞게 구독 메시지 생성
        # 예: {"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]}
        return await super()._sending_socket_parameter(params)


class GateioWebsocketHandler(BaseGlobalWebsocketHandler):
    """게이트아이오 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Gate.io는 ping 메시지 전송 방식 사용
        self.set_heartbeat(kind="text", message='{"method": "ping"}')

    async def prepare_subscription_message(
        self, params: dict[str, Any] | list[Any]
    ) -> str:
        """Gate.io 구독 메시지 형식으로 변환"""
        # Gate.io WebSocket API 형식에 맞게 구독 메시지 생성
        # 예: {"method": "ticker.subscribe", "params": ["BTC_USDT"], "id": 1}
        return await super()._sending_socket_parameter(params)


class HuobiWebsocketHandler(BaseGlobalWebsocketHandler):
    """후오비(HTX) 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Huobi는 ping 메시지 전송 방식 사용
        self.set_heartbeat(kind="text", message='{"ping": 1}')

    async def prepare_subscription_message(
        self, params: dict[str, Any] | list[Any]
    ) -> str:
        """Huobi 구독 메시지 형식으로 변환"""
        # Huobi WebSocket API 형식에 맞게 구독 메시지 생성
        # 예: {"sub": "market.btcusdt.ticker", "id": "id1"}
        return await super()._sending_socket_parameter(params)


class OKXWebsocketHandler(BaseGlobalWebsocketHandler):
    """오케이엑스 거래소 웹소켓 핸들러"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # OKX는 ping 메시지 전송 방식 사용
        self.set_heartbeat(kind="text", message="ping")

    async def prepare_subscription_message(
        self, params: dict[str, Any] | list[Any]
    ) -> str:
        """OKX 구독 메시지 형식으로 변환"""
        # OKX WebSocket API 형식에 맞게 구독 메시지 생성
        # 예: {"op": "subscribe", "args": [{"channel": "tickers", "instId": "BTC-USDT"}]}
        return await super()._sending_socket_parameter(params)
