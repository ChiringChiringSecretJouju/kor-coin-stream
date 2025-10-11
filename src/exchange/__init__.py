from .korea import (
    BithumbWebsocketHandler,
    CoinoneWebsocketHandler,
    KorbitWebsocketHandler,
    UpbitWebsocketHandler,
)

WKoreanExchangeHandler = (
    UpbitWebsocketHandler
    | KorbitWebsocketHandler
    | BithumbWebsocketHandler
    | CoinoneWebsocketHandler
)

__all__ = [
    "WKoreanExchangeHandler",
    "UpbitWebsocketHandler",
    "KorbitWebsocketHandler",
    "BithumbWebsocketHandler",
    "CoinoneWebsocketHandler",
]
