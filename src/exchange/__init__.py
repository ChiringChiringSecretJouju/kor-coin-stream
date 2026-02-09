from .asia import (
    BinanceWebsocketHandler,
    BybitWebsocketHandler,
    OKXWebsocketHandler,
)
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


WAsianExchangeHandler = (
    BinanceWebsocketHandler
    | BybitWebsocketHandler
    | OKXWebsocketHandler
)


__all__ = [
    "WKoreanExchangeHandler",
    "UpbitWebsocketHandler",
    "KorbitWebsocketHandler",
    "BithumbWebsocketHandler",
    "CoinoneWebsocketHandler",
    "WAsianExchangeHandler",
    "BinanceWebsocketHandler",
    "BybitWebsocketHandler",
    "OKXWebsocketHandler",
]
