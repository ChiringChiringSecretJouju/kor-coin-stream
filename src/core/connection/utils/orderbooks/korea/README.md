# 한국 거래소 Orderbook 파서

거래소별 orderbook 메시지를 표준 포맷으로 변환합니다.

## 📁 폴더 구조

```
parsers/orderbooks/korea/
├── base.py           # 인터페이스 + TypedDict + 공통 함수
├── upbit.py          # Upbit (Pair → Separate)
├── bithumb.py        # Bithumb (Upbit 상속)
├── korbit.py         # Korbit (qty → size)
├── coinone.py        # Coinone (qty → size)
├── dispatcher.py     # 자동 파서 선택
├── __init__.py       # 공개 API
└── README.md         # 문서
```

## 🎯 표준 포맷 (TypedDict)

### StandardOrderbook

```python
class StandardOrderbook(TypedDict):
    symbol: str              # 심볼 (BTC, ETH 등)
    quote_currency: str      # 기준 통화 (KRW, USDT 등)
    timestamp: int           # 현재 시각 (ms)
    asks: list[OrderbookItem]
    bids: list[OrderbookItem]

class OrderbookItem(TypedDict):
    price: str               # 가격
    size: str                # 수량
```

### 출력 예시

```python
{
    "symbol": "BTC",
    "quote_currency": "KRW",
    "timestamp": 1746601573804,
    "asks": [
        {"price": "137002000", "size": "0.10623869"},
        {"price": "137023000", "size": "0.06144079"}
    ],
    "bids": [
        {"price": "137001000", "size": "0.03656812"},
        {"price": "137000000", "size": "0.33543284"}
    ]
}
```

## 🔧 거래소별 변환

| 거래소 | 원본 구조 | 변환 |
|--------|-----------|------|
| **Upbit** | `orderbook_units[i]` (Pair) | → `asks[]`, `bids[]` (Separate) |
| **Bithumb** | Upbit과 동일 | Upbit 상속 |
| **Korbit** | `data.asks[]`, `data.bids[]` | `qty` → `size` |
| **Coinone** | `data.asks[]`, `data.bids[]` | `qty` → `size` |

## 💡 사용법

```python
from src.core.connection.parsers import get_korea_orderbook_dispatcher

# 자동 파서 선택
dispatcher = get_korea_orderbook_dispatcher()

# Upbit 메시지
upbit_message = {
    "code": "KRW-BTC",
    "orderbook_units": [
        {"ask_price": 137002000, "bid_price": 137001000, 
         "ask_size": 0.106, "bid_size": 0.036}
    ]
}
result: StandardOrderbook = dispatcher.parse(upbit_message)
# → {"symbol": "BTC", "quote_currency": "KRW", "asks": [...], "bids": [...]}

# Korbit 메시지
korbit_message = {
    "symbol": "btc_krw",
    "data": {
        "asks": [{"price": "73304000", "qty": "0.00985212"}],
        "bids": [{"price": "73303000", "qty": "0.00898326"}]
    }
}
result: StandardOrderbook = dispatcher.parse(korbit_message)
# → {"symbol": "BTC", "quote_currency": "KRW", "asks": [...], "bids": [...]}
```

## ✨ 타입 안전성

### TypedDict 사용

```python
from src.core.connection.parsers import StandardOrderbook, OrderbookItem

# 타입 체크 완벽 지원
def process_orderbook(data: StandardOrderbook) -> None:
    symbol: str = data["symbol"]  # ✅ 타입 안전
    asks: list[OrderbookItem] = data["asks"]  # ✅ 타입 안전
    
    for item in asks:
        price: str = item["price"]  # ✅ 타입 안전
        size: str = item["size"]    # ✅ 타입 안전
```

### 강화된 타입힌트

```python
class UpbitOrderbookParser(OrderbookParser):
    def can_parse(self, message: dict[str, Any]) -> bool:
        """타입 시그니처 명확"""
        ...
    
    def parse(self, message: dict[str, Any]) -> StandardOrderbook:
        """반환 타입 TypedDict"""
        code: str = message.get("code", "")
        units: list[dict[str, Any]] = message.get("orderbook_units", [])
        
        asks: list[OrderbookItem] = [...]
        bids: list[OrderbookItem] = [...]
        
        return StandardOrderbook(
            symbol=symbol,
            quote_currency=quote or "",
            timestamp=int(time.time() * 1000),
            asks=asks,
            bids=bids,
        )
```

## 🎨 공통 코드 패턴

### 모든 파서가 동일한 구조

```python
def parse(self, message: dict[str, Any]) -> StandardOrderbook:
    # 1. 심볼 추출 (타입 명시)
    symbol: str
    quote: str | None
    symbol, quote = parse_symbol(...)
    
    # 2. asks/bids 변환 (리스트 컴프리헨션 + 타입)
    asks: list[OrderbookItem] = [
        {"price": str(...), "size": str(...)}
        for item in ...
        if isinstance(item, dict) and "price" in item
    ]
    
    bids: list[OrderbookItem] = [
        {"price": str(...), "size": str(...)}
        for item in ...
        if isinstance(item, dict) and "price" in item
    ]
    
    # 3. TypedDict 반환
    return StandardOrderbook(
        symbol=symbol,
        quote_currency=quote or "",
        timestamp=int(time.time() * 1000),
        asks=asks,
        bids=bids,
    )
```

## 🚀 개선사항

| 항목 | 개선 |
|------|------|
| **폴더 구조** | `parsers/orderbooks/korea/` 계층화 |
| **타입 안전성** | TypedDict + 강화된 타입힌트 |
| **필드명 통일** | `price`, `size` 100% 통일 |
| **코드 일관성** | 모든 파서가 동일한 패턴 |
| **리스트 컴프리헨션** | 간결하고 파이썬스러운 코드 |

## 📦 API

```python
from src.core.connection.parsers import (
    OrderbookItem,              # TypedDict: {"price": str, "size": str}
    StandardOrderbook,          # TypedDict: 표준 포맷
    get_korea_orderbook_dispatcher,  # 디스패처 싱글톤
)
```
