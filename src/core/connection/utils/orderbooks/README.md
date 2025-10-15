# Orderbook 파서 모듈

## 📋 개요

전 세계 거래소의 OrderBook 메시지를 표준 포맷으로 변환하는 통합 파서 모듈입니다.

**Strategy Pattern**을 사용하여 거래소별 메시지 구조를 자동 감지하고, **Pydantic DTO**로 런타임 검증을 수행합니다.

---

## 🌍 지원 거래소

### 한국 (Korea)
- **Upbit** - `orderbook_units` 쌍 구조
- **Bithumb** - Upbit와 동일
- **Korbit** - `data.asks/bids` 분리 구조
- **Coinone** - 이미 분리된 구조

### 아시아 (Asia)
- **Binance Spot** - `depthUpdate` 이벤트
- **Bybit v5** - `orderbook.{depth}.{symbol}` 토픽
- **Huobi Spot** - `market.{symbol}.depth.{type}` 채널
- **OKX Spot** - `books` 채널

### 북미 (North America)
- **Coinbase Advanced Trade** - `l2_data` 채널
- **Kraken v2** - `book` 채널

---

## 🏗️ 아키텍처

```
orderbooks/
├── korea/          # 한국 거래소
│   ├── base.py          # ABC 인터페이스 + parse_symbol 유틸
│   ├── upbit.py
│   ├── bithumb.py
│   ├── coinone.py
│   ├── korbit.py
│   └── dispatcher.py    # 자동 선택 디스패처
│
├── asia/           # 아시아 거래소
│   ├── base.py
│   ├── binance.py
│   ├── bybit.py
│   ├── huobi.py
│   ├── okx.py
│   └── dispatcher.py
│
└── na/             # 북미 거래소
    ├── base.py
    ├── coinbase.py
    ├── kraken.py
    └── dispatcher.py
```

---

## 🎯 핵심 원리

### 1. Strategy Pattern
```python
# 파서 인터페이스 (ABC)
class OrderbookParser(ABC):
    @abstractmethod
    def can_parse(self, message: dict[str, Any]) -> bool:
        """파싱 가능 여부 판단"""
        pass
    
    @abstractmethod
    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        """표준 포맷으로 변환"""
        pass
```

### 2. 자동 감지 (Chain of Responsibility)
```python
class AsiaOrderbookDispatcher:
    def __init__(self):
        self._parsers = [
            HuobiOrderbookParser(),   # 가장 구체적
            BybitOrderbookParser(),
            OKXOrderbookParser(),
            BinanceOrderbookParser(), # 가장 일반적
        ]
    
    def parse(self, message: dict[str, Any]) -> StandardOrderbookDTO:
        for parser in self._parsers:
            if parser.can_parse(message):
                return parser.parse(message)
        raise ValueError("Unsupported format")
```

### 3. 표준 DTO (Pydantic)
```python
class StandardOrderbookDTO(BaseModel):
    symbol: SymbolStr              # "BTC"
    quote_currency: QuoteCurrencyStr  # "USDT"
    timestamp: StrictInt           # Unix milliseconds
    asks: list[OrderbookItemDTO]
    bids: list[OrderbookItemDTO]
    
    model_config = OPTIMIZED_CONFIG  # frozen=True, slots=True
```

---

## 📚 사용법

### 한국 거래소
```python
from src.core.connection.utils.orderbooks.korea import get_korea_orderbook_dispatcher

dispatcher = get_korea_orderbook_dispatcher()

# Upbit 메시지
upbit_msg = {
    "code": "KRW-BTC",
    "orderbook_units": [
        {"ask_price": 100000, "ask_size": 0.1, "bid_price": 99000, "bid_size": 0.2}
    ]
}

orderbook = dispatcher.parse(upbit_msg)
# → StandardOrderbookDTO(symbol="BTC", quote_currency="KRW", ...)
```

### 아시아 거래소
```python
from src.core.connection.utils.orderbooks.asia import get_asia_orderbook_dispatcher

dispatcher = get_asia_orderbook_dispatcher()

# Binance 메시지
binance_msg = {
    "e": "depthUpdate",
    "s": "BTCUSDT",
    "b": [["50000.00", "1.5"]],
    "a": [["50001.00", "0.8"]]
}

orderbook = dispatcher.parse(binance_msg)
# → StandardOrderbookDTO(symbol="BTC", quote_currency="USDT", ...)
```

### 북미 거래소
```python
from src.core.connection.utils.orderbooks.na import get_na_orderbook_dispatcher

dispatcher = get_na_orderbook_dispatcher()

# Kraken 메시지
kraken_msg = {
    "channel": "book",
    "data": [{
        "symbol": "BTC/USD",
        "bids": [{"price": 50000, "qty": 1.5}],
        "asks": [{"price": 50001, "qty": 0.8}]
    }]
}

orderbook = dispatcher.parse(kraken_msg)
# → StandardOrderbookDTO(symbol="BTC", quote_currency="USD", ...)
```

---

## 🔄 데이터 변환 흐름

```
원본 메시지 (거래소별 포맷)
    ↓
can_parse() → 거래소 자동 감지
    ↓
parse() → 표준 DTO 변환
    ↓
Pydantic 검증 (런타임)
    ↓
StandardOrderbookDTO
    ↓
Kafka 전송 / 로컬 처리
```

---

## ✅ 장점

1. **확장 용이**: 새 거래소 추가 시 파서 클래스만 추가
2. **타입 안전**: Pydantic 자동 검증
3. **불변성**: `frozen=True`로 버그 방지
4. **자동 감지**: 거래소 명시 불필요
5. **재사용**: 한국 거래소 패턴을 글로벌에 그대로 적용

---

## 🎨 거래소별 특징

| 거래소 | Price/Size 타입 | 구조 | 특이사항 |
|--------|----------------|------|----------|
| **Binance** | 문자열 | 배열 | - |
| **Bybit** | 문자열 | 배열 | Spot/Linear 통합 |
| **Huobi** | **숫자** | 배열 | ch에서 심볼 추출 |
| **OKX** | 문자열 | 배열(4개) | [price, size, 0, count] |
| **Coinbase** | 문자열 | 객체 | price_level/new_quantity |
| **Kraken** | **숫자** | 객체 | price/qty |

---

## 🚀 성능 최적화

- **Singleton Dispatcher**: 인스턴스 재사용
- **Pydantic Slots**: 메모리 최적화
- **Frozen DTO**: 캐시 가능, 스레드 안전
- **Early Return**: 첫 매치 시 즉시 반환

---

## 📝 TODO

- [ ] 로깅 추가 (디버깅 용이)
- [ ] 에러 메시지 개선 (힌트 제공)
- [ ] 배치 처리 지원 (여러 메시지 동시 파싱)
- [ ] 성능 벤치마크
