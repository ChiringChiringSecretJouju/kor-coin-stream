# Trade 파서 모듈

## 📋 개요

전 세계 거래소의 Trade 메시지를 **Upbit 표준 포맷**으로 변환하는 통합 파서 모듈입니다.

**Strategy Pattern**을 사용하여 거래소별 메시지 구조를 자동 감지하고, **Pydantic DTO**로 런타임 검증을 수행합니다.

---

## 🌍 지원 거래소

### 한국 (Korea)
- **Upbit** - 기준 포맷 (변환 없음)
- **Bithumb** - Upbit와 동일 구조
- **Korbit** - `symbol` → `code` 변환, 데이터 평탄화
- **Coinone** - `target_currency` + `quote_currency` 조합

### 아시아 (Asia)
- **Binance Spot** - Raw Trade (`e: "trade"`)
- **Bybit v5** - `publicTrade.{symbol}` 토픽
- **Huobi Spot** - `market.{symbol}.trade.detail` 채널
- **OKX Spot** - `trades` 채널

### 북미 (North America)
- **Coinbase Advanced Trade** - `market_trades` 채널
- **Kraken v2** - `trade` 채널

---

## 🏗️ 아키텍처

```
trades/
├── korea/          # 한국 거래소
│   ├── base.py          # ABC 인터페이스
│   ├── upbit.py         # 기준 포맷
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

## 🎯 표준 포맷 (Upbit 기준)

```python
class StandardTradeDTO(BaseModel):
    code: MarketCodeStr              # "KRW-BTC", "BTC-USDT"
    trade_timestamp: StrictInt       # Unix milliseconds
    trade_price: StrictFloat         # 체결 가격
    trade_volume: StrictFloat        # 체결 수량
    ask_bid: Literal["ASK", "BID"]  # 매수/매도
    sequential_id: SequentialIdStr   # 거래 고유 ID
    
    model_config = OPTIMIZED_CONFIG  # frozen=True, slots=True
```

---

## 📚 사용법

### 한국 거래소
```python
from src.core.connection.utils.trades.korea import get_korea_trade_dispatcher

dispatcher = get_korea_trade_dispatcher()

# Upbit 메시지 (기준 포맷)
upbit_msg = {
    "code": "KRW-BTC",
    "trade_timestamp": 1730336862047,
    "trade_price": 100473000.0,
    "trade_volume": 0.00014208,
    "ask_bid": "BID",
    "sequential_id": "17303368620470000"
}

trade = dispatcher.parse(upbit_msg)
# → StandardTradeDTO (검증 완료)
```

### 아시아 거래소
```python
from src.core.connection.utils.trades.asia import get_asia_trade_dispatcher

dispatcher = get_asia_trade_dispatcher()

# Binance 메시지
binance_msg = {
    "e": "trade",
    "s": "BTCUSDT",
    "t": 12345,
    "p": "50000.00",
    "q": "0.01",
    "T": 1672515782136,
    "m": False  # False=BUY, True=SELL
}

trade = dispatcher.parse(binance_msg)
# → StandardTradeDTO(code="BTC-USDT", ask_bid="BID", ...)
```

### 북미 거래소
```python
from src.core.connection.utils.trades.na import get_na_trade_dispatcher

dispatcher = get_na_trade_dispatcher()

# Kraken 메시지
kraken_msg = {
    "channel": "trade",
    "data": [{
        "symbol": "BTC/USD",
        "side": "buy",
        "price": 50000.0,
        "qty": 0.01,
        "trade_id": 4665906,
        "timestamp": "2023-09-25T07:49:37.708706Z"
    }]
}

trade = dispatcher.parse(kraken_msg)
# → StandardTradeDTO(code="BTC-USD", ask_bid="BID", ...)
```

---

## 🔄 Side 필드 변환 규칙

| 거래소 | 원본 형식 | 변환 규칙 |
|--------|----------|-----------|
| **Binance** | `m: bool` | `True` → ASK, `False` → BID |
| **Bybit** | `S: "Buy"/"Sell"` | `"Buy"` → BID, `"Sell"` → ASK |
| **Huobi** | `direction: "buy"/"sell"` | `"buy"` → BID, `"sell"` → ASK |
| **OKX** | `side: "buy"/"sell"` | `"buy"` → BID, `"sell"` → ASK |
| **Coinbase** | `side: "BUY"/"SELL"` | `"BUY"` → BID, `"SELL"` → ASK |
| **Kraken** | `side: "buy"/"sell"` | `"buy"` → BID, `"sell"` → ASK |

---

## 🔄 Symbol 포맷 변환

| 거래소 | 원본 | 변환 후 |
|--------|------|---------|
| **Binance** | `BTCUSDT` | `BTC-USDT` |
| **Bybit** | `BTCUSDT` | `BTC-USDT` |
| **Huobi** | `btcusdt` | `BTC-USDT` |
| **OKX** | `BTC-USDT` | `BTC-USDT` (유지) |
| **Coinbase** | `BTC-USD` | `BTC-USD` (유지) |
| **Kraken** | `BTC/USD` | `BTC-USD` |

---

## 🕐 Timestamp 변환

### Unix Timestamp (밀리초) - 직접 사용
- Binance: `T` 필드
- Bybit: `T` 필드
- Huobi: `ts` 필드
- OKX: `ts` 필드 (문자열 → int)

### ISO 8601 → Unix 변환
- Coinbase: `"2019-08-14T20:42:27.265Z"`
- Kraken: `"2023-09-25T07:49:37.708706Z"`

```python
from datetime import datetime

def parse_iso_timestamp(time_str: str) -> int:
    dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)
```

---

## ⚠️ 주의사항

### 1. 배치 메시지 처리
일부 거래소는 **data 배열**에 여러 거래를 포함:
- Bybit: `data: [trade1, trade2, ...]`
- Huobi: `tick.data: [trade1, trade2, ...]`
- OKX: `data: [trade1, trade2, ...]`

**현재 구현**: 첫 번째 거래만 반환  
**TODO**: 모든 거래를 리스트로 반환하도록 확장

### 2. 빈 메시지 처리
빈 데이터 수신 시 기본값 DTO 반환:
```python
StandardTradeDTO(
    code="UNKNOWN",
    trade_timestamp=0,
    trade_price=0.0,
    trade_volume=0.0,
    ask_bid="BID",
    sequential_id="0"
)
```

---

## 🎨 거래소별 특징

| 거래소 | Price/Volume 타입 | Timestamp 형식 | 특이사항 |
|--------|------------------|----------------|----------|
| **Binance** | 문자열 | Unix (ms) | m 필드가 bool |
| **Bybit** | 문자열 | Unix (ms) | data 배열 |
| **Huobi** | 숫자 | Unix (ms) | ch에서 심볼 추출 |
| **OKX** | 문자열 | Unix (ms, 문자열) | instId 그대로 |
| **Coinbase** | 문자열 | ISO 8601 | trades 배열 |
| **Kraken** | 숫자 | ISO 8601 | data 배열 |

---

## 🚀 성능 최적화

- **Singleton Dispatcher**: 인스턴스 재사용
- **Pydantic Slots**: 메모리 최적화
- **Frozen DTO**: 캐시 가능, 스레드 안전
- **Early Return**: 첫 매치 시 즉시 반환
- **문자열 변환 최소화**: 필요한 곳에서만 변환

---

## 📝 TODO

- [ ] 배치 메시지 전체 처리 (리스트 반환)
- [ ] 로깅 추가 (디버깅 용이)
- [ ] 에러 메시지 개선
- [ ] Aggregate Trade vs Raw Trade 구분
- [ ] 성능 벤치마크
