# Exchange 모듈

거래소별 WebSocket 핸들러 및 플러그인 시스템

## 개요

**Exchange 모듈**은 각 암호화폐 거래소의 WebSocket API를 통합하는 Adapter 레이어입니다. 플러그인 아키텍처를 통해 새로운 거래소를 5-10분 만에 추가할 수 있습니다.

### 이 모듈의 역할

1. **거래소별 Adapter 구현**: 각 거래소의 WebSocket API 프로토콜 통합
2. **플러그인 시스템**: 핸들러 동적 선택 및 등록
3. **프로토콜 표준화**: 다양한 거래소 → 통일된 인터페이스
4. **거래소 특화 로직**: GZIP 압축, 특수 heartbeat 등

## 📁 파일 구조

```
exchange/
├─ __init__.py         # HANDLER_MAP 및 Factory
├─ korea.py           # 한국 거래소 (4개)
├─ asia.py            # 아시아 거래소 (4개)
├─ na.py              # 북미 거래소 (2개)
└─ europe.py          # 유럽 거래소 (1개)
```

**파일 수**: 5개  
**총 코드 라인**: ~1,000줄  
**지원 거래소**: 11개

## 지원 거래소

### 한국 (korea.py)
```python
- UpbitWebsocketHandler      # Upbit
- BithumbWebsocketHandler    # Bithumb
- CoinoneWebsocketHandler    # Coinone (단일 구독*)
- KorbitWebsocketHandler     # Korbit
```

### 아시아 (asia.py)
```python
- BinanceWebsocketHandler    # Binance
- BybitWebsocketHandler      # Bybit
- OKXWebsocketHandler        # OKX
- HuobiWebsocketHandler      # Huobi (단일 구독*, GZIP 압축)
```

### 북미 (na.py)
```python
- CoinbaseWebsocketHandler   # Coinbase
- KrakenWebsocketHandler     # Kraken
```

### 유럽 (europe.py)
```python
- BitfinexWebsocketHandler   # Bitfinex
```

**단일 구독***: 한 연결당 하나의 심볼만 구독 가능 (자동 분리 처리)

## 핵심 컴포넌트

### 1. **HANDLER_MAP** (Factory Pattern)

```python
# exchange/__init__.py
HANDLER_MAP: dict[str, dict[str, type]] = {
    "korea": {
        "upbit": UpbitWebsocketHandler,
        "bithumb": BithumbWebsocketHandler,
        "coinone": CoinoneWebsocketHandler,
        "korbit": KorbitWebsocketHandler,
    },
    "asia": {
        "binance": BinanceWebsocketHandler,
        "bybit": BybitWebsocketHandler,
        "okx": OKXWebsocketHandler,
        "huobi": HuobiWebsocketHandler,
    },
    "na": {
        "coinbase": CoinbaseWebsocketHandler,
        "kraken": KrakenWebsocketHandler,
    },
    "europe": {
        "bitfinex": BitfinexWebsocketHandler,
    },
}
```

**사용법:**
```python
# 동적 핸들러 선택
HandlerClass = HANDLER_MAP[region][exchange]
handler = HandlerClass(scope, projection, orchestrator)
```

### 2. **거래소 핸들러 구조**

#### 기본 템플릿

```python
class UpbitWebsocketHandler(BaseKoreaWebsocketHandler):
    """
    Upbit 거래소 WebSocket 핸들러
    
    자동 제공 기능 (BaseHandler로부터):
    - 지수 백오프 재연결
    - 하트비트 관리
    - 에러 처리 및 DLQ
    - 메트릭 수집
    - 구독 관리
    
    커스터마이징 필수:
    - _get_subscribe_message()
    - (선택) _extract_symbol()
    - (선택) _parse_message()
    """
    
    def _get_subscribe_message(
        self, 
        symbols: list[str]
    ) -> dict[str, Any]:
        """
        구독 메시지 생성 (거래소 API 형식)
        
        Upbit 형식:
        [
            {"ticket": "test"},
            {"type": "ticker", "codes": ["KRW-BTC", "KRW-ETH"]}
        ]
        """
        return [
            {"ticket": "unique_ticket"},
            {
                "type": "ticker",
                "codes": [f"KRW-{s}" for s in symbols]
            }
        ]
```

#### 자동 제공 기능 (90%)

1. **연결 관리**
   - 지수 백오프 재연결 (1초 → 2초 → 4초 → ...)
   - 최대 재시도 횟수 제한
   - 안전한 정리 (cleanup)

2. **하트비트**
   - 주기적 ping/pong
   - 타임아웃 감지
   - 자동 재연결

3. **메시지 처리**
   - JSON 파싱
   - 메시지 타입별 라우팅 (ticker/orderbook/trade)
   - 에러 메시지 처리

4. **메트릭 수집**
   - 분 단위 배치 집계
   - 심볼별 카운트
   - Kafka 자동 발행

5. **에러 처리**
   - 표준화된 에러 이벤트 발행
   - DLQ 전송
   - 상관관계 ID 추적

### 3. **거래소별 특화 기능**

#### Huobi - GZIP 압축

```python
class HuobiWebsocketHandler(BaseGlobalWebsocketHandler):
    """
    Huobi 특화: 모든 메시지 GZIP 압축
    """
    
    @override
    def _parse_message(self, raw: bytes) -> dict | None:
        """GZIP 압축 해제"""
        # GZIP 매직 넘버 확인
        if raw[:2] == b'\x1f\x8b':
            decompressed = gzip.decompress(raw)
            return orjson.loads(decompressed)
        return super()._parse_message(raw)
    
    @override
    async def ticker_message(self, message: dict) -> None:
        """Huobi ping/pong 처리"""
        if "ping" in message:
            await self._ws.send_json({"pong": message["ping"]})
            return
        await super().ticker_message(message)
```

#### Binance - 심볼 추출

```python
class BinanceWebsocketHandler(BaseGlobalWebsocketHandler):
    """
    Binance 특화: "s" 필드에서 심볼 추출
    """
    
    @override
    def _extract_symbol(self, message: dict) -> str | None:
        """BTCUSDT → BTC_COUNT 변환"""
        symbol = message.get("s")  # "BTCUSDT"
        if isinstance(symbol, str) and symbol:
            quote_currencies = ["USDT", "BUSD", "USDC"]
            for quote in quote_currencies:
                if symbol.endswith(quote):
                    base = symbol[:-len(quote)]
                    return f"{base.upper()}_COUNT"
        return None
```

#### Coinone - 단일 구독

```python
# 단일 구독 거래소는 자동으로 심볼별 분리
SINGLE_SUBSCRIPTION_ONLY = frozenset({
    "coinone",  # 한국
    "huobi",    # 아시아
})

# StreamOrchestrator에서 자동 처리
if scope.exchange in SINGLE_SUBSCRIPTION_ONLY:
    # 심볼별로 별도 연결 생성
    for symbol in symbols:
        await create_connection(exchange, [symbol])
```

## 새 거래소 추가

### Step 1: 핸들러 클래스 생성

```python
# exchange/asia.py에 추가
class GateIOWebsocketHandler(BaseGlobalWebsocketHandler):
    """Gate.io 거래소 핸들러"""
    
    def _get_subscribe_message(self, symbols: list[str]) -> dict:
        """
        Gate.io 구독 메시지 형식:
        {
            "time": 123456789,
            "channel": "spot.tickers",
            "event": "subscribe",
            "payload": ["BTC_USDT", "ETH_USDT"]
        }
        """
        return {
            "time": int(time.time()),
            "channel": "spot.tickers",
            "event": "subscribe",
            "payload": [f"{s}_USDT" for s in symbols]
        }
```

### Step 2: HANDLER_MAP 등록

```python
# exchange/__init__.py
HANDLER_MAP = {
    "asia": {
        # ... 기존 핸들러들
        "gateio": GateIOWebsocketHandler,  # 추가!
    }
}
```

### Step 3: 테스트

```bash
# Kafka 메시지 발행 (ws.command 토픽)
{
  "type": "command",
  "action": "connect_and_subscribe",
  "target": {
    "exchange": "gateio",  # 새 거래소
    "region": "asia",
    "request_type": "ticker"
  },
  "connection": {
    "socket_params": {
      "subscribe_type": "ticker",
      "symbols": ["BTC", "ETH"]
    }
  }
}
```

### 자동 제공 기능

- 재연결 로직
- 하트비트 관리
- 에러 처리
- 메트릭 수집
- 구독 관리

## 메시지 처리 흐름

```
WebSocket 연결
  ↓
메시지 수신 (bytes)
  ↓
_parse_message() 
  ├─ GZIP 압축 해제 (Huobi)
  ├─ JSON 파싱 (orjson)
  └─ 검증
  ↓
메시지 타입 판별
  ├─ ticker → ticker_message()
  ├─ orderbook → orderbook_message()
  ├─ trade → trade_message()
  └─ error → error_message()
  ↓
_extract_symbol() (심볼 추출)
  ↓
메트릭 카운팅 (_counter.increment)
  ↓
실시간 데이터 발행 (Kafka)
```

## 설계 원칙

### 1. **플러그인 아키텍처**

```python
# 핵심 로직은 Base 클래스에
class BaseWebsocketHandler(ABC):
    # 90% 기능 제공
    
# 거래소별 커스터마이징만
class UpbitHandler(BaseWebsocketHandler):
    # 10% 커스터마이징
```

### 2. **Template Method 패턴**

```python
# Base 클래스가 흐름 정의
async def connect_and_run(self):
    while True:
        try:
            await self._connect()
            await self._subscribe()
            await self._receive_loop()  # 하위 클래스 구현
        except Exception:
            await self._reconnect()
```

### 3. **Open-Closed Principle**

```python
# ✅ 확장에는 열려있고
HANDLER_MAP["asia"]["new_exchange"] = NewHandler

# ✅ 수정에는 닫혀있음
# BaseHandler 코드 수정 불필요!
```

## 테스트

### 핸들러 테스트

```python
# tests/exchange/test_upbit.py
async def test_upbit_subscribe():
    handler = UpbitWebsocketHandler(
        scope=scope,
        projection=None,
        orchestrator=mock_orchestrator
    )
    
    message = handler._get_subscribe_message(["BTC", "ETH"])
    
    assert message[1]["type"] == "ticker"
    assert "KRW-BTC" in message[1]["codes"]
```

### Mock WebSocket

```python
class MockWebSocket:
    async def send_json(self, data):
        self.sent.append(data)
    
    async def receive_json(self):
        return {"type": "ticker", "code": "KRW-BTC"}
```

## 거래소 API 문서

### 한국
- [Upbit API](https://docs.upbit.com/docs/upbit-quotation-websocket)
- [Bithumb API](https://apidocs.bithumb.com/)
- [Coinone API](https://doc.coinone.co.kr/)
- [Korbit API](https://apidocs.korbit.co.kr/)

### 아시아
- [Binance API](https://binance-docs.github.io/apidocs/spot/en/)
- [Bybit API](https://bybit-exchange.github.io/docs/)
- [OKX API](https://www.okx.com/docs-v5/en/)
- [Huobi API](https://huobiapi.github.io/docs/)

### 북미 & 유럽
- [Coinbase API](https://docs.cloud.coinbase.com/exchange/docs)
- [Kraken API](https://docs.kraken.com/websockets/)
- [Bitfinex API](https://docs.bitfinex.com/docs)

## 관련 모듈

- **Core**: BaseWebsocketHandler 상속
- **Application**: StreamOrchestrator가 핸들러 생성
- **Config**: DI Container에서 Factory 주입
