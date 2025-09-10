# exchange 폴더 README

## 1) 역할/범위 (Scope)
- 거래소별(WebSocket) 프로토콜 차이를 캡슐화한 핸들러 구현을 제공합니다.
- `_BaseKoreaWebsocketHandler`를 상속하여 하트비트/구독 메시지/파싱 규칙을 거래소별로 정의합니다.

## 2) 포함 파일 (Files)
- `korea.py`
  - `UpbitWebsocketHandler`: ping/pong 프레임 하트비트
  - `BithumbWebsocketHandler`: 텍스트(JSON) ping: `{"cmd":"ping"}`
  - `CoinoneWebsocketHandler`: 텍스트(JSON) ping: `{"type":"ping"}`
  - `KorbitWebsocketHandler`: 기본 ping 프레임

## 3) 공통 동작/계약 (Contracts)
- 구독 파라미터 전송 후 메시지 루프에서 `ticker | orderbook | trade` 처리
- `projection: list[str] | None` 지정 시, 티커 메시지에서 해당 필드만 필터링 반환
- 오류는 `common.handle_exchange_exceptions`로 로깅/매핑

## 4) 페이로드 예시 (Examples)
```json
{
  "response_type": "DATA",
  "timestamp": 1720000000,
  "data": {"code": "KRW-BTC", "trade_price": 100.0}
}
```
Projection이 `["code", "trade_price"]`라면 처리 결과는:
```json
{"code": "KRW-BTC", "trade_price": 100.0}
```

## 5) 테스트 전략 (Testing)
- Mock WebSocket 서버로 ping/pong, 구독, 데이터 프레임 시나리오 검증
- `process_*_message` 단위 테스트: `projection` 유무, `data` 병합(`_utils.update_dict`) 확인
- 예외 시 재접속 백오프/로그 확인

## 6) 운영/모니터링 (Operability)
- 핸들러 로그 컴포넌트: `connection`
- 수신/처리 속도: `RateCounter` 로그로 관찰

## 7) 날짜
- 2025-08-20
