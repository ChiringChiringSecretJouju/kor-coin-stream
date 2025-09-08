# common 폴더 README

## 1) 역할/범위 (Scope)
- 애플리케이션 전반에서 재사용되는 공통 인프라 유틸리티를 제공합니다.
- 포함: 로깅, 예외 계층/매핑, 카프카 설정 로더, 직렬화/역직렬화, 경량 메트릭.
- 비포함: 비즈니스 도메인 로직, 외부 API 호출 구체 구현.

## 2) 공개 인터페이스/계약 (Contracts)
- `common.logger.PipelineLogger`
  - 구조적 로깅 포맷: `%(asctime)s %(name)s %(levelname)s [%(component)s] [%(exchange)s] msg`
  - 파일 로테이션: 일자 기준(TimedRotatingFileHandler, backup 7일)
  - 로그 경로: `logs/{location2?}/{component?}/{name}_YYYY-MM-DD.log`
- `common.exceptions.handle_exchange_exceptions`
  - 비동기/카프카 예외를 도메인 예외로 매핑 및 로깅 일원화
  - 기본 매핑: Timeout -> ConnectionTimeoutException, JSONDecodeError -> JSONParsingException
- 직렬화 규약
  - `common.serde.to_bytes`: UTF-8 JSON. Decimal->str, deque->list 변환
- 설정 로더
  - `common.kafka_setting`: `config/kafka_config.conf`를 읽어 BOOTSTRAP_SERVER, ACKS, LINGER_MS 등 노출

## 3) 설정/환경 (Settings)
- 필수 파일: `config/kafka_config.conf`
- 민감정보는 이 파일에 직접 넣지 말고, 배포별 오버레이/환경변수로 주입 권장

## 4) 사용법 (Usage)
```python
from common.logger import PipelineLogger
from common.exceptions import handle_exchange_exceptions
from common.serde import to_bytes

log = PipelineLogger.get_logger("websocket_handler", component="connection")

@handle_exchange_exceptions()
async def run():
    log.info("start")
    payload = {"ok": True}
    b = to_bytes(payload)
    # ... send b to Kafka
```

## 5) 운영/모니터링 (Operability)
- 로그: 콘솔 + 파일(일자 회전). 큐 기반 비동기 로깅으로 성능 영향 최소화
- 메트릭: `common.metrics.RateCounter`로 초당 처리량(MPS) 로그 남김

## 6) 품질 게이트 (Quality Gates)
- 타입: Python 3.12+ 타입힌트 필수, `Any` 남용 금지(주석 사유 명시)
- 테스트: 로깅/예외 데코레이터는 단위 테스트에서 side-effect 최소화해 검증

## 7) 보안/컴플라이언스 (Security)
- 로그에 PII/시크릿 금지. 필요시 마스킹
- 로그 샘플링/레벨 정책은 배포 환경별로 조정

## 8) 날짜
- 2025-08-20
