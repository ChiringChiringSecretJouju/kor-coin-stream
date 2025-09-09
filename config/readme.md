# config 폴더 README

## 1) 역할/범위 (Scope)
- 애플리케이션 전역 설정을 중앙에서 관리하여 코드 변경 없이 런타임 구성을 변경할 수 있게 합니다.
- 배포 환경별(로컬/스테이징/프로덕션) 오버레이 구성을 권장합니다.

## 2) 파일 설명 (Files)
- `kafka_config.conf`
  - 섹션: `[KAFKA]`
  - 주요 항목: `bootstrap_servers`, `security_protocol`, `acks`, `linger_ms`, `max_batch_size`, `max_request_size`
  - 로딩 위치: `common/kafka_setting.py`
- `urls.conf`
  - 외부 API/웹소켓 엔드포인트 목록(예: 거래소 WebSocket URL)

## 3) 환경변수 (Environment)
- 민감정보(자격증명 등)는 파일에 직접 저장하지 말고 환경변수로 주입하세요.
- 예: `export KAFKA_BOOTSTRAP_SERVERS=localhost:9092`
  - 필요 시 `common/kafka_setting.py`에서 환경변수 우선 사용으로 확장하세요.

## 4) 보안 (Security)
- 저장소에 비밀키/토큰을 커밋하지 않습니다.
- 프로덕션용 설정은 별도 보안 저장소 혹은 시크릿 매니저 사용을 권장합니다.

## 5) 변경 관리 (Change Management)
- 설정 스키마 변경 시 영향 범위를 README에 기록하고, 호환성(Backward/FULL) 정책을 명시하세요.
- 기본값이 바뀌면 관련 모듈(`messaging/`, `common/`)의 동작에 영향을 줄 수 있으므로 변경 이력을 남기세요.

## 6) 날짜
- 2025-08-20
