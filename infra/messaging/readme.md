# 🔄 messaging 모듈

## 배경 (Problem)
실시간 스트림을 저장·분배하려면 **높은 처리량·신뢰성**을 제공하는 브로커가 필요합니다.  
Kafka는 훌륭하지만, 프로듀서/컨슈머 설정·모니터링·토픽 관리가 복잡합니다.

## 목표 (Solution)
`messaging` 모듈은 Kafka 사용을 추상화하여
1. **토픽 자동 관리**: 필요 시 토픽 생성/검증
2. **Producer/Consumer 팩토리**: 안전한 기본 파라미터 제공
3. **파티셔닝 전략**: 키 해싱·Composite Key 지원
4. **로컬 개발 스택**: `kafka-docker` 로 단일 명령 배포

## 계약 (Contracts)
- 토픽: 기본 명령 토픽 `market_connect_v1`
- 키 전략: `region|exchange|request_type[|correlation_id]` (예: `korea|upbit|ticker|abcd-1234`)
- 직렬화: value = JSON(UTF-8), key = UTF-8 문자열 또는 bytes
- 컨슈머 기본: `auto_offset_reset=latest`, `enable_auto_commit=true`

## 작동 흐름 (Cause–Development)
1. 프로듀서/컨슈머는 `messaging/clients/clients.py` 의 팩토리를 사용합니다.
   - `create_producer(**overrides)` / `create_consumer(topic, **overrides)`
2. 업무 로직은 `messaging/connect/` 의 고수준 클라이언트를 사용합니다.
   - Producer: `ConnectRequestProducer` (연결 명령 이벤트 전송)
   - Consumer: `KafkaConsumerClient` (명령 소비 후 오케스트레이션)
3. 파티션 키는 비즈니스 키를 사용합니다(예: `region|exchange|request_type`).
   - 커스텀 파티셔너는 제거되었고, 브로커 기본 파티셔닝을 사용합니다.
4. 운영자는 `kafka-docker` 의 Grafana/Prometheus 대시보드로 오프셋·TPS를 모니터링합니다.

## 폴더 구조
```bash
messaging/
├── 📂 clients/                 # Producer/Consumer 팩토리 및 공용 설정
│   └── 🐍 clients.py           # create_producer / create_consumer
├── 📂 connect/                 # 고수준 Producer/Consumer 구현
│   ├── 🐍 producer_client.py   # KafkaProducerClient, ConnectRequestProducer
│   └── 🐍 consumer_client.py   # KafkaConsumerClient
├── 📂 kafka-docker             # 🐳 Kafka 관련 Docker 설정 파일
│   ├── 🐳 docker_container_remove.sh
│   ├── 🐳 fluentd-cluster.yml
│   ├── 📂 jmx_exporter
│   │   ├── 🐳 jmx_prometheus_javaagent-1.0.1.jar
│   │   └── 🐳 kafka-broker.yml
│   ├── 🐳 kafka-compose.yml
│   ├── 📂 kui
│   │   └── 🐳 config.yml
│   └── 📂 visualization
│       ├── 📂 grafana
│       └── 📂 prometheus
│           └── 📂 config
│               └── 🐳 prometheus.yml
├── 📂 types/
│   └── 🐍 commands.py          # Pydantic 타입/계약
└── 🐍 readme.md

## 기대 효과 (Result)
- **배포 편의성**: `docker-compose up` 으로 로컬 Kafka 클러스터 기동.
- **안정성**: 표준 설정 + 재시도 로직으로 메시지 손실 최소화.
- **운영 가시성**: JMX Exporter + Grafana로 실시간 모니터링.

## 운영/모니터링 (Operability)
- 로그: `common.PipelineLogger` 사용, 컴포넌트=app/messaging
- 대시보드: Grafana(오프셋 지연, TPS, 에러율) / Prometheus 스크레이프 설정 참조
- DLQ/재처리: 필요 시 별도 토픽 운영 권장(정책 문서 링크 자리)

## 품질 게이트 (Quality Gates)
- 타입: Python 3.12+, Pydantic v2(TypeAdapter)로 런타임 검증
- 테스트: 프로듀서/컨슈머 통합 테스트는 로컬 `kafka-docker`로 수행 권장
- 린트/포맷: ruff/black 규칙 준수(레포 루트 가이드 참조)

## 소유자/연락처 (Ownership)
- 팀: <팀명/채널>  
- 온콜: <온콜 담당/링크>

## 맺음말 (Conclusion)
`messaging` 모듈은 Kafka의 복잡성을 숨기고, 개발자가 **데이터 가치 창출**에 집중하도록 돕습니다.

```bash
# 예시: ConnectRequestProducer 로 명령 전송
from messaging.connect.producer_client import ConnectRequestProducer

producer = ConnectRequestProducer()  # 기본 토픽: market_connect_v1
event = {
  "type": "command",
  "action": "connect_and_subscribe",
  "target": {"exchange": "upbit", "region": "korea", "request_type": "ticker"},
  "connection": {
    "url": "wss://api.upbit.com/websocket/v1",
    "socket_params": {"subscribe_type": "ticker", "symbols": ["KRW-BTC"]}
  },
  "projection": ["code", "trade_price", "timestamp"],
}

import asyncio
async def run():
  ok = await producer.send_event(event, key="korea|upbit|ticker")
  print("published" if ok else "failed")
  await producer.stop_producer()

asyncio.run(run())

# 예시: KafkaConsumerClient 로 명령 소비
# from messaging.connect.consumer_client import KafkaConsumerClient
# from main import StreamOrchestrator
#
# orchestrator = StreamOrchestrator()
# consumer = KafkaConsumerClient(orchestrator=orchestrator, topic="market_connect_v1")
# asyncio.run(consumer.run())
```
