# 🌊 백프레셔 모니터링 시스템

> **작성일**: 2025-10-10  
> **목적**: Producer 큐 백프레셔를 실시간으로 Kafka에 전송하여 모니터링 및 알림 제공

---

## 📋 개요

**백프레셔 모니터링 시스템**은 Producer 큐 상태를 Kafka로 전송하여 실시간 모니터링과 알림을 제공합니다.

### 핵심 기능

1. ✅ **백프레셔 이벤트** - High Watermark 초과 시 자동 전송
2. ✅ **주기적 모니터링** - 큐 상태를 주기적으로 전송 (선택적)
3. ✅ **알림 지원** - 백프레셔 발생/해제 이벤트 추적
4. ✅ **선택적 통합** - 필요한 Producer에만 적용
5. ✅ **무한 루프 방지** - BackpressureEventProducer는 자기 자신의 이벤트 전송 불가

---

## 🏗️ 아키텍처

### 컴포넌트

```
┌─────────────────────────────────────────────────────────┐
│ Producer (MetricsProducer, RealtimeDataProducer 등)   │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │ AsyncBaseClient (백프레셔 관리)                   │  │
│  │                                                    │  │
│  │  1. 큐 크기 체크 (qsize >= 800)                   │  │
│  │  2. 백프레셔 활성화 감지                          │  │
│  │  3. BackpressureEventProducer 호출 ───────────┐  │  │
│  └──────────────────────────────────────────────────┘  │  │
└─────────────────────────────────────────────────────────┘  │
                                                             │
                                                             ▼
                                    ┌──────────────────────────────────────┐
                                    │ BackpressureEventProducer            │
                                    │                                      │
                                    │ send_backpressure_event()            │
                                    └──────────────────────────────────────┘
                                                             │
                                                             ▼
                                    ┌──────────────────────────────────────┐
                                    │ Kafka Topic                          │
                                    │ ws.backpressure.events               │
                                    └──────────────────────────────────────┘
                                                             │
                                                             ▼
                                    ┌──────────────────────────────────────┐
                                    │ 모니터링/알림 시스템                  │
                                    │ - Grafana Dashboard                  │
                                    │ - Alertmanager                       │
                                    │ - Slack/Email 알림                   │
                                    └──────────────────────────────────────┘
```

### 이벤트 플로우

```
1. Producer 큐 크기 증가 (800/1000)
   ↓
2. High Watermark 초과 감지 (_should_throttle() = True)
   ↓
3. 백프레셔 활성화 이벤트 Kafka 전송
   {
     "action": "backpressure_activated",
     "producer_name": "MetricsProducer",
     "status": {
       "queue_size": 850,
       "usage_percent": 85.0,
       "is_throttled": True
     }
   }
   ↓
4. 대기 (Low Watermark까지)
   ↓
5. 큐 크기 감소 (200/1000 이하)
   ↓
6. 백프레셔 해제 이벤트 Kafka 전송
   {
     "action": "backpressure_deactivated",
     "producer_name": "MetricsProducer",
     "status": {
       "queue_size": 180,
       "usage_percent": 18.0,
       "is_throttled": False
     }
   }
```

---

## 🚀 사용법

### 1. BackpressureEventProducer 생성 및 시작

```python
from src.infra.messaging.connect.producer_client import BackpressureEventProducer

# 백프레셔 이벤트 Producer 생성
backpressure_producer = BackpressureEventProducer()

# 시작
await backpressure_producer.start_producer()
```

### 2. 기존 Producer에 연결

#### 옵션 1: 백프레셔 이벤트만

```python
from src.infra.messaging.connect.producer_client import MetricsProducer

# 메트릭 Producer 생성
metrics_producer = MetricsProducer()
await metrics_producer.start_producer()

# 백프레셔 이벤트만 (기본값)
metrics_producer.producer.set_backpressure_event_producer(backpressure_producer)
```

#### 옵션 2: 백프레셔 + 주기적 모니터링

```python
# 백프레셔 이벤트 + 주기적 큐 상태 리포트 (30초마다)
metrics_producer.producer.set_backpressure_event_producer(
    backpressure_producer,
    enable_periodic_monitoring=True  # 주기적 모니터링 활성화
)
```

### 3. 실제 사용 예시 (main.py)

```python
async def main() -> None:
    """메인 실행 함수"""
    logger.info("암호화폐 거래소 웹소켓 스트림 파이프라인 시작 (Kafka 소비 모드)")
    
    # Redis 초기화
    redis_mgr = RedisConnectionManager.get_instance()
    await redis_mgr.initialize()
    
    # 백프레셔 이벤트 Producer 생성 (전역)
    backpressure_producer = BackpressureEventProducer()
    await backpressure_producer.start_producer()
    
    # Orchestrator 생성
    orchestrator = StreamOrchestrator()
    
    # Orchestrator 내부 Producer들에 백프레셔 모니터링 설정
    # (예: MetricsProducer, RealtimeDataProducer 등)
    if hasattr(orchestrator, 'metrics_producer'):
        orchestrator.metrics_producer.producer.set_backpressure_event_producer(
            backpressure_producer
        )
    
    # Consumer 시작
    command_consumer = KafkaConsumerClient(
        orchestrator=orchestrator,
        topic=kafka_settings.STATUS_TOPIC,
    )
    
    # ... 나머지 로직 ...
    
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 종료되었습니다")
    finally:
        # 정리
        await backpressure_producer.stop_producer()
        await orchestrator.shutdown()
        await redis_mgr.close()
```

---

## 📊 이벤트 스키마

### BackpressureEventDTO

#### 1. 백프레셔 활성화 이벤트

```python
{
  "ticket_id": "550e8400-e29b-41d4-a716-446655440000",
  "action": "backpressure_activated",
  "event_timestamp_utc": "2025-10-10T05:30:00.123456+00:00",
  "producer_name": "MetricsProducer",
  "producer_type": "AsyncProducerBase",
  "status": {
    "queue_size": 850,
    "queue_max_size": 1000,
    "usage_percent": 85.0,
    "is_throttled": true,
    "high_watermark": 800,
    "low_watermark": 200
  },
  "message": "MetricsProducer backpressure_activated"
}
```

#### 2. 백프레셔 비활성화 이벤트

```python
{
  "ticket_id": "550e8400-e29b-41d4-a716-446655440001",
  "action": "backpressure_deactivated",
  "event_timestamp_utc": "2025-10-10T05:31:00.123456+00:00",
  "producer_name": "MetricsProducer",
  "status": {
    "queue_size": 180,
    "usage_percent": 18.0,
    "is_throttled": false,
    ...
  }
}
```

#### 3. 주기적 큐 상태 리포트 (NEW)

```python
{
  "ticket_id": "550e8400-e29b-41d4-a716-446655440002",
  "action": "queue_status_report",  # 주기적 리포트
  "event_timestamp_utc": "2025-10-10T05:32:00.123456+00:00",
  "producer_name": "MetricsProducer",
  "status": {
    "queue_size": 450,
    "queue_max_size": 1000,
    "usage_percent": 45.0,
    "is_throttled": false,
    "high_watermark": 800,
    "low_watermark": 200
  },
  "message": "MetricsProducer queue_status_report"
}
```

### 필드 설명

| 필드 | 타입 | 설명 |
|------|------|------|
| **ticket_id** | string | 이벤트 고유 ID (UUID) |
| **action** | enum | `backpressure_activated` \| `backpressure_deactivated` \| `queue_status_report` |
| **event_timestamp_utc** | string | 이벤트 발생 시각 (UTC ISO 8601) |
| **producer_name** | string | Producer 클래스명 (예: MetricsProducer) |
| **producer_type** | string | Producer 타입 (기본: AsyncProducerBase) |
| **status.queue_size** | int | 현재 큐 크기 |
| **status.queue_max_size** | int | 큐 최대 크기 |
| **status.usage_percent** | float | 큐 사용률 (%) |
| **status.is_throttled** | bool | 백프레셔 활성화 여부 |
| **status.high_watermark** | int | High Watermark (throttle 시작) |
| **status.low_watermark** | int | Low Watermark (throttle 해제) |
| **message** | string | 추가 메시지 (optional) |

---

## ⚙️ 주기적 모니터링 설정

### 기본 설정

```python
# BackpressureConfig 기본값
queue_max_size = 1000
high_watermark = 800  # 80%
low_watermark = 200   # 20%
monitoring_interval_sec = 30  # 30초마다 리포트
```

### 커스터마이징

```python
from src.infra.messaging.clients.cb.base import BackpressureConfig

# 커스텀 백프레셔 설정
custom_config = BackpressureConfig(
    queue_max_size=500,
    high_watermark=400,  # 80%
    low_watermark=100,   # 20%
    throttle_sleep_ms=50,
    enable_periodic_monitoring=True,
    monitoring_interval_sec=10  # 10초마다 리포트
)

# Producer에 적용
producer._backpressure_config = custom_config
```

### 주기적 모니터링 동작

```
시간 (30초 간격)
   0s     30s    60s    90s    120s
   │      │      │      │      │
   │      │      │      │      │
   └──────┴──────┴──────┴──────┴───▶ 시간
      │      │      │      │
      ▼      ▼      ▼      ▼
    Report Report Report Report
   (45%)  (52%)  (38%)  (61%)
```

**주기적 리포트**:
- 30초마다 큐 상태를 Kafka로 전송
- 백프레셔 여부와 관계없이 지속적으로 모니터링
- 시계열 분석 및 트렌드 파악 가능

---

## 🎯 모니터링 전략

### Kafka Consumer 예시

```python
from confluent_kafka import Consumer

# 백프레셔 이벤트 Consumer
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'backpressure-monitoring',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(config)
consumer.subscribe(['ws.backpressure.events'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    
    if msg.error():
        print(f"Error: {msg.error()}")
        continue
    
    event = json.loads(msg.value().decode('utf-8'))
    
    # 알림 로직
    if event['action'] == 'backpressure_activated':
        if event['status']['usage_percent'] > 90:
            send_critical_alert(event)  # 🚨 위험 알림
        else:
            send_warning_alert(event)  # ⚠️ 경고 알림
    elif event['action'] == 'backpressure_deactivated':
        send_info_alert(event)  # ✅ 복구 알림
    elif event['action'] == 'queue_status_report':
        # 주기적 리포트는 메트릭으로만 저장
        store_metrics(event)
```

### Grafana 대시보드

#### 1. 백프레셔 발생 빈도

```promql
# 최근 1시간 백프레셔 발생 횟수
count by (producer_name) (
  kafka_consumer_messages_total{
    topic="ws.backpressure.events",
    action="backpressure_activated"
  }[1h]
)
```

#### 2. 큐 사용률 시계열 (주기적 리포트)

```promql
# 주기적 리포트를 통한 실시간 큐 사용률
avg by (producer_name) (
  kafka_backpressure_queue_usage_percent{
    action="queue_status_report"
  }
)
```

#### 3. 현재 백프레셔 상태

```promql
# 현재 백프레셔 활성화된 Producer 수
sum by (producer_name) (
  kafka_backpressure_is_throttled{is_throttled="true"}
)
```

#### 4. 큐 크기 트렌드

```promql
# 5분 평균 큐 크기
avg_over_time(
  kafka_backpressure_queue_size{action="queue_status_report"}[5m]
)
```

### 알림 규칙

```yaml
# Alertmanager 규칙
groups:
  - name: backpressure_alerts
    rules:
      # 백프레셔 90% 이상
      - alert: HighBackpressure
        expr: kafka_backpressure_queue_usage_percent > 90
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.producer_name }} 백프레셔 위험"
          description: "큐 사용률 {{ $value }}%"
      
      # 백프레셔 80% 이상
      - alert: MediumBackpressure
        expr: kafka_backpressure_queue_usage_percent > 80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "{{ $labels.producer_name }} 백프레셔 경고"
          description: "큐 사용률 {{ $value }}%"
```

---

## ⚠️ 주의사항

### 1. 무한 루프 방지

**BackpressureEventProducer는 자기 자신의 백프레셔 이벤트를 전송하지 않습니다.**

```python
# ❌ 절대 하지 마세요!
backpressure_producer = BackpressureEventProducer()
backpressure_producer.producer.set_backpressure_event_producer(backpressure_producer)
- [x] BackpressureEventProducer 구현
- [x] AsyncBaseClient 통합
- [x] 무한 루프 방지
- [x] 이벤트 전송 실패 처리
- [x] 문서화 완료

---

**구현 완료일**: 2025-10-10  
**작성자**: Cascade AI  
**검토**: 사용자 확인 대기

---

**다음 단계**: 
1. 실제 환경에 적용
2. Grafana 대시보드 구성
3. Alertmanager 규칙 설정
4. Slack/Email 알림 연동 🚀
