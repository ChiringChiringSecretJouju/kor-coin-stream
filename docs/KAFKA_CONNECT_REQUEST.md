# Kafka Connect 요청 가이드

Kafka Connect/MinIO 인프라는 `market-kafka-infra`에서 관리하고,
`kor-coin-stream`은 Connector 등록 요청만 수행합니다.

## 빠른 실행 (Python)

```bash
cd kor-coin-stream

# 기본: market-kafka-infra의 connector json 사용, 1회 등록
python examples/register_kafka_connectors.py
```

## 커스텀 옵션

```bash
python examples/register_kafka_connectors.py \
  --connect-url http://localhost:8083 \
  --connector-dir ../market-kafka-infra/compose/kafka-connect/connectors
```

드라이런:

```bash
python examples/register_kafka_connectors.py --dry-run
```

## 수동 API 호출 예시

```bash
curl -X PUT http://localhost:8083/connectors/1mbars-parquet-sink/config \
  -H "Content-Type: application/json" \
  -d @../market-kafka-infra/compose/kafka-connect/connectors/1mbars-parquet-sink.json
```
