from __future__ import annotations

# Avro subject names used by producer layer.
CONNECT_SUCCESS_SUBJECT = "connect_success"
REALTIME_TICKER_SUBJECT = "realtime_ticker"

# market-kafka-infra/schemas 기준으로 ticker/trade 실시간 배치는 단일 스키마를 사용한다.
TICKER_DATA_SUBJECT = REALTIME_TICKER_SUBJECT
TRADE_DATA_SUBJECT = REALTIME_TICKER_SUBJECT

RECEPTION_METRICS_SUBJECT = "reception_metrics"
PROCESSING_METRICS_SUBJECT = "processing_metrics"
QUALITY_METRICS_SUBJECT = "quality_metrics"
METRICS_EVENT_SUBJECT = "metrics_event"
PRODUCER_METRICS_SUBJECT = "producer_metrics"
