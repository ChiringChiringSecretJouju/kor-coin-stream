from __future__ import annotations

import json
from pathlib import Path

from fastavro import parse_schema
from fastavro.validation import validate

from src.core.dto.io.metrics import (
    ProcessingBatchDTO,
    ProcessingMetricsDTO,
    ProcessingMetricsMessage,
    QualityBatchDTO,
    QualityMetricsDTO,
    QualityMetricsMessage,
    ReceptionBatchDTO,
    ReceptionMetricsDTO,
    ReceptionMetricsMessage,
)


def _load_schema(file_name: str) -> dict[str, object]:
    schema_path = (
        Path(__file__).resolve().parents[1] / "src" / "infra" / "messaging" / "schemas" / file_name
    )
    return json.loads(schema_path.read_text(encoding="utf-8"))


def test_reception_metrics_matches_avro_schema() -> None:
    message = ReceptionMetricsMessage(
        ticket_id="d84a908b-6d4a-4e53-8754-66531be2974f",
        region="korea",
        exchange="upbit",
        request_type="ticker",
        batch=ReceptionBatchDTO(
            ticket_id="35ef8c4f-f7cb-4fdb-b4f7-c64faf9a81a1",
            range_start_ts_kst=1739600400,
            range_end_ts_kst=1739600460,
            bucket_size_sec=60,
            items=[
                ReceptionMetricsDTO(
                    minute_start_ts_kst=1739600400,
                    total_received=120,
                    total_parsed=118,
                    total_parse_failed=2,
                    bytes_received=20480,
                )
            ],
            version=1,
        ),
    )
    schema = parse_schema(_load_schema("reception_metrics.avsc"))
    assert validate(message.model_dump(mode="json"), schema)


def test_processing_metrics_matches_avro_schema() -> None:
    message = ProcessingMetricsMessage(
        ticket_id="82e9165a-d73e-4a48-9471-3787f8695f3c",
        region="asia",
        exchange="binance",
        request_type="trade",
        batch=ProcessingBatchDTO(
            ticket_id="7f9f84cb-0d7a-478f-b4e6-0f11fc1e674f",
            range_start_ts_kst=1739600400,
            range_end_ts_kst=1739600460,
            bucket_size_sec=60,
            items=[
                ProcessingMetricsDTO(
                    minute_start_ts_kst=1739600400,
                    total_processed=240,
                    total_failed=4,
                    details={"BTCUSDT_COUNT": 160, "ETHUSDT_COUNT": 80},
                )
            ],
            version=1,
        ),
    )
    schema = parse_schema(_load_schema("processing_metrics.avsc"))
    assert validate(message.model_dump(mode="json"), schema)


def test_quality_metrics_matches_avro_schema() -> None:
    message = QualityMetricsMessage(
        ticket_id="5f57dc2b-2722-46bc-b8ba-6544b4f9bdca",
        region="na",
        exchange="okx",
        request_type="ticker",
        batch=QualityBatchDTO(
            ticket_id="1855e65c-f04a-40f8-85af-7346178fd6ac",
            range_start_ts_kst=1739600400,
            range_end_ts_kst=1739600460,
            bucket_size_sec=60,
            items=[
                QualityMetricsDTO(
                    minute_start_ts_kst=1739600400,
                    data_completeness=0.98,
                    symbol_coverage=42,
                    avg_latency_ms=12.3,
                    p95_latency_ms=25.1,
                    p99_latency_ms=32.8,
                    health_score=96.5,
                )
            ],
            version=1,
        ),
    )
    schema = parse_schema(_load_schema("quality_metrics.avsc"))
    assert validate(message.model_dump(mode="json"), schema)
