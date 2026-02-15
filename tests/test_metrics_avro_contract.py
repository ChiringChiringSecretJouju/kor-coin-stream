from __future__ import annotations

import json
from pathlib import Path

from fastavro import parse_schema
from fastavro.validation import validate

from src.core.dto.io.metrics import (
    ProcessingMetricsMessage,
    QualityMetricsMessage,
    ReceptionMetricsMessage,
)
from tests.factory_builders import (
    build_processing_metrics_message_payload,
    build_quality_metrics_message_payload,
    build_reception_metrics_message_payload,
)


def _load_schema(file_name: str) -> dict[str, object]:
    schema_path = (
        Path(__file__).resolve().parents[1] / "src" / "infra" / "messaging" / "schemas" / file_name
    )
    return json.loads(schema_path.read_text(encoding="utf-8"))


def test_reception_metrics_matches_avro_schema() -> None:
    message = ReceptionMetricsMessage.model_validate(build_reception_metrics_message_payload())
    schema = parse_schema(_load_schema("reception_metrics.avsc"))
    assert validate(message.model_dump(mode="json"), schema)


def test_processing_metrics_matches_avro_schema() -> None:
    message = ProcessingMetricsMessage.model_validate(build_processing_metrics_message_payload())
    schema = parse_schema(_load_schema("processing_metrics.avsc"))
    assert validate(message.model_dump(mode="json"), schema)


def test_quality_metrics_matches_avro_schema() -> None:
    message = QualityMetricsMessage.model_validate(build_quality_metrics_message_payload())
    schema = parse_schema(_load_schema("quality_metrics.avsc"))
    assert validate(message.model_dump(mode="json"), schema)
