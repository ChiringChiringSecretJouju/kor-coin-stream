from __future__ import annotations

from pathlib import Path

from src.infra.messaging.avro.subjects import (
    CONNECT_SUCCESS_SUBJECT,
    PROCESSING_METRICS_SUBJECT,
    QUALITY_METRICS_SUBJECT,
    REALTIME_TICKER_SUBJECT,
    RECEPTION_METRICS_SUBJECT,
)

_SUBJECT_TO_SCHEMA_FILE = {
    CONNECT_SUCCESS_SUBJECT: "connect_success.avsc",
    REALTIME_TICKER_SUBJECT: "realtime_ticker.avsc",
    RECEPTION_METRICS_SUBJECT: "reception_metrics.avsc",
    PROCESSING_METRICS_SUBJECT: "processing_metrics.avsc",
    QUALITY_METRICS_SUBJECT: "quality_metrics.avsc",
}


def validate_avro_subject_schemas(base_dir: Path | None = None) -> None:
    """Validate required Avro schema files for enabled subjects.

    Args:
        base_dir: Repository root path. If omitted, infer from current module path.

    Raises:
        RuntimeError: If one or more required schema files are missing.
    """
    root = base_dir or Path(__file__).resolve().parents[4]
    schema_dir = root / "src" / "infra" / "messaging" / "schemas"

    missing: list[str] = []
    for subject, schema_file in _SUBJECT_TO_SCHEMA_FILE.items():
        if not (schema_dir / schema_file).exists():
            missing.append(f"{subject} -> {schema_file}")

    if missing:
        details = ", ".join(missing)
        raise RuntimeError(f"Avro preflight failed. Missing schema files: {details}")
