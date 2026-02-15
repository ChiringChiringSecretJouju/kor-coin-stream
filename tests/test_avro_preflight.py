from __future__ import annotations

import pytest

from src.infra.messaging.avro.preflight import validate_avro_subject_schemas


def test_avro_preflight_passes_for_repo_schemas() -> None:
    validate_avro_subject_schemas()


def test_avro_preflight_fails_for_missing_schema_dir(tmp_path) -> None:
    with pytest.raises(RuntimeError):
        validate_avro_subject_schemas(base_dir=tmp_path)
