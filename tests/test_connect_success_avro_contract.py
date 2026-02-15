from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastavro import parse_schema
from fastavro._validate_common import ValidationError as AvroValidationError
from fastavro.validation import validate

from src.core.dto.io.commands import ConnectSuccessEventDTO
from src.infra.messaging.avro.serializers import _fixed_subject_name_strategy
from tests.factory_builders import build_connect_success_payload


def _load_connect_success_schema() -> dict[str, object]:
    schema_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "infra"
        / "messaging"
        / "schemas"
        / "connect_success.avsc"
    )
    return json.loads(schema_path.read_text(encoding="utf-8"))


def test_connect_success_dto_matches_avro_schema() -> None:
    event = ConnectSuccessEventDTO.model_validate(build_connect_success_payload())

    schema = parse_schema(_load_connect_success_schema())
    payload = event.model_dump(mode="json")

    assert validate(payload, schema)


def test_connect_success_schema_rejects_missing_required_meta_field() -> None:
    event = ConnectSuccessEventDTO.model_validate(build_connect_success_payload())

    schema = parse_schema(_load_connect_success_schema())
    payload = event.model_dump(mode="json")
    payload["meta"].pop("ticket_id", None)

    with pytest.raises(AvroValidationError):
        validate(payload, schema)


def test_fixed_subject_strategy_ignores_topic_name() -> None:
    strategy = _fixed_subject_name_strategy("connect_success")

    class _Ctx:
        topic = "ws.connect_success.korea"

    assert strategy(_Ctx(), "ConnectSuccessEvent") == "connect_success"
