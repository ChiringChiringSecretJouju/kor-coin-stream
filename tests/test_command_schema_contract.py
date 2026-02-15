from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.core.dto.io.commands import CommandDTO, DisconnectCommandDTO
from tests.factory_builders import (
    build_command_payload,
    build_connection_target,
    build_disconnect_payload,
    build_target_missing_request_type_payload,
    build_target_with_unexpected_field_payload,
)


def test_command_target_is_typed_model() -> None:
    dto = CommandDTO.model_validate(build_command_payload())
    assert dto.target.model_dump(mode="json") == build_connection_target()


def test_command_target_missing_required_field_fails() -> None:
    payload = build_command_payload(target=build_target_missing_request_type_payload())

    with pytest.raises(ValidationError):
        CommandDTO.model_validate(payload)


def test_disconnect_target_extra_field_fails() -> None:
    payload = build_disconnect_payload(target=build_target_with_unexpected_field_payload())

    with pytest.raises(ValidationError):
        DisconnectCommandDTO.model_validate(payload)
