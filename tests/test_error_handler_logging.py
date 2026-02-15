from __future__ import annotations

from src.core.connection.services.error_handler import ConnectionErrorHandler
from tests.factory_builders import build_scope_domain


def test_error_handler_scope_log_extra_has_standard_keys() -> None:
    scope = build_scope_domain()
    handler = ConnectionErrorHandler(scope=scope)

    payload = handler._scope_log_extra("connection_error", attempt=1)

    assert payload["exchange"] == "upbit"
    assert payload["region"] == "korea"
    assert payload["request_type"] == "ticker"
    assert payload["phase"] == "connection_error"
    assert payload["attempt"] == 1
