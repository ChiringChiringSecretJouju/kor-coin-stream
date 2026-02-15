from __future__ import annotations

from src.core.connection.services.health_monitor import ConnectionHealthMonitor
from tests.factory_builders import build_connection_policy_domain, build_scope_domain


def _build_monitor() -> ConnectionHealthMonitor:
    scope = build_scope_domain()
    policy = build_connection_policy_domain()
    return ConnectionHealthMonitor(scope=scope, policy=policy)


def test_health_monitor_scope_log_extra_has_standard_keys() -> None:
    monitor = _build_monitor()

    payload = monitor._scope_log_extra("heartbeat_send", error="timeout")

    assert payload["exchange"] == "upbit"
    assert payload["region"] == "korea"
    assert payload["request_type"] == "ticker"
    assert payload["phase"] == "heartbeat_send"
    assert payload["error"] == "timeout"
