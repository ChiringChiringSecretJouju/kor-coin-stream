import importlib
import os
from types import ModuleType

import pytest


def _reload_settings_with_env(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str | None]
) -> ModuleType:
    # Clear related envs first to avoid leakage across tests
    prefixes = ("KAFKA_", "REDIS_", "LOG_", "WS_", "FX_")
    for key in list(os.environ.keys()):
        if key.startswith(prefixes):
            monkeypatch.delenv(key, raising=False)

    # Apply desired env values
    for k, v in env.items():
        if v is None:
            monkeypatch.delenv(k, raising=False)
        else:
            monkeypatch.setenv(k, str(v))

    # Import and reload the settings module to reconstruct settings instances with new env
    import src.config.settings as settings

    settings = importlib.reload(settings)
    return settings


def test_kafka_settings_defaults(monkeypatch: pytest.MonkeyPatch):
    settings = _reload_settings_with_env(monkeypatch, {})

    assert isinstance(settings.kafka_settings.bootstrap_servers, str)
    assert ":" in settings.kafka_settings.bootstrap_servers
    assert isinstance(settings.kafka_settings.consumer_group_id, str)
    assert settings.kafka_settings.consumer_group_id
    assert settings.kafka_settings.auto_offset_reset in {"earliest", "latest"}
    assert str(settings.kafka_settings.acks) in {"all", "1", "0"}
    assert isinstance(settings.kafka_settings.linger_ms, int)


def test_kafka_settings_env_override(monkeypatch: pytest.MonkeyPatch):
    settings = _reload_settings_with_env(
        monkeypatch,
        {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka1:19092,kafka2:19093",
            "KAFKA_CONSUMER_GROUP_ID": "test-group",
            "KAFKA_AUTO_OFFSET_RESET": "latest",
            "KAFKA_ACKS": "1",
            "KAFKA_LINGER_MS": "25",
        },
    )

    assert settings.kafka_settings.bootstrap_servers == "kafka1:19092,kafka2:19093"
    assert settings.kafka_settings.consumer_group_id == "test-group"
    assert settings.kafka_settings.auto_offset_reset == "latest"
    assert str(settings.kafka_settings.acks) == "1"
    assert settings.kafka_settings.linger_ms == 25


def test_redis_settings_defaults_and_url(monkeypatch: pytest.MonkeyPatch):
    settings = _reload_settings_with_env(monkeypatch, {})

    assert settings.redis_settings.host == "localhost"
    assert settings.redis_settings.port == 6379
    assert settings.redis_settings.db == 0
    # Depending on config/.env, PASSWORD may be unset (None) or set to empty string
    assert settings.redis_settings.password in (None, "")
    assert settings.redis_settings.ssl is False
    assert settings.redis_settings.connection_timeout == 10
    assert settings.redis_settings.default_ttl == 3600

    # url without password and without SSL
    assert settings.redis_settings.url == "redis://localhost:6379/0"


def test_redis_settings_url_with_password_and_ssl(monkeypatch: pytest.MonkeyPatch):
    settings = _reload_settings_with_env(
        monkeypatch,
        {
            "REDIS_HOST": "redis.example.local",
            "REDIS_PORT": "6380",
            "REDIS_DB": "2",
            "REDIS_PASSWORD": "s3cr3t",
            "REDIS_SSL": "true",
        },
    )

    assert settings.redis_settings.url == "rediss://:s3cr3t@redis.example.local:6380/2"


def test_logging_settings_default(monkeypatch: pytest.MonkeyPatch):
    pytest.skip("logging_settings is no longer exposed in src.config.settings")


def test_logging_settings_env_override(monkeypatch: pytest.MonkeyPatch):
    pytest.skip("logging_settings is no longer exposed in src.config.settings")


def test_websocket_settings_defaults(monkeypatch: pytest.MonkeyPatch):
    settings = _reload_settings_with_env(monkeypatch, {})

    assert settings.websocket_settings.heartbeat_interval == 30
    assert settings.websocket_settings.heartbeat_timeout == 10
    assert settings.websocket_settings.heartbeat_fail_limit == 3
    assert settings.websocket_settings.receive_idle_timeout >= 0


def test_websocket_settings_env_override(monkeypatch: pytest.MonkeyPatch):
    settings = _reload_settings_with_env(
        monkeypatch,
        {
            "WS_HEARTBEAT_INTERVAL": "45",
            "WS_HEARTBEAT_TIMEOUT": "12",
            "WS_HEARTBEAT_FAIL_LIMIT": "5",
            "WS_RECEIVE_IDLE_TIMEOUT": "180",
        },
    )

    assert settings.websocket_settings.heartbeat_interval == 45
    assert settings.websocket_settings.heartbeat_timeout == 12
    assert settings.websocket_settings.heartbeat_fail_limit == 5
    assert settings.websocket_settings.receive_idle_timeout == 180


def test_fx_settings_defaults(monkeypatch: pytest.MonkeyPatch):
    settings = _reload_settings_with_env(monkeypatch, {})

    assert settings.fx_settings.enabled is True
    assert settings.fx_settings.ttl_sec > 0
    assert settings.fx_settings.fallback_usd_krw > 0
    assert settings.fx_settings.timeout_sec > 0


def test_fx_settings_env_override(monkeypatch: pytest.MonkeyPatch):
    settings = _reload_settings_with_env(
        monkeypatch,
        {
            "FX_ENABLED": "false",
            "FX_TTL_SEC": "120",
            "FX_FALLBACK_USD_KRW": "1400.5",
            "FX_TIMEOUT_SEC": "1.2",
        },
    )

    assert settings.fx_settings.enabled is False
    assert settings.fx_settings.ttl_sec == 120
    assert settings.fx_settings.fallback_usd_krw == 1400.5
    assert settings.fx_settings.timeout_sec == 1.2
