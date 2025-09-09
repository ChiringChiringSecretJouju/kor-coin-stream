import importlib
import os
from types import ModuleType

import pytest


def _reload_settings_with_env(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str | None]
) -> ModuleType:
    # Clear related envs first to avoid leakage across tests
    prefixes = ("KAFKA_", "REDIS_", "LOG_", "WS_")
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
    import config.settings as settings

    settings = importlib.reload(settings)
    return settings


def test_kafka_settings_defaults(monkeypatch: pytest.MonkeyPatch):
    settings = _reload_settings_with_env(monkeypatch, {})

    assert (
        settings.kafka_settings.BOOTSTRAP_SERVERS
        == "kafka1:19092,kafka2:29092,kafka3:39092"
    )
    assert settings.kafka_settings.CONSUMER_GROUP_ID == "korea-coin-stream"
    assert settings.kafka_settings.COMMAND_TOPIC == "ws.command"
    assert settings.kafka_settings.STATUS_TOPIC == "ws.status"
    assert settings.kafka_settings.EVENT_TOPIC == "ws.event"
    assert settings.kafka_settings.AUTO_OFFSET_RESET == "earliest"
    assert settings.kafka_settings.ACKS == "all"
    assert settings.kafka_settings.LINGER_MS == 10


def test_kafka_settings_env_override(monkeypatch: pytest.MonkeyPatch):
    settings = _reload_settings_with_env(
        monkeypatch,
        {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka1:19092,kafka2:19093",
            "KAFKA_CONSUMER_GROUP_ID": "test-group",
            "KAFKA_COMMAND_TOPIC": "cmd.topic",
            "KAFKA_STATUS_TOPIC": "status.topic",
            "KAFKA_EVENT_TOPIC": "event.topic",
            "KAFKA_AUTO_OFFSET_RESET": "latest",
            "KAFKA_ACKS": "1",
            "KAFKA_LINGER_MS": "25",
        },
    )

    assert settings.kafka_settings.BOOTSTRAP_SERVERS == "kafka1:19092,kafka2:19093"
    assert settings.kafka_settings.CONSUMER_GROUP_ID == "test-group"
    assert settings.kafka_settings.COMMAND_TOPIC == "cmd.topic"
    assert settings.kafka_settings.STATUS_TOPIC == "status.topic"
    assert settings.kafka_settings.EVENT_TOPIC == "event.topic"
    assert settings.kafka_settings.AUTO_OFFSET_RESET == "latest"
    assert str(settings.kafka_settings.ACKS) == "1"
    assert settings.kafka_settings.LINGER_MS == 25


def test_redis_settings_defaults_and_url(monkeypatch: pytest.MonkeyPatch):
    settings = _reload_settings_with_env(monkeypatch, {})

    assert settings.redis_settings.HOST == "localhost"
    assert settings.redis_settings.PORT == 6379
    assert settings.redis_settings.DB == 0
    # Depending on config/.env, PASSWORD may be unset (None) or set to empty string
    assert settings.redis_settings.PASSWORD in (None, "")
    assert settings.redis_settings.SSL is False
    assert settings.redis_settings.CONNECTION_TIMEOUT == 10
    assert settings.redis_settings.DEFAULT_TTL == 3600

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
    settings = _reload_settings_with_env(monkeypatch, {})
    assert settings.logging_settings.LEVEL == "INFO"


def test_logging_settings_env_override(monkeypatch: pytest.MonkeyPatch):
    settings = _reload_settings_with_env(monkeypatch, {"LOG_LEVEL": "DEBUG"})
    assert settings.logging_settings.LEVEL == "DEBUG"


def test_websocket_settings_defaults(monkeypatch: pytest.MonkeyPatch):
    settings = _reload_settings_with_env(monkeypatch, {})

    assert settings.websocket_settings.HEARTBEAT_INTERVAL == 30
    assert settings.websocket_settings.HEARTBEAT_TIMEOUT == 10
    assert settings.websocket_settings.HEARTBEAT_FAIL_LIMIT == 3
    assert settings.websocket_settings.RECEIVE_IDLE_TIMEOUT == 120


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

    assert settings.websocket_settings.HEARTBEAT_INTERVAL == 45
    assert settings.websocket_settings.HEARTBEAT_TIMEOUT == 12
    assert settings.websocket_settings.HEARTBEAT_FAIL_LIMIT == 5
    assert settings.websocket_settings.RECEIVE_IDLE_TIMEOUT == 180
