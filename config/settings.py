from __future__ import annotations

from typing import Final

from pydantic_settings import BaseSettings, SettingsConfigDict


def env_settings(prefix: str) -> SettingsConfigDict:
    return SettingsConfigDict(
        env_prefix=prefix,
        env_file="config/.env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )


class KafkaSettings(BaseSettings):
    """Kafka 관련 설정"""

    BOOTSTRAP_SERVERS: str = "localhost:9092"
    CONSUMER_GROUP_ID: str = "korea-coin-stream"
    COMMAND_TOPIC: str = "ws.command"
    STATUS_TOPIC: str = "ws.status"
    DISCONNECTION_TOPIC: str = "ws.disconnection"
    EVENT_TOPIC: str = "ws.event"
    AUTO_OFFSET_RESET: str = "latest"
    ACKS: str | int = "all"
    LINGER_MS: int = 10

    model_config = env_settings("KAFKA_")


class RedisSettings(BaseSettings):
    """Redis 관련 설정"""

    HOST: str = "localhost"
    PORT: int = 6379
    DB: int = 0
    PASSWORD: str | None = None
    SSL: bool = False

    # 연결 타임아웃 설정
    CONNECTION_TIMEOUT: int = 10

    # 기본 TTL 설정
    DEFAULT_TTL: int = 3600  # 1시간

    @property
    def url(self) -> str:
        """Redis 연결 URL 반환"""
        protocol = "rediss" if self.SSL else "redis"
        auth = f":{self.PASSWORD}@" if self.PASSWORD else ""
        return f"{protocol}://{auth}{self.HOST}:{self.PORT}/{self.DB}"

    model_config = env_settings("REDIS_")


class LoggingSettings(BaseSettings):
    """로깅 관련 설정"""

    LEVEL: str = "INFO"

    model_config = env_settings("LOG_")


# WebSocket/Connection 관련 설정
class WebsocketSettings(BaseSettings):
    """WebSocket 연결 관리 기본 설정

    환경 변수(prefix: WS_)로 오버라이드 가능합니다.
    """

    HEARTBEAT_INTERVAL: int = 30  # 하트비트 전송 간격(초)
    HEARTBEAT_TIMEOUT: int = 10  # frame pong 대기 타임아웃(초)
    HEARTBEAT_FAIL_LIMIT: int = 3  # 하트비트 연속 실패 허용 횟수
    RECEIVE_IDLE_TIMEOUT: int = 120  # 수신 정지 워치독 타임아웃(초)
    RECONNECT_MAX_ATTEMPTS: int = 4  # 연속 재접속 시도 허용 횟수

    model_config = env_settings("WS_")


class MetricsSettings(BaseSettings):
    """Metrics/Counting 관련 설정

    환경 변수(prefix: METRICS_)로 오버라이드 가능합니다.
    """

    COUNTING_BATCH_SIZE: int = 1  # 분 배치 크기(기본 1)

    model_config = env_settings("METRICS_")


# 설정 인스턴스 생성
kafka_settings = KafkaSettings()
redis_settings = RedisSettings()
logging_settings = LoggingSettings()
websocket_settings = WebsocketSettings()
metrics_settings = MetricsSettings()
