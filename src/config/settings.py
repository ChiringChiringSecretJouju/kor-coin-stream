"""통합 Settings 모듈 - 환경변수 기반

이 모듈의 역할:
    1. 코드에 합리적인 기본값 제공
    2. 환경변수로 오버라이드 (우선순위 높음)
    3. 타입 안전성 보장 (Pydantic 자동 검증)

설정 우선순위:
    1. 환경변수 (최우선) - export KAFKA_BOOTSTRAP_SERVERS=...
    2. .env 파일 - config/.env
    3. 코드 기본값 (settings.py 내부)

사용 예시:
    # 개발 환경 (기본값 사용)
    python main.py
    # → 코드의 기본값 사용 (localhost:9092 등)

    # 프로덕션 (환경변수 오버라이드)
    export KAFKA_BOOTSTRAP_SERVERS=prod-kafka:9092
    export KAFKA_COMPRESSION_TYPE=zstd
    python main.py
    # → 환경변수 우선!

장점:
    ✅ 설정 파일 불필요 (모두 코드에)
    ✅ 환경변수로 즉시 오버라이드 가능
    ✅ 타입 검증 자동 (Pydantic)
    ✅ IDE 자동완성 지원
    ✅ 보안 (비밀번호는 환경변수로)
    ✅ 단순함 (파일 관리 불필요)
"""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# 설정 파일 경로
config_dir = Path(__file__).parent.parent / "config"


def yaml_settings(prefix: str) -> SettingsConfigDict:
    """YAML + 환경변수 통합 설정

    Args:
        prefix: 환경변수 접두사 (예: KAFKA_, REDIS_)

    Returns:
        Pydantic 설정 딕셔너리

    우선순위:
        1. 환경변수 (export KAFKA_BOOTSTRAP_SERVERS=...)
        2. .env 파일 (config/.env)
    """
    return SettingsConfigDict(
        env_prefix=prefix,
        env_file=config_dir / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


# ========================================
# 통합 Settings (YAML + 환경변수)
# ========================================
# YAML에서 기본값 자동 로드 + 환경변수 오버라이드


class AppSettings(BaseSettings):
    """애플리케이션 일반 설정

    환경변수 오버라이드:
        APP_ENV: 실행 환경 (dev, prod, test) (기본: dev)
    """

    environment: str = "dev"
    debug: bool = False

    model_config = yaml_settings("APP_")


class KafkaSettings(BaseSettings):
    """Kafka 설정 (환경변수 기반)

    환경변수 오버라이드:
        KAFKA_BOOTSTRAP_SERVERS: 브로커 주소 (기본: localhost:9092)
        KAFKA_CONSUMER_GROUP_ID: Consumer 그룹 ID (기본: coin-stream)
        KAFKA_AUTO_OFFSET_RESET: 오프셋 리셋 정책 (기본: latest)
        KAFKA_ACKS: Producer acks 설정 (기본: all)
        KAFKA_LINGER_MS: Batch linger 시간 (기본: 10)
        KAFKA_COMPRESSION_TYPE: 압축 타입 (gzip, snappy, lz4, zstd)
        KAFKA_BATCH_SIZE: Batch 크기
        KAFKA_SCHEMA_REGISTER: Schema Registry URL (기본: http://localhost:8082)

    사용 예시:
        # 개발: 기본값 사용
        python main.py

        # 프로덕션: 환경변수 오버라이드
        export KAFKA_BOOTSTRAP_SERVERS=prod-kafka:9092
        export KAFKA_COMPRESSION_TYPE=zstd
        python main.py
    """

    bootstrap_servers: str = "localhost:9092"
    consumer_group_id: str = "coin-stream"
    auto_offset_reset: str = "latest"
    acks: str | int = "all"
    linger_ms: int = 10
    compression_type: str | None = None  # 선택사항
    batch_size: int | None = None  # 선택사항
    schema_register: str = "http://localhost:8082"
    use_array_format: bool = False  # Array-Format 직렬화 사용 여부

    # Kafka 설정은 KAFKA_ prefix를 사용하지만, Global 설정에 포함될 수 있음
    model_config = yaml_settings("KAFKA_")


class RedisSettings(BaseSettings):
    """Redis 설정 (환경변수 기반)

    환경변수 오버라이드:
        REDIS_HOST: Redis 호스트 (기본: localhost)
        REDIS_PORT: Redis 포트 (기본: 6379)
        REDIS_DB: Redis DB 번호 (기본: 0)
        REDIS_PASSWORD: Redis 비밀번호 (보안상 환경변수 권장)
        REDIS_SSL: SSL 사용 여부 (기본: false)
        REDIS_CONNECTION_TIMEOUT: 연결 타임아웃 (기본: 10초)
        REDIS_DEFAULT_TTL: 기본 TTL (기본: 3600초)
    """

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str | None = None  # 선택사항 (환경변수로만)
    ssl: bool = False
    connection_timeout: int = 10
    default_ttl: int = 3600

    model_config = yaml_settings("REDIS_")

    @property
    def url(self) -> str:
        """Redis URL 생성 (redis:// 또는 rediss://)"""
        protocol = "rediss" if self.ssl else "redis"
        auth = f":{self.password}@" if self.password else ""
        return f"{protocol}://{auth}{self.host}:{self.port}/{self.db}"


class WebsocketSettings(BaseSettings):
    """WebSocket 설정 (환경변수 기반)

    환경변수 오버라이드 (모든 타이밍 설정은 초 단위):
        WS_HEARTBEAT_INTERVAL: 하트비트 전송 간격 (기본: 30초)
        WS_HEARTBEAT_TIMEOUT: Pong 대기 타임아웃 (기본: 10초)
        WS_HEARTBEAT_FAIL_LIMIT: 하트비트 실패 허용 횟수 (기본: 3회)
        WS_RECEIVE_IDLE_TIMEOUT: 수신 정지 워치독 타임아웃 (기본: 120초)
        WS_RECONNECT_MAX_ATTEMPTS: 재연결 최대 시도 횟수 (기본: 4회)
    """

    heartbeat_interval: int = 30
    heartbeat_timeout: int = 10
    heartbeat_fail_limit: int = 3
    receive_idle_timeout: int = 120
    reconnect_max_attempts: int = 1000  # ✅ 넉넉하게 설정 (사실상 무한 재접속 지향)

    model_config = yaml_settings("WS_")


class MetricsSettings(BaseSettings):
    """Metrics 설정 (환경변수 기반)

    환경변수 오버라이드:
        METRICS_COUNTING_BATCH_SIZE: 분 단위 배치 크기 (기본: 1분)
    """

    counting_batch_size: int = 1

    model_config = yaml_settings("METRICS_")


class FxSettings(BaseSettings):
    """환율(FX) 설정 (환경변수 기반).

    환경변수 오버라이드:
        FX_ENABLED: 환율 기능 활성화 여부 (기본: true)
        FX_TTL_SEC: USD/KRW 환율 캐시 TTL (기본: 60초)
        FX_FALLBACK_USD_KRW: 환율 조회 실패 시 기본값 (기본: 1350.0)
        FX_TIMEOUT_SEC: 환율 조회 타임아웃 (기본: 2.5초)
    """

    enabled: bool = True
    ttl_sec: int = 60
    fallback_usd_krw: float = 1350.0
    timeout_sec: float = 2.5

    model_config = yaml_settings("FX_")


# ========================================
# 설정 인스턴스 (싱글톤)
# ========================================
# 환경변수 로드 (환경변수 없으면 기본값 사용)

app_settings = AppSettings()
kafka_settings = KafkaSettings()
redis_settings = RedisSettings()
websocket_settings = WebsocketSettings()
metrics_settings = MetricsSettings()
fx_settings = FxSettings()
