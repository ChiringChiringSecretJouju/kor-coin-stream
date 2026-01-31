"""
Dependency Injection Containers

이 모듈은 애플리케이션의 모든 의존성을 관리하는 DI 컨테이너를 정의합니다.

아키텍처:
- InfrastructureContainer: Redis 등 인프라 레이어 + Settings 주입
- MessagingContainer: Kafka Producers (Resource로 자동 라이프사이클 관리) + Settings 주입
- HandlerContainer: 거래소 핸들러 팩토리 (FactoryAggregate)
- ApplicationContainer: 최상위 컨테이너 (Orchestrator 등)

주요 패턴:
- Resource Provider: async init/shutdown 자동 관리
- Object Provider: settings.py 싱글톤 주입 (DI)
- FactoryAggregate: 동적 핸들러 선택
- Configuration: YAML/ENV 설정 외부화
- Wiring: @inject 데코레이터 지원

Settings 주입 (NEW!):
    # settings.py의 싱글톤을 DI Container에 주입
    infra.redis_config()  # → redis_settings 인스턴스
    infra.websocket_config()  # → websocket_settings 인스턴스
    messaging.kafka_config()  # → kafka_settings 인스턴스
    messaging.metrics_config()  # → metrics_settings 인스턴스

    # 사용 예시
    redis_host = container.infra.redis_config().host
    kafka_bootstrap = container.messaging.kafka_config().bootstrap_servers
"""

from dependency_injector import containers, providers

from src.application.connection_registry import ConnectionRegistry
from src.application.orchestrator import (
    StreamOrchestrator,
    WebsocketConnector,
)
from src.config.init_infra import (
    init_error_producer,
    init_metrics_producer,
    # init_orderbook_producer,  # Removed
    init_redis,
    init_ticker_producer,
    init_trade_producer,
)
from src.config.settings import (
    kafka_settings,
    metrics_settings,
    redis_settings,
    websocket_settings,
)
from src.exchange.asia import (
    BinanceWebsocketHandler,
    BybitWebsocketHandler,
    HuobiWebsocketHandler,
    OKXWebsocketHandler,
)
from src.exchange.korea import (
    BithumbWebsocketHandler,
    CoinoneWebsocketHandler,
    KorbitWebsocketHandler,
    UpbitWebsocketHandler,
)
from src.exchange.na import CoinbaseWebsocketHandler, KrakenWebsocketHandler
from src.infra.messaging.connect.consumer_client import KafkaConsumerClient
from src.infra.messaging.connect.disconnection_consumer import (
    KafkaDisconnectionConsumerClient,
)

# Handler 클래스 매핑 (거래소 이름 → 클래스)
# 이것만 코드에 정의 (클래스는 import 필요하므로)
HANDLER_CLASS_MAP = {
    # 한국
    "upbit": UpbitWebsocketHandler,
    "bithumb": BithumbWebsocketHandler,
    "coinone": CoinoneWebsocketHandler,
    "korbit": KorbitWebsocketHandler,
    # 아시아
    "binance": BinanceWebsocketHandler,
    "bybit": BybitWebsocketHandler,
    "okx": OKXWebsocketHandler,
    "huobi": HuobiWebsocketHandler,
    # 유럽

    # 북미
    "coinbase": CoinbaseWebsocketHandler,
    "kraken": KrakenWebsocketHandler,
}


# ========================================
# 1. Infrastructure Container (인프라 레이어)
# ========================================
class InfrastructureContainer(containers.DeclarativeContainer):
    """인프라 컨테이너

    - Redis, Kafka 등 인프라 컴포넌트 관리
    - 싱글톤 패턴으로 전역 공유
    - Settings: settings.py 싱글톤 주입 (DI)
    """

    config = providers.Configuration()

    # ===== Settings 주입 (DI) =====
    # settings.py의 싱글톤 인스턴스들을 Object로 주입
    redis_config = providers.Object(redis_settings)
    websocket_config = providers.Object(websocket_settings)

    redis_manager = providers.Resource(init_redis)


# ========================================
# 2. Messaging Container (메시징 레이어)
# ========================================
class MessagingContainer(containers.DeclarativeContainer):
    """메시징 레이어: Kafka Producers

    Features:
    - Resource provider로 Producer 라이프사이클 자동 관리
    - async start_producer/stop_producer 자동 호출
    - 설정 외부화 (use_avro)
    - Settings: settings.py 싱글톤 주입 (DI)
    """

    config = providers.Configuration()

    # ===== Settings 주입 (DI) =====
    # settings.py의 싱글톤 인스턴스들을 Object로 주입
    kafka_config = providers.Object(kafka_settings)
    metrics_config = providers.Object(metrics_settings)

    error_producer = providers.Resource(
        init_error_producer,
        use_avro=config.use_avro.as_(bool),
    )

    metrics_producer = providers.Resource(
        init_metrics_producer,
        use_avro=config.use_avro.as_(bool),
    )

    # ===== Realtime Data Producers =====
    ticker_producer = providers.Resource(
        init_ticker_producer,
        use_avro=config.use_avro.as_(bool),
    )

    # orderbook_producer = providers.Resource(
    #     init_orderbook_producer,
    #     use_avro=config.use_avro.as_(bool),
    # )

    trade_producer = providers.Resource(
        init_trade_producer,
        use_avro=config.use_avro.as_(bool),
    )


# ========================================
# 3-1. Korea Handler Container (한국 거래소)
# ========================================
class KoreaHandlerContainer(containers.DeclarativeContainer):
    """한국 거래소 핸들러 컨테이너

    Exchanges:
        - Upbit (업비트)
        - Bithumb (빗썸)
        - Coinone (코인원)
        - Korbit (코빗)

    Features:
        - 100% YAML 기반 설정
        - heartbeat 설정 자동 주입
    """

    config = providers.Configuration()
    ticker_producer = providers.Dependency()
    trade_producer = providers.Dependency()

    upbit = providers.Factory(
        HANDLER_CLASS_MAP["upbit"],
        exchange_name="upbit",
        region=config.handlers.upbit.region,
        request_type=providers.Dependency(),
        heartbeat_kind=config.handlers.upbit.heartbeat_kind,
        heartbeat_message=config.handlers.upbit.heartbeat_message,
        ticker_producer=ticker_producer,
        trade_producer=trade_producer,
    )

    bithumb = providers.Factory(
        HANDLER_CLASS_MAP["bithumb"],
        exchange_name="bithumb",
        region=config.handlers.bithumb.region,
        request_type=providers.Dependency(),
        heartbeat_kind=config.handlers.bithumb.heartbeat_kind,
        heartbeat_message=config.handlers.bithumb.heartbeat_message,
        ticker_producer=ticker_producer,
        trade_producer=trade_producer,
    )

    coinone = providers.Factory(
        HANDLER_CLASS_MAP["coinone"],
        exchange_name="coinone",
        region=config.handlers.coinone.region,
        request_type=providers.Dependency(),
        heartbeat_kind=config.handlers.coinone.heartbeat_kind,
        heartbeat_message=config.handlers.coinone.heartbeat_message,
        ticker_producer=ticker_producer,
        trade_producer=trade_producer,
    )

    korbit = providers.Factory(
        HANDLER_CLASS_MAP["korbit"],
        exchange_name="korbit",
        region=config.handlers.korbit.region,
        request_type=providers.Dependency(),
        heartbeat_kind=config.handlers.korbit.heartbeat_kind,
        heartbeat_message=config.handlers.korbit.heartbeat_message,
        ticker_producer=ticker_producer,
        trade_producer=trade_producer,
    )

    # 한국 거래소 전용 FactoryAggregate
    korea_handlers = providers.FactoryAggregate(
        upbit=upbit,
        bithumb=bithumb,
        coinone=coinone,
        korbit=korbit,
    )


# ========================================
# 3-2. Asia Handler Container (아시아 거래소)
# ========================================
class AsiaHandlerContainer(containers.DeclarativeContainer):
    """아시아 거래소 핸들러 컨테이너

    Exchanges:
        - Binance (바이낸스)
        - Bybit (바이비트)
        - OKX (오케이엑스)
        - Huobi (후오비/HTX)

    Features:
        - 100% YAML 기반 설정
        - heartbeat 설정 자동 주입
    """

    config = providers.Configuration()
    ticker_producer = providers.Dependency()
    trade_producer = providers.Dependency()

    binance = providers.Factory(
        HANDLER_CLASS_MAP["binance"],
        exchange_name="binance",
        region=config.handlers.binance.region,
        request_type=providers.Dependency(),
        heartbeat_kind=config.handlers.binance.heartbeat_kind,
        heartbeat_message=config.handlers.binance.heartbeat_message,
        ticker_producer=ticker_producer,
        trade_producer=trade_producer,
    )

    bybit = providers.Factory(
        HANDLER_CLASS_MAP["bybit"],
        exchange_name="bybit",
        region=config.handlers.bybit.region,
        request_type=providers.Dependency(),
        heartbeat_kind=config.handlers.bybit.heartbeat_kind,
        heartbeat_message=config.handlers.bybit.heartbeat_message,
        ticker_producer=ticker_producer,
        trade_producer=trade_producer,
    )

    okx = providers.Factory(
        HANDLER_CLASS_MAP["okx"],
        exchange_name="okx",
        region=config.handlers.okx.region,
        request_type=providers.Dependency(),
        heartbeat_kind=config.handlers.okx.heartbeat_kind,
        heartbeat_message=config.handlers.okx.heartbeat_message,
        ticker_producer=ticker_producer,
        trade_producer=trade_producer,
    )

    huobi = providers.Factory(
        HANDLER_CLASS_MAP["huobi"],
        exchange_name="huobi",
        region=config.handlers.huobi.region,
        request_type=providers.Dependency(),
        heartbeat_kind=config.handlers.huobi.heartbeat_kind,
        heartbeat_message=config.handlers.huobi.heartbeat_message,
        ticker_producer=ticker_producer,
        trade_producer=trade_producer,
    )

    # 아시아 거래소 전용 FactoryAggregate
    asia_handlers = providers.FactoryAggregate(
        binance=binance,
        bybit=bybit,
        okx=okx,
        huobi=huobi,
    )


# ========================================
# 3-3. Europe Handler Container (유럽 거래소)
# ========================================



# ========================================
# 3-4. North America Handler Container (북미 거래소)
# ========================================
class NorthAmericaHandlerContainer(containers.DeclarativeContainer):
    """북미 거래소 핸들러 컨테이너

    Exchanges:
        - Coinbase (코인베이스)
        - Kraken (크라켄)

    Features:
        - 100% YAML 기반 설정
        - heartbeat 설정 자동 주입
    """

    config = providers.Configuration()
    ticker_producer = providers.Dependency()
    trade_producer = providers.Dependency()

    coinbase = providers.Factory(
        HANDLER_CLASS_MAP["coinbase"],
        exchange_name="coinbase",
        region=config.handlers.coinbase.region,
        request_type=providers.Dependency(),
        heartbeat_kind=config.handlers.coinbase.heartbeat_kind,
        heartbeat_message=config.handlers.coinbase.heartbeat_message,
        ticker_producer=ticker_producer,
        trade_producer=trade_producer,
    )

    kraken = providers.Factory(
        HANDLER_CLASS_MAP["kraken"],
        exchange_name="kraken",
        region=config.handlers.kraken.region,
        request_type=providers.Dependency(),
        heartbeat_kind=config.handlers.kraken.heartbeat_kind,
        heartbeat_message=config.handlers.kraken.heartbeat_message,
        ticker_producer=ticker_producer,
        trade_producer=trade_producer,
    )

    # 북미 거래소 전용 FactoryAggregate
    na_handlers = providers.FactoryAggregate(coinbase=coinbase, kraken=kraken)


# ========================================
# 3-5. Unified Handler Container (통합 컨테이너)
# ========================================
class HandlerContainer(containers.DeclarativeContainer):
    """통합 거래소 핸들러 컨테이너

    Features:
        - 지역별 Container 통합 (Korea, Asia, Europe, NA)
        - 모든 거래소 핸들러를 단일 FactoryAggregate로 제공
        - 100% YAML 기반 DI

    Architecture:
        KoreaHandlerContainer (4개)
        AsiaHandlerContainer (4개)
        EuropeHandlerContainer (1개)
        NorthAmericaHandlerContainer (2개)
        ↓
        handler_factory (통합 - 11개)

    YAML 설정 예시:
        handlers:
          upbit:
            enabled: true
            region: korea
            heartbeat_kind: frame

    Usage:
        handler = container.handlers.handler_factory("upbit", request_type="ticker")
        # 모든 지역의 거래소를 동일한 인터페이스로 접근
    """

    config = providers.Configuration()
    ticker_producer = providers.Dependency()
    trade_producer = providers.Dependency()

    # 지역별 Container 포함
    korea = providers.Container(
        KoreaHandlerContainer,
        config=config,
        ticker_producer=ticker_producer,
        trade_producer=trade_producer,
    )
    asia = providers.Container(
        AsiaHandlerContainer,
        config=config,
        ticker_producer=ticker_producer,
        trade_producer=trade_producer,
    )
    north_america = providers.Container(
        NorthAmericaHandlerContainer,
        config=config,
        ticker_producer=ticker_producer,
        trade_producer=trade_producer,
    )

    # 모든 지역의 핸들러를 통합한 FactoryAggregate
    handler_factory = providers.FactoryAggregate(
        # Korea (4)
        upbit=korea.upbit,
        bithumb=korea.bithumb,
        coinone=korea.coinone,
        korbit=korea.korbit,
        # Asia (4)
        binance=asia.binance,
        bybit=asia.bybit,
        okx=asia.okx,
        huobi=asia.huobi,
        # North America (2)
        coinbase=north_america.coinbase,
        kraken=north_america.kraken,
    )


# ========================================
# 4. Application Container (최상위)
# ========================================
class ApplicationContainer(containers.DeclarativeContainer):
    """애플리케이션 최상위 컨테이너

    Features:
    - 모든 레이어의 컨테이너 통합
    - StreamOrchestrator 의존성 자동 주입
    - Consumer 팩토리
    - Wiring 지원

    Wiring 모듈:
    - src.application.orchestrator
    - src.application.connection_registry
    - src.application.error_coordinator
    """

    config = providers.Configuration()

    # ===== 하위 컨테이너 포함 =====
    infra = providers.Container(InfrastructureContainer, config=config.infra)
    messaging = providers.Container(MessagingContainer, config=config.messaging)
    handlers = providers.Container(
        HandlerContainer,
        config=config,
        ticker_producer=messaging.ticker_producer,
        trade_producer=messaging.trade_producer,
    )

    # ===== Core Components (StreamOrchestrator 의존성) =====
    connection_registry = providers.Factory(ConnectionRegistry)

    websocket_connector = providers.Singleton(
        WebsocketConnector, handler_map=handlers.handler_factory
    )

    # ===== StreamOrchestrator (모든 의존성 주입) =====
    orchestrator = providers.Singleton(
        StreamOrchestrator,
        error_producer=messaging.error_producer,
        registry=connection_registry,
        connector=websocket_connector,
    )

    # ===== Kafka Consumers =====
    status_consumer = providers.Factory(
        KafkaConsumerClient,
        orchestrator=orchestrator,
        topic=config.kafka.status_topic,
    )

    disconnection_consumer = providers.Factory(
        KafkaDisconnectionConsumerClient,
        orchestrator=orchestrator,
        topic=config.kafka.disconnection_topic,
    )
