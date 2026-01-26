from contextlib import asynccontextmanager
from typing import AsyncIterator

from src.infra.cache.cache_client import RedisConnectionManager
from src.infra.messaging.connect.producers.error.error_event import (
    ErrorEventProducer,
)
from src.infra.messaging.connect.producers.metrics.metrics import MetricsProducer
from src.infra.messaging.connect.producers.realtime.ticker import TickerDataProducer
from src.infra.messaging.connect.producers.realtime.trade import TradeDataProducer


@asynccontextmanager
async def init_redis() -> AsyncIterator[RedisConnectionManager]:
    """Redis 초기화 및 정리를 위한 async context manager"""
    manager = RedisConnectionManager.get_instance()
    await manager.initialize()
    yield manager
    await manager.close()


@asynccontextmanager
async def init_error_producer(use_avro: bool) -> AsyncIterator[ErrorEventProducer]:
    """ErrorEventProducer 초기화 및 정리"""
    producer = ErrorEventProducer(use_avro=use_avro)
    await producer.start_producer()
    yield producer
    await producer.stop_producer()


@asynccontextmanager
async def init_metrics_producer(use_avro: bool) -> AsyncIterator[MetricsProducer]:
    """MetricsProducer 초기화 및 정리"""
    producer = MetricsProducer(use_avro=use_avro)
    await producer.start_producer()
    yield producer
    await producer.stop_producer()


@asynccontextmanager
async def init_ticker_producer(use_avro: bool) -> AsyncIterator[TickerDataProducer]:
    """TickerDataProducer 초기화 및 정리"""
    producer = TickerDataProducer(use_avro=use_avro)
    await producer.start_producer()
    yield producer
    await producer.stop_producer()





@asynccontextmanager
async def init_trade_producer(use_avro: bool) -> AsyncIterator[TradeDataProducer]:
    """TradeDataProducer 초기화 및 정리"""
    producer = TradeDataProducer(use_avro=use_avro)
    await producer.start_producer()
    yield producer
    await producer.stop_producer()
