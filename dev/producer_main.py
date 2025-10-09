from __future__ import annotations

import argparse
import asyncio
from typing import Sequence

from src.infra.messaging.connect.producer_client import ConnectRequestProducer


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish a connect_and_subscribe command to Kafka (market_connect_v1)",
    )
    parser.add_argument("--exchange", required=True, help="exchange name e.g. upbit")
    parser.add_argument(
        "--symbols",
        required=True,
        help="comma-separated symbols, e.g. KRW-BTC,KRW-ETH",
    )
    parser.add_argument(
        "--region",
        default="korea",
        help="region (default: korea)",
    )
    parser.add_argument(
        "--request-type",
        default="ticker",
        dest="request_type",
        help="request type: ticker|orderbook|trade (default: ticker)",
    )
    parser.add_argument(
        "--subscribe-type",
        default="ticker",
        dest="subscribe_type",
        help="socket subscribe type (default: ticker)",
    )
    parser.add_argument(
        "--projection",
        default="",
        help="comma-separated fields to project (optional)",
    )
    return parser.parse_args(argv)


async def run() -> None:
    args = parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    projection = [p.strip() for p in args.projection.split(",") if p.strip()]

    payload = {
        "type": "command",
        "action": "connect_and_subscribe",
        "target": {
            "exchange": args.exchange,
            "region": args.region,
            "request_type": args.request_type,
        },
        "connection": {
            "socket_params": {
                "subscribe_type": args.subscribe_type,
                "symbols": symbols,
            },
        },
    }
    if projection:
        payload["projection"] = projection

    key = f"{args.region}|{args.exchange}|{args.request_type}"

    producer = ConnectRequestProducer(topic="ws.command")
    try:
        ok = await producer.send_event(payload, key=key)
        print("published" if ok else "failed")
    except Exception as e:
        print(e)
    finally:
        await producer.stop_producer()


if __name__ == "__main__":
    asyncio.run(run())
