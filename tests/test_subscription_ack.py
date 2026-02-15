from __future__ import annotations

from src.core.connection.utils.subscriptions.subscription_ack import (
    decide_global_subscription_ack,
    decide_korea_subscription_ack,
)
from tests.factory_builders import (
    build_subscription_ack_payload_global,
    build_subscription_ack_payload_korea,
)


def test_korea_subscribed_emits_ack_and_skips_message() -> None:
    decision = decide_korea_subscription_ack(
        build_subscription_ack_payload_korea(response_type="SUBSCRIBED")
    )
    assert decision.should_skip_message is True
    assert decision.should_emit_ack is True
    assert decision.status == "subscribed"
    assert decision.reason == "SUBSCRIBED"


def test_korea_data_does_not_skip() -> None:
    decision = decide_korea_subscription_ack(
        build_subscription_ack_payload_korea(response_type="DATA", value=1)
    )
    assert decision.should_skip_message is False
    assert decision.should_emit_ack is False


def test_global_result_success_emits_ack() -> None:
    decision = decide_global_subscription_ack(
        build_subscription_ack_payload_global(result="success")
    )
    assert decision.should_skip_message is True
    assert decision.should_emit_ack is True
    assert decision.status == "subscribed"
    assert decision.reason == "result=success"


def test_global_subscribed_event_emits_ack() -> None:
    decision = decide_global_subscription_ack(
        build_subscription_ack_payload_global(event="subscribed")
    )
    assert decision.should_skip_message is True
    assert decision.should_emit_ack is True
    assert decision.reason == "event=subscribed"


def test_global_trade_payload_does_not_skip() -> None:
    decision = decide_global_subscription_ack(
        build_subscription_ack_payload_global(data={"symbol": "BTCUSDT"})
    )
    assert decision.should_skip_message is False
    assert decision.should_emit_ack is False
