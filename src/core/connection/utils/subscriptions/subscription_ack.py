from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AckDecision:
    should_skip_message: bool
    should_emit_ack: bool
    status: str | None = None
    reason: str | None = None


def decide_korea_subscription_ack(message: dict[str, Any]) -> AckDecision:
    response_type = message.get("response_type", "")
    match response_type:
        case "" | "DATA":
            return AckDecision(should_skip_message=False, should_emit_ack=False)
        case "SUBSCRIBED":
            return AckDecision(
                should_skip_message=True,
                should_emit_ack=True,
                status="subscribed",
                reason="SUBSCRIBED",
            )
        case "CONNECTED":
            return AckDecision(
                should_skip_message=True,
                should_emit_ack=False,
                reason="CONNECTED",
            )
        case _:
            return AckDecision(
                should_skip_message=True,
                should_emit_ack=False,
                reason=f"non_data:{response_type}",
            )


def decide_global_subscription_ack(message: dict[str, Any]) -> AckDecision:
    match message.get("result"), message.get("event"):
        case "success", _:
            return AckDecision(
                should_skip_message=True,
                should_emit_ack=True,
                status="subscribed",
                reason="result=success",
            )
        case _, "subscribe" | "subscribed" as event:
            return AckDecision(
                should_skip_message=True,
                should_emit_ack=True,
                status="subscribed",
                reason=f"event={event}",
            )
        case _:
            return AckDecision(should_skip_message=False, should_emit_ack=False)
