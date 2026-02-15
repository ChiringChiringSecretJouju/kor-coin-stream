from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class PydanticFilter(BaseModel):
    """Dynamic payload filter using Pydantic BaseModel serialization."""

    model_config = ConfigDict(extra="allow")

    @classmethod
    def filter_dict(cls, payload: dict[str, Any]) -> dict[str, Any]:
        return cls(**payload).model_dump(exclude_none=True)
