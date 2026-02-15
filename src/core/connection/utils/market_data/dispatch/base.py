from __future__ import annotations

from typing import Any, Generic, Protocol, TypeVar

ModelT = TypeVar("ModelT")


class ParserProtocol(Protocol[ModelT]):
    def can_parse(self, message: dict[str, Any]) -> bool: ...

    def parse(self, message: dict[str, Any]) -> ModelT: ...


class ParserDispatcher(Generic[ModelT]):
    def __init__(
        self,
        parsers: list[ParserProtocol[ModelT]],
        *,
        unsupported_message: str,
    ) -> None:
        self._parsers = parsers
        self._unsupported_message = unsupported_message

    def parse(self, message: dict[str, Any]) -> ModelT:
        parser = next((item for item in self._parsers if item.can_parse(message)), None)
        if parser is None:
            raise ValueError(f"{self._unsupported_message}: {list(message.keys())}")
        return parser.parse(message)
