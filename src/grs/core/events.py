from __future__ import annotations

from collections import defaultdict
from typing import Callable, DefaultDict

from grs.contracts import NarrativeEvent

NarrativeHandler = Callable[[NarrativeEvent], None]


class EventBus:
    def __init__(self) -> None:
        self._narrative_handlers: list[NarrativeHandler] = []
        self._counter: DefaultDict[str, int] = defaultdict(int)

    def subscribe_narrative(self, handler: NarrativeHandler) -> None:
        self._narrative_handlers.append(handler)

    def publish_narrative(self, event: NarrativeEvent) -> None:
        self._counter[event.scope] += 1
        for handler in self._narrative_handlers:
            handler(event)

    def emitted_count(self, scope: str | None = None) -> int:
        if scope is None:
            return sum(self._counter.values())
        return self._counter[scope]
