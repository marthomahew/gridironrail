from __future__ import annotations

import hashlib
import random
from typing import Any, Sequence

from grs.contracts import RandomSource


class PythonRandomSource(RandomSource):
    """Injected randomness source for gameplay and test determinism."""

    def __init__(self, seed: int | None = None) -> None:
        self._seed = seed
        self._rng = random.Random(seed)

    def rand(self) -> float:
        return self._rng.random()

    def randint(self, a: int, b: int) -> int:
        return self._rng.randint(a, b)

    def choice(self, items: Sequence[Any]) -> Any:
        if not items:
            raise ValueError("choice items must not be empty")
        return self._rng.choice(items)

    def shuffle(self, items: list[Any]) -> None:
        self._rng.shuffle(items)

    def spawn(self, substream_id: str) -> RandomSource:
        seed = self._seed
        if seed is None:
            return PythonRandomSource(seed=None)
        digest = hashlib.sha256(f"{seed}:{substream_id}".encode("ascii", "ignore")).hexdigest()
        child_seed = int(digest[:16], 16)
        return PythonRandomSource(seed=child_seed)


def gameplay_random() -> PythonRandomSource:
    return PythonRandomSource(seed=None)


def seeded_random(seed: int) -> PythonRandomSource:
    return PythonRandomSource(seed=seed)
