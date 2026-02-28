from __future__ import annotations

from grs.contracts import StrictExecutionPolicy


def strict_execution_policy() -> StrictExecutionPolicy:
    return StrictExecutionPolicy()
