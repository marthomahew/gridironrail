from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4


def now_utc() -> datetime:
    return datetime.now(UTC)


def make_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"
