from __future__ import annotations

from pathlib import Path

from grs.persistence.duckdb_store import AnalyticsStore


def run_weekly_etl(sqlite_path: Path, duckdb_path: Path) -> None:
    store = AnalyticsStore(duckdb_path)
    store.refresh_from_sqlite(sqlite_path)
