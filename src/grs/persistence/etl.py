from __future__ import annotations

from pathlib import Path

from grs.persistence.duckdb_store import AnalyticsStore


def run_weekly_etl(sqlite_path: Path, duckdb_path: Path, season: int, week: int) -> None:
    store = AnalyticsStore(duckdb_path)
    store.refresh_from_sqlite_for_week(sqlite_path, season=season, week=week)
