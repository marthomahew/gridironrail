from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

try:
    import duckdb
except ModuleNotFoundError:  # pragma: no cover - exercised via runtime environments without duckdb
    duckdb = None  # type: ignore[assignment]


class AnalyticsStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> Any:
        if duckdb is None:
            raise RuntimeError("duckdb is required for analytics store operations")
        return duckdb.connect(str(self.db_path))

    def initialize_schema(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS mart_game_summaries (
                    game_id VARCHAR,
                    home_team_id VARCHAR,
                    away_team_id VARCHAR,
                    home_score INTEGER,
                    away_score INTEGER,
                    plays INTEGER,
                    turnovers INTEGER,
                    penalties INTEGER
                );

                CREATE TABLE IF NOT EXISTS mart_transactions (
                    tx_id VARCHAR,
                    season INTEGER,
                    week INTEGER,
                    tx_type VARCHAR,
                    summary VARCHAR,
                    team_id VARCHAR
                );

                CREATE TABLE IF NOT EXISTS mart_cap_history (
                    entry_id VARCHAR,
                    team_id VARCHAR,
                    season INTEGER,
                    reason VARCHAR,
                    amount INTEGER
                );

                CREATE TABLE IF NOT EXISTS mart_play_events (
                    play_id VARCHAR,
                    game_id VARCHAR,
                    yards INTEGER,
                    turnover BOOLEAN,
                    turnover_type VARCHAR,
                    score_event VARCHAR,
                    clock_delta INTEGER,
                    conditioned BOOLEAN,
                    attempts INTEGER
                );
                """
            )

    def refresh_from_sqlite(self, sqlite_path: Path) -> None:
        self.initialize_schema()
        with sqlite3.connect(sqlite_path) as sconn, self.connect() as dconn:
            self._replace_table(dconn, "mart_game_summaries", self._fetch_rows(sconn, "SELECT game_id, home_team_id, away_team_id, home_score, away_score, plays, turnovers, penalties FROM game_summaries"))
            self._replace_table(dconn, "mart_transactions", self._fetch_rows(sconn, "SELECT tx_id, season, week, tx_type, summary, team_id FROM transactions"))
            self._replace_table(dconn, "mart_cap_history", self._fetch_rows(sconn, "SELECT entry_id, team_id, season, reason, amount FROM cap_ledger"))
            self._replace_table(dconn, "mart_play_events", self._fetch_rows(sconn, "SELECT play_id, game_id, yards, turnover, turnover_type, score_event, clock_delta, conditioned, attempts FROM play_results"))

    def mark_exported(self, game_ids: list[str]) -> None:
        if not game_ids:
            return
        with self.connect() as conn:
            placeholders = ",".join(["?"] * len(game_ids))
            conn.execute(f"UPDATE mart_game_summaries SET plays = plays WHERE game_id IN ({placeholders})", game_ids)

    def _replace_table(self, conn: Any, table: str, rows: list[tuple]) -> None:
        conn.execute(f"DELETE FROM {table}")
        if not rows:
            return
        placeholders = ",".join(["?"] * len(rows[0]))
        conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

    def _fetch_rows(self, conn: sqlite3.Connection, query: str) -> list[tuple]:
        cur = conn.execute(query)
        return cur.fetchall()
