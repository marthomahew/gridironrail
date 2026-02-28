from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

try:
    import duckdb
except ModuleNotFoundError:  # pragma: no cover
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
                    game_id VARCHAR PRIMARY KEY,
                    season INTEGER,
                    week INTEGER,
                    home_team_id VARCHAR,
                    away_team_id VARCHAR,
                    home_score INTEGER,
                    away_score INTEGER,
                    plays INTEGER,
                    turnovers INTEGER,
                    penalties INTEGER
                );

                CREATE TABLE IF NOT EXISTS mart_transactions (
                    tx_id VARCHAR PRIMARY KEY,
                    season INTEGER,
                    week INTEGER,
                    tx_type VARCHAR,
                    summary VARCHAR,
                    team_id VARCHAR,
                    context_json VARCHAR
                );

                CREATE TABLE IF NOT EXISTS mart_cap_history (
                    entry_id VARCHAR PRIMARY KEY,
                    team_id VARCHAR,
                    season INTEGER,
                    reason VARCHAR,
                    amount INTEGER
                );

                CREATE TABLE IF NOT EXISTS mart_play_events (
                    play_id VARCHAR PRIMARY KEY,
                    game_id VARCHAR,
                    yards INTEGER,
                    turnover BOOLEAN,
                    turnover_type VARCHAR,
                    score_event VARCHAR,
                    clock_delta INTEGER,
                    conditioned BOOLEAN,
                    attempts INTEGER
                );

                CREATE TABLE IF NOT EXISTS mart_traditional_stats (
                    team_id VARCHAR,
                    season INTEGER,
                    week INTEGER,
                    points_for INTEGER,
                    points_against INTEGER,
                    plays INTEGER,
                    turnovers INTEGER,
                    penalties INTEGER,
                    PRIMARY KEY(team_id, season, week)
                );

                CREATE TABLE IF NOT EXISTS mart_pressure_coverage (
                    game_id VARCHAR,
                    pressure_reps INTEGER,
                    coverage_reps INTEGER,
                    run_fit_reps INTEGER,
                    PRIMARY KEY(game_id)
                );

                CREATE TABLE IF NOT EXISTS mart_turnover_causality (
                    game_id VARCHAR,
                    turnover_event VARCHAR,
                    source_id VARCHAR,
                    weight DOUBLE,
                    description VARCHAR,
                    PRIMARY KEY(game_id, turnover_event, source_id)
                );

                CREATE TABLE IF NOT EXISTS mart_shared_responsibility (
                    game_id VARCHAR,
                    actor_id VARCHAR,
                    rep_count INTEGER,
                    total_weight DOUBLE,
                    PRIMARY KEY(game_id, actor_id)
                );
                """
            )

    def refresh_from_sqlite_for_week(self, sqlite_path: Path, season: int, week: int) -> None:
        self.initialize_schema()
        with sqlite3.connect(sqlite_path) as sconn, self.connect() as dconn:
            game_rows = sconn.execute(
                """
                SELECT game_id, season, week, home_team_id, away_team_id, home_score, away_score, plays, turnovers, penalties
                FROM game_summaries
                WHERE season = ? AND week = ?
                """,
                (season, week),
            ).fetchall()
            game_ids = [r[0] for r in game_rows]

            self._upsert_game_summaries(dconn, game_rows)

            tx_rows = sconn.execute(
                "SELECT tx_id, season, week, tx_type, summary, team_id, context_json FROM transactions WHERE season = ? AND week = ?",
                (season, week),
            ).fetchall()
            self._upsert_rows(dconn, "mart_transactions", "tx_id", tx_rows)

            cap_rows = sconn.execute(
                "SELECT entry_id, team_id, season, reason, amount FROM cap_ledger WHERE season = ?",
                (season,),
            ).fetchall()
            self._upsert_rows(dconn, "mart_cap_history", "entry_id", cap_rows)

            if game_ids:
                placeholders = ",".join(["?"] * len(game_ids))
                play_rows = sconn.execute(
                    f"SELECT play_id, game_id, yards, turnover, turnover_type, score_event, clock_delta, conditioned, attempts FROM play_results WHERE game_id IN ({placeholders})",
                    tuple(game_ids),
                ).fetchall()
                self._upsert_rows(dconn, "mart_play_events", "play_id", play_rows)
                self._refresh_derived_marts_for_games(dconn, sconn, season, week, game_ids)

    def _upsert_game_summaries(self, conn: Any, rows: list[tuple]) -> None:
        if not rows:
            return
        ids = [r[0] for r in rows]
        placeholders = ",".join(["?"] * len(ids))
        conn.execute(f"DELETE FROM mart_game_summaries WHERE game_id IN ({placeholders})", ids)
        conn.executemany(
            "INSERT INTO mart_game_summaries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )

    def _upsert_rows(self, conn: Any, table: str, key_col: str, rows: list[tuple]) -> None:
        if not rows:
            return
        keys = [r[0] for r in rows]
        placeholders = ",".join(["?"] * len(keys))
        conn.execute(f"DELETE FROM {table} WHERE {key_col} IN ({placeholders})", keys)
        values_placeholder = ",".join(["?"] * len(rows[0]))
        conn.executemany(f"INSERT INTO {table} VALUES ({values_placeholder})", rows)

    def _refresh_derived_marts_for_games(
        self,
        dconn: Any,
        sconn: sqlite3.Connection,
        season: int,
        week: int,
        game_ids: list[str],
    ) -> None:
        placeholders = ",".join(["?"] * len(game_ids))

        # Traditional stats by team/week.
        dconn.execute("DELETE FROM mart_traditional_stats WHERE season = ? AND week = ?", [season, week])
        rows = sconn.execute(
            f"""
            SELECT home_team_id AS team_id, season, week, home_score, away_score, plays, turnovers, penalties
            FROM game_summaries WHERE game_id IN ({placeholders})
            UNION ALL
            SELECT away_team_id AS team_id, season, week, away_score, home_score, plays, turnovers, penalties
            FROM game_summaries WHERE game_id IN ({placeholders})
            """,
            tuple(game_ids + game_ids),
        ).fetchall()
        agg: dict[tuple[str, int, int], list[int]] = {}
        for team_id, s, w, pf, pa, plays, tos, pens in rows:
            key = (team_id, s, w)
            if key not in agg:
                agg[key] = [0, 0, 0, 0, 0]
            agg[key][0] += int(pf)
            agg[key][1] += int(pa)
            agg[key][2] += int(plays)
            agg[key][3] += int(tos)
            agg[key][4] += int(pens)
        dconn.executemany(
            "INSERT INTO mart_traditional_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [(k[0], k[1], k[2], v[0], v[1], v[2], v[3], v[4]) for k, v in agg.items()],
        )

        # Pressure / coverage / run-fit contest counts by game.
        dconn.execute(f"DELETE FROM mart_pressure_coverage WHERE game_id IN ({placeholders})", game_ids)
        rep_rows = sconn.execute(
            f"""
            SELECT p.game_id,
                   SUM(CASE WHEN c.family IN ('pressure_emergence', 'block_pressure') THEN 1 ELSE 0 END) AS pressure_reps,
                   SUM(CASE WHEN c.family IN ('separation_window', 'catch_point_contest', 'coverage_lane_integrity') THEN 1 ELSE 0 END) AS coverage_reps,
                   SUM(CASE WHEN c.family IN ('lane_creation', 'fit_integrity', 'tackle_finish') THEN 1 ELSE 0 END) AS run_fit_reps
            FROM contest_resolutions c
            JOIN play_results p ON p.play_id = c.play_id
            WHERE p.game_id IN ({placeholders})
            GROUP BY p.game_id
            """,
            tuple(game_ids),
        ).fetchall()
        if rep_rows:
            dconn.executemany("INSERT INTO mart_pressure_coverage VALUES (?, ?, ?, ?)", rep_rows)

        # Turnover causality.
        dconn.execute(f"DELETE FROM mart_turnover_causality WHERE game_id IN ({placeholders})", game_ids)
        ca_rows = sconn.execute(
            f"""
            SELECT g.game_id, c.terminal_event, c.source_id, c.weight, c.description
            FROM causality_nodes c
            JOIN play_results p ON p.play_id = c.play_id
            JOIN games g ON g.game_id = p.game_id
            WHERE g.game_id IN ({placeholders})
              AND c.terminal_event IN ('interception', 'fumble')
            """,
            tuple(game_ids),
        ).fetchall()
        if ca_rows:
            dconn.executemany("INSERT INTO mart_turnover_causality VALUES (?, ?, ?, ?, ?)", ca_rows)

        # Shared responsibility rollup from contest contributors.
        dconn.execute(f"DELETE FROM mart_shared_responsibility WHERE game_id IN ({placeholders})", game_ids)
        sr_rows = sconn.execute(
            f"""
            SELECT p.game_id, c.contributor_json
            FROM contest_resolutions c
            JOIN play_results p ON p.play_id = c.play_id
            WHERE p.game_id IN ({placeholders})
            """,
            tuple(game_ids),
        ).fetchall()
        if sr_rows:
            import json

            agg_sr: dict[tuple[str, str], list[float]] = {}
            for game_id, contributor_json in sr_rows:
                data = json.loads(contributor_json)
                for actor_id, weight in data.items():
                    sr_key = (str(game_id), str(actor_id))
                    if sr_key not in agg_sr:
                        agg_sr[sr_key] = [0.0, 0.0]
                    agg_sr[sr_key][0] += 1.0
                    agg_sr[sr_key][1] += abs(float(weight))
            dconn.executemany(
                "INSERT INTO mart_shared_responsibility VALUES (?, ?, ?, ?)",
                [(game_id, actor_id, int(vals[0]), float(vals[1])) for (game_id, actor_id), vals in agg_sr.items()],
            )
