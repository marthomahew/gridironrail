from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb

from grs.contracts import ActionRequest, ActionType
from grs.core import make_id
from grs.simulation import DynastyRuntime


def _count(conn: sqlite3.Connection, query: str, params: tuple = ()) -> int:
    return int(conn.execute(query, params).fetchone()[0])


def test_full_week_execution_finalizes_all_games_and_no_orphans(tmp_path: Path):
    runtime = DynastyRuntime(root=tmp_path, seed=7)
    res = runtime.handle_action(ActionRequest(make_id("req"), ActionType.ADVANCE_WEEK, {}, "T01"))
    assert res.success

    with sqlite3.connect(runtime.paths.sqlite_path) as conn:
        season, week = 2026, 1
        scheduled = _count(conn, "SELECT COUNT(*) FROM schedule WHERE season = ? AND week = ?", (season, week))
        finalized = _count(conn, "SELECT COUNT(*) FROM schedule WHERE season = ? AND week = ? AND status = 'final'", (season, week))
        assert scheduled == finalized

        orphan = _count(
            conn,
            """
            SELECT COUNT(*)
            FROM game_state gs
            LEFT JOIN games g ON g.game_id = gs.game_id
            WHERE g.game_id IS NULL
            """,
        )
        assert orphan == 0


def test_retention_policy_keeps_retained_and_purges_non_retained(tmp_path: Path):
    runtime = DynastyRuntime(root=tmp_path, seed=8)
    runtime.handle_action(ActionRequest(make_id("req"), ActionType.ADVANCE_WEEK, {}, "T01"))

    with sqlite3.connect(runtime.paths.sqlite_path) as conn:
        retained_game_ids = [r[0] for r in conn.execute("SELECT game_id FROM games WHERE retained = 1").fetchall()]
        non_retained_game_ids = [r[0] for r in conn.execute("SELECT game_id FROM games WHERE retained = 0").fetchall()]

        assert retained_game_ids
        assert non_retained_game_ids

        retained_reps = _count(
            conn,
            "SELECT COUNT(*) FROM rep_ledger WHERE play_id IN (SELECT play_id FROM play_results WHERE game_id = ?)",
            (retained_game_ids[0],),
        )
        non_retained_reps = _count(
            conn,
            "SELECT COUNT(*) FROM rep_ledger WHERE play_id IN (SELECT play_id FROM play_results WHERE game_id = ?)",
            (non_retained_game_ids[0],),
        )

        assert retained_reps > 0
        assert non_retained_reps == 0


def test_export_csv_parquet_row_count_parity(tmp_path: Path):
    runtime = DynastyRuntime(root=tmp_path, seed=11)
    runtime.handle_action(ActionRequest(make_id("req"), ActionType.ADVANCE_WEEK, {}, "T01"))

    outputs = runtime.export()
    csv_files = [p for p in outputs if p.suffix == ".csv"]
    parquet_files = [p for p in outputs if p.suffix == ".parquet"]
    assert csv_files and parquet_files

    with duckdb.connect() as conn:
        for csv_path in csv_files:
            parquet_path = csv_path.with_suffix(".parquet")
            csv_count = conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{csv_path.as_posix()}')").fetchone()[0]
            parquet_count = conn.execute(f"SELECT COUNT(*) FROM parquet_scan('{parquet_path.as_posix()}')").fetchone()[0]
            assert csv_count == parquet_count
