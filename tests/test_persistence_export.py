from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from grs.contracts import ActionRequest
from grs.core import make_id
from grs.simulation import DynastyRuntime


def _count(conn: sqlite3.Connection, query: str, params: tuple = ()) -> int:
    return int(conn.execute(query, params).fetchone()[0])


def test_retention_non_retained_purges_deep_logs(tmp_path: Path):
    runtime = DynastyRuntime(root=tmp_path, seed=7)
    runtime.org_state.phase = "regular"
    runtime.handle_action(ActionRequest(make_id("req"), "play_snap", {}, "USER_TEAM"))

    with sqlite3.connect(runtime.paths.sqlite_path) as conn:
        game_id = conn.execute("SELECT game_id FROM games ORDER BY created_at DESC LIMIT 1").fetchone()[0]
        retained = conn.execute("SELECT retained FROM games WHERE game_id = ?", (game_id,)).fetchone()[0]
        reps = _count(
            conn,
            "SELECT COUNT(*) FROM rep_ledger WHERE play_id IN (SELECT play_id FROM play_results WHERE game_id = ?)",
            (game_id,),
        )
        assert retained == 0
        assert reps == 0


def test_retention_playoff_keeps_deep_logs(tmp_path: Path):
    runtime = DynastyRuntime(root=tmp_path, seed=8)
    runtime.org_state.phase = "postseason"
    runtime.org_state.week = 2
    runtime.handle_action(ActionRequest(make_id("req"), "play_snap", {}, "USER_TEAM"))

    with sqlite3.connect(runtime.paths.sqlite_path) as conn:
        game_id = conn.execute("SELECT game_id FROM games ORDER BY created_at DESC LIMIT 1").fetchone()[0]
        retained = conn.execute("SELECT retained FROM games WHERE game_id = ?", (game_id,)).fetchone()[0]
        reps = _count(
            conn,
            "SELECT COUNT(*) FROM rep_ledger WHERE play_id IN (SELECT play_id FROM play_results WHERE game_id = ?)",
            (game_id,),
        )
        assert retained == 1
        assert reps > 0


def test_export_csv_parquet_row_count_parity(tmp_path: Path):
    duckdb = pytest.importorskip("duckdb")
    runtime = DynastyRuntime(root=tmp_path, seed=11)
    runtime.handle_action(ActionRequest(make_id("req"), "play_snap", {}, "USER_TEAM"))
    runtime.store.save_transactions(runtime.org_state.transactions)

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
