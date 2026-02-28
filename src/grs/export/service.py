from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    import duckdb
except ModuleNotFoundError:  # pragma: no cover - exercised via runtime environments without duckdb
    duckdb = None  # type: ignore[assignment]


class ExportService:
    def __init__(self, analytics_db: Path) -> None:
        self.analytics_db = analytics_db

    def export_required_datasets(self, output_dir: Path) -> list[Path]:
        if duckdb is None:
            raise RuntimeError("duckdb is required for exports")
        output_dir.mkdir(parents=True, exist_ok=True)
        outputs: list[Path] = []
        with duckdb.connect(str(self.analytics_db)) as conn:
            outputs.extend(self._export_table(conn, "mart_game_summaries", output_dir / "game_summaries"))
            outputs.extend(self._export_table(conn, "mart_transactions", output_dir / "transactions"))
            outputs.extend(self._export_table(conn, "mart_cap_history", output_dir / "cap_history"))
            outputs.extend(self._export_table(conn, "mart_play_events", output_dir / "play_events"))
        return outputs

    def _export_table(self, conn: Any, table: str, stem: Path) -> list[Path]:
        csv_path = stem.with_suffix(".csv")
        parquet_path = stem.with_suffix(".parquet")
        conn.execute(f"COPY (SELECT * FROM {table}) TO '{csv_path.as_posix()}' (HEADER, DELIMITER ',')")
        conn.execute(f"COPY (SELECT * FROM {table}) TO '{parquet_path.as_posix()}' (FORMAT PARQUET)")
        return [csv_path, parquet_path]
