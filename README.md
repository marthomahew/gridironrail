# Gridiron Rail: Sundays (GR:S)

Data-first pro football dynasty simulation vertical slice for 1.0.

## Quick start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .[dev]
pytest
python -m grs.cli --weeks 2 --play-user-game
```

## What this slice includes

- Full weekly slate simulation (8-team league, 18-week schedule)
- Complete game session engine (multi-snap game flow, score/clock/drive progression)
- Shared football resolver across play/sim/off-screen modes
- Organizational constraints (cap/roster/depth-chart validation)
- Authoritative SQLite + incremental DuckDB analytics ETL
- Retained-game film room artifacts + deep log retention policy
- CSV + Parquet export pipeline from analytics marts
- Deterministic replay harness for seed-based regression checks

## North Star Docs

- `NORTHSTAR.md` - project-wide intent, constraints, and non-negotiables
- `NORTHSTAR_FOOTBALL.md` - football-layer input/data/contracts charter (required before deeper phasal expansion)
