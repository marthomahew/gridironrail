# Gridiron Rail: Sundays (GR:S)

Data-first pro football dynasty simulation on the 1.0 MVP path.

## Quick start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .[dev]
pytest
python -m grs.cli --weeks 2 --play-user-game
```

## Current status (implemented)

- Full weekly slate simulation (8-team league, 18-week schedule)
- Multi-snap game sessions (clock/quarter/drive/possession transitions)
- One football resolver path across `play`, `sim`, and `off-screen` modes
- Strict pre-sim validation gate with hard-fail + forensic artifact on invalid inputs
- Externalized, versioned football resources:
- personnel packages
- formations
- offensive/defensive concepts
- coaching policies
- Capability-driven canonical trait contract (lean core, no arbitrary count) with atomic storage (`player_id`, `trait_code`) and range/completeness enforcement
- Trait canon lock approved for 77 total traits (72 active now + 5 reserved for phasal retrofit)
- SQLite authoritative store + DuckDB analytics marts with weekly ETL
- Retained-game deep logs + non-retained purge-after-derivation policy
- CSV and Parquet exports from DuckDB marts
- Replay harness and seeded determinism tests for dev/regression

## Guardrails that are intentionally absent

- No soft-fail/default-and-continue simulation paths
- No per-team cheats or hidden balancing
- No OVR fallback when required atomic traits are missing
- No UI-side simulation logic or direct authoritative state mutation

## Near-term goals

- Integrate atomic trait weighting deeper into football resolution math
- Expand phasal causality depth while preserving derivability and auditability
- Continue hard-fail contract coverage for all sim-triggering action routes
- Keep packaging-safe boundaries (PySide6-only, freeze-friendly resource loading)

## North Star Docs

- `NORTHSTAR.md` - project-wide intent, constraints, implementation status, and next milestones
- `NORTHSTAR_FOOTBALL.md` - football-layer data/contracts charter and pre-sim gate requirements
