# Gridiron Rail: Sundays (GR:S)

Data-first pro football dynasty simulation skeleton for 1.0.

## Quick start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .[dev]
pytest
python -m grs.cli
```

## Notes

- Gameplay randomness is non-deterministic by default.
- Seeded determinism is exposed for tests/dev only.
- UI is PySide6 Qt Widgets with chart adapter abstraction.
- Authoritative store: SQLite.
- Analytics store: DuckDB.
