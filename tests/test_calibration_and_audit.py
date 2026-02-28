from __future__ import annotations

import inspect
import sys
from pathlib import Path

import duckdb

from grs.contracts import ActionRequest, ActionType
from grs.core import make_id
from grs.football import FootballContractAuditor, ResourceResolver
from grs.football.calibration import CalibrationService
from grs.football.session import GameSessionEngine
from grs.simulation import DynastyRuntime
from tests.helpers import bootstrap_profile


def test_football_contract_audit_matrix_runs() -> None:
    report = FootballContractAuditor().run()
    assert report.checks
    assert any(check.check_id == "mode_invariance" for check in report.checks)
    assert all(isinstance(check.passed, bool) for check in report.checks)
    assert report.passed


def test_calibration_service_profiles_exposed() -> None:
    service = CalibrationService(base_resolver=ResourceResolver())
    profiles = service.list_tuning_profiles()
    assert profiles
    assert any(profile.profile_id == "neutral" for profile in profiles)


def test_runtime_dev_actions_blocked_without_dev_mode(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=7, dev_mode=False)
    blocked = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.RUN_CALIBRATION_BATCH, {"play_type": "pass", "sample_count": 50}, "T01")
    )
    assert not blocked.success
    assert "dev mode required" in blocked.message


def test_runtime_dev_calibration_action_with_audit_stamp(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=8, dev_mode=True)
    bootstrap_profile(runtime)
    assert runtime.store is not None

    profiles = runtime.handle_action(ActionRequest(make_id("req"), ActionType.GET_TUNING_PROFILES, {}, "T01"))
    assert profiles.success
    assert profiles.data["profiles"]

    selected = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.SET_TUNING_PROFILE, {"profile_id": "neutral"}, "T01")
    )
    assert selected.success
    assert selected.data["active_profile_id"] == "neutral"

    batch = runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.RUN_CALIBRATION_BATCH,
            {"play_type": "pass", "sample_count": 50, "trait_profile": "uniform_50", "seed": 123},
            "T01",
        )
    )
    assert batch.success
    assert batch.data["sample_count"] == 50
    assert batch.data["trait_profile"] == "uniform_50"
    assert (tmp_path / "data" / "profiles" / "profile_test" / "analytics.duckdb").exists()

    audit = runtime.handle_action(ActionRequest(make_id("req"), ActionType.RUN_FOOTBALL_AUDIT, {}, "T01"))
    assert audit.success
    assert audit.data["checks"]

    exported = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.EXPORT_CALIBRATION_REPORT, {}, "T01")
    )
    assert exported.success
    assert exported.data["exported_files"]
    assert "dev_calibration_runs" in exported.data["row_counts"]
    assert "dev_calibration_terminal_distribution" in exported.data["row_counts"]
    files = [Path(p) for p in exported.data["exported_files"]]
    csv_files = [p for p in files if p.suffix == ".csv"]
    with duckdb.connect() as conn:
        for csv_path in csv_files:
            parquet_path = csv_path.with_suffix(".parquet")
            csv_count = conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{csv_path.as_posix()}')").fetchone()[0]
            parquet_count = conn.execute(f"SELECT COUNT(*) FROM parquet_scan('{parquet_path.as_posix()}')").fetchone()[0]
            assert csv_count == parquet_count

    with runtime.store.connect() as conn:
        dev_events = conn.execute("SELECT COUNT(*) FROM narrative_events WHERE scope = 'dev'").fetchone()[0]
    assert dev_events >= 3


def test_calibration_profiles_do_not_leak_into_session_runtime() -> None:
    source = inspect.getsource(GameSessionEngine)
    assert "CalibrationTraitProfile" not in source


def test_runtime_non_dev_does_not_import_devtools_modules(tmp_path: Path) -> None:
    before = set(sys.modules.keys())
    DynastyRuntime(root=tmp_path, seed=10, dev_mode=False)
    after = set(sys.modules.keys())
    assert "grs.devtools.calibration_gateway" not in (after - before)


def test_runtime_strict_audit_action_requires_dev_mode(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=11, dev_mode=False)
    blocked = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.RUN_STRICT_AUDIT, {}, "T01")
    )
    assert not blocked.success
    assert "dev mode required" in blocked.message


def test_runtime_strict_audit_runs_in_dev_mode(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=12, dev_mode=True)
    result = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.RUN_STRICT_AUDIT, {}, "T01")
    )
    assert result.success
    assert "sections" in result.data


def test_runtime_export_calibration_requires_dev_mode(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=9, dev_mode=False)
    blocked = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.EXPORT_CALIBRATION_REPORT, {}, "T01")
    )
    assert not blocked.success
    assert "dev mode required" in blocked.message
