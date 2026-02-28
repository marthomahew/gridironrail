from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from grs.contracts import (
    BatchRunRequest,
    BatchRunResult,
    CalibrationRunRequest,
    CalibrationSessionRef,
    CalibrationWriteAdapter,
    CalibrationExportAdapter,
    CalibrationReadAdapter,
    CalibrationRunAdapter,
    DevCalibrationGateway,
    TuningPatchRequest,
    TuningPatchResult,
    TuningProfile,
)
from grs.core import make_id
from grs.football.calibration import CalibrationService


class LocalDevCalibrationGateway(
    DevCalibrationGateway,
    CalibrationReadAdapter,
    CalibrationWriteAdapter,
    CalibrationRunAdapter,
    CalibrationExportAdapter,
):
    def __init__(self, *, service: CalibrationService, duckdb_path: Path, export_dir: Path) -> None:
        self._service = service
        self._duckdb_path = duckdb_path
        self._export_dir = export_dir
        profiles = self._service.list_tuning_profiles()
        if not profiles:
            raise ValueError("calibration service must provide at least one tuning profile")
        self._active_profile_id = profiles[0].profile_id

    def list_tuning_profiles(self) -> list[TuningProfile]:
        return self._service.list_tuning_profiles()

    def list_profiles(self) -> list[TuningProfile]:
        return self.list_tuning_profiles()

    def active_profile(self) -> str:
        return self._active_profile_id

    def set_tuning_profile(self, profile_id: str) -> TuningProfile:
        profile = self._service.get_tuning_profile(profile_id)
        self._active_profile_id = profile.profile_id
        return profile

    def set_profile(self, profile_id: str) -> TuningPatchResult:
        profile = self.set_tuning_profile(profile_id)
        return TuningPatchResult(profile=profile, patched_at=datetime.now(UTC), actor_team_id="dev")

    def patch_profile(self, request: TuningPatchRequest, actor_team_id: str) -> TuningPatchResult:
        existing = self._service.get_tuning_profile(request.profile_id)
        merged = replace(
            existing,
            family_weight_multipliers={**existing.family_weight_multipliers, **request.family_weight_multipliers},
            outcome_multipliers={**existing.outcome_multipliers, **request.outcome_multipliers},
        )
        self._service.upsert_tuning_profile(merged)
        self._active_profile_id = merged.profile_id
        return TuningPatchResult(profile=merged, patched_at=datetime.now(UTC), actor_team_id=actor_team_id)

    def run_batch(self, request: BatchRunRequest, actor_team_id: str) -> BatchRunResult:
        concrete = CalibrationRunRequest(
            play_type=request.play_type,
            sample_count=request.sample_count,
            trait_profile=request.trait_profile,
            seed=request.seed,
            tuning_profile_id=self._active_profile_id,
        )
        run = self._service.run_batch(concrete)
        self._service.persist_result(run, self._duckdb_path)
        return BatchRunResult(
            session=CalibrationSessionRef(
                session_id=make_id("calsess"),
                created_at=datetime.now(UTC),
                actor_team_id=actor_team_id,
                active_tuning_profile_id=self._active_profile_id,
                seed=concrete.seed,
            ),
            run=run,
        )

    def export_reports(self) -> tuple[list[str], dict[str, int]]:
        outputs, row_counts = self._service.export_reports(
            duckdb_path=self._duckdb_path,
            output_dir=self._export_dir,
        )
        return [str(p) for p in outputs], row_counts
