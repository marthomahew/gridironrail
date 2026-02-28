from __future__ import annotations

import copy
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from grs.contracts import (
    ActorRef,
    CalibrationRunRequest,
    CalibrationRunResult,
    CalibrationTraitProfile,
    InGameState,
    ParameterizedIntent,
    PlayType,
    SimMode,
    Situation,
    SnapContextPackage,
    TuningProfile,
    RandomSource,
)
from grs.core import gameplay_random, make_id, seeded_random
from grs.football.resolver import FootballEngine, FootballResolver
from grs.football.resources import ResourceResolver
from grs.football.traits import canonical_trait_catalog, required_trait_codes
from grs.football.validation import PreSimValidator


class CalibrationService:
    """Dev-only batch calibration/tuning service isolated from normal gameplay flows."""

    def __init__(self, *, base_resolver: ResourceResolver) -> None:
        self._base_resolver = base_resolver
        self._profiles = self._default_tuning_profiles()

    def list_tuning_profiles(self) -> list[TuningProfile]:
        return list(self._profiles.values())

    def get_tuning_profile(self, profile_id: str) -> TuningProfile:
        profile = self._profiles.get(profile_id)
        if profile is None:
            raise ValueError(f"unknown tuning profile '{profile_id}'")
        return profile

    def upsert_tuning_profile(self, profile: TuningProfile) -> None:
        self._profiles[profile.profile_id] = profile

    def run_batch(self, request: CalibrationRunRequest) -> CalibrationRunResult:
        if request.sample_count <= 0:
            raise ValueError("sample_count must be > 0")
        tuning = self.get_tuning_profile(request.tuning_profile_id)
        random_source = seeded_random(request.seed) if request.seed is not None else gameplay_random()
        resolver_random = seeded_random(request.seed) if request.seed is not None else gameplay_random()

        resolver = self._build_tuned_resolver(tuning)
        engine = FootballEngine(
            resolver=FootballResolver(
                random_source=resolver_random,
                resource_resolver=resolver,
            ),
            validator=PreSimValidator(resource_resolver=resolver, trait_catalog=canonical_trait_catalog()),
        )

        total_yards = 0
        turnover_count = 0
        score_count = 0
        penalty_count = 0
        terminal_distribution: dict[str, int] = {}

        for idx in range(request.sample_count):
            substream = random_source.spawn(f"calibration:{idx}")
            scp = self._build_context(
                play_id=f"CAL_{request.play_type.value}_{idx:04d}",
                play_type=request.play_type,
                trait_profile=request.trait_profile,
                random_source=substream,
            )
            snap = engine.run_snap(scp)
            total_yards += snap.play_result.yards
            turnover_count += int(snap.play_result.turnover)
            score_count += int(snap.play_result.score_event is not None)
            penalty_count += len(snap.play_result.penalties)
            terminal = snap.causality_chain.terminal_event
            terminal_distribution[terminal] = terminal_distribution.get(terminal, 0) + 1

        return CalibrationRunResult(
            run_id=make_id("cal"),
            play_type=request.play_type,
            sample_count=request.sample_count,
            trait_profile=request.trait_profile,
            tuning_profile_id=tuning.profile_id,
            mean_yards=total_yards / request.sample_count,
            turnover_rate=turnover_count / request.sample_count,
            score_rate=score_count / request.sample_count,
            penalty_rate=penalty_count / request.sample_count,
            terminal_distribution=terminal_distribution,
            seed=request.seed,
        )

    def persist_result(self, result: CalibrationRunResult, duckdb_path: Path) -> None:
        try:
            import duckdb
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError("duckdb is required for calibration persistence") from exc

        duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(str(duckdb_path)) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dev_calibration_runs (
                    run_id VARCHAR PRIMARY KEY,
                    play_type VARCHAR,
                    sample_count INTEGER,
                    trait_profile VARCHAR,
                    tuning_profile_id VARCHAR,
                    mean_yards DOUBLE,
                    turnover_rate DOUBLE,
                    score_rate DOUBLE,
                    penalty_rate DOUBLE,
                    terminal_distribution_json VARCHAR,
                    seed BIGINT,
                    persisted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dev_calibration_terminal_distribution (
                    run_id VARCHAR NOT NULL,
                    terminal_event VARCHAR NOT NULL,
                    event_count INTEGER NOT NULL,
                    event_rate DOUBLE NOT NULL,
                    PRIMARY KEY (run_id, terminal_event)
                )
                """
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO dev_calibration_runs(
                    run_id, play_type, sample_count, trait_profile, tuning_profile_id,
                    mean_yards, turnover_rate, score_rate, penalty_rate, terminal_distribution_json, seed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    result.run_id,
                    result.play_type.value,
                    result.sample_count,
                    result.trait_profile.value,
                    result.tuning_profile_id,
                    result.mean_yards,
                    result.turnover_rate,
                    result.score_rate,
                    result.penalty_rate,
                    json.dumps(result.terminal_distribution),
                    result.seed,
                ],
            )
            conn.execute(
                "DELETE FROM dev_calibration_terminal_distribution WHERE run_id = ?",
                [result.run_id],
            )
            rows = [
                (
                    result.run_id,
                    terminal_event,
                    count,
                    (count / result.sample_count),
                )
                for terminal_event, count in sorted(result.terminal_distribution.items())
            ]
            if rows:
                conn.executemany(
                    """
                    INSERT INTO dev_calibration_terminal_distribution(
                        run_id, terminal_event, event_count, event_rate
                    ) VALUES (?, ?, ?, ?)
                    """,
                    rows,
                )

    def export_reports(self, duckdb_path: Path, output_dir: Path) -> tuple[list[Path], dict[str, int]]:
        try:
            import duckdb
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError("duckdb is required for calibration export") from exc

        output_dir.mkdir(parents=True, exist_ok=True)
        stems = {
            "dev_calibration_runs": output_dir / "dev_calibration_runs",
            "dev_calibration_terminal_distribution": output_dir / "dev_calibration_terminal_distribution",
        }
        outputs: list[Path] = []
        row_counts: dict[str, int] = {}
        with duckdb.connect(str(duckdb_path)) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dev_calibration_runs (
                    run_id VARCHAR PRIMARY KEY,
                    play_type VARCHAR,
                    sample_count INTEGER,
                    trait_profile VARCHAR,
                    tuning_profile_id VARCHAR,
                    mean_yards DOUBLE,
                    turnover_rate DOUBLE,
                    score_rate DOUBLE,
                    penalty_rate DOUBLE,
                    terminal_distribution_json VARCHAR,
                    seed BIGINT,
                    persisted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dev_calibration_terminal_distribution (
                    run_id VARCHAR NOT NULL,
                    terminal_event VARCHAR NOT NULL,
                    event_count INTEGER NOT NULL,
                    event_rate DOUBLE NOT NULL,
                    PRIMARY KEY (run_id, terminal_event)
                )
                """
            )
            for table, stem in stems.items():
                count_row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                row_counts[table] = int(count_row[0]) if count_row is not None else 0
                csv_path = stem.with_suffix(".csv")
                parquet_path = stem.with_suffix(".parquet")
                conn.execute(f"COPY (SELECT * FROM {table}) TO '{csv_path.as_posix()}' (HEADER, DELIMITER ',')")
                conn.execute(f"COPY (SELECT * FROM {table}) TO '{parquet_path.as_posix()}' (FORMAT PARQUET)")
                outputs.extend([csv_path, parquet_path])
        return outputs, row_counts

    def _build_tuned_resolver(self, tuning: TuningProfile) -> ResourceResolver:
        payload = self._load_trait_influence_payload()
        if "resources" not in payload:
            raise ValueError("trait_influences payload missing resources")
        resources = payload["resources"]
        if not isinstance(resources, list):
            raise ValueError("trait_influences payload resources must be list")
        tuned_resources = copy.deepcopy(resources)
        for play_resource in tuned_resources:
            if "families" not in play_resource or not isinstance(play_resource["families"], list):
                raise ValueError("trait influence play resource must provide families list")
            families = play_resource["families"]
            for family in families:
                if "family" not in family:
                    raise ValueError("trait influence family entry missing family key")
                family_name = str(family["family"])
                multiplier = tuning.family_weight_multipliers.get(family_name, 1.0)
                if multiplier != 1.0:
                    self._scale_weights(family, "offense_weights", multiplier)
                    self._scale_weights(family, "defense_weights", multiplier)
            if "outcome_profile" not in play_resource or not isinstance(play_resource["outcome_profile"], dict):
                raise ValueError("trait influence play resource missing outcome_profile")
            outcome = play_resource["outcome_profile"]
            for key, mult in tuning.outcome_multipliers.items():
                if key in outcome:
                    outcome[key] = float(outcome[key]) * mult

        override = {"manifest": payload["manifest"], "resources": tuned_resources}
        self._update_checksum(override)
        return ResourceResolver(bundle_overrides={"trait_influences.json": override})

    def _load_trait_influence_payload(self) -> dict[str, Any]:
        import importlib.resources as resources

        package = resources.files("grs.resources.football")
        raw = (package / "trait_influences.json").read_text(encoding="utf-8")
        return json.loads(raw)

    def _scale_weights(self, family: dict[str, Any], key: str, multiplier: float) -> None:
        if key not in family:
            raise ValueError(f"trait influence family missing '{key}'")
        weights = family[key]
        if not isinstance(weights, dict):
            raise ValueError(f"trait influence family '{key}' must be an object")
        for trait_code in list(weights.keys()):
            weights[trait_code] = float(weights[trait_code]) * multiplier

    def _update_checksum(self, payload: dict[str, Any]) -> None:
        import hashlib

        canonical = json.dumps(payload["resources"], sort_keys=True, separators=(",", ":")).encode("utf-8")
        payload["manifest"]["checksum"] = hashlib.sha256(canonical).hexdigest()

    def _default_tuning_profiles(self) -> dict[str, TuningProfile]:
        profiles = [
            TuningProfile(profile_id="neutral", description="No tuning multipliers"),
            TuningProfile(
                profile_id="pressure_heavy",
                description="Increase pressure and decision contest influence for diagnostics",
                family_weight_multipliers={"pressure_emergence": 1.15, "decision_risk": 1.1},
                outcome_multipliers={"turnover_scale": 1.1},
            ),
            TuningProfile(
                profile_id="explosive_heavy",
                description="Increase continuation contest influence for explosive diagnostics",
                family_weight_multipliers={"yac_continuation": 1.2, "lane_creation": 1.1},
                outcome_multipliers={"noise_scale": 1.05},
            ),
        ]
        return {profile.profile_id: profile for profile in profiles}

    def _build_context(
        self,
        *,
        play_id: str,
        play_type: PlayType,
        trait_profile: CalibrationTraitProfile,
        random_source: RandomSource,
    ) -> SnapContextPackage:
        offense_roles, defense_roles = self._roles_for_play_type(play_type)
        participants: list[ActorRef] = []
        for idx, role in enumerate(offense_roles):
            participants.append(ActorRef(actor_id=f"A_{idx}", team_id="A", role=role))
        for idx, role in enumerate(defense_roles):
            participants.append(ActorRef(actor_id=f"B_{idx}", team_id="B", role=role))

        states = {
            p.actor_id: InGameState(
                fatigue=0.2,
                acute_wear=0.15,
                confidence_tilt=0.0,
                discipline_risk=0.45,
            )
            for p in participants
        }
        trait_vectors = self._trait_vectors(participants, trait_profile, random_source)
        personnel, formation, offense_concept, defense_concept = self._intent_for_play_type(play_type)
        return SnapContextPackage(
            game_id="CALIBRATION",
            play_id=play_id,
            mode=SimMode.OFFSCREEN,
            situation=Situation(
                quarter=2,
                clock_seconds=720,
                down=2,
                distance=7,
                yard_line=55,
                possession_team_id="A",
                score_diff=0,
                timeouts_offense=3,
                timeouts_defense=3,
            ),
            participants=participants,
            in_game_states=states,
            trait_vectors=trait_vectors,
            intent=ParameterizedIntent(
                personnel=personnel,
                formation=formation,
                offensive_concept=offense_concept,
                defensive_concept=defense_concept,
                play_type=play_type,
            ),
            weather_flags=["clear"],
        )

    def _trait_vectors(
        self,
        participants: list[ActorRef],
        profile: CalibrationTraitProfile,
        random_source: RandomSource,
    ) -> dict[str, dict[str, float]]:
        codes = required_trait_codes()
        out: dict[str, dict[str, float]] = {}
        for participant in participants:
            values: dict[str, float] = {}
            for code in codes:
                if profile == CalibrationTraitProfile.UNIFORM_50:
                    values[code] = 50.0
                elif profile == CalibrationTraitProfile.NARROW_45_55:
                    values[code] = 45.0 + (random_source.rand() * 10.0)
                else:
                    values[code] = 40.0 + (random_source.rand() * 20.0)
            out[participant.actor_id] = values
        return out

    def _roles_for_play_type(self, play_type: PlayType) -> tuple[list[str], list[str]]:
        if play_type == PlayType.PUNT:
            return (
                ["P", "OL", "OL", "OL", "OL", "OL", "TE", "WR", "WR", "CB", "S"],
                ["DE", "DT", "DT", "DE", "LB", "LB", "LB", "CB", "CB", "S", "RB"],
            )
        if play_type == PlayType.KICKOFF:
            return (
                ["K", "LB", "LB", "LB", "CB", "CB", "S", "S", "DE", "DE", "WR"],
                ["RB", "WR", "WR", "WR", "TE", "LB", "LB", "CB", "S", "S", "DE"],
            )
        if play_type in {PlayType.FIELD_GOAL, PlayType.EXTRA_POINT}:
            return (
                ["K", "OL", "OL", "OL", "OL", "OL", "TE", "LB", "LB", "DE", "DE"],
                ["DE", "DE", "DT", "DT", "LB", "LB", "LB", "CB", "CB", "S", "S"],
            )
        return (
            ["QB", "RB", "WR", "WR", "WR", "TE", "OL", "OL", "OL", "OL", "OL"],
            ["DE", "DT", "DT", "DE", "LB", "LB", "LB", "CB", "CB", "S", "S"],
        )

    def _intent_for_play_type(self, play_type: PlayType) -> tuple[str, str, str, str]:
        if play_type == PlayType.RUN:
            return ("11", "singleback", "inside_zone", "base_over")
        if play_type == PlayType.PASS:
            return ("11", "gun_trips", "spacing", "cover3_match")
        if play_type == PlayType.PUNT:
            return ("punt", "punt_spread", "punt_safe", "punt_return_safe")
        if play_type == PlayType.KICKOFF:
            return ("kickoff", "kickoff_standard", "kickoff_sky", "kickoff_return")
        if play_type == PlayType.FIELD_GOAL:
            return ("field_goal", "field_goal_heavy", "field_goal_unit", "field_goal_block")
        if play_type == PlayType.EXTRA_POINT:
            return ("extra_point", "field_goal_heavy", "field_goal_unit", "field_goal_block")
        return ("two_point", "gun_trips", "two_point_mesh", "cover3_match")


def calibration_result_to_dict(result: CalibrationRunResult) -> dict[str, Any]:
    data = asdict(result)
    data["play_type"] = result.play_type.value
    data["trait_profile"] = result.trait_profile.value
    return data
