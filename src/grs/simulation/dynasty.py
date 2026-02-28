from __future__ import annotations

import importlib
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from grs.contracts import (
    ActionRequest,
    ActionResult,
    ActionType,
    BatchRunRequest,
    CapabilityDomain,
    CapabilityPolicy,
    CalibrationTraitProfile,
    CapPolicyConfig,
    DevCalibrationGateway,
    Difficulty,
    FranchiseProfile,
    GameSessionState,
    LeagueSetupConfig,
    LeagueSetupValidationReport,
    LeagueSnapshotRef,
    ManagementMode,
    NarrativeEvent,
    PlayType,
    PlaycallRequest,
    RosterPolicyConfig,
    TuningPatchRequest,
    RetentionPolicy,
    SchedulePolicyConfig,
    ScheduleEntry,
    SimMode,
    ValidationError,
    WeekSimulationResult,
)
from grs.core import (
    EventBus,
    EngineIntegrityError,
    build_forensic_artifact,
    default_difficulty_profiles,
    gameplay_random,
    make_id,
    now_utc,
    persist_forensic_artifact,
    seeded_random,
    strict_execution_policy,
)
from grs.export import ExportService
from grs.football import (
    FootballEngine,
    FootballResolver,
    GameSessionEngine,
    GameSessionResult,
    PolicyDrivenCoachDecisionEngine,
    PreSimValidator,
    ResourceResolver,
    run_football_contract_audit,
)
from grs.football.traits import canonical_trait_catalog
from grs.org import (
    CapabilityEnforcementService,
    LeagueSetupValidator,
    LeagueState,
    LeagueStructureCompiler,
    OrganizationalEngine,
    RosterGenerationService,
    ScheduleGenerationService,
    TeamBlueprint,
    TeamSelectionPlanner,
    build_league_from_setup,
    rank_standings,
)
from grs.org.entities import Franchise
from grs.persistence import (
    AuthoritativeStore,
    GameRetentionContext,
    ProfileStore,
    run_weekly_etl,
    should_retain_game,
)


class RuntimePaths:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.profile_id: str | None = None

    def bind_profile(self, profile_id: str) -> None:
        self.profile_id = profile_id

    @property
    def profile_store_path(self) -> Path:
        return self.root / "data" / "profiles.sqlite3"

    @property
    def profiles_root(self) -> Path:
        return self.root / "data" / "profiles"

    def _require_profile_id(self) -> str:
        if self.profile_id is None:
            raise RuntimeError("active profile is required for profile-scoped path resolution")
        return self.profile_id

    @property
    def sqlite_path(self) -> Path:
        profile_id = self._require_profile_id()
        return self.profiles_root / profile_id / "authoritative.sqlite3"

    @property
    def duckdb_path(self) -> Path:
        profile_id = self._require_profile_id()
        return self.profiles_root / profile_id / "analytics.duckdb"

    @property
    def export_dir(self) -> Path:
        profile_id = self._require_profile_id()
        return self.root / "exports" / profile_id

    @property
    def forensic_dir(self) -> Path:
        profile_id = self._require_profile_id()
        return self.root / "forensics" / profile_id

    @property
    def snapshot_dir(self) -> Path:
        profile_id = self._require_profile_id()
        return self.profiles_root / profile_id / "snapshots"


class DynastyRuntime:
    def __init__(
        self,
        root: Path,
        difficulty: Difficulty = Difficulty.PRO,
        seed: int | None = None,
        user_team_id: str = "T01",
        dev_mode: bool = False,
    ) -> None:
        self.paths = RuntimePaths(root)
        self.paths.profile_store_path.parent.mkdir(parents=True, exist_ok=True)
        self.paths.profiles_root.mkdir(parents=True, exist_ok=True)

        profiles = default_difficulty_profiles()
        self.difficulty = profiles[difficulty]
        self.seed = seed
        self.dev_mode = dev_mode
        self.strict_policy = strict_execution_policy()

        self.rand = seeded_random(seed) if seed is not None else gameplay_random()
        self.event_bus = EventBus()
        self.profile_store = ProfileStore(self.paths.profile_store_path)
        self.profile_store.initialize_schema()
        self.store: AuthoritativeStore | None = None
        self.resource_resolver = ResourceResolver()
        self.pre_sim_validator = PreSimValidator(
            resource_resolver=self.resource_resolver,
            trait_catalog=canonical_trait_catalog(),
        )

        self.active_profile: FranchiseProfile | None = None
        self.org_state: LeagueState | None = None
        self.regular_season_weeks: int | None = None
        self.user_team_id = user_team_id
        self.capability_policy: CapabilityPolicy | None = None
        self.setup_validator = LeagueSetupValidator(self.resource_resolver)
        self.structure_compiler = LeagueStructureCompiler()
        self.team_selection_planner = TeamSelectionPlanner()
        self.schedule_service = ScheduleGenerationService()
        self.roster_generation_service = RosterGenerationService()
        self.capability_service = CapabilityEnforcementService()
        self.org_engine = OrganizationalEngine(
            rand=self.rand.spawn("org"),
            difficulty=self.difficulty,
            regular_season_weeks=18,
        )
        self.football = FootballEngine(
            resolver=FootballResolver(
                random_source=self.rand.spawn("football"),
                resource_resolver=self.resource_resolver,
            ),
            validator=self.pre_sim_validator,
        )
        self.coach_engine = PolicyDrivenCoachDecisionEngine(repository=self.resource_resolver)
        self.game_session = GameSessionEngine(
            self.football,
            self.coach_engine,
            validator=self.pre_sim_validator,
            random_source=self.rand.spawn("session"),
        )
        self.retention_policy = RetentionPolicy()
        self.dev_calibration: DevCalibrationGateway | None = None

        self.halted = False
        self.last_forensic_path: str | None = None
        self.pending_user_playcall: PlaycallRequest | None = None
        self.last_user_game_result: GameSessionResult | None = None

    def handle_action(self, request: ActionRequest) -> ActionResult:
        if self.halted:
            return ActionResult(
                request.request_id,
                False,
                f"runtime halted after integrity failure; forensic={self.last_forensic_path}",
                {"forensic_path": self.last_forensic_path},
            )

        try:
            return self._handle_action_core(request)
        except EngineIntegrityError as exc:
            self.last_forensic_path = str(persist_forensic_artifact(exc.artifact, self._forensic_dir()))
            self.halted = True
            return ActionResult(
                request.request_id,
                False,
                f"integrity failure: {exc.artifact.error_code}",
                {"forensic_path": self.last_forensic_path},
            )
        except Exception as exc:
            season = self.org_state.season if self.org_state is not None else -1
            week = self.org_state.week if self.org_state is not None else -1
            phase = self.org_state.phase if self.org_state is not None else "uninitialized"
            artifact = build_forensic_artifact(
                engine_scope="runtime",
                error_code="UNHANDLED_RUNTIME_EXCEPTION",
                message=str(exc),
                state_snapshot={"season": season, "week": week, "phase": phase},
                context={"action_type": str(request.action_type), "payload": request.payload},
                identifiers={"request_id": request.request_id, "team_id": request.actor_team_id},
                causal_fragment=["runtime_dispatch"],
            )
            self.last_forensic_path = str(persist_forensic_artifact(artifact, self._forensic_dir()))
            self.halted = True
            return ActionResult(
                request.request_id,
                False,
                f"runtime hard-stopped: {exc}",
                {"forensic_path": self.last_forensic_path},
            )

    def _handle_action_core(self, request: ActionRequest) -> ActionResult:
        action = self._normalize_action(request.action_type)

        if action == ActionType.LIST_PROFILES:
            profiles = self.profile_store.list_profiles()
            return ActionResult(
                request.request_id,
                True,
                "profiles",
                data={"profiles": [asdict(p) for p in profiles], "active_profile_id": self.active_profile.profile_id if self.active_profile else None},
            )

        if action == ActionType.CREATE_PROFILE:
            profile_name = str(request.payload["profile_name"]) if "profile_name" in request.payload else ""
            if not profile_name.strip():
                return ActionResult(request.request_id, False, "profile_name is required")
            profile_id = str(request.payload["profile_id"]) if "profile_id" in request.payload else make_id("profile")
            now = now_utc()
            profile = FranchiseProfile(
                profile_id=profile_id,
                profile_name=profile_name.strip(),
                created_at=now,
                last_opened_at=now,
                league_config_ref="",
                selected_user_team_id="",
                active_mode=ManagementMode.OWNER,
            )
            self.profile_store.save_profile(profile)
            return ActionResult(request.request_id, True, "profile created", data=asdict(profile))

        if action == ActionType.DELETE_PROFILE:
            profile_id = str(request.payload["profile_id"]) if "profile_id" in request.payload else ""
            if not profile_id:
                return ActionResult(request.request_id, False, "profile_id is required")
            self.profile_store.delete_profile(profile_id)
            if self.active_profile and self.active_profile.profile_id == profile_id:
                self.active_profile = None
                self.org_state = None
                self.store = None
                self.dev_calibration = None
            return ActionResult(request.request_id, True, "profile deleted", data={"profile_id": profile_id})

        if action == ActionType.LOAD_PROFILE:
            profile_id = str(request.payload["profile_id"]) if "profile_id" in request.payload else ""
            if not profile_id:
                return ActionResult(request.request_id, False, "profile_id is required")
            loaded = self._load_profile(profile_id)
            if loaded is None:
                return ActionResult(request.request_id, False, f"profile '{profile_id}' not found")
            return ActionResult(
                request.request_id,
                True,
                "profile loaded",
                data={"profile_id": loaded.profile_id, "profile_name": loaded.profile_name, "user_team_id": loaded.selected_user_team_id},
            )

        if action == ActionType.VALIDATE_LEAGUE_SETUP:
            profile_id = str(request.payload["profile_id"]) if "profile_id" in request.payload else ""
            if not profile_id:
                return ActionResult(request.request_id, False, "profile_id is required")
            existing_profile = self.profile_store.load_profile(profile_id)
            if existing_profile is None:
                now = now_utc()
                self.profile_store.save_profile(
                    FranchiseProfile(
                        profile_id=profile_id,
                        profile_name=str(request.payload["profile_name"]) if "profile_name" in request.payload else profile_id,
                        created_at=now,
                        last_opened_at=now,
                        league_config_ref="",
                        selected_user_team_id="",
                        active_mode=ManagementMode.OWNER,
                    )
                )
            try:
                setup_config = self._parse_setup_config(request.payload["setup"])
            except (KeyError, ValueError) as exc:
                return ActionResult(request.request_id, False, f"invalid setup payload: {exc}")
            report = self._validate_setup(profile_id=profile_id, setup_config=setup_config)
            team_candidates: list[str] = []
            if not report.blocking_issues:
                team_candidates = self._team_candidates_for_setup(setup_config)
            return ActionResult(
                request.request_id,
                True,
                "setup validated",
                data={
                    "ok": len(report.blocking_issues) == 0,
                    "report_id": report.report_id,
                    "issues": [asdict(issue) for issue in report.blocking_issues],
                    "team_candidates": team_candidates,
                },
            )

        if action == ActionType.CREATE_NEW_FRANCHISE_SAVE:
            profile_id = str(request.payload["profile_id"]) if "profile_id" in request.payload else ""
            if not profile_id:
                return ActionResult(request.request_id, False, "profile_id is required")
            try:
                setup_config = self._parse_setup_config(request.payload["setup"])
            except (KeyError, ValueError) as exc:
                return ActionResult(request.request_id, False, f"invalid setup payload: {exc}")
            selected_team_id = str(request.payload["selected_user_team_id"]) if "selected_user_team_id" in request.payload else ""
            if not selected_team_id:
                return ActionResult(request.request_id, False, "selected_user_team_id is required")
            try:
                created = self._create_new_franchise_save(
                    profile_id=profile_id,
                    profile_name=str(request.payload["profile_name"]) if "profile_name" in request.payload else profile_id,
                    selected_team_id=selected_team_id,
                    setup_config=setup_config,
                    actor_team_id=request.actor_team_id,
                )
            except ValueError as exc:
                return ActionResult(request.request_id, False, str(exc))
            return ActionResult(
                request.request_id,
                True,
                "franchise save created",
                data=created,
            )

        if action == ActionType.SET_ACTIVE_MODE:
            self._require_active_profile_action()
            if "mode" not in request.payload:
                return ActionResult(request.request_id, False, "mode is required")
            try:
                mode = ManagementMode(str(request.payload["mode"]))
            except ValueError:
                return ActionResult(request.request_id, False, f"invalid mode '{request.payload['mode']}'")
            reason = str(request.payload["reason"]) if "reason" in request.payload else "mode_changed"
            assert self.active_profile is not None
            assert self.org_state is not None
            self.capability_policy = self.capability_service.build_policy(
                mode=mode,
                overrides={},
                updated_by_team_id=request.actor_team_id,
                reason=reason,
            )
            self.active_profile.active_mode = mode
            self.org_state.capability_policy = self.capability_policy
            self.profile_store.save_mode_policy(self.active_profile.profile_id, self.capability_policy)
            self.profile_store.save_profile(self.active_profile)
            self._emit_dev_event(
                event_type="mode_changed",
                claims=[f"management mode changed to {mode.value}"],
                evidence_handles=[f"mode:{mode.value}", f"profile:{self.active_profile.profile_id}"],
            )
            return ActionResult(request.request_id, True, "active mode updated", data={"mode": mode.value})

        if action == ActionType.SET_CAPABILITY_OVERRIDE:
            self._require_active_profile_action()
            if "domain" not in request.payload or "enabled" not in request.payload:
                return ActionResult(request.request_id, False, "domain and enabled are required")
            try:
                domain = CapabilityDomain(str(request.payload["domain"]))
            except ValueError:
                return ActionResult(request.request_id, False, f"invalid capability domain '{request.payload['domain']}'")
            enabled = bool(request.payload["enabled"])
            reason = str(request.payload["reason"]) if "reason" in request.payload else "override_changed"
            if self.capability_policy is None:
                return ActionResult(request.request_id, False, "capability policy not initialized")
            overrides = dict(self.capability_policy.override_capabilities)
            overrides[domain] = enabled
            self.capability_policy = self.capability_service.build_policy(
                mode=self.capability_policy.mode,
                overrides=overrides,
                updated_by_team_id=request.actor_team_id,
                reason=reason,
            )
            assert self.active_profile is not None
            assert self.org_state is not None
            self.org_state.capability_policy = self.capability_policy
            self.profile_store.save_mode_policy(self.active_profile.profile_id, self.capability_policy)
            return ActionResult(
                request.request_id,
                True,
                "capability override updated",
                data={"domain": domain.value, "enabled": enabled},
            )

        if action == ActionType.GET_ORG_DASHBOARD:
            self._require_active_profile_action()
            assert self.org_state is not None
            assert self.active_profile is not None
            team = self._team(self.user_team_id)
            return ActionResult(
                request.request_id,
                True,
                "org dashboard",
                data={
                    "profile": asdict(self.active_profile),
                    "mode": self.active_profile.active_mode.value,
                    "capabilities": self._capability_view(),
                    "team_id": team.team_id,
                    "team_name": team.name,
                    "conference_id": team.conference_id,
                    "division_id": team.division_id,
                    "cap_space": team.cap_space,
                    "roster_size": len(team.roster),
                    "owner": team.owner.name,
                    "mandate": team.owner.mandate,
                    "transactions": [asdict(t) for t in self.org_state.transactions[-12:]],
                },
            )

        profile_free_actions = {
            ActionType.LIST_PROFILES,
            ActionType.CREATE_PROFILE,
            ActionType.LOAD_PROFILE,
            ActionType.DELETE_PROFILE,
            ActionType.VALIDATE_LEAGUE_SETUP,
            ActionType.CREATE_NEW_FRANCHISE_SAVE,
            ActionType.RUN_FOOTBALL_AUDIT,
            ActionType.RUN_STRICT_AUDIT,
            ActionType.GET_TUNING_PROFILES,
            ActionType.SET_TUNING_PROFILE,
            ActionType.PATCH_TUNING_PROFILE,
            ActionType.RUN_CALIBRATION_BATCH,
            ActionType.EXPORT_CALIBRATION_REPORT,
        }
        if action not in profile_free_actions:
            profile_error = self._require_active_profile_action()
            if profile_error is not None:
                return ActionResult(request.request_id, False, profile_error)
            assert self.org_state is not None
            assert self.store is not None

        if action == ActionType.SET_PLAYCALL:
            deny = self._require_capability(CapabilityDomain.PLAYCALL_OVERRIDE)
            if deny is not None:
                return ActionResult(request.request_id, False, deny)
            payload = request.payload
            required = {"personnel", "formation", "offensive_concept", "defensive_concept", "play_type", "tempo", "aggression"}
            missing = sorted(required - set(payload.keys()))
            if missing:
                return ActionResult(
                    request.request_id,
                    False,
                    f"missing playcall fields: {', '.join(missing)}",
                )
            try:
                play_type = PlayType(str(payload["play_type"]))
            except ValueError:
                return ActionResult(request.request_id, False, f"invalid play_type '{payload['play_type']}'")
            self.pending_user_playcall = PlaycallRequest(
                team_id=request.actor_team_id,
                personnel=str(payload["personnel"]),
                formation=str(payload["formation"]),
                offensive_concept=str(payload["offensive_concept"]),
                defensive_concept=str(payload["defensive_concept"]),
                playbook_entry_id=str(payload["playbook_entry_id"]) if "playbook_entry_id" in payload else None,
                tempo=str(payload["tempo"]),
                aggression=str(payload["aggression"]),
                play_type=play_type,
            )
            try:
                self.pre_sim_validator.validate_playcall(self.pending_user_playcall)
            except ValidationError as exc:
                return ActionResult(
                    request.request_id,
                    False,
                    "playcall rejected by pre-sim gate",
                    data={"issues": [asdict(i) for i in exc.issues]},
                )
            return ActionResult(request.request_id, True, "playcall updated", data=asdict(self.pending_user_playcall))

        if action == ActionType.GET_TUNING_PROFILES:
            if not self.dev_mode:
                return ActionResult(request.request_id, False, "dev mode required for tuning actions")
            profile_error = self._require_active_profile_action()
            if profile_error is not None:
                return ActionResult(request.request_id, False, profile_error)
            calibration = self._require_dev_calibration()
            tuning_profiles = [asdict(p) for p in calibration.list_tuning_profiles()]
            return ActionResult(
                request.request_id,
                True,
                "tuning profiles",
                data={"profiles": tuning_profiles, "active_profile_id": calibration.active_profile()},
            )

        if action == ActionType.SET_TUNING_PROFILE:
            if not self.dev_mode:
                return ActionResult(request.request_id, False, "dev mode required for tuning actions")
            profile_error = self._require_active_profile_action()
            if profile_error is not None:
                return ActionResult(request.request_id, False, profile_error)
            if "profile_id" not in request.payload:
                return ActionResult(request.request_id, False, "missing required payload field 'profile_id'")
            profile_id = str(request.payload["profile_id"])
            calibration = self._require_dev_calibration()
            try:
                updated = calibration.set_tuning_profile(profile_id)
            except ValueError as exc:
                return ActionResult(request.request_id, False, str(exc))
            self._emit_dev_event(
                event_type="tuning_profile_set",
                claims=[f"active tuning profile set to {profile_id}"],
                evidence_handles=[f"tuning:{profile_id}"],
            )
            return ActionResult(request.request_id, True, "tuning profile updated", data={"active_profile_id": updated.profile_id})

        if action == ActionType.PATCH_TUNING_PROFILE:
            if not self.dev_mode:
                return ActionResult(request.request_id, False, "dev mode required for tuning actions")
            profile_error = self._require_active_profile_action()
            if profile_error is not None:
                return ActionResult(request.request_id, False, profile_error)
            if "profile_id" not in request.payload:
                return ActionResult(request.request_id, False, "missing required payload field 'profile_id'")
            patch = TuningPatchRequest(
                profile_id=str(request.payload["profile_id"]),
                family_weight_multipliers=dict(request.payload["family_weight_multipliers"]) if "family_weight_multipliers" in request.payload else {},
                outcome_multipliers=dict(request.payload["outcome_multipliers"]) if "outcome_multipliers" in request.payload else {},
            )
            calibration = self._require_dev_calibration()
            patched = calibration.patch_profile(patch, actor_team_id=request.actor_team_id)
            self._emit_dev_event(
                event_type="tuning_profile_patched",
                claims=[f"patched tuning profile {patched.profile.profile_id}"],
                evidence_handles=[f"tuning_patch:{patched.profile.profile_id}"],
            )
            return ActionResult(
                request.request_id,
                True,
                "tuning profile patched",
                data={"profile": asdict(patched.profile), "patched_at": patched.patched_at.isoformat(), "actor_team_id": patched.actor_team_id},
            )

        if action == ActionType.RUN_CALIBRATION_BATCH:
            if not self.dev_mode:
                return ActionResult(request.request_id, False, "dev mode required for tuning actions")
            profile_error = self._require_active_profile_action()
            if profile_error is not None:
                return ActionResult(request.request_id, False, profile_error)
            required = {"play_type", "sample_count", "trait_profile"}
            missing = sorted(required - set(request.payload.keys()))
            if missing:
                return ActionResult(request.request_id, False, f"missing calibration fields: {', '.join(missing)}")
            try:
                play_type = PlayType(str(request.payload["play_type"]))
                sample_count = int(request.payload["sample_count"])
                trait_profile = CalibrationTraitProfile(str(request.payload["trait_profile"]))
                seed = request.payload["seed"] if "seed" in request.payload else None
                seed_value = int(seed) if seed is not None else None
            except ValueError as exc:
                return ActionResult(request.request_id, False, f"invalid calibration payload: {exc}")
            run_request = BatchRunRequest(
                play_type=play_type,
                sample_count=sample_count,
                trait_profile=trait_profile,
                seed=seed_value,
            )
            calibration = self._require_dev_calibration()
            result = calibration.run_batch(run_request, actor_team_id=request.actor_team_id)
            self._emit_dev_event(
                event_type="calibration_batch_run",
                claims=[f"calibration batch {result.run.run_id} completed"],
                evidence_handles=[f"calibration:{result.run.run_id}", f"play_type:{result.run.play_type.value}", f"profile:{result.run.trait_profile.value}"],
            )
            data = asdict(result.run)
            data["play_type"] = result.run.play_type.value
            data["trait_profile"] = result.run.trait_profile.value
            data["session"] = asdict(result.session)
            return ActionResult(request.request_id, True, "calibration batch completed", data=data)

        if action == ActionType.RUN_FOOTBALL_AUDIT:
            if not self.dev_mode:
                return ActionResult(request.request_id, False, "dev mode required for audit actions")
            audit_report = run_football_contract_audit()
            self._emit_dev_event(
                event_type="football_contract_audit",
                claims=[f"football audit run {audit_report.report_id} passed={audit_report.passed}"],
                evidence_handles=[f"audit:{audit_report.report_id}"],
            )
            return ActionResult(
                request.request_id,
                True,
                "football audit complete",
                data={
                    "report_id": audit_report.report_id,
                    "generated_at": audit_report.generated_at.isoformat(),
                    "scope": audit_report.scope,
                    "passed": audit_report.passed,
                    "checks": [asdict(c) for c in audit_report.checks],
                },
            )

        if action == ActionType.EXPORT_CALIBRATION_REPORT:
            if not self.dev_mode:
                return ActionResult(request.request_id, False, "dev mode required for calibration export actions")
            profile_error = self._require_active_profile_action()
            if profile_error is not None:
                return ActionResult(request.request_id, False, profile_error)
            calibration = self._require_dev_calibration()
            outputs, row_counts = calibration.export_reports()
            self._emit_dev_event(
                event_type="calibration_report_exported",
                claims=[f"exported calibration report files={len(outputs)}"],
                evidence_handles=[f"calibration_export:{Path(p).name}" for p in outputs],
            )
            return ActionResult(
                request.request_id,
                True,
                "calibration report exported",
                data={
                    "exported_files": outputs,
                    "row_counts": row_counts,
                },
            )

        if action == ActionType.RUN_STRICT_AUDIT:
            if not self.dev_mode:
                return ActionResult(request.request_id, False, "dev mode required for strict audit actions")
            service = self._load_strict_audit_service()
            report = service.run(repo_root=Path(__file__).resolve().parents[3])
            self._emit_dev_event(
                event_type="strict_audit_run",
                claims=[f"strict audit run {report.report_id} passed={report.passed}"],
                evidence_handles=[f"strict_audit:{report.report_id}"],
            )
            return ActionResult(
                request.request_id,
                True,
                "strict audit complete",
                data={
                    "report_id": report.report_id,
                    "generated_at": report.generated_at.isoformat(),
                    "passed": report.passed,
                    "sections": [asdict(s) for s in report.sections],
                },
            )

        if action in {ActionType.PLAY_USER_GAME, ActionType.PLAY_SNAP, ActionType.SIM_DRIVE}:
            deny = self._require_capability(CapabilityDomain.GAMEPLAN)
            if deny is not None:
                return ActionResult(request.request_id, False, deny)
            game_result = self._simulate_user_game(mode=SimMode.PLAY if action != ActionType.SIM_DRIVE else SimMode.SIM)
            return ActionResult(
                request.request_id,
                True,
                f"User game finalized {game_result.home_score}-{game_result.away_score}",
                data={
                    "game_id": game_result.final_state.game_id,
                    "home_team_id": game_result.home_team_id,
                    "away_team_id": game_result.away_team_id,
                    "home_score": game_result.home_score,
                    "away_score": game_result.away_score,
                    "snaps": len(game_result.snaps),
                },
            )

        if action == ActionType.ADVANCE_WEEK:
            deny = self._require_capability(CapabilityDomain.GAMEPLAN)
            if deny is not None:
                return ActionResult(request.request_id, False, deny)
            week_result = self._advance_pipeline()
            assert self.org_state is not None
            return ActionResult(
                request.request_id,
                True,
                f"Advanced to S{self.org_state.season} W{self.org_state.week} {self.org_state.phase}",
                data=asdict(week_result),
            )

        if action == ActionType.GET_ORG_OVERVIEW:
            assert self.org_state is not None
            team = self._team(self.user_team_id)
            cards = self.org_engine.perceived_cards_for_team(self.org_state, team.team_id)
            return ActionResult(
                request.request_id,
                True,
                "organization overview",
                data={
                    "team_id": team.team_id,
                    "team_name": team.name,
                    "cap_space": team.cap_space,
                    "owner": team.owner.name,
                    "mandate": team.owner.mandate,
                    "roster_size": len(team.roster),
                    "depth_chart": [asdict(d) for d in team.depth_chart],
                    "perceived_top": [
                        {
                            "player_id": c.player_id,
                            "scout_estimate": c.scout_metrics[0].estimate,
                            "confidence": c.scout_metrics[0].confidence,
                        }
                        for c in cards[:10]
                    ],
                    "transactions": [asdict(t) for t in self.org_state.transactions[-12:]],
                },
            )

        if action == ActionType.GET_STANDINGS:
            assert self.org_state is not None
            ranked = rank_standings(self.org_state.standings.entries)
            return ActionResult(
                request.request_id,
                True,
                "standings",
                data={"standings": [asdict(r) for r in ranked]},
            )

        if action == ActionType.GET_GAME_STATE:
            if not self.last_user_game_result:
                return ActionResult(request.request_id, True, "no user game played yet", data={})
            g = self.last_user_game_result
            play_type_by_id: dict[str, str] = {}
            for event in g.action_stream:
                play_id = event.get("play_id")
                play_type_value = event.get("play_type")
                if isinstance(play_id, str) and isinstance(play_type_value, str):
                    play_type_by_id[play_id] = play_type_value
            return ActionResult(
                request.request_id,
                True,
                "latest user game",
                data={
                    "state": asdict(g.final_state),
                    "snap_count": len(g.snaps),
                    "action_count": len(g.action_stream),
                    "snaps": [
                        {
                            "play_id": s.play_result.play_id,
                            "play_type": play_type_by_id.get(s.play_result.play_id, "unknown"),
                            "yards": s.play_result.yards,
                            "event": s.causality_chain.terminal_event,
                            "score_event": s.play_result.score_event,
                            "turnover": s.play_result.turnover,
                            "turnover_type": s.play_result.turnover_type,
                            "penalty_count": len(s.play_result.penalties),
                            "rep_count": len(s.rep_ledger),
                            "contest_count": len(s.contest_outputs),
                            "clock_delta": s.play_result.clock_delta,
                            "conditioned": s.conditioned,
                            "attempts": s.attempts,
                        }
                        for s in g.snaps[-80:]
                    ],
                },
            )

        if action in {ActionType.GET_RETAINED_GAMES, ActionType.LOAD_RETAINED}:
            assert self.store is not None
            rows = self.store.list_retained_games()
            return ActionResult(
                request.request_id,
                True,
                "retained games",
                data={"games": [{"game_id": r[0], "season": r[1], "week": r[2]} for r in rows]},
            )

        if action == ActionType.GET_FILM_ROOM_GAME:
            assert self.store is not None
            game_id = request.payload["game_id"] if "game_id" in request.payload else None
            if not game_id:
                return ActionResult(request.request_id, False, "game_id required")
            return ActionResult(request.request_id, True, "film room", data=self.store.load_film_room_game(game_id))

        if action == ActionType.GET_ANALYTICS_SERIES:
            return ActionResult(request.request_id, True, "analytics", data=self._analytics_series())

        if action == ActionType.DEBUG_TRUTH:
            team = self._team(self.user_team_id)
            top = sorted(team.roster, key=lambda p: p.overall_truth, reverse=True)[:5]
            return ActionResult(
                request.request_id,
                True,
                "debug truth",
                data={"players": [{"name": p.name, "truth": p.overall_truth} for p in top]},
            )

        return ActionResult(request.request_id, False, f"Unsupported action '{request.action_type}'")

    def _advance_pipeline(self) -> WeekSimulationResult:
        assert self.org_state is not None
        assert self.store is not None
        season = self.org_state.season
        week = self.org_state.week

        finalized_game_ids: list[str] = []
        integrity_checks: list[str] = []
        injuries: list[str] = []

        if self.org_state.phase in {"regular", "postseason"}:
            entries = self.store.get_schedule_for_week(season, week)
            if not entries:
                self._ensure_schedule_for_season(season)
                entries = self.store.get_schedule_for_week(season, week)

            for entry in entries:
                if entry.status == "final":
                    continue
                mode = SimMode.OFFSCREEN
                if entry.is_user_game and self.last_user_game_result and self.last_user_game_result.final_state.game_id == entry.game_id:
                    continue
                if entry.is_user_game:
                    mode = SimMode.SIM
                game_result = self._run_scheduled_game(entry, mode)
                finalized_game_ids.append(game_result.final_state.game_id)
                injuries.extend(list(game_result.final_state.active_injuries.keys()))

            self.store.save_standings_week(season, week, self.org_state.standings.entries)
            integrity_checks.append("standings_saved")
            self.store.season_rollover_integrity_check(season)
            integrity_checks.append("integrity_check_passed")
            run_weekly_etl(self.paths.sqlite_path, self.paths.duckdb_path, season, week)
            self._save_snapshot()

        else:
            self._run_offseason_gate()
            run_weekly_etl(self.paths.sqlite_path, self.paths.duckdb_path, season, week)
            self._save_snapshot()

        standings_delta = {
            team_id: {
                "wins": s.wins,
                "losses": s.losses,
                "ties": s.ties,
                "point_diff": s.point_diff,
            }
            for team_id, s in self.org_state.standings.entries.items()
        }

        tx_summaries = [t.summary for t in self.org_state.transactions if t.season == season and t.week == week]

        self.store.save_transactions(self.org_state.transactions)
        self.store.save_cap_ledger(self.org_state.cap_ledger)
        self.store.save_narrative_events(self.org_state.narrative_events)
        self._persist_league_state()

        week_result = WeekSimulationResult(
            season=season,
            week=week,
            finalized_game_ids=finalized_game_ids,
            standings_delta=standings_delta,
            injuries=injuries,
            transactions=tx_summaries,
            integrity_checks=integrity_checks,
        )

        self.org_engine.advance_week(self.org_state)
        if self.org_state.phase == "regular":
            self._ensure_schedule_for_season(self.org_state.season)
        self._persist_league_state()
        return week_result

    def _simulate_user_game(self, mode: SimMode) -> GameSessionResult:
        assert self.org_state is not None
        assert self.store is not None
        entry = self._get_user_schedule_entry(self.org_state.season, self.org_state.week)
        if entry is None:
            raise ValueError("no scheduled user game for current week")
        if entry.status == "final":
            existing = self.store.load_game_session_state(entry.game_id)
            if existing is None:
                raise ValueError("scheduled game marked final but no session state found")
            raise ValueError("user game already finalized this week")
        result = self._run_scheduled_game(entry, mode)
        self.last_user_game_result = result
        return result

    def _run_scheduled_game(self, entry: ScheduleEntry, mode: SimMode) -> GameSessionResult:
        assert self.org_state is not None
        assert self.store is not None
        home = self._team(entry.home_team_id)
        away = self._team(entry.away_team_id)

        initial_state = GameSessionState(
            game_id=entry.game_id,
            season=self.org_state.season,
            week=self.org_state.week,
            home_team_id=home.team_id,
            away_team_id=away.team_id,
            quarter=1,
            clock_seconds=900,
            home_score=0,
            away_score=0,
            possession_team_id=home.team_id,
            down=1,
            distance=10,
            yard_line=25,
            drive_index=1,
            timeouts_home=3,
            timeouts_away=3,
        )
        self._validate_game_readiness(entry, home, away, initial_state)

        self.org_engine.ensure_depth_chart_valid(home)
        self.org_engine.ensure_depth_chart_valid(away)
        self.org_engine.validate_franchise_constraints(home)
        self.org_engine.validate_franchise_constraints(away)

        context = GameRetentionContext(
            game_id=entry.game_id,
            phase=self.org_state.phase,
            is_championship=self.org_state.phase == "postseason" and self.org_state.week == 4,
            is_playoff=self.org_state.phase == "postseason",
            is_rivalry=False,
            is_record_game=False,
            tagged_instant_classic=entry.is_user_game,
        )
        retained = should_retain_game(self.retention_policy, context)

        self.store.register_game(
            game_id=entry.game_id,
            season=self.org_state.season,
            week=self.org_state.week,
            phase=self.org_state.phase,
            home_team_id=home.team_id,
            away_team_id=away.team_id,
            retained=retained,
        )

        def provider(state: GameSessionState, offense_team_id: str, defense_team_id: str) -> PlaycallRequest | None:
            if offense_team_id == self.user_team_id and self.pending_user_playcall:
                return self.pending_user_playcall
            return None

        session_result = self.game_session.run_game(initial_state, home, away, mode=mode, playcall_provider=provider)

        total_turnovers = 0
        total_penalties = 0
        for snap in session_result.snaps:
            self.store.save_snap_resolution(entry.game_id, snap, retained=retained)
            total_turnovers += int(snap.play_result.turnover)
            total_penalties += len(snap.play_result.penalties)
            for event in snap.narrative_events:
                self.event_bus.publish_narrative(event)

        self.store.save_game_summary(
            game_id=entry.game_id,
            home_team_id=home.team_id,
            away_team_id=away.team_id,
            home_score=session_result.home_score,
            away_score=session_result.away_score,
            plays=len(session_result.snaps),
            turnovers=total_turnovers,
            penalties=total_penalties,
            season=self.org_state.season,
            week=self.org_state.week,
        )
        self.store.save_game_session_result(
            season=self.org_state.season,
            week=self.org_state.week,
            mode=mode.value,
            result=session_result,
            retained=retained,
            seed=self.seed,
        )

        self.store.set_game_status(entry.game_id, "final")
        self.store.update_schedule_status(entry.game_id, "final")
        if not retained:
            self.store.purge_non_retained_deep_logs(entry.game_id)

        self.org_engine.apply_game_result(
            self.org_state,
            home_team_id=home.team_id,
            away_team_id=away.team_id,
            home_score=session_result.home_score,
            away_score=session_result.away_score,
        )
        if entry.is_user_game:
            self.last_user_game_result = session_result
        return session_result

    def _validate_game_readiness(
        self,
        entry: ScheduleEntry,
        home: Franchise,
        away: Franchise,
        initial_state: GameSessionState,
    ) -> None:
        assert self.org_state is not None
        assert self.store is not None
        try:
            self.pre_sim_validator.validate_game_input(
                season=self.org_state.season,
                week=self.org_state.week,
                game_id=entry.game_id,
                home=home,
                away=away,
                session_state=initial_state,
                random_source=self.rand,
            )
            report = self.pre_sim_validator.readiness_report(
                season=self.org_state.season,
                week=self.org_state.week,
                game_id=entry.game_id,
                home_team_id=home.team_id,
                away_team_id=away.team_id,
                issues=[],
            )
            self.store.save_validation_report(report, status="passed")
        except ValidationError as exc:
            report = self.pre_sim_validator.readiness_report(
                season=self.org_state.season,
                week=self.org_state.week,
                game_id=entry.game_id,
                home_team_id=home.team_id,
                away_team_id=away.team_id,
                issues=exc.issues,
            )
            self.store.save_validation_report(report, status="failed")
            artifact = build_forensic_artifact(
                engine_scope="runtime",
                error_code="PRE_SIM_VALIDATION_FAILED",
                message="game readiness validation failed",
                state_snapshot={
                    "season": self.org_state.season,
                    "week": self.org_state.week,
                    "phase": self.org_state.phase,
                    "game_id": entry.game_id,
                },
                context={"issues": [asdict(i) for i in exc.issues]},
                identifiers={"game_id": entry.game_id, "home": home.team_id, "away": away.team_id},
                causal_fragment=["pre_sim_gate", "game_readiness"],
            )
            raise EngineIntegrityError(artifact) from exc

    def _run_offseason_gate(self) -> None:
        assert self.org_state is not None
        gate = self.org_engine.offseason_gate(self.org_state)
        if gate == "re_signing":
            self.org_engine.develop_players(self.org_state)
        elif gate == "free_agency":
            free_agents = []
            for team in self.org_state.teams:
                free_agents.extend(team.roster[-1:])
            self.org_engine.run_free_agency(self.org_state, free_agents)
        elif gate == "draft":
            if not self.org_state.prospects:
                self.org_engine.generate_draft_class(self.org_state)
            self.org_engine.run_draft_round(self.org_state)
        else:
            self.org_engine.run_trade_window(self.org_state)
            self.org_engine.develop_players(self.org_state)

    def _ensure_schedule_for_season(self, season: int) -> None:
        assert self.org_state is not None
        assert self.store is not None
        existing = self.store.get_schedule_for_week(season, 1)
        if existing:
            return
        if self.org_state.schedule_policy_id == "":
            raise ValueError("missing schedule policy id")
        setup_weeks = self._current_regular_season_weeks()
        blueprints = [
            TeamBlueprint(
                team_id=t.team_id,
                team_name=t.name,
                conference_id=t.conference_id,
                conference_name=t.conference_id,
                division_id=t.division_id,
                division_name=t.division_id,
            )
            for t in self.org_state.teams
        ]
        schedule = self.schedule_service.generate(
            blueprints=blueprints,
            season=season,
            user_team_id=self.user_team_id,
            weeks=setup_weeks,
            policy_id=self.org_state.schedule_policy_id,
            rand=self.rand.spawn(f"schedule:{season}:{self.org_state.schedule_policy_id}"),
        )
        self.org_state.schedule = schedule
        self.store.save_schedule_entries(schedule)

    def _get_user_schedule_entry(self, season: int, week: int) -> ScheduleEntry | None:
        assert self.store is not None
        entries = self.store.get_schedule_for_week(season, week)
        for e in entries:
            if e.is_user_game:
                return e
        return None

    def _persist_league_state(self) -> None:
        assert self.org_state is not None
        assert self.store is not None
        metadata = {
            "phase": self.org_state.phase,
            "week": self.org_state.week,
            "season": self.org_state.season,
            "user_team_id": self.user_team_id,
            "profile_id": self.org_state.profile_id,
            "league_config_id": self.org_state.league_config_id,
            "league_format_id": self.org_state.league_format_id,
            "league_format_version": self.org_state.league_format_version,
            "ruleset_id": self.org_state.ruleset_id,
            "ruleset_version": self.org_state.ruleset_version,
            "schedule_policy_id": self.org_state.schedule_policy_id,
            "schedule_policy_version": self.org_state.schedule_policy_version,
        }
        self.store.save_league_state(
            season=self.org_state.season,
            week=self.org_state.week,
            phase=self.org_state.phase,
            teams=self.org_state.teams,
            metadata=metadata,
        )
        self.store.save_contracts(self.org_state.contracts)

    def _save_snapshot(self) -> None:
        assert self.org_state is not None
        assert self.store is not None
        snapshot_id = make_id("snap")
        payload = {
            "season": self.org_state.season,
            "week": self.org_state.week,
            "phase": self.org_state.phase,
            "standings": {k: asdict(v) for k, v in self.org_state.standings.entries.items()},
            "transactions": [asdict(t) for t in self.org_state.transactions if t.season == self.org_state.season and t.week == self.org_state.week],
        }
        path = self.paths.snapshot_dir / f"{snapshot_id}.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        ref = LeagueSnapshotRef(
            snapshot_id=snapshot_id,
            season=self.org_state.season,
            week=self.org_state.week,
            created_at=now_utc(),
            blob_path=str(path),
        )
        self.org_state.snapshots.append(ref)
        self.store.save_week_snapshot(ref, payload)

    def _team(self, team_id: str) -> Franchise:
        assert self.org_state is not None
        return next(t for t in self.org_state.teams if t.team_id == team_id)

    def _normalize_action(self, action: ActionType | str) -> ActionType:
        if isinstance(action, ActionType):
            return action
        return ActionType(action)

    def _parse_setup_config(self, payload: dict[str, Any]) -> LeagueSetupConfig:
        conference_count = int(payload["conference_count"])
        divisions = [int(v) for v in list(payload["divisions_per_conference"])]
        teams_matrix = [[int(v) for v in list(row)] for row in list(payload["teams_per_division"])]
        roster_policy_raw = dict(payload["roster_policy"])
        cap_policy_raw = dict(payload["cap_policy"])
        schedule_policy_raw = dict(payload["schedule_policy"])

        overrides_raw = dict(payload["capability_overrides"]) if "capability_overrides" in payload else {}
        overrides: dict[CapabilityDomain, bool] = {
            CapabilityDomain(str(key)): bool(value) for key, value in overrides_raw.items()
        }
        config = LeagueSetupConfig(
            conference_count=conference_count,
            divisions_per_conference=divisions,
            teams_per_division=teams_matrix,
            roster_policy=RosterPolicyConfig(
                players_per_team=int(roster_policy_raw["players_per_team"]),
                active_gameday_min=int(roster_policy_raw.get("active_gameday_min", 22)),
                active_gameday_max=int(roster_policy_raw.get("active_gameday_max", 53)),
            ),
            cap_policy=CapPolicyConfig(
                cap_amount=int(cap_policy_raw["cap_amount"]),
                dead_money_penalty_multiplier=float(cap_policy_raw.get("dead_money_penalty_multiplier", 1.0)),
            ),
            schedule_policy=SchedulePolicyConfig(
                policy_id=str(schedule_policy_raw["policy_id"]),
                regular_season_weeks=int(schedule_policy_raw.get("regular_season_weeks", 18)),
            ),
            ruleset_id=str(payload["ruleset_id"]),
            difficulty_profile_id=str(payload["difficulty_profile_id"]),
            talent_profile_id=str(payload["talent_profile_id"]),
            user_mode=ManagementMode(str(payload["user_mode"])),
            capability_overrides=overrides,
            league_format_id=str(payload["league_format_id"]) if "league_format_id" in payload else "custom_flexible_v1",
            league_format_version=str(payload["league_format_version"]) if "league_format_version" in payload else "1.0.0",
        )
        return config

    def _validate_setup(self, *, profile_id: str, setup_config: LeagueSetupConfig) -> LeagueSetupValidationReport:
        result = self.setup_validator.validate(setup_config)
        blocking = [issue for issue in result.issues if issue.severity == "blocking"]
        warnings = [issue for issue in result.issues if issue.severity != "blocking"]
        report = LeagueSetupValidationReport(
            report_id=make_id("setupval"),
            profile_id=profile_id,
            setup_config_ref=make_id("setupcfg"),
            blocking_issues=blocking,
            warning_issues=warnings,
            validated_at=now_utc(),
        )
        self.profile_store.save_validation_report(report)
        return report

    def _team_candidates_for_setup(self, setup_config: LeagueSetupConfig) -> list[str]:
        blueprints = self.structure_compiler.compile(setup_config)
        return self.team_selection_planner.team_ids(blueprints)

    def _create_new_franchise_save(
        self,
        *,
        profile_id: str,
        profile_name: str,
        selected_team_id: str,
        setup_config: LeagueSetupConfig,
        actor_team_id: str,
    ) -> dict[str, Any]:
        now = now_utc()
        existing_profile = self.profile_store.load_profile(profile_id)
        profile = FranchiseProfile(
            profile_id=profile_id,
            profile_name=profile_name.strip() or profile_id,
            created_at=existing_profile.created_at if existing_profile else now,
            last_opened_at=now,
            league_config_ref=existing_profile.league_config_ref if existing_profile else "",
            selected_user_team_id=selected_team_id,
            active_mode=setup_config.user_mode,
        )
        self.profile_store.save_profile(profile)

        report = self._validate_setup(profile_id=profile_id, setup_config=setup_config)
        if report.blocking_issues:
            raise ValueError(
                "setup validation failed: "
                + "; ".join(f"{issue.code}:{issue.message}" for issue in report.blocking_issues)
            )

        candidates = self._team_candidates_for_setup(setup_config)
        if selected_team_id not in candidates:
            raise ValueError(
                f"selected_user_team_id '{selected_team_id}' is not valid for the generated league topology"
            )

        league_config_id = make_id("lgcfg")
        profile.league_config_ref = league_config_id
        profile.selected_user_team_id = selected_team_id
        profile.active_mode = setup_config.user_mode

        blueprints = self.structure_compiler.compile(setup_config)
        self.profile_store.save_profile(profile)
        self.profile_store.save_league_setup(
            profile_id=profile.profile_id,
            league_config_id=league_config_id,
            config=setup_config,
            blueprints=[asdict(blueprint) for blueprint in blueprints],
        )

        league_state = build_league_from_setup(
            config=setup_config,
            season=2026,
            compiler=self.structure_compiler,
            roster_generator=self.roster_generation_service,
            rand=self.rand.spawn(f"league_create:{profile.profile_id}"),
        )
        league_state.profile_id = profile.profile_id
        league_state.league_config_id = league_config_id
        league_state.ruleset_id = setup_config.ruleset_id
        league_state.schedule_policy_id = setup_config.schedule_policy.policy_id
        policy = self.capability_service.build_policy(
            mode=setup_config.user_mode,
            overrides=setup_config.capability_overrides,
            updated_by_team_id=actor_team_id,
            reason="initial_profile_setup",
        )
        league_state.capability_policy = policy
        self.profile_store.save_mode_policy(profile.profile_id, policy)

        self._activate_profile_runtime(
            profile=profile,
            state=league_state,
            capability_policy=policy,
            regular_season_weeks=setup_config.schedule_policy.regular_season_weeks,
            difficulty_profile_id=setup_config.difficulty_profile_id,
        )
        assert self.org_state is not None
        assert self.store is not None
        self._ensure_schedule_for_season(self.org_state.season)
        self._persist_league_state()
        self._emit_org_event(
            event_type="league_created",
            claims=[
                f"profile {profile.profile_id} league created",
                f"schedule_policy={setup_config.schedule_policy.policy_id}",
                f"talent_profile={setup_config.talent_profile_id}",
            ],
            evidence_handles=[f"profile:{profile.profile_id}", f"league_config:{league_config_id}"],
        )

        return {
            "profile_id": profile.profile_id,
            "profile_name": profile.profile_name,
            "league_config_id": league_config_id,
            "selected_user_team_id": selected_team_id,
            "team_count": len(blueprints),
            "conference_count": setup_config.conference_count,
            "weeks": setup_config.schedule_policy.regular_season_weeks,
            "mode": setup_config.user_mode.value,
        }

    def _activate_profile_runtime(
        self,
        *,
        profile: FranchiseProfile,
        state: LeagueState,
        capability_policy: CapabilityPolicy,
        regular_season_weeks: int,
        difficulty_profile_id: str,
    ) -> None:
        self.paths.bind_profile(profile.profile_id)
        self.paths.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.paths.snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.paths.forensic_dir.mkdir(parents=True, exist_ok=True)
        self.paths.export_dir.mkdir(parents=True, exist_ok=True)
        self.store = AuthoritativeStore(self.paths.sqlite_path)
        self.store.initialize_schema()
        self.store.save_trait_catalog(self.pre_sim_validator.trait_catalog())

        self.active_profile = profile
        self.user_team_id = profile.selected_user_team_id
        state.capability_policy = capability_policy
        self.org_state = state
        self.capability_policy = capability_policy
        self.regular_season_weeks = regular_season_weeks
        self.last_user_game_result = None
        self.pending_user_playcall = None
        self.halted = False
        self.last_forensic_path = None

        difficulty_key = Difficulty(difficulty_profile_id)
        self.difficulty = default_difficulty_profiles()[difficulty_key]
        self.org_engine = OrganizationalEngine(
            rand=self.rand.spawn("org"),
            difficulty=self.difficulty,
            regular_season_weeks=regular_season_weeks,
        )

        self.dev_calibration = None
        if self.dev_mode:
            self.dev_calibration = self._load_dev_calibration_gateway()

    def _load_profile(self, profile_id: str) -> FranchiseProfile | None:
        profile = self.profile_store.load_profile(profile_id)
        if profile is None:
            return None
        if not profile.league_config_ref:
            return None

        config_row = self.profile_store.load_setup_config_row(profile.league_config_ref)
        if config_row is None:
            raise ValueError(f"profile '{profile_id}' has missing league config '{profile.league_config_ref}'")

        self.paths.bind_profile(profile.profile_id)
        self.paths.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.paths.snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.paths.forensic_dir.mkdir(parents=True, exist_ok=True)
        self.paths.export_dir.mkdir(parents=True, exist_ok=True)
        self.store = AuthoritativeStore(self.paths.sqlite_path)
        self.store.initialize_schema()
        self.store.save_trait_catalog(self.pre_sim_validator.trait_catalog())
        loaded_state = self.store.load_runtime_league_state()
        if loaded_state is None:
            raise ValueError(f"profile '{profile_id}' has no persisted league state")
        loaded_state.profile_id = profile.profile_id
        loaded_state.league_config_id = profile.league_config_ref
        loaded_state.ruleset_id = str(config_row["ruleset_id"])
        loaded_state.league_format_id = str(config_row["league_format_id"])
        loaded_state.league_format_version = str(config_row["league_format_version"])
        loaded_state.schedule_policy_id = str(config_row["schedule_policy_id"])
        topology = self.profile_store.load_team_topology(profile.league_config_ref)
        for team in loaded_state.teams:
            mapped = topology.get(team.team_id)
            if mapped:
                team.conference_id = mapped["conference_id"]
                team.division_id = mapped["division_id"]
                team.name = mapped["team_name"]
        mode_policy = self.profile_store.load_mode_policy(profile.profile_id)
        if mode_policy is None:
            mode_policy = self.capability_service.build_policy(
                mode=profile.active_mode,
                overrides={},
                updated_by_team_id=profile.selected_user_team_id,
                reason="profile_load_default_policy",
            )
            self.profile_store.save_mode_policy(profile.profile_id, mode_policy)
        loaded_state.capability_policy = mode_policy
        self._activate_profile_runtime(
            profile=profile,
            state=loaded_state,
            capability_policy=mode_policy,
            regular_season_weeks=int(config_row.get("regular_season_weeks", 18)),
            difficulty_profile_id=str(config_row["difficulty_profile_id"]),
        )
        self.profile_store.touch_profile(profile.profile_id, now_utc())
        return profile

    def _require_active_profile_action(self) -> str | None:
        if self.active_profile is None or self.org_state is None or self.store is None:
            return "no active franchise profile loaded; create/load a franchise profile first"
        return None

    def _require_capability(self, domain: CapabilityDomain) -> str | None:
        if self.capability_policy is None:
            return "capability policy not initialized"
        if not self.capability_service.has_capability(self.capability_policy, domain):
            return f"capability denied for domain '{domain.value}' in mode '{self.capability_policy.mode.value}'"
        return None

    def _capability_view(self) -> dict[str, Any]:
        if self.capability_policy is None:
            return {"mode": None, "baseline": [], "overrides": {}}
        effective: dict[str, bool] = {}
        for domain in CapabilityDomain:
            effective[domain.value] = self.capability_service.has_capability(self.capability_policy, domain)
        return {
            "mode": self.capability_policy.mode.value,
            "baseline": [domain.value for domain in self.capability_policy.baseline_capabilities],
            "overrides": {domain.value: enabled for domain, enabled in self.capability_policy.override_capabilities.items()},
            "effective": effective,
            "updated_by_team_id": self.capability_policy.updated_by_team_id,
            "updated_at": self.capability_policy.updated_at.isoformat(),
            "reason": self.capability_policy.reason,
        }

    def _forensic_dir(self) -> Path:
        if self.active_profile is not None:
            self.paths.forensic_dir.mkdir(parents=True, exist_ok=True)
            return self.paths.forensic_dir
        path = self.paths.root / "forensics" / "runtime"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _current_regular_season_weeks(self) -> int:
        if self.regular_season_weeks is None:
            raise ValueError("regular season weeks are not initialized")
        return self.regular_season_weeks

    def _emit_org_event(self, *, event_type: str, claims: list[str], evidence_handles: list[str]) -> None:
        if self.org_state is None:
            return
        event = NarrativeEvent(
            event_id=make_id("ne"),
            time=now_utc(),
            scope="org",
            event_type=event_type,
            actors=[self.user_team_id],
            claims=claims,
            evidence_handles=evidence_handles,
            severity="normal",
            confidentiality_tier="internal",
        )
        self.org_state.narrative_events.append(event)
        if self.store is not None:
            self.store.save_narrative_events([event])
        self.event_bus.publish_narrative(event)

    def _analytics_series(self) -> dict[str, Any]:
        assert self.org_state is not None
        try:
            import duckdb
        except ModuleNotFoundError:
            artifact = build_forensic_artifact(
                engine_scope="runtime",
                error_code="MISSING_REQUIRED_RUNTIME_CONFIG",
                message="duckdb dependency is required for analytics reads",
                state_snapshot={"season": self.org_state.season, "week": self.org_state.week},
                context={},
                identifiers={"team_id": self.user_team_id},
                causal_fragment=["analytics_query"],
            )
            raise EngineIntegrityError(artifact)

        with duckdb.connect(str(self.paths.duckdb_path)) as conn:
            rows = conn.execute(
                """
                SELECT week, SUM(points_for - points_against) AS value
                FROM mart_traditional_stats
                WHERE team_id = ? AND season = ?
                GROUP BY week
                ORDER BY week
                """,
                [self.user_team_id, self.org_state.season],
            ).fetchall()
        return {
            "labels": [f"W{r[0]}" for r in rows],
            "values": [float(r[1]) for r in rows],
        }

    def export(self) -> list[Path]:
        if self.org_state is None:
            raise RuntimeError("cannot export without an active loaded profile")
        etl_week = self.org_state.week - 1
        if etl_week < 1:
            etl_week = 1
        run_weekly_etl(self.paths.sqlite_path, self.paths.duckdb_path, self.org_state.season, etl_week)
        service = ExportService(self.paths.duckdb_path)
        return service.export_required_datasets(self.paths.export_dir)

    def _require_dev_calibration(self) -> DevCalibrationGateway:
        if self.dev_calibration is None:
            artifact = build_forensic_artifact(
                engine_scope="runtime",
                error_code="DEV_MODULE_IMPORTED_IN_GAMEPLAY_RUNTIME",
                message="dev calibration gateway was requested without dev mode",
                state_snapshot={"dev_mode": self.dev_mode},
                context={},
                identifiers={"team_id": self.user_team_id},
                causal_fragment=["dev_calibration_gateway"],
            )
            raise EngineIntegrityError(artifact)
        return self.dev_calibration

    def _load_dev_calibration_gateway(self) -> DevCalibrationGateway:
        gateway_mod = importlib.import_module("grs.devtools.calibration_gateway")
        calibration_mod = importlib.import_module("grs.football.calibration")
        gateway_cls = getattr(gateway_mod, "LocalDevCalibrationGateway")
        service_cls = getattr(calibration_mod, "CalibrationService")
        service = service_cls(base_resolver=self.resource_resolver)
        return gateway_cls(
            service=service,
            duckdb_path=self.paths.duckdb_path,
            export_dir=self.paths.export_dir / "dev_calibration",
        )

    def _load_strict_audit_service(self):
        module = importlib.import_module("grs.devtools.strict_audit")
        service_cls = getattr(module, "StrictAuditService")
        return service_cls()

    def _emit_dev_event(self, *, event_type: str, claims: list[str], evidence_handles: list[str]) -> None:
        if self.org_state is None:
            return
        event = NarrativeEvent(
            event_id=make_id("ne"),
            time=now_utc(),
            scope="dev",
            event_type=event_type,
            actors=[self.user_team_id],
            claims=claims,
            evidence_handles=evidence_handles,
            severity="normal",
            confidentiality_tier="internal",
        )
        self.org_state.narrative_events.append(event)
        if self.store is not None:
            self.store.save_narrative_events([event])
        self.event_bus.publish_narrative(event)
