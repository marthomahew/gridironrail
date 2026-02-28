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
    CalibrationTraitProfile,
    DevCalibrationGateway,
    Difficulty,
    GameSessionState,
    LeagueSnapshotRef,
    NarrativeEvent,
    PlayType,
    PlaycallRequest,
    TuningPatchRequest,
    RetentionPolicy,
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
from grs.org import LeagueState, OrganizationalEngine, build_default_league, generate_season_schedule, rank_standings
from grs.org.entities import Franchise
from grs.persistence import AuthoritativeStore, GameRetentionContext, run_weekly_etl, should_retain_game


class RuntimePaths:
    def __init__(self, root: Path) -> None:
        self.root = root

    @property
    def sqlite_path(self) -> Path:
        return self.root / "data" / "authoritative.sqlite3"

    @property
    def duckdb_path(self) -> Path:
        return self.root / "data" / "analytics.duckdb"

    @property
    def export_dir(self) -> Path:
        return self.root / "exports"

    @property
    def forensic_dir(self) -> Path:
        return self.root / "forensics"

    @property
    def snapshot_dir(self) -> Path:
        return self.root / "data" / "snapshots"


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
        self.paths.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.paths.snapshot_dir.mkdir(parents=True, exist_ok=True)

        profiles = default_difficulty_profiles()
        self.difficulty = profiles[difficulty]
        self.seed = seed
        self.dev_mode = dev_mode
        self.strict_policy = strict_execution_policy()

        self.rand = seeded_random(seed) if seed is not None else gameplay_random()
        self.event_bus = EventBus()
        self.store = AuthoritativeStore(self.paths.sqlite_path)
        self.store.initialize_schema()
        self.resource_resolver = ResourceResolver()
        self.pre_sim_validator = PreSimValidator(
            resource_resolver=self.resource_resolver,
            trait_catalog=canonical_trait_catalog(),
        )
        self.store.save_trait_catalog(self.pre_sim_validator.trait_catalog())

        self.org_state: LeagueState = build_default_league(team_count=8)
        self.user_team_id = user_team_id
        self.org_engine = OrganizationalEngine(rand=self.rand.spawn("org"), difficulty=self.difficulty)
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
        if self.dev_mode:
            self.dev_calibration = self._load_dev_calibration_gateway()

        self.halted = False
        self.last_forensic_path: str | None = None
        self.pending_user_playcall: PlaycallRequest | None = None
        self.last_user_game_result: GameSessionResult | None = None

        self._ensure_schedule_for_season(self.org_state.season)
        self._persist_league_state()

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
            self.last_forensic_path = str(persist_forensic_artifact(exc.artifact, self.paths.forensic_dir))
            self.halted = True
            return ActionResult(
                request.request_id,
                False,
                f"integrity failure: {exc.artifact.error_code}",
                {"forensic_path": self.last_forensic_path},
            )
        except Exception as exc:
            artifact = build_forensic_artifact(
                engine_scope="runtime",
                error_code="UNHANDLED_RUNTIME_EXCEPTION",
                message=str(exc),
                state_snapshot={"season": self.org_state.season, "week": self.org_state.week, "phase": self.org_state.phase},
                context={"action_type": str(request.action_type), "payload": request.payload},
                identifiers={"request_id": request.request_id, "team_id": request.actor_team_id},
                causal_fragment=["runtime_dispatch"],
            )
            self.last_forensic_path = str(persist_forensic_artifact(artifact, self.paths.forensic_dir))
            self.halted = True
            return ActionResult(
                request.request_id,
                False,
                f"runtime hard-stopped: {exc}",
                {"forensic_path": self.last_forensic_path},
            )

    def _handle_action_core(self, request: ActionRequest) -> ActionResult:
        action = self._normalize_action(request.action_type)

        if action == ActionType.SET_PLAYCALL:
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
            calibration = self._require_dev_calibration()
            profiles = [asdict(p) for p in calibration.list_tuning_profiles()]
            return ActionResult(request.request_id, True, "tuning profiles", data={"profiles": profiles, "active_profile_id": calibration.active_profile()})

        if action == ActionType.SET_TUNING_PROFILE:
            if not self.dev_mode:
                return ActionResult(request.request_id, False, "dev mode required for tuning actions")
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
            report = run_football_contract_audit()
            self._emit_dev_event(
                event_type="football_contract_audit",
                claims=[f"football audit run {report.report_id} passed={report.passed}"],
                evidence_handles=[f"audit:{report.report_id}"],
            )
            return ActionResult(
                request.request_id,
                True,
                "football audit complete",
                data={
                    "report_id": report.report_id,
                    "generated_at": report.generated_at.isoformat(),
                    "scope": report.scope,
                    "passed": report.passed,
                    "checks": [asdict(c) for c in report.checks],
                },
            )

        if action == ActionType.EXPORT_CALIBRATION_REPORT:
            if not self.dev_mode:
                return ActionResult(request.request_id, False, "dev mode required for calibration export actions")
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
            week_result = self._advance_pipeline()
            return ActionResult(
                request.request_id,
                True,
                f"Advanced to S{self.org_state.season} W{self.org_state.week} {self.org_state.phase}",
                data=asdict(week_result),
            )

        if action == ActionType.GET_ORG_OVERVIEW:
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
            rows = self.store.list_retained_games()
            return ActionResult(
                request.request_id,
                True,
                "retained games",
                data={"games": [{"game_id": r[0], "season": r[1], "week": r[2]} for r in rows]},
            )

        if action == ActionType.GET_FILM_ROOM_GAME:
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
        existing = self.store.get_schedule_for_week(season, 1)
        if existing:
            return
        team_ids = [t.team_id for t in self.org_state.teams]
        schedule = generate_season_schedule(team_ids, season=season, user_team_id=self.user_team_id, weeks=18)
        self.org_state.schedule = schedule
        self.store.save_schedule_entries(schedule)

    def _get_user_schedule_entry(self, season: int, week: int) -> ScheduleEntry | None:
        entries = self.store.get_schedule_for_week(season, week)
        for e in entries:
            if e.is_user_game:
                return e
        return None

    def _persist_league_state(self) -> None:
        metadata = {
            "phase": self.org_state.phase,
            "week": self.org_state.week,
            "season": self.org_state.season,
            "user_team_id": self.user_team_id,
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
        return next(t for t in self.org_state.teams if t.team_id == team_id)

    def _normalize_action(self, action: ActionType | str) -> ActionType:
        if isinstance(action, ActionType):
            return action
        return ActionType(action)

    def _analytics_series(self) -> dict[str, Any]:
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
        self.store.save_narrative_events([event])
        self.event_bus.publish_narrative(event)
