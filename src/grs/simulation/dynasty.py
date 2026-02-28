from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from grs.contracts import (
    ActionRequest,
    ActionResult,
    ActionType,
    Difficulty,
    GameSessionState,
    LeagueSnapshotRef,
    PlayType,
    PlaycallRequest,
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
)
from grs.export import ExportService
from grs.football import (
    FootballEngine,
    FootballResolver,
    GameSessionEngine,
    GameSessionResult,
    PreSimValidator,
    ResourceResolver,
)
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
    ) -> None:
        self.paths = RuntimePaths(root)
        self.paths.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.paths.snapshot_dir.mkdir(parents=True, exist_ok=True)

        profiles = default_difficulty_profiles()
        self.difficulty = profiles[difficulty]
        self.seed = seed

        self.rand = seeded_random(seed) if seed is not None else gameplay_random()
        self.event_bus = EventBus()
        self.store = AuthoritativeStore(self.paths.sqlite_path)
        self.store.initialize_schema()
        self.resource_resolver = ResourceResolver()
        self.pre_sim_validator = PreSimValidator(resource_resolver=self.resource_resolver)
        self.store.save_trait_catalog(self.pre_sim_validator.trait_catalog())

        self.org_state: LeagueState = build_default_league(team_count=8)
        self.user_team_id = user_team_id
        self.org_engine = OrganizationalEngine(rand=self.rand.spawn("org"), difficulty=self.difficulty)
        self.football = FootballEngine(
            FootballResolver(random_source=self.rand.spawn("football")),
            validator=self.pre_sim_validator,
        )
        self.game_session = GameSessionEngine(
            self.football,
            validator=self.pre_sim_validator,
            resource_resolver=self.resource_resolver,
            coaching_policy_id="balanced_default",
            random_source=self.rand.spawn("session"),
        )
        self.retention_policy = RetentionPolicy()

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
            required = {"personnel", "formation", "offensive_concept", "defensive_concept", "play_type"}
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
                tempo=str(payload.get("tempo", "normal")),
                aggression=str(payload.get("aggression", "balanced")),
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

        if action in {ActionType.PLAY_USER_GAME, ActionType.PLAY_SNAP, ActionType.SIM_DRIVE}:
            result = self._simulate_user_game(mode=SimMode.PLAY if action != ActionType.SIM_DRIVE else SimMode.SIM)
            return ActionResult(
                request.request_id,
                True,
                f"User game finalized {result.home_score}-{result.away_score}",
                data={
                    "game_id": result.final_state.game_id,
                    "home_team_id": result.home_team_id,
                    "away_team_id": result.away_team_id,
                    "home_score": result.home_score,
                    "away_score": result.away_score,
                    "snaps": len(result.snaps),
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
            return ActionResult(
                request.request_id,
                True,
                "latest user game",
                data={
                    "state": asdict(g.final_state),
                    "snaps": [
                        {
                            "play_id": s.play_result.play_id,
                            "yards": s.play_result.yards,
                            "event": s.causality_chain.terminal_event,
                        }
                        for s in g.snaps[-40:]
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
            game_id = request.payload.get("game_id")
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

        def provider(state: GameSessionState, offense_team_id: str, defense_team_id: str) -> PlaycallRequest:
            if offense_team_id == self.user_team_id and self.pending_user_playcall:
                return self.pending_user_playcall
            return self.game_session.build_default_playcall(state, offense_team_id)

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
                coaching_policy_id="balanced_default",
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
            return {"labels": [], "values": []}

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
        run_weekly_etl(self.paths.sqlite_path, self.paths.duckdb_path, self.org_state.season, max(1, self.org_state.week - 1))
        service = ExportService(self.paths.duckdb_path)
        return service.export_required_datasets(self.paths.export_dir)
