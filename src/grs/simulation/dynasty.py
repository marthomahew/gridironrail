from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from grs.contracts import (
    ActionRequest,
    ActionResult,
    ActorRef,
    Difficulty,
    InGameState,
    ParameterizedIntent,
    PlayType,
    RetentionPolicy,
    SimMode,
    Situation,
    SnapContextPackage,
)
from grs.core import EventBus, default_difficulty_profiles, gameplay_random, make_id, seeded_random
from grs.export import ExportService
from grs.football import FootballEngine, FootballResolver
from grs.org import LeagueState, OrganizationalEngine, build_default_league
from grs.persistence import AuthoritativeStore, GameRetentionContext, run_weekly_etl, should_retain_game


@dataclass(slots=True)
class RuntimePaths:
    root: Path

    @property
    def sqlite_path(self) -> Path:
        return self.root / "data" / "authoritative.sqlite3"

    @property
    def duckdb_path(self) -> Path:
        return self.root / "data" / "analytics.duckdb"

    @property
    def export_dir(self) -> Path:
        return self.root / "exports"


class DynastyRuntime:
    def __init__(self, root: Path, difficulty: Difficulty = Difficulty.PRO, seed: int | None = None) -> None:
        self.paths = RuntimePaths(root)
        self.paths.sqlite_path.parent.mkdir(parents=True, exist_ok=True)

        profiles = default_difficulty_profiles()
        self.difficulty = profiles[difficulty]

        self.rand = seeded_random(seed) if seed is not None else gameplay_random()
        self.event_bus = EventBus()
        self.store = AuthoritativeStore(self.paths.sqlite_path)
        self.store.initialize_schema()

        self.org_state: LeagueState = build_default_league()
        self.org_engine = OrganizationalEngine(rand=self.rand.spawn("org"), difficulty=self.difficulty)
        self.football = FootballEngine(FootballResolver(random_source=self.rand.spawn("football")))
        self.retention_policy = RetentionPolicy()

    def handle_action(self, request: ActionRequest) -> ActionResult:
        if request.action_type == "advance_week":
            self._advance_week()
            return ActionResult(request.request_id, True, f"Advanced to S{self.org_state.season} W{self.org_state.week} {self.org_state.phase}")

        if request.action_type == "play_snap":
            result = self._play_single_snap(mode=SimMode.PLAY)
            return ActionResult(request.request_id, True, f"Snap: {result.play_result.yards} yards, event={result.causality_chain.terminal_event}")

        if request.action_type == "sim_drive":
            total = 0
            plays = 0
            for _ in range(6):
                res = self._play_single_snap(mode=SimMode.SIM)
                total += res.play_result.yards
                plays += 1
            return ActionResult(request.request_id, True, f"Simulated drive with {plays} plays, net {total} yards")

        if request.action_type == "load_retained":
            return ActionResult(request.request_id, True, "Retained-game drilldown ready from authoritative store")

        if request.action_type == "debug_truth":
            top = sorted(self.org_state.teams[0].roster, key=lambda p: p.overall_truth, reverse=True)[:3]
            payload = {"players": [{"name": p.name, "truth": p.overall_truth} for p in top]}
            return ActionResult(request.request_id, True, "Debug ground-truth view generated", payload)

        return ActionResult(request.request_id, False, f"Unsupported action '{request.action_type}'")

    def _advance_week(self) -> None:
        self.org_engine.advance_week(self.org_state)
        if self.org_state.phase == "offseason" and self.org_state.week == 1:
            self.org_engine.generate_draft_class(self.org_state)
            self.org_engine.run_draft_round(self.org_state)
            self.org_engine.run_trade_window(self.org_state)
            self.org_engine.develop_players(self.org_state)

        self.store.save_transactions(self.org_state.transactions)
        self.store.save_cap_ledger(self.org_state.cap_ledger)
        self.store.save_narrative_events(self.org_state.narrative_events)
        run_weekly_etl(self.paths.sqlite_path, self.paths.duckdb_path)

    def _play_single_snap(self, mode: SimMode):
        game_id = f"S{self.org_state.season}_W{self.org_state.week}_G01"
        home = self.org_state.teams[0]
        away = self.org_state.teams[1]
        context = GameRetentionContext(
            game_id=game_id,
            phase=self.org_state.phase,
            is_championship=self.org_state.phase == "postseason" and self.org_state.week == 4,
            is_playoff=self.org_state.phase == "postseason",
            is_rivalry=False,
            is_record_game=False,
            tagged_instant_classic=False,
        )
        retained = should_retain_game(self.retention_policy, context)
        self.store.register_game(
            game_id=game_id,
            season=self.org_state.season,
            week=self.org_state.week,
            phase=self.org_state.phase,
            home_team_id=home.team_id,
            away_team_id=away.team_id,
            retained=retained,
        )

        scp = self._build_snap_context(game_id, home.team_id, away.team_id, mode)
        resolution = self.football.run_mode_invariant(scp, mode)

        self.store.save_snap_resolution(game_id, resolution, retained=retained)
        self.store.save_game_summary(
            game_id=game_id,
            home_team_id=home.team_id,
            away_team_id=away.team_id,
            home_score=7 if resolution.play_result.score_event else 0,
            away_score=0,
            plays=1,
            turnovers=1 if resolution.play_result.turnover else 0,
            penalties=len(resolution.play_result.penalties),
        )
        self.store.set_game_status(game_id, "final")
        if not retained:
            self.store.purge_non_retained_deep_logs(game_id)

        for event in resolution.narrative_events:
            self.event_bus.publish_narrative(event)
        return resolution

    def _build_snap_context(self, game_id: str, offense_team_id: str, defense_team_id: str, mode: SimMode) -> SnapContextPackage:
        participants: list[ActorRef] = []
        offense_positions = ["QB", "RB", "WR", "WR", "WR", "TE", "LT", "LG", "C", "RG", "RT"]
        defense_positions = ["DE", "DT", "DT", "DE", "LB", "LB", "LB", "CB", "CB", "S", "S"]
        for idx, role in enumerate(offense_positions):
            participants.append(ActorRef(actor_id=f"{offense_team_id}_O_{idx}", team_id=offense_team_id, role=role))
        for idx, role in enumerate(defense_positions):
            participants.append(ActorRef(actor_id=f"{defense_team_id}_D_{idx}", team_id=defense_team_id, role=role))

        states = {
            p.actor_id: InGameState(
                fatigue=0.2 + self.rand.rand() * 0.6,
                acute_wear=0.1 + self.rand.rand() * 0.5,
                confidence_tilt=self.rand.rand() * 2 - 1,
                injury_limitation="none",
                discipline_risk=self.rand.rand(),
            )
            for p in participants
        }

        play_type = self.rand.choice(
            [PlayType.RUN, PlayType.PASS, PlayType.PASS, PlayType.PUNT, PlayType.FIELD_GOAL, PlayType.KICKOFF, PlayType.EXTRA_POINT, PlayType.TWO_POINT]
        )
        return SnapContextPackage(
            game_id=game_id,
            play_id=make_id("play"),
            mode=mode,
            situation=Situation(
                quarter=self.rand.randint(1, 4),
                clock_seconds=self.rand.randint(12, 900),
                down=self.rand.randint(1, 4),
                distance=self.rand.randint(1, 20),
                yard_line=self.rand.randint(1, 99),
                possession_team_id=offense_team_id,
                score_diff=self.rand.randint(-24, 24),
                timeouts_offense=self.rand.randint(0, 3),
                timeouts_defense=self.rand.randint(0, 3),
            ),
            participants=participants,
            in_game_states=states,
            intent=ParameterizedIntent(
                personnel="11",
                formation="gun_trips",
                offensive_concept="spacing",
                defensive_concept="cover3_match",
                tempo="normal",
                aggression="balanced",
                allows_audible=True,
                play_type=play_type,
            ),
            weather_flags=["clear"],
        )

    def export(self) -> list[Path]:
        run_weekly_etl(self.paths.sqlite_path, self.paths.duckdb_path)
        service = ExportService(self.paths.duckdb_path)
        return service.export_required_datasets(self.paths.export_dir)
