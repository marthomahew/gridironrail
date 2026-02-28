from __future__ import annotations

from dataclasses import asdict, replace
import hashlib
from typing import Callable

from grs.contracts import (
    ActorRef,
    GameSessionState,
    InGameState,
    ParameterizedIntent,
    PlayType,
    PlaycallRequest,
    RandomSource,
    SimMode,
    Situation,
    SnapContextPackage,
    ValidationError,
    ValidationIssue,
)
from grs.core import EngineIntegrityError, build_forensic_artifact
from grs.football.models import GameSessionResult, SnapResolution
from grs.football.resources import ResourceResolver
from grs.football.resolver import FootballEngine
from grs.football.validation import PreSimValidator
from grs.org.entities import Franchise

PlaycallProvider = Callable[[GameSessionState, str, str], PlaycallRequest]


class GameSessionEngine:
    OFFENSE_SLOTS = ["QB1", "RB1", "WR1", "WR2", "WR3", "TE1", "LT", "LG", "C", "RG", "RT"]
    DEFENSE_SLOTS = ["DE1", "DT1", "DT2", "DE2", "LB1", "LB2", "LB3", "CB1", "CB2", "S1", "S2"]

    def __init__(
        self,
        football_engine: FootballEngine,
        *,
        validator: PreSimValidator | None = None,
        resource_resolver: ResourceResolver | None = None,
        coaching_policy_id: str = "balanced_default",
        random_source: RandomSource | None = None,
    ) -> None:
        self._football_engine = football_engine
        self._validator = validator or PreSimValidator()
        self._resource_resolver = resource_resolver or ResourceResolver()
        self._coaching_policy_id = coaching_policy_id
        self._random_source = random_source

    def run_game(
        self,
        session_state: GameSessionState,
        home: Franchise,
        away: Franchise,
        mode: SimMode,
        playcall_provider: PlaycallProvider | None = None,
        max_snaps: int = 240,
    ) -> GameSessionResult:
        snaps: list[SnapResolution] = []
        state = replace(session_state)
        action_stream: list[dict[str, str | int | float]] = []
        fatigue: dict[str, float] = {}
        overtime_possessions: set[str] = set()
        player_lookup = {p.player_id: p for p in home.roster + away.roster}

        for snap_index in range(1, max_snaps + 1):
            if state.completed:
                break

            offense_team = home if state.possession_team_id == home.team_id else away
            defense_team = away if offense_team.team_id == home.team_id else home

            if playcall_provider:
                call = playcall_provider(state, offense_team.team_id, defense_team.team_id)
            else:
                call = self.build_default_playcall(state, offense_team.team_id)
            try:
                self._validator.validate_playcall(call)
            except ValidationError as exc:
                raise self._validation_hard_fail(
                    game_id=state.game_id,
                    play_id=f"{state.game_id}_P{snap_index:03d}",
                    issues=exc.issues,
                    phase="playcall_validation",
                ) from exc

            participants = self._participants(offense_team, defense_team)
            in_game_states = self._in_game_states(participants, fatigue, state.active_injuries)

            scp = SnapContextPackage(
                game_id=state.game_id,
                play_id=f"{state.game_id}_P{snap_index:03d}",
                mode=mode,
                situation=Situation(
                    quarter=state.quarter,
                    clock_seconds=state.clock_seconds,
                    down=state.down,
                    distance=state.distance,
                    yard_line=state.yard_line,
                    possession_team_id=state.possession_team_id,
                    score_diff=state.home_score - state.away_score,
                    timeouts_offense=state.timeouts_home if offense_team.team_id == home.team_id else state.timeouts_away,
                    timeouts_defense=state.timeouts_away if offense_team.team_id == home.team_id else state.timeouts_home,
                ),
                participants=participants,
                in_game_states=in_game_states,
                intent=ParameterizedIntent(
                    personnel=call.personnel,
                    formation=call.formation,
                    offensive_concept=call.offensive_concept,
                    defensive_concept=call.defensive_concept,
                    tempo=call.tempo,
                    aggression=call.aggression,
                    allows_audible=True,
                    play_type=call.play_type,
                ),
                weather_flags=["clear"],
            )
            try:
                self._validator.validate_snap_context(scp, player_lookup=player_lookup)
            except ValidationError as exc:
                raise self._validation_hard_fail(
                    game_id=state.game_id,
                    play_id=scp.play_id,
                    issues=exc.issues,
                    phase="snap_validation",
                ) from exc

            resolution = self._football_engine.run_mode_invariant(scp, mode)
            snaps.append(resolution)
            action_stream.append(
                {
                    "snap_index": snap_index,
                    "play_id": scp.play_id,
                    "mode": mode.value,
                    "offense_team": offense_team.team_id,
                    "defense_team": defense_team.team_id,
                    "play_type": call.play_type.value,
                    "terminal_event": resolution.causality_chain.terminal_event,
                }
            )

            state.active_penalties.extend(resolution.play_result.penalties)
            self._update_injuries(state, resolution)
            self._apply_resolution_to_state(state, resolution, offense_team.team_id, defense_team.team_id)

            for actor_id in in_game_states:
                fatigue[actor_id] = min(1.0, fatigue.get(actor_id, 0.0) + 0.01)

            if state.is_overtime:
                overtime_possessions.add(offense_team.team_id)
                if state.home_score != state.away_score and len(overtime_possessions) >= 2:
                    state.completed = True

        if not state.completed:
            state.completed = True

        return GameSessionResult(
            final_state=state,
            snaps=snaps,
            home_team_id=home.team_id,
            away_team_id=away.team_id,
            home_score=state.home_score,
            away_score=state.away_score,
            action_stream=action_stream,
        )

    def _participants(self, offense_team: Franchise, defense_team: Franchise) -> list[ActorRef]:
        offense = self._resolve_side(offense_team, self.OFFENSE_SLOTS)
        defense = self._resolve_side(defense_team, self.DEFENSE_SLOTS)
        return offense + defense

    def _resolve_side(self, team: Franchise, slots: list[str]) -> list[ActorRef]:
        by_id = {p.player_id: p for p in team.roster}
        assigned = {d.slot_role: d for d in team.depth_chart if d.active_flag}
        actors: list[ActorRef] = []

        for slot in slots:
            assn = assigned.get(slot)
            if assn is None:
                raise ValueError(f"team {team.team_id} missing active depth slot '{slot}'")
            if assn.player_id not in by_id:
                raise ValueError(f"team {team.team_id} slot '{slot}' references unknown player '{assn.player_id}'")
            p = by_id[assn.player_id]
            actors.append(ActorRef(actor_id=p.player_id, team_id=team.team_id, role=p.position))

        if len(actors) != 11:
            raise ValueError(f"team {team.team_id} cannot field 11 players")
        return actors

    def _in_game_states(
        self,
        participants: list[ActorRef],
        fatigue_map: dict[str, float],
        injuries: dict[str, str],
    ) -> dict[str, InGameState]:
        states: dict[str, InGameState] = {}
        for p in participants:
            fatigue = fatigue_map.get(p.actor_id, 0.1)
            limitation = injuries.get(p.actor_id, "none")
            discipline_seed = (abs(hash((p.actor_id, p.role))) % 100) / 100.0
            states[p.actor_id] = InGameState(
                fatigue=fatigue,
                acute_wear=min(1.0, fatigue * 0.9),
                confidence_tilt=0.0,
                injury_limitation=limitation,
                discipline_risk=min(1.0, 0.25 + (discipline_seed * 0.6)),
            )
        return states

    def build_default_playcall(self, state: GameSessionState, offense_team_id: str) -> PlaycallRequest:
        policy = self._resource_resolver.resolve_policy(self._coaching_policy_id)
        defaults = policy.get("defaults", {})
        if not isinstance(defaults, dict):
            raise ValueError(f"coaching policy '{self._coaching_policy_id}' defaults must be an object")

        posture = "normal"
        if state.down >= 3 and state.distance >= 7:
            posture = "third_and_long"
        elif state.distance <= 2:
            posture = "short_yardage"
        posture_cfg = defaults.get(posture)
        if not isinstance(posture_cfg, dict):
            raise ValueError(f"coaching policy '{self._coaching_policy_id}' missing posture '{posture}'")

        if state.down >= 3 and state.distance >= 7:
            play_type = PlayType.PASS
            personnel = self._required_policy_key(posture_cfg, "personnel")
            formation = self._required_policy_key(posture_cfg, "formation_pass")
            offense_concept = self._required_policy_key(posture_cfg, "offense_pass")
            defense_concept = self._required_policy_key(posture_cfg, "defense_base")
        elif state.yard_line > 90 and state.down == 4:
            play_type = PlayType.FIELD_GOAL
            personnel = "field_goal"
            formation = "field_goal_heavy"
            offense_concept = "field_goal_unit"
            defense_concept = "field_goal_block"
        elif state.down == 4:
            play_type = PlayType.PUNT
            personnel = "punt"
            formation = "punt_spread"
            offense_concept = "punt_safe"
            defense_concept = "punt_return_safe"
        else:
            play_type = PlayType.RUN if state.distance <= 4 else PlayType.PASS
            personnel = self._required_policy_key(posture_cfg, "personnel")
            if play_type == PlayType.RUN:
                formation = self._required_policy_key(posture_cfg, "formation_run")
                offense_concept = self._required_policy_key(posture_cfg, "offense_run")
            else:
                formation = self._required_policy_key(posture_cfg, "formation_pass")
                offense_concept = self._required_policy_key(posture_cfg, "offense_pass")
            defense_concept = self._required_policy_key(posture_cfg, "defense_base")

        return PlaycallRequest(
            team_id=offense_team_id,
            personnel=personnel,
            formation=formation,
            offensive_concept=offense_concept,
            defensive_concept=defense_concept,
            tempo="normal",
            aggression="balanced",
            play_type=play_type,
        )

    def _apply_resolution_to_state(
        self,
        state: GameSessionState,
        resolution: SnapResolution,
        offense_team_id: str,
        defense_team_id: str,
    ) -> None:
        play = resolution.play_result

        state.clock_seconds -= play.clock_delta
        if state.clock_seconds <= 0:
            state.quarter += 1
            if state.quarter <= 4:
                state.clock_seconds = 900
            elif state.quarter == 5 and state.home_score == state.away_score:
                state.is_overtime = True
                state.clock_seconds = 600
            else:
                state.completed = True
                return

        self._apply_score(state, play.score_event, offense_team_id, defense_team_id)
        if play.score_event and state.is_overtime and state.home_score != state.away_score:
            state.completed = True
            return

        state.possession_team_id = play.next_possession_team_id
        state.down = play.next_down
        state.distance = play.next_distance
        state.yard_line = play.new_spot

        if play.next_down == 1:
            state.drive_index += 1

        if state.quarter > 4 and state.home_score != state.away_score and not state.is_overtime:
            state.completed = True

    def _apply_score(self, state: GameSessionState, score_event: str | None, offense_team_id: str, defense_team_id: str) -> None:
        if score_event is None:
            if state.yard_line >= 99 and state.down == 1:
                self._add_points(state, offense_team_id, 6)
            return

        if score_event == "FG_GOOD":
            self._add_points(state, offense_team_id, 3)
        elif score_event == "XP_GOOD":
            self._add_points(state, offense_team_id, 1)
        elif score_event == "TWO_PT_GOOD":
            self._add_points(state, offense_team_id, 2)
        elif score_event in {"PUNT_RETURN_TD", "KICK_RETURN_TD"}:
            self._add_points(state, defense_team_id, 6)

    def _add_points(self, state: GameSessionState, team_id: str, points: int) -> None:
        if team_id == state.home_team_id:
            state.home_score += points
        else:
            state.away_score += points

    def _update_injuries(self, state: GameSessionState, resolution: SnapResolution) -> None:
        for rep in resolution.rep_ledger:
            if rep.rep_type not in {"tackle", "pursuit", "contest", "run_fit"}:
                continue
            key = f"{resolution.play_result.play_id}:{rep.rep_type}:{rep.actors[0].actor_id}".encode("ascii", "ignore")
            injury_roll = int(hashlib.sha256(key).hexdigest()[:8], 16) % 1000
            if injury_roll < 4:
                actor_id = rep.actors[0].actor_id
                state.active_injuries[actor_id] = "limited"

    def _required_policy_key(self, posture_cfg: dict[str, str], key: str) -> str:
        value = posture_cfg.get(key)
        if not value:
            raise ValueError(f"coaching policy '{self._coaching_policy_id}' missing key '{key}'")
        return str(value)

    def _validation_hard_fail(
        self,
        *,
        game_id: str,
        play_id: str,
        issues: list[ValidationIssue],
        phase: str,
    ) -> EngineIntegrityError:
        artifact = build_forensic_artifact(
            engine_scope="football_session",
            error_code="PRE_SIM_VALIDATION_FAILED",
            message=f"{phase} failed",
            state_snapshot={"game_id": game_id, "play_id": play_id, "issue_count": len(issues)},
            context={"issues": [asdict(issue) for issue in issues], "phase": phase},
            identifiers={"game_id": game_id, "play_id": play_id},
            causal_fragment=["pre_sim_gate", phase],
        )
        return EngineIntegrityError(artifact)
