from __future__ import annotations

import hashlib
from dataclasses import asdict, replace
from typing import Callable

from grs.contracts import (
    ActorRef,
    CoachDecisionEngine,
    GameSessionState,
    InGameState,
    ParameterizedIntent,
    PlayType,
    PlaycallRequest,
    RandomSource,
    SimMode,
    Situation,
    SnapContextPackage,
    TeamGamePackage,
    ValidationError,
    ValidationIssue,
)
from grs.core import EngineIntegrityError, build_forensic_artifact, gameplay_random
from grs.football.coaching import PolicyDrivenCoachDecisionEngine, intent_to_playcall
from grs.football.models import GameSessionResult, SnapResolution
from grs.football.resources import ResourceResolver
from grs.football.resolver import FootballEngine
from grs.football.validation import PreSimValidator
from grs.org.entities import Franchise, Player

PlaycallProvider = Callable[[GameSessionState, str, str], PlaycallRequest | None]


class GameSessionEngine:
    OFFENSE_SLOTS = ["QB1", "RB1", "WR1", "WR2", "WR3", "TE1", "LT", "LG", "C", "RG", "RT"]
    DEFENSE_SLOTS = ["DE1", "DT1", "DT2", "DE2", "LB1", "LB2", "LB3", "CB1", "CB2", "S1", "S2"]
    PUNT_OFFENSE_SLOTS = ["P", "LT", "LG", "C", "RG", "RT", "TE1", "WR1", "WR2", "CB1", "S1"]
    FIELD_GOAL_OFFENSE_SLOTS = ["K", "LT", "LG", "C", "RG", "RT", "TE1", "LB1", "LB2", "DE1", "DE2"]
    KICKOFF_OFFENSE_SLOTS = ["K", "LB1", "LB2", "LB3", "CB1", "CB2", "S1", "S2", "DE1", "DE2", "WR1"]
    KICKOFF_RETURN_SLOTS = ["RB1", "WR1", "WR2", "WR3", "TE1", "LB1", "LB2", "CB1", "S1", "S2", "DE1"]

    def __init__(
        self,
        football_engine: FootballEngine,
        coach_engine: CoachDecisionEngine | None = None,
        *,
        validator: PreSimValidator | None = None,
        random_source: RandomSource | None = None,
    ) -> None:
        self._football_engine = football_engine
        self._coach_engine = coach_engine or PolicyDrivenCoachDecisionEngine(repository=ResourceResolver(), policy_id="balanced_default")
        self._validator = validator or PreSimValidator()
        self._random_source = random_source or gameplay_random()

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
            offense_package = self._team_package(offense_team)
            defense_package = self._team_package(defense_team)

            if playcall_provider:
                provided = playcall_provider(state, offense_team.team_id, defense_team.team_id)
            else:
                provided = None
            if provided is not None:
                call = provided
            else:
                intent = self._coach_engine.decide_play_intent(
                    session_state=state,
                    offense_package=offense_package,
                    defense_package=defense_package,
                    random_source=self._random_source.spawn(f"coach:{state.game_id}:{snap_index}"),
                )
                call = intent_to_playcall(offense_team.team_id, intent)

            try:
                self._validator.validate_playcall(call)
            except ValidationError as exc:
                raise self._validation_hard_fail(state.game_id, f"{state.game_id}_P{snap_index:03d}", exc.issues, "playcall_validation") from exc

            participants = self._participants(offense_team, defense_team, call.play_type)
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
                trait_vectors={p.actor_id: dict(player_lookup[p.actor_id].traits) for p in participants},
                intent=ParameterizedIntent(
                    personnel=call.personnel,
                    formation=call.formation,
                    offensive_concept=call.offensive_concept,
                    defensive_concept=call.defensive_concept,
                    playbook_entry_id=call.playbook_entry_id,
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
                raise self._validation_hard_fail(state.game_id, scp.play_id, exc.issues, "snap_validation") from exc

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
            self._update_injuries(state, resolution, player_lookup)
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

    def _team_package(self, team: Franchise) -> TeamGamePackage:
        depth = {d.slot_role: d.player_id for d in team.depth_chart if d.active_flag}
        active = sorted(set(depth.values()))
        return TeamGamePackage(
            team_id=team.team_id,
            active_players=active,
            depth_slots=depth,
            perceived_inputs={
                "owner_patience": team.owner.patience,
                "owner_risk": team.owner.risk_tolerance,
                "cap_space": float(team.cap_space),
            },
            coaching_policy_id="balanced_default",
        )

    def _participants(self, offense_team: Franchise, defense_team: Franchise, play_type: PlayType) -> list[ActorRef]:
        offense_slots = self._offense_slots_for_play_type(play_type)
        defense_slots = self._defense_slots_for_play_type(play_type)
        return self._resolve_side(offense_team, offense_slots) + self._resolve_side(defense_team, defense_slots)

    def _offense_slots_for_play_type(self, play_type: PlayType) -> list[str]:
        if play_type == PlayType.PUNT:
            return self.PUNT_OFFENSE_SLOTS
        if play_type in {PlayType.FIELD_GOAL, PlayType.EXTRA_POINT}:
            return self.FIELD_GOAL_OFFENSE_SLOTS
        if play_type == PlayType.KICKOFF:
            return self.KICKOFF_OFFENSE_SLOTS
        return self.OFFENSE_SLOTS

    def _defense_slots_for_play_type(self, play_type: PlayType) -> list[str]:
        return self.KICKOFF_RETURN_SLOTS if play_type == PlayType.KICKOFF else self.DEFENSE_SLOTS

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

    def _in_game_states(self, participants: list[ActorRef], fatigue_map: dict[str, float], injuries: dict[str, str]) -> dict[str, InGameState]:
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

    def _apply_resolution_to_state(self, state: GameSessionState, resolution: SnapResolution, offense_team_id: str, defense_team_id: str) -> None:
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

    def _update_injuries(self, state: GameSessionState, resolution: SnapResolution, player_lookup: dict[str, Player]) -> None:
        for rep in resolution.rep_ledger:
            actor_id = rep.actors[0].actor_id
            player = player_lookup.get(actor_id)
            if player is None:
                continue
            traits = getattr(player, "traits", {})
            key = f"{resolution.play_result.play_id}:{rep.rep_type}:{actor_id}".encode("ascii", "ignore")
            jitter = (int(hashlib.sha256(key).hexdigest()[:8], 16) / 0xFFFFFFFF) * 0.03
            contact = float(traits.get("contact_injury_risk", 50.0))
            soft = float(traits.get("soft_tissue_risk", 50.0))
            dur = float(traits.get("durability", 50.0))
            injury_prob = (((contact - 1.0) / 98.0) * 0.012) + (((soft - 1.0) / 98.0) * 0.008) + ((1.0 - ((dur - 1.0) / 98.0)) * 0.006) + jitter
            if injury_prob > 0.018:
                state.active_injuries[actor_id] = "limited"

    def _validation_hard_fail(self, game_id: str, play_id: str, issues: list[ValidationIssue], phase: str) -> EngineIntegrityError:
        artifact = build_forensic_artifact(
            engine_scope="football_session",
            error_code="PRE_SIM_VALIDATION_FAILED",
            message=f"{phase} failed",
            state_snapshot={"game_id": game_id, "play_id": play_id, "issue_count": len(issues)},
            context={"issues": [asdict(i) for i in issues], "phase": phase},
            identifiers={"game_id": game_id, "play_id": play_id},
            causal_fragment=["pre_sim_gate", phase],
        )
        return EngineIntegrityError(artifact)
