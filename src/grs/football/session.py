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
    PlaycallRequest,
    RandomSource,
    SimMode,
    Situation,
    SnapContextPackage,
    TeamGamePackage,
    ValidationError,
    ValidationIssue,
)
from grs.core import EngineIntegrityError, build_forensic_artifact
from grs.football.coaching import intent_to_playcall
from grs.football.injury import InjuryEvaluationError, InjuryEvaluator
from grs.football.models import GameSessionResult, SnapResolution
from grs.football.packages import PACKAGE_SLOT_REQUIREMENTS, resolve_package_ids
from grs.football.resources import ResourceResolver
from grs.football.resolver import FootballEngine
from grs.football.validation import PreSimValidator
from grs.org.entities import Franchise, Player

PlaycallProvider = Callable[[GameSessionState, str, str], PlaycallRequest | None]


class GameSessionEngine:
    def __init__(
        self,
        football_engine: FootballEngine,
        coach_engine: CoachDecisionEngine,
        *,
        validator: PreSimValidator,
        random_source: RandomSource,
        resource_resolver: ResourceResolver,
    ) -> None:
        self._football_engine = football_engine
        self._coach_engine = coach_engine
        self._validator = validator
        self._random_source = random_source
        self._resource_resolver = resource_resolver
        self._injury_evaluator = InjuryEvaluator()

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
        fatigue: dict[str, float] = {
            p.player_id: 0.1 for p in home.roster + away.roster
        }
        overtime_possessions: set[str] = set()
        player_lookup = {p.player_id: p for p in home.roster + away.roster}
        state.active_injuries = {player_id: "none" for player_id in player_lookup}

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

            participants = self._participants(offense_team, defense_team, call)
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
                current = fatigue[actor_id]
                if current < 0.0 or current > 1.0:
                    raise ValueError(f"fatigue out of domain for actor '{actor_id}': {current}")
                fatigue[actor_id] = current + ((1.0 - current) * 0.01)

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
            coaching_policy_id=team.coaching_policy_id,
            rules_profile_id=team.rules_profile_id,
        )

    def _participants(self, offense_team: Franchise, defense_team: Franchise, call: PlaycallRequest) -> list[ActorRef]:
        offense_package_id, defense_package_id = resolve_package_ids(call.play_type, call.personnel)
        formation = self._resource_resolver.resolve_formation(call.formation)
        required_slots_raw = formation.get("required_slots")
        if not isinstance(required_slots_raw, list) or not required_slots_raw:
            raise ValueError(f"formation '{call.formation}' missing required_slots")
        required_slots = [str(slot) for slot in required_slots_raw]
        offense_template_slots = PACKAGE_SLOT_REQUIREMENTS[offense_package_id]
        offense_slots = list(dict.fromkeys(required_slots + offense_template_slots))
        defense_slots = list(PACKAGE_SLOT_REQUIREMENTS[defense_package_id])
        offense = self._resolve_side(
            team=offense_team,
            package_id=offense_package_id,
            required_slots=offense_slots,
        )
        defense = self._resolve_side(
            team=defense_team,
            package_id=defense_package_id,
            required_slots=defense_slots,
        )
        return offense + defense

    def _resolve_side(self, *, team: Franchise, package_id: str, required_slots: list[str]) -> list[ActorRef]:
        by_id = {p.player_id: p for p in team.roster}
        if package_id not in team.package_book:
            raise ValueError(f"team {team.team_id} is missing package '{package_id}'")
        package_assignment = team.package_book[package_id]
        actors: list[ActorRef] = []
        resolved_player_ids: list[str] = []
        for slot in required_slots:
            if slot not in package_assignment:
                raise ValueError(f"team {team.team_id} package '{package_id}' missing slot '{slot}'")
            player_id = str(package_assignment[slot])
            if player_id not in by_id:
                raise ValueError(f"team {team.team_id} package '{package_id}' has unknown player '{player_id}'")
            if player_id not in resolved_player_ids:
                resolved_player_ids.append(player_id)
        if len(resolved_player_ids) < 11:
            template_slots = PACKAGE_SLOT_REQUIREMENTS[package_id]
            for slot in template_slots:
                player_id = str(package_assignment.get(slot, ""))
                if not player_id:
                    continue
                if player_id not in by_id:
                    raise ValueError(f"team {team.team_id} package '{package_id}' has unknown player '{player_id}'")
                if player_id not in resolved_player_ids:
                    resolved_player_ids.append(player_id)
                if len(resolved_player_ids) == 11:
                    break
        if len(resolved_player_ids) != 11:
            raise ValueError(
                f"team {team.team_id} package '{package_id}' cannot field 11 unique participants (got {len(resolved_player_ids)})"
            )
        for player_id in resolved_player_ids:
            p = by_id[player_id]
            actors.append(ActorRef(actor_id=p.player_id, team_id=team.team_id, role=p.position))
        return actors

    def _in_game_states(self, participants: list[ActorRef], fatigue_map: dict[str, float], injuries: dict[str, str]) -> dict[str, InGameState]:
        states: dict[str, InGameState] = {}
        for p in participants:
            if p.actor_id not in fatigue_map:
                raise ValueError(f"participant '{p.actor_id}' missing fatigue baseline")
            if p.actor_id not in injuries:
                raise ValueError(f"participant '{p.actor_id}' missing injury status")
            fatigue = fatigue_map[p.actor_id]
            limitation = injuries[p.actor_id]
            if fatigue < 0.0 or fatigue > 1.0:
                raise ValueError(f"fatigue out of domain for actor '{p.actor_id}': {fatigue}")
            hash_key = f"{p.actor_id}:{p.role}".encode("ascii", "ignore")
            discipline_seed = (int(hashlib.sha256(hash_key).hexdigest()[:8], 16) / 0xFFFFFFFF)
            states[p.actor_id] = InGameState(
                fatigue=fatigue,
                acute_wear=fatigue * 0.9,
                confidence_tilt=0.0,
                injury_limitation=limitation,
                discipline_risk=0.25 + (discipline_seed * 0.6),
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
            return
        if score_event == "OFF_TD":
            self._add_points(state, offense_team_id, 6)
        elif score_event == "FG_GOOD":
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
        try:
            injuries = self._injury_evaluator.evaluate(
                resolution=resolution,
                player_lookup=player_lookup,
                random_source=self._random_source,
            )
        except InjuryEvaluationError as exc:
            artifact = build_forensic_artifact(
                engine_scope="football_session",
                error_code="MISSING_REQUIRED_TRAIT_AT_RUNTIME",
                message=str(exc),
                state_snapshot={
                    "game_id": state.game_id,
                    "play_id": resolution.play_result.play_id,
                },
                context={},
                identifiers={"game_id": state.game_id, "play_id": resolution.play_result.play_id},
                causal_fragment=["injury_evaluation"],
            )
            raise EngineIntegrityError(artifact) from exc
        state.active_injuries.update(injuries)

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
