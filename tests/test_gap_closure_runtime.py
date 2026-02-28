from __future__ import annotations

import random

import pytest

from grs.contracts import (
    ActorRef,
    GameSessionState,
    InGameState,
    ParameterizedIntent,
    PlayType,
    SimMode,
    Situation,
    SnapContextPackage,
)
from grs.core import EngineIntegrityError, seeded_random
from grs.football import FootballEngine, FootballResolver, GameSessionEngine
from grs.football.traits import required_trait_codes
from grs.org.entities import Player


def _build_context(play_id: str, play_type: PlayType) -> SnapContextPackage:
    offense_roles, defense_roles = _roles_for_play_type(play_type)
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
            discipline_risk=0.4,
        )
        for p in participants
    }
    trait_vectors = {p.actor_id: {code: 55.0 for code in required_trait_codes()} for p in participants}
    personnel, formation, offense_concept, defense_concept = _intent_for_play_type(play_type)
    return SnapContextPackage(
        game_id="G_GAP",
        play_id=play_id,
        mode=SimMode.SIM,
        situation=Situation(
            quarter=2,
            clock_seconds=660,
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
    )


def _roles_for_play_type(play_type: PlayType) -> tuple[list[str], list[str]]:
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


def _intent_for_play_type(play_type: PlayType) -> tuple[str, str, str, str]:
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


def test_matchup_compile_is_deterministic_under_shuffled_participants() -> None:
    resolver = FootballResolver(random_source=seeded_random(500))
    base = _build_context("P_SHUFFLE", PlayType.PASS)
    shuffled = _build_context("P_SHUFFLE", PlayType.PASS)
    random.Random(42).shuffle(shuffled.participants)
    shuffled.in_game_states = {p.actor_id: shuffled.in_game_states[p.actor_id] for p in shuffled.participants}
    shuffled.trait_vectors = {p.actor_id: shuffled.trait_vectors[p.actor_id] for p in shuffled.participants}
    a = resolver.resolve_snap(base)
    b = resolver.resolve_snap(shuffled)
    edge_pairs_a = sorted((e.offense_actor_id, e.defense_actor_id, e.technique) for e in a.artifact_bundle.pre_snap_plan.graph.edges)
    edge_pairs_b = sorted((e.offense_actor_id, e.defense_actor_id, e.technique) for e in b.artifact_bundle.pre_snap_plan.graph.edges)
    assert edge_pairs_a == edge_pairs_b


def test_no_static_multi_actor_exchange_rep_injected() -> None:
    resolver = FootballResolver(random_source=seeded_random(501))
    res = resolver.resolve_snap(_build_context("P_MULTI", PlayType.PASS))
    assert not any(rep.rep_type == "multi_actor_exchange" for rep in res.rep_ledger)
    assert any("multi_actor" in rep.context_tags for rep in res.rep_ledger)


def test_kicks_and_conversion_use_adjudicated_defense_possession() -> None:
    resolver = FootballResolver(random_source=seeded_random(502))
    for play_type in [PlayType.FIELD_GOAL, PlayType.EXTRA_POINT, PlayType.TWO_POINT]:
        res = resolver.resolve_snap(_build_context(f"P_{play_type.value}", play_type))
        assert res.play_result.next_possession_team_id == "B"


def test_missing_runtime_injury_trait_hard_fails() -> None:
    resolver = FootballResolver(random_source=seeded_random(503))
    engine = FootballEngine(resolver)
    session_engine = GameSessionEngine(engine, random_source=seeded_random(504))
    resolution = engine.run_snap(_build_context("P_INJ", PlayType.PASS))
    player_lookup: dict[str, Player] = {}
    for actor_id in {a.actor_id for rep in resolution.rep_ledger for a in rep.actors}:
        player_lookup[actor_id] = Player(
            player_id=actor_id,
            team_id="A" if actor_id.startswith("A_") else "B",
            name=actor_id,
            position="QB",
            age=25,
            overall_truth=50.0,
            volatility_truth=0.5,
            injury_susceptibility_truth=0.5,
            hidden_dev_curve=0.5,
            traits={code: 55.0 for code in required_trait_codes()},
        )
    target_actor = next(iter(player_lookup))
    player_lookup[target_actor].traits.pop("durability", None)
    state = GameSessionState(
        game_id="G_INJ",
        season=2026,
        week=1,
        home_team_id="A",
        away_team_id="B",
        quarter=1,
        clock_seconds=600,
        home_score=0,
        away_score=0,
        possession_team_id="A",
        down=1,
        distance=10,
        yard_line=35,
        drive_index=1,
        timeouts_home=3,
        timeouts_away=3,
    )
    with pytest.raises(EngineIntegrityError) as ex:
        session_engine._update_injuries(state, resolution, player_lookup)
    assert ex.value.artifact.error_code == "MISSING_REQUIRED_TRAIT_AT_RUNTIME"
