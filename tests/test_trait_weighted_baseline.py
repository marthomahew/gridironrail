from __future__ import annotations

from grs.contracts import (
    ActorRef,
    InGameState,
    ParameterizedIntent,
    PlayType,
    SimMode,
    Situation,
    SnapContextPackage,
)
from grs.core import seeded_random
from grs.football import FootballResolver, ResourceResolver
from grs.football.contest import required_influence_families
from grs.football.traits import required_trait_codes


def _build_context(
    *,
    play_id: str,
    play_type: PlayType,
    offense_trait_overrides: dict[str, float] | None = None,
    defense_trait_overrides: dict[str, float] | None = None,
) -> SnapContextPackage:
    offense_roles, defense_roles = _roles_for_play_type(play_type)
    participants: list[ActorRef] = []
    for idx, role in enumerate(offense_roles):
        participants.append(ActorRef(actor_id=f"A_{idx}", team_id="A", role=role))
    for idx, role in enumerate(defense_roles):
        participants.append(ActorRef(actor_id=f"B_{idx}", team_id="B", role=role))

    in_game_states = {
        actor.actor_id: InGameState(
            fatigue=0.2,
            acute_wear=0.15,
            confidence_tilt=0.0,
            discipline_risk=0.45,
        )
        for actor in participants
    }

    trait_vectors = {actor.actor_id: {code: 55.0 for code in required_trait_codes()} for actor in participants}
    for actor in participants:
        if actor.team_id == "A" and offense_trait_overrides:
            trait_vectors[actor.actor_id].update(offense_trait_overrides)
        if actor.team_id == "B" and defense_trait_overrides:
            trait_vectors[actor.actor_id].update(defense_trait_overrides)

    personnel, formation, offense_concept, defense_concept = _intent_values(play_type)
    return SnapContextPackage(
        game_id="G_TEST",
        play_id=play_id,
        mode=SimMode.SIM,
        situation=Situation(
            quarter=2,
            clock_seconds=700,
            down=2,
            distance=7,
            yard_line=55,
            possession_team_id="A",
            score_diff=0,
            timeouts_offense=3,
            timeouts_defense=3,
        ),
        participants=participants,
        in_game_states=in_game_states,
        trait_vectors=trait_vectors,
        intent=ParameterizedIntent(
            personnel=personnel,
            formation=formation,
            offensive_concept=offense_concept,
            defensive_concept=defense_concept,
            tempo="normal",
            aggression="balanced",
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


def _intent_values(play_type: PlayType) -> tuple[str, str, str, str]:
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


def _mean_yards(
    *,
    play_type: PlayType,
    offense_trait_overrides: dict[str, float] | None = None,
    defense_trait_overrides: dict[str, float] | None = None,
    sample_count: int = 80,
) -> tuple[float, list[str]]:
    resolver = FootballResolver(random_source=seeded_random(900), resource_resolver=ResourceResolver())
    total_yards = 0
    terminals: list[str] = []
    for idx in range(sample_count):
        scp = _build_context(
            play_id=f"{play_type.value}_{idx:03d}",
            play_type=play_type,
            offense_trait_overrides=offense_trait_overrides,
            defense_trait_overrides=defense_trait_overrides,
        )
        res = resolver.resolve_snap(scp)
        total_yards += res.play_result.yards
        terminals.append(res.causality_chain.terminal_event)
    return total_yards / sample_count, terminals


def _score_event_count(
    *,
    play_type: PlayType,
    offense_trait_overrides: dict[str, float] | None = None,
    sample_count: int = 80,
    expected_score_event: str,
) -> int:
    resolver = FootballResolver(random_source=seeded_random(901), resource_resolver=ResourceResolver())
    hits = 0
    for idx in range(sample_count):
        scp = _build_context(
            play_id=f"{play_type.value}_score_{idx:03d}",
            play_type=play_type,
            offense_trait_overrides=offense_trait_overrides,
        )
        res = resolver.resolve_snap(scp)
        if res.play_result.score_event == expected_score_event:
            hits += 1
    return hits


def _turnover_rate(play_type: PlayType, offense_trait_overrides: dict[str, float], sample_count: int = 80) -> float:
    resolver = FootballResolver(random_source=seeded_random(902), resource_resolver=ResourceResolver())
    turnovers = 0
    for idx in range(sample_count):
        scp = _build_context(
            play_id=f"{play_type.value}_to_{idx:03d}",
            play_type=play_type,
            offense_trait_overrides=offense_trait_overrides,
        )
        res = resolver.resolve_snap(scp)
        turnovers += int(res.play_result.turnover)
    return turnovers / sample_count


def test_trait_sensitivity_run_outcomes_shift_with_traits():
    low_offense = {
        "run_block_drive": 25.0,
        "run_block_positioning": 25.0,
        "combo_coordination": 25.0,
        "leverage_control": 25.0,
        "ball_security": 30.0,
    }
    high_offense = {
        "run_block_drive": 90.0,
        "run_block_positioning": 90.0,
        "combo_coordination": 90.0,
        "leverage_control": 90.0,
        "ball_security": 90.0,
    }
    low_mean, _ = _mean_yards(play_type=PlayType.RUN, offense_trait_overrides=low_offense)
    high_mean, _ = _mean_yards(play_type=PlayType.RUN, offense_trait_overrides=high_offense)
    assert high_mean > low_mean


def test_trait_sensitivity_pass_outcomes_shift_with_traits():
    low_offense = {
        "decision_quality": 25.0,
        "processing_speed": 25.0,
        "timing_precision": 25.0,
        "route_fidelity": 25.0,
        "release_quality": 25.0,
        "hands": 25.0,
        "ball_security": 25.0,
    }
    high_offense = {
        "decision_quality": 90.0,
        "processing_speed": 90.0,
        "timing_precision": 90.0,
        "route_fidelity": 90.0,
        "release_quality": 90.0,
        "hands": 90.0,
        "ball_security": 90.0,
    }
    low_mean, _ = _mean_yards(play_type=PlayType.PASS, offense_trait_overrides=low_offense)
    high_mean, _ = _mean_yards(play_type=PlayType.PASS, offense_trait_overrides=high_offense)
    low_turnover = _turnover_rate(PlayType.PASS, low_offense)
    high_turnover = _turnover_rate(PlayType.PASS, high_offense)
    assert high_mean > low_mean
    assert high_turnover < low_turnover


def test_trait_sensitivity_special_teams_field_goal_make_rate():
    low_kick = {
        "throw_power": 20.0,
        "timing_precision": 20.0,
        "composure": 20.0,
        "balance": 20.0,
    }
    high_kick = {
        "throw_power": 92.0,
        "timing_precision": 92.0,
        "composure": 92.0,
        "balance": 92.0,
    }
    low_makes = _score_event_count(
        play_type=PlayType.FIELD_GOAL,
        offense_trait_overrides=low_kick,
        expected_score_event="FG_GOOD",
    )
    high_makes = _score_event_count(
        play_type=PlayType.FIELD_GOAL,
        offense_trait_overrides=high_kick,
        expected_score_event="FG_GOOD",
    )
    assert high_makes > low_makes


def test_special_teams_and_two_point_use_contest_based_resolution():
    resolver = FootballResolver(random_source=seeded_random(903), resource_resolver=ResourceResolver())
    for play_type in [
        PlayType.PUNT,
        PlayType.KICKOFF,
        PlayType.FIELD_GOAL,
        PlayType.EXTRA_POINT,
        PlayType.TWO_POINT,
    ]:
        res = resolver.resolve_snap(
            _build_context(
                play_id=f"{play_type.value}_contest",
                play_type=play_type,
            )
        )
        families = {contest.family for contest in res.artifact_bundle.contest_resolutions}
        expected = required_influence_families(play_type.value)
        assert expected.issubset(families)
        assert any(node.source_type == "contest" for node in res.causality_chain.nodes)


def test_causality_chains_for_non_turnover_events_remain_accountable():
    resolver = FootballResolver(random_source=seeded_random(904), resource_resolver=ResourceResolver())
    observed_non_turnover = 0
    for play_type in [PlayType.RUN, PlayType.PASS, PlayType.PUNT, PlayType.KICKOFF, PlayType.FIELD_GOAL, PlayType.EXTRA_POINT]:
        for idx in range(20):
            res = resolver.resolve_snap(
                _build_context(
                    play_id=f"{play_type.value}_causal_{idx:03d}",
                    play_type=play_type,
                )
            )
            terminal = res.causality_chain.terminal_event
            if terminal in {"interception", "fumble"}:
                continue
            observed_non_turnover += 1
            assert res.causality_chain.nodes
            assert abs(sum(node.weight for node in res.causality_chain.nodes) - 1.0) < 0.001
            assert any(node.source_type == "contest" for node in res.causality_chain.nodes)
            assert any(node.source_type == "rep" for node in res.causality_chain.nodes)
    assert observed_non_turnover > 0
