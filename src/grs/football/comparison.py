from __future__ import annotations

from dataclasses import dataclass

from grs.contracts import ActorRef, InGameState, ParameterizedIntent, PlayType, SimMode, Situation, SnapContextPackage
from grs.core import seeded_random
from grs.football.resolver import FootballResolver
from grs.football.traits import required_trait_codes


@dataclass(slots=True)
class ResolverComparisonResult:
    sample_count: int
    trait_weighted_mean_yards: float
    legacy_mean_yards: float
    trait_weighted_turnover_rate: float
    legacy_turnover_rate: float
    trait_weighted_scores: int
    legacy_scores: int


def compare_trait_weighted_to_legacy(
    *,
    play_type: PlayType,
    sample_count: int = 200,
    seed: int = 500,
) -> ResolverComparisonResult:
    weighted = FootballResolver(
        random_source=seeded_random(seed),
        trait_weighted_enabled=True,
    )
    legacy = FootballResolver(
        random_source=seeded_random(seed),
        trait_weighted_enabled=False,
    )
    weighted_total_yards = 0
    legacy_total_yards = 0
    weighted_turnovers = 0
    legacy_turnovers = 0
    weighted_scores = 0
    legacy_scores = 0

    for idx in range(sample_count):
        scp = _build_sample_context(play_id=f"{play_type.value}_{idx:03d}", play_type=play_type)
        weighted_res = weighted.resolve_snap(scp)
        legacy_res = legacy.resolve_snap(scp)
        weighted_total_yards += weighted_res.play_result.yards
        legacy_total_yards += legacy_res.play_result.yards
        weighted_turnovers += int(weighted_res.play_result.turnover)
        legacy_turnovers += int(legacy_res.play_result.turnover)
        weighted_scores += int(weighted_res.play_result.score_event is not None)
        legacy_scores += int(legacy_res.play_result.score_event is not None)

    return ResolverComparisonResult(
        sample_count=sample_count,
        trait_weighted_mean_yards=weighted_total_yards / sample_count,
        legacy_mean_yards=legacy_total_yards / sample_count,
        trait_weighted_turnover_rate=weighted_turnovers / sample_count,
        legacy_turnover_rate=legacy_turnovers / sample_count,
        trait_weighted_scores=weighted_scores,
        legacy_scores=legacy_scores,
    )


def _build_sample_context(play_id: str, play_type: PlayType) -> SnapContextPackage:
    participants: list[ActorRef] = []
    offense_roles = ["QB", "RB", "WR", "WR", "WR", "TE", "OL", "OL", "OL", "OL", "OL"]
    defense_roles = ["DE", "DT", "DT", "DE", "LB", "LB", "LB", "CB", "CB", "S", "S"]
    for idx, role in enumerate(offense_roles):
        participants.append(ActorRef(actor_id=f"A_{idx}", team_id="A", role=role))
    for idx, role in enumerate(defense_roles):
        participants.append(ActorRef(actor_id=f"B_{idx}", team_id="B", role=role))
    in_game_states = {
        p.actor_id: InGameState(fatigue=0.22, acute_wear=0.18, confidence_tilt=0.0, discipline_risk=0.4)
        for p in participants
    }
    trait_vectors = {p.actor_id: {code: 60.0 for code in required_trait_codes()} for p in participants}
    # Provide stronger offense separation/decision traits to create sensitivity signal.
    for p in participants:
        if p.team_id == "A":
            trait_vectors[p.actor_id]["separation_window"] = 70.0 if "separation_window" in trait_vectors[p.actor_id] else trait_vectors[p.actor_id]["route_fidelity"]
            trait_vectors[p.actor_id]["route_fidelity"] = 70.0
            trait_vectors[p.actor_id]["decision_quality"] = 72.0
            trait_vectors[p.actor_id]["throw_power"] = 68.0
        else:
            trait_vectors[p.actor_id]["route_match_skill"] = 58.0
            trait_vectors[p.actor_id]["man_footwork"] = 57.0

    if play_type == PlayType.RUN:
        offense_concept = "inside_zone"
    elif play_type in {PlayType.FIELD_GOAL, PlayType.EXTRA_POINT}:
        offense_concept = "field_goal_unit"
    elif play_type == PlayType.PUNT:
        offense_concept = "punt_safe"
    elif play_type == PlayType.KICKOFF:
        offense_concept = "kickoff_sky"
    elif play_type == PlayType.TWO_POINT:
        offense_concept = "two_point_mesh"
    else:
        offense_concept = "spacing"

    return SnapContextPackage(
        game_id="CMP",
        play_id=play_id,
        mode=SimMode.OFFSCREEN,
        situation=Situation(
            quarter=2,
            clock_seconds=640,
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
            personnel="11" if play_type in {PlayType.RUN, PlayType.PASS} else play_type.value if play_type in {PlayType.PUNT, PlayType.KICKOFF, PlayType.FIELD_GOAL, PlayType.EXTRA_POINT, PlayType.TWO_POINT} else "11",
            formation="gun_trips" if play_type == PlayType.PASS else "singleback",
            offensive_concept=offense_concept,
            defensive_concept="cover3_match" if play_type in {PlayType.RUN, PlayType.PASS, PlayType.TWO_POINT} else "punt_return_safe",
            play_type=play_type,
        ),
    )

