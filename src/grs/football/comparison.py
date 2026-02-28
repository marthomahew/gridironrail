from __future__ import annotations

from dataclasses import dataclass

from grs.contracts import ActorRef, InGameState, ParameterizedIntent, PlayType, SimMode, Situation, SnapContextPackage
from grs.core import seeded_random
from grs.football.resources import ResourceResolver
from grs.football.resolver import FootballResolver
from grs.football.traits import required_trait_codes


@dataclass(slots=True)
class ResolverDistributionReport:
    sample_count: int
    mean_yards: float
    turnover_rate: float
    score_rate: float


def run_distribution_report(*, play_type: PlayType, sample_count: int = 200, seed: int = 500) -> ResolverDistributionReport:
    resolver = FootballResolver(random_source=seeded_random(seed), resource_resolver=ResourceResolver())
    total_yards = 0
    turnovers = 0
    scores = 0
    for idx in range(sample_count):
        result = resolver.resolve_snap(_build_context(f"{play_type.value}_{idx:03d}", play_type))
        total_yards += result.play_result.yards
        turnovers += int(result.play_result.turnover)
        scores += int(result.play_result.score_event is not None)
    return ResolverDistributionReport(
        sample_count=sample_count,
        mean_yards=total_yards / sample_count,
        turnover_rate=turnovers / sample_count,
        score_rate=scores / sample_count,
    )


def _build_context(play_id: str, play_type: PlayType) -> SnapContextPackage:
    participants: list[ActorRef] = []
    offense_roles = ["QB", "RB", "WR", "WR", "WR", "TE", "OL", "OL", "OL", "OL", "OL"]
    defense_roles = ["DE", "DT", "DT", "DE", "LB", "LB", "LB", "CB", "CB", "S", "S"]
    for idx, role in enumerate(offense_roles):
        participants.append(ActorRef(actor_id=f"A_{idx}", team_id="A", role=role))
    for idx, role in enumerate(defense_roles):
        participants.append(ActorRef(actor_id=f"B_{idx}", team_id="B", role=role))
    states = {p.actor_id: InGameState(fatigue=0.2, acute_wear=0.15, confidence_tilt=0.0, discipline_risk=0.4) for p in participants}
    traits = {p.actor_id: {code: 60.0 for code in required_trait_codes()} for p in participants}
    return SnapContextPackage(
        game_id="CMP",
        play_id=play_id,
        mode=SimMode.OFFSCREEN,
        situation=Situation(quarter=2, clock_seconds=640, down=2, distance=7, yard_line=55, possession_team_id="A", score_diff=0, timeouts_offense=3, timeouts_defense=3),
        participants=participants,
        in_game_states=states,
        trait_vectors=traits,
        intent=ParameterizedIntent(
            personnel="11" if play_type in {PlayType.RUN, PlayType.PASS} else play_type.value,
            formation="gun_trips" if play_type in {PlayType.PASS, PlayType.TWO_POINT} else "singleback" if play_type == PlayType.RUN else "field_goal_heavy" if play_type in {PlayType.FIELD_GOAL, PlayType.EXTRA_POINT} else "punt_spread" if play_type == PlayType.PUNT else "kickoff_standard",
            offensive_concept="spacing" if play_type == PlayType.PASS else "inside_zone" if play_type == PlayType.RUN else "punt_safe" if play_type == PlayType.PUNT else "kickoff_sky" if play_type == PlayType.KICKOFF else "field_goal_unit" if play_type in {PlayType.FIELD_GOAL, PlayType.EXTRA_POINT} else "two_point_mesh",
            defensive_concept="cover3_match" if play_type in {PlayType.RUN, PlayType.PASS, PlayType.TWO_POINT} else "punt_return_safe" if play_type == PlayType.PUNT else "kickoff_return" if play_type == PlayType.KICKOFF else "field_goal_block",
            play_type=play_type,
        ),
    )
