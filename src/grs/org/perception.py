from __future__ import annotations

from datetime import UTC, datetime
import math

from grs.contracts import PerceivedMetric, PerceivedPlayerCard, RandomSource
from grs.org.entities import Player, StaffMember


def build_perceived_card(
    player: Player,
    team_id: str,
    scouts: list[StaffMember],
    coaches: list[StaffMember],
    medical: list[StaffMember],
    rand: RandomSource,
    scouting_noise_multiplier: float,
) -> PerceivedPlayerCard:
    if not scouts or not coaches or not medical:
        raise ValueError("perception pipeline requires scout, coach, and medical staff")
    scout_quality = _avg([s.evaluation for s in scouts])
    coach_quality = _avg([c.evaluation for c in coaches])
    med_quality = _avg([m.evaluation for m in medical])

    scout = _metric(
        "overall",
        truth=player.overall_truth,
        quality=scout_quality,
        rand=rand,
        noise_mult=scouting_noise_multiplier,
    )
    coach = _metric(
        "fit",
        truth=player.hidden_dev_curve,
        quality=coach_quality,
        rand=rand,
        noise_mult=scouting_noise_multiplier,
    )
    med = _metric(
        "injury_risk",
        truth=1.0 - player.injury_susceptibility_truth,
        quality=med_quality,
        rand=rand,
        noise_mult=scouting_noise_multiplier,
    )

    return PerceivedPlayerCard(
        player_id=player.player_id,
        team_id=team_id,
        scout_metrics=[scout],
        coach_metrics=[coach],
        medical_metrics=[med],
        updated_at=datetime.now(UTC),
    )


def _metric(label: str, truth: float, quality: float, rand: RandomSource, noise_mult: float) -> PerceivedMetric:
    if quality < 0.0 or quality > 1.0:
        raise ValueError(f"quality out of domain for perceived metric '{label}': {quality}")
    base_noise = (1.0 - quality) * 22.0 * noise_mult
    sample = (rand.rand() * 2.0 - 1.0) * base_noise
    estimate = _bounded_scale(truth + sample, center=50.0, slope=0.08)
    spread = 2.0 + (base_noise * 0.6)
    confidence = _unit_sigmoid((quality - 0.5) * 5.0)
    lower_room = estimate - 1.0
    upper_room = 99.0 - estimate
    half_span = _balanced_span(lower_room, upper_room, spread / 2.0)
    return PerceivedMetric(
        label=label,
        estimate=estimate,
        lower_bound=estimate - half_span,
        upper_bound=estimate + half_span,
        confidence=confidence,
    )


def _avg(values: list[float]) -> float:
    if not values:
        raise ValueError("cannot average empty list")
    return sum(values) / len(values)


def _unit_sigmoid(signal: float) -> float:
    return 1.0 / (1.0 + math.exp(-signal))


def _bounded_scale(signal: float, *, center: float, slope: float) -> float:
    return 1.0 + 98.0 * _unit_sigmoid((signal - center) * slope)


def _balanced_span(lower_room: float, upper_room: float, requested_half: float) -> float:
    if lower_room <= 0.0 or upper_room <= 0.0:
        raise ValueError("estimate room is invalid for confidence interval")
    harmonic_room = (2.0 * lower_room * upper_room) / (lower_room + upper_room)
    return harmonic_room * _unit_sigmoid(requested_half)
