from __future__ import annotations

from datetime import UTC, datetime

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
    scout_quality = _avg([s.evaluation for s in scouts], default=0.5)
    coach_quality = _avg([c.evaluation for c in coaches], default=0.5)
    med_quality = _avg([m.evaluation for m in medical], default=0.5)

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
    base_noise = (1.0 - quality) * 22.0 * noise_mult
    sample = (rand.rand() * 2.0 - 1.0) * base_noise
    estimate = max(1.0, min(99.0, truth + sample))
    spread = max(2.0, base_noise)
    confidence = max(0.1, min(0.99, quality))
    return PerceivedMetric(
        label=label,
        estimate=estimate,
        lower_bound=max(1.0, estimate - spread),
        upper_bound=min(99.0, estimate + spread),
        confidence=confidence,
    )


def _avg(values: list[float], default: float) -> float:
    return sum(values) / len(values) if values else default
