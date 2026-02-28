from __future__ import annotations

from dataclasses import dataclass

from grs.contracts import RandomSource
from grs.football.models import SnapResolution
from grs.org.entities import Player


@dataclass(slots=True)
class InjuryCandidate:
    actor_id: str
    intensity: float


class InjuryEvaluationError(ValueError):
    pass


class InjuryEvaluator:
    REQUIRED_TRAITS = ("contact_injury_risk", "soft_tissue_risk", "durability")

    def evaluate(
        self,
        *,
        resolution: SnapResolution,
        player_lookup: dict[str, Player],
        random_source: RandomSource,
    ) -> dict[str, str]:
        candidates = self._collision_candidates(resolution)
        injuries: dict[str, str] = {}
        for actor_id, intensity in candidates.items():
            player = player_lookup.get(actor_id)
            if player is None:
                raise InjuryEvaluationError(f"injury candidate actor '{actor_id}' missing from player lookup")
            for trait_code in self.REQUIRED_TRAITS:
                if trait_code not in player.traits:
                    raise InjuryEvaluationError(f"actor '{actor_id}' missing required trait '{trait_code}'")
            contact = self._norm(player.traits["contact_injury_risk"])
            soft = self._norm(player.traits["soft_tissue_risk"])
            dur = self._norm(player.traits["durability"])
            # High contact/soft risk and low durability increase injury odds.
            injury_prob = (intensity * 0.5) * ((contact * 0.018) + (soft * 0.012) + ((1.0 - dur) * 0.015))
            roll = random_source.spawn(f"injury:{resolution.play_result.play_id}:{actor_id}").rand()
            if roll < injury_prob:
                severity_roll = random_source.spawn(f"injury:sev:{resolution.play_result.play_id}:{actor_id}").rand()
                injuries[actor_id] = "out" if severity_roll < 0.25 else "limited"
        return injuries

    def _collision_candidates(self, resolution: SnapResolution) -> dict[str, float]:
        candidates: dict[str, float] = {}
        contest_by_family = {c.family: c for c in resolution.artifact_bundle.contest_resolutions}
        for rep in resolution.rep_ledger:
            rep_type = rep.rep_type
            if rep_type == "multi_actor_exchange":
                intensity = 0.55
            else:
                contest = contest_by_family.get(rep_type)
                if contest is None:
                    continue
                # Mid/close contests imply higher collision probability.
                proximity = 1.0 - (abs(contest.score - 0.5) * 2.0)
                intensity = 0.1 + (0.9 * proximity)
            for actor in rep.actors:
                current = candidates.get(actor.actor_id, 0.0)
                if intensity > current:
                    candidates[actor.actor_id] = intensity
        return candidates

    def _norm(self, value: float) -> float:
        return (float(value) - 1.0) / 98.0
