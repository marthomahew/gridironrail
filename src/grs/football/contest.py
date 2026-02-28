from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping

from grs.contracts import (
    ContestInput,
    ContestOutput,
    InGameState,
    OutcomeResolutionProfile,
    PlayFamilyInfluenceProfile,
    RandomSource,
    Situation,
)


@dataclass(slots=True)
class GroupBreakdown:
    group_score: float
    actor_contributions: dict[str, float]
    trait_contributions: dict[str, float]


class ContestEvaluator:
    def evaluate(
        self,
        contest_input: ContestInput,
        *,
        trait_vectors: dict[str, dict[str, float]],
        random_source: RandomSource,
    ) -> ContestOutput:
        profile = contest_input.influence_profile
        offense_breakdown = self._group_breakdown(
            actor_ids=contest_input.offense_actor_ids,
            trait_vectors=trait_vectors,
            trait_weights=profile.offense_weights,
            side="offense",
            fatigue_sensitivity=profile.fatigue_sensitivity,
            wear_sensitivity=profile.wear_sensitivity,
            in_game_states=contest_input.in_game_states,
        )
        defense_breakdown = self._group_breakdown(
            actor_ids=contest_input.defense_actor_ids,
            trait_vectors=trait_vectors,
            trait_weights=profile.defense_weights,
            side="defense",
            fatigue_sensitivity=profile.fatigue_sensitivity,
            wear_sensitivity=profile.wear_sensitivity,
            in_game_states=contest_input.in_game_states,
        )

        context_adj = self._context_adjustment(profile.context_modifiers, contest_input.situation)
        raw = (offense_breakdown.group_score - defense_breakdown.group_score) + context_adj
        score = 1.0 / (1.0 + math.exp(-raw * 3.0))

        actor_contrib: dict[str, float] = {}
        actor_contrib.update(offense_breakdown.actor_contributions)
        actor_contrib.update(defense_breakdown.actor_contributions)
        trait_contrib: dict[str, float] = {}
        for k, v in offense_breakdown.trait_contributions.items():
            trait_contrib[k] = trait_contrib.get(k, 0.0) + v
        for k, v in defense_breakdown.trait_contributions.items():
            trait_contrib[k] = trait_contrib.get(k, 0.0) + v

        volatility_values = []
        for actor_id in contest_input.offense_actor_ids + contest_input.defense_actor_ids:
            volatility_values.append(_normalized_trait(trait_vectors, actor_id, "volatility_profile"))
        variance_hint = sum(volatility_values) / len(volatility_values)

        return ContestOutput(
            contest_id=contest_input.contest_id,
            play_id=contest_input.play_id,
            play_type=contest_input.play_type,
            family=contest_input.family,
            score=round(score, 6),
            offense_score=round(offense_breakdown.group_score, 6),
            defense_score=round(defense_breakdown.group_score, 6),
            actor_contributions={k: round(v, 6) for k, v in actor_contrib.items()},
            trait_contributions={k: round(v, 6) for k, v in trait_contrib.items()},
            variance_hint=round(variance_hint, 6),
            evidence_handles=[
                f"contest:{contest_input.contest_id}",
                f"family:{contest_input.family}",
                f"play_type:{contest_input.play_type}",
            ],
        )

    def _group_breakdown(
        self,
        *,
        actor_ids: list[str],
        trait_vectors: dict[str, dict[str, float]],
        trait_weights: dict[str, float],
        side: str,
        fatigue_sensitivity: float,
        wear_sensitivity: float,
        in_game_states: Mapping[str, InGameState],
    ) -> GroupBreakdown:
        if not actor_ids:
            raise ValueError(f"{side} actor group is empty")
        if not trait_weights:
            raise ValueError(f"{side} trait weights are empty")
        total_weight = sum(trait_weights.values())
        if total_weight <= 0:
            raise ValueError(f"{side} trait weights must sum to a positive value")

        actor_contrib: dict[str, float] = {}
        trait_contrib: dict[str, float] = {trait: 0.0 for trait in trait_weights}
        actor_scores: list[float] = []
        for actor_id in actor_ids:
            trait_vector = trait_vectors.get(actor_id)
            if trait_vector is None:
                raise ValueError(f"missing trait vector for actor '{actor_id}'")
            weighted = 0.0
            for trait_code, weight in trait_weights.items():
                if trait_code not in trait_vector:
                    raise ValueError(f"actor '{actor_id}' missing required trait '{trait_code}'")
                value = (trait_vector[trait_code] - 1.0) / 98.0
                weighted += value * weight
                trait_contrib[trait_code] += value * weight
            actor_score = weighted / total_weight
            actor_scores.append(actor_score)
            actor_contrib[actor_id] = actor_score if side == "offense" else -actor_score

        group_score = sum(actor_scores) / len(actor_scores)

        fatigue = 0.0
        wear = 0.0
        count = 0
        for actor_id in actor_ids:
            state = in_game_states.get(actor_id)
            if state is None:
                raise ValueError(f"missing in_game_state for actor '{actor_id}'")
            fatigue += float(getattr(state, "fatigue"))
            wear += float(getattr(state, "acute_wear"))
            count += 1
        fatigue_avg = fatigue / count
        wear_avg = wear / count
        modifier = 1.0 - (fatigue_avg * fatigue_sensitivity) - (wear_avg * wear_sensitivity)
        adjusted_group = group_score * modifier

        # Normalize trait contributions to signed directional influence for explainability.
        trait_contrib = {k: (v / len(actor_ids)) / total_weight for k, v in trait_contrib.items()}
        if side == "defense":
            trait_contrib = {k: -v for k, v in trait_contrib.items()}

        return GroupBreakdown(
            group_score=adjusted_group,
            actor_contributions=actor_contrib,
            trait_contributions=trait_contrib,
        )

    def _context_adjustment(self, modifiers: dict[str, float], situation: Situation) -> float:
        adjustment = 0.0
        for key, value in modifiers.items():
            if key == "short_yardage_bonus" and int(getattr(situation, "distance")) <= 2:
                adjustment += value
            elif key == "long_yardage_bonus" and int(getattr(situation, "distance")) >= 8:
                adjustment += value
            elif key == "redzone_bonus" and int(getattr(situation, "yard_line")) >= 80:
                adjustment += value
            elif key == "goal_line_bonus" and int(getattr(situation, "yard_line")) >= 95:
                adjustment += value
            elif key == "trailing_bonus" and int(getattr(situation, "score_diff")) < 0:
                adjustment += value
            elif key == "leading_bonus" and int(getattr(situation, "score_diff")) > 0:
                adjustment += value
            elif key in {
                "short_yardage_bonus",
                "long_yardage_bonus",
                "redzone_bonus",
                "goal_line_bonus",
                "trailing_bonus",
                "leading_bonus",
            }:
                continue
            else:
                raise ValueError(f"unknown context modifier '{key}'")
        return adjustment


def parse_influence_profiles(resource: dict[str, object]) -> tuple[dict[str, PlayFamilyInfluenceProfile], OutcomeResolutionProfile]:
    if "id" not in resource:
        raise ValueError("trait influence resource missing 'id'")
    play_type = str(resource["id"])
    if not play_type:
        raise ValueError("trait influence resource missing 'id'")
    families_raw = resource["families"] if "families" in resource else None
    if not isinstance(families_raw, list) or not families_raw:
        raise ValueError(f"play_type '{play_type}' must provide non-empty families list")
    by_family: dict[str, PlayFamilyInfluenceProfile] = {}
    for family_raw in families_raw:
        if not isinstance(family_raw, dict):
            raise ValueError(f"play_type '{play_type}' has non-object family entry")
        if "family" not in family_raw:
            raise ValueError(f"play_type '{play_type}' family entry missing 'family'")
        family = str(family_raw["family"])
        if not family:
            raise ValueError(f"play_type '{play_type}' family entry missing 'family'")
        required = {"offense_weights", "defense_weights", "fatigue_sensitivity", "wear_sensitivity"}
        missing = sorted(required - set(family_raw.keys()))
        if missing:
            raise ValueError(f"play_type '{play_type}' family '{family}' missing required fields {missing}")
        profile = PlayFamilyInfluenceProfile(
            play_type=play_type,
            family=family,
            offense_weights={str(k): float(v) for k, v in dict(family_raw["offense_weights"]).items()},
            defense_weights={str(k): float(v) for k, v in dict(family_raw["defense_weights"]).items()},
            fatigue_sensitivity=float(family_raw["fatigue_sensitivity"]),
            wear_sensitivity=float(family_raw["wear_sensitivity"]),
            context_modifiers={str(k): float(v) for k, v in dict(family_raw.get("context_modifiers", {})).items()},
        )
        by_family[family] = profile

    out_raw = resource["outcome_profile"] if "outcome_profile" in resource else None
    if not isinstance(out_raw, dict):
        raise ValueError(f"play_type '{play_type}' missing outcome_profile")
    required_outcome = {"noise_scale", "explosive_threshold", "turnover_scale", "score_scale", "clock_delta_min", "clock_delta_max"}
    missing_outcome = sorted(required_outcome - set(out_raw.keys()))
    if missing_outcome:
        raise ValueError(f"play_type '{play_type}' outcome_profile missing required fields {missing_outcome}")
    outcome = OutcomeResolutionProfile(
        play_type=play_type,
        noise_scale=float(out_raw["noise_scale"]),
        explosive_threshold=int(out_raw["explosive_threshold"]),
        turnover_scale=float(out_raw["turnover_scale"]),
        score_scale=float(out_raw["score_scale"]),
        clock_delta_min=int(out_raw["clock_delta_min"]),
        clock_delta_max=int(out_raw["clock_delta_max"]),
        context_modifiers={str(k): float(v) for k, v in dict(out_raw.get("context_modifiers", {})).items()},
    )
    return by_family, outcome


def required_influence_families(play_type: str) -> set[str]:
    mapping = {
        "run": {"lane_creation", "fit_integrity", "tackle_finish", "ball_security"},
        "pass": {
            "pressure_emergence",
            "separation_window",
            "decision_risk",
            "catch_point_contest",
            "yac_continuation",
            "ball_security",
        },
        "punt": {"kick_quality", "block_pressure", "coverage_lane_integrity", "return_vision_convergence"},
        "kickoff": {"kick_quality", "block_pressure", "coverage_lane_integrity", "return_vision_convergence"},
        "field_goal": {"kick_quality", "block_pressure"},
        "extra_point": {"kick_quality", "block_pressure"},
        "two_point": {
            "pressure_emergence",
            "separation_window",
            "decision_risk",
            "catch_point_contest",
            "tackle_finish",
            "ball_security",
        },
    }
    if play_type not in mapping:
        raise ValueError(f"unsupported play_type '{play_type}' for influence requirements")
    return mapping[play_type]


def _normalized_trait(trait_vectors: dict[str, dict[str, float]], actor_id: str, trait_code: str) -> float:
    traits = trait_vectors.get(actor_id)
    if traits is None:
        raise ValueError(f"missing trait vector for actor '{actor_id}'")
    if trait_code not in traits:
        raise ValueError(f"missing trait '{trait_code}' for actor '{actor_id}'")
    return (traits[trait_code] - 1.0) / 98.0
