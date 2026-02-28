from __future__ import annotations

import hashlib
from typing import Iterable

from grs.contracts import TraitCatalogEntry, TraitStatus, ValidationIssue

CORE_TRAIT_DEFS: list[tuple[str, str, str]] = [
    ("strength", "athletic", "Functional strength"),
    ("burst", "athletic", "Explosive first-step burst"),
    ("top_speed", "athletic", "Maximum speed"),
    ("acceleration", "athletic", "Acceleration profile"),
    ("agility", "athletic", "Change-of-direction agility"),
    ("balance", "athletic", "Contact and body balance"),
    ("stamina", "athletic", "Sustained effort stamina"),
    ("body_control", "movement", "Body control through contact"),
    ("leverage_control", "movement", "Pad-level and leverage control"),
    ("momentum_management", "movement", "Momentum control in transition"),
    ("pursuit_angles", "movement", "Pursuit and tracking angles"),
    ("awareness", "cognition", "Field awareness"),
    ("processing_speed", "cognition", "Mental processing speed"),
    ("recognition", "cognition", "Pattern and key recognition"),
    ("anticipation", "cognition", "Anticipatory reaction"),
    ("discipline", "cognition", "Discipline and assignment fidelity"),
    ("decision_quality", "cognition", "Decision quality under pressure"),
    ("communication", "cognition", "Communication quality"),
    ("communication_secondary", "cognition", "Coverage communication"),
    ("composure", "cognition", "Composure under stress"),
    ("short_accuracy", "qb", "Short throw placement"),
    ("intermediate_accuracy", "qb", "Intermediate throw placement"),
    ("deep_accuracy", "qb", "Deep throw placement"),
    ("throw_power", "qb", "Throw velocity/power"),
    ("throw_touch", "qb", "Trajectory and touch control"),
    ("release_quickness", "qb", "Release speed"),
    ("pocket_sense", "qb", "Pocket movement and pressure sense"),
    ("timing_precision", "qb", "Timing precision"),
    ("play_action_craft", "qb", "Play-action sell and sequencing"),
    ("blitz_identification", "qb", "Pre/post-snap blitz identification"),
    ("cadence_control", "qb", "Cadence and count manipulation"),
    ("snap_operation", "qb", "Snap exchange and operation integrity"),
    ("hands", "ball", "Hands reliability"),
    ("catch_radius", "ball", "Catch radius"),
    ("contested_catch", "ball", "Contested catch skill"),
    ("ball_tracking", "ball", "Ball tracking"),
    ("route_fidelity", "ball", "Route detail fidelity"),
    ("release_quality", "ball", "Release quality"),
    ("yac_vision", "ball", "Vision after catch/contact"),
    ("ball_security", "ball", "Ball security"),
    ("pass_set", "blocking", "Pass protection set quality"),
    ("hand_placement", "blocking", "Hand placement"),
    ("mirror_skill", "blocking", "Pass-pro mirror skill"),
    ("anchor", "blocking", "Anchor against power"),
    ("recovery_blocking", "blocking", "Recovery ability when initially beaten"),
    ("run_block_drive", "blocking", "Run block drive"),
    ("run_block_positioning", "blocking", "Run block positioning"),
    ("combo_coordination", "blocking", "Combo block coordination"),
    ("get_off", "front7", "Pass rush get-off"),
    ("hand_fighting", "front7", "Hand usage in trench contests"),
    ("rush_plan_diversity", "front7", "Rush plan diversity"),
    ("edge_contain", "front7", "Edge contain discipline"),
    ("block_shed", "front7", "Block shedding"),
    ("gap_integrity", "front7", "Gap integrity"),
    ("stack_shed", "front7", "Stack and shed"),
    ("closing_speed", "front7", "Closing speed"),
    ("tackle_power", "front7", "Tackle power"),
    ("tackle_form", "front7", "Tackle form"),
    ("man_footwork", "coverage", "Man coverage footwork"),
    ("route_match_skill", "coverage", "Route-matching skill"),
    ("leverage_management", "coverage", "Leverage management"),
    ("transition_speed", "coverage", "Transition speed"),
    ("ball_skills_defense", "coverage", "Defensive ball skills"),
    ("press_technique", "coverage", "Press technique"),
    ("jam_strength", "coverage", "Jam force and redirection strength"),
    ("recovery_speed", "coverage", "Recovery speed in coverage"),
    ("dpi_risk_control", "coverage", "DPI risk control"),
    ("kick_power", "special_teams", "Kick distance power"),
    ("kick_accuracy", "special_teams", "Kick directional accuracy"),
    ("hang_time_control", "special_teams", "Kick hang-time control"),
    ("soft_tissue_risk", "availability", "Soft tissue injury risk"),
    ("contact_injury_risk", "availability", "Contact injury risk"),
    ("re_injury_risk", "availability", "Re-injury risk"),
    ("durability", "availability", "Durability"),
    ("pain_tolerance", "availability", "Pain tolerance"),
    ("recovery_rate", "availability", "Recovery rate"),
    ("volatility_profile", "availability", "Performance volatility profile"),
]

_codes = [code for code, _, _ in CORE_TRAIT_DEFS]
if len(_codes) != len(set(_codes)):
    raise ValueError("duplicate trait_code detected in CORE_TRAIT_DEFS")


def canonical_trait_catalog(version: str = "1.0") -> list[TraitCatalogEntry]:
    return [
        TraitCatalogEntry(
            trait_code=code,
            dtype="float",
            min_value=1.0,
            max_value=99.0,
            required=True,
            description=desc,
            category=category,
            status=_trait_status(code),
            version=version,
        )
        for code, category, desc in CORE_TRAIT_DEFS
    ]


def required_trait_codes() -> list[str]:
    return [c for c, _, _ in CORE_TRAIT_DEFS]


def generate_player_traits(
    player_id: str,
    position: str,
    overall_truth: float,
    volatility_truth: float,
    injury_susceptibility_truth: float,
) -> dict[str, float]:
    traits: dict[str, float] = {}
    for code in required_trait_codes():
        traits[code] = _derive_trait_value(
            player_id=player_id,
            position=position,
            code=code,
            overall_truth=overall_truth,
            volatility_truth=volatility_truth,
            injury_susceptibility_truth=injury_susceptibility_truth,
        )
    return traits


def validate_traits(player_id: str, traits: dict[str, float], catalog: Iterable[TraitCatalogEntry]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    by_code = {c.trait_code: c for c in catalog}

    for code, entry in by_code.items():
        if entry.required and code not in traits:
            issues.append(
                ValidationIssue(
                    code="MISSING_TRAIT",
                    severity="blocking",
                    field_path=f"traits.{code}",
                    entity_id=player_id,
                    message="required trait is missing",
                )
            )
            continue
        if code not in traits:
            continue
        val = traits[code]
        if not isinstance(val, (int, float)):
            issues.append(
                ValidationIssue(
                    code="INVALID_TRAIT_TYPE",
                    severity="blocking",
                    field_path=f"traits.{code}",
                    entity_id=player_id,
                    message="trait must be numeric",
                )
            )
            continue
        if val < entry.min_value or val > entry.max_value:
            issues.append(
                ValidationIssue(
                    code="TRAIT_OUT_OF_RANGE",
                    severity="blocking",
                    field_path=f"traits.{code}",
                    entity_id=player_id,
                    message=f"trait out of range [{entry.min_value}, {entry.max_value}]",
                )
            )
    return issues


def _derive_trait_value(
    player_id: str,
    position: str,
    code: str,
    overall_truth: float,
    volatility_truth: float,
    injury_susceptibility_truth: float,
) -> float:
    key = f"{player_id}:{position}:{code}".encode("ascii", "ignore")
    digest = hashlib.sha256(key).hexdigest()
    jitter = (int(digest[:8], 16) / 0xFFFFFFFF) * 10.0 - 5.0

    base = overall_truth + jitter
    if code.endswith("risk"):
        base = (100.0 - (injury_susceptibility_truth * 100.0)) + jitter
    if code == "volatility_profile":
        base = 100.0 - (volatility_truth * 100.0) + jitter

    value = round(base, 3)
    if value < 1.0 or value > 99.0:
        raise ValueError(f"derived trait value out of domain for '{code}' on player '{player_id}'")
    return value


def _trait_status(code: str) -> TraitStatus:
    reserved = {
        "play_action_craft",
        "cadence_control",
        "snap_operation",
        "jam_strength",
        "volatility_profile",
    }
    return TraitStatus.RESERVED_PHASAL if code in reserved else TraitStatus.CORE_NOW
