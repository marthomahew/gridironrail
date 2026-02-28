from __future__ import annotations

import hashlib
from typing import Iterable

from grs.contracts import TraitCatalogEntry, ValidationIssue

TRAIT_DEFS: list[tuple[str, str, str]] = [
    ("height", "physical", "Height profile"),
    ("weight", "physical", "Weight profile"),
    ("length", "physical", "Limb length"),
    ("strength", "physical", "Functional strength"),
    ("burst", "physical", "Burst ability"),
    ("top_speed", "physical", "Maximum speed"),
    ("acceleration", "physical", "Acceleration"),
    ("agility", "physical", "Agility"),
    ("flexibility", "physical", "Flexibility"),
    ("balance", "physical", "Balance"),
    ("stamina", "physical", "Stamina"),
    ("recovery", "physical", "Recovery rate"),
    ("short_area_change", "movement", "Short-area change"),
    ("long_arc_bend", "movement", "Long-arc bend"),
    ("footwork_precision", "movement", "Footwork precision"),
    ("body_control", "movement", "Body control"),
    ("redirection", "movement", "Redirection"),
    ("anchor_stability", "movement", "Anchor stability"),
    ("leverage_control", "movement", "Leverage control"),
    ("momentum_management", "movement", "Momentum management"),
    ("stop_start_efficiency", "movement", "Stop-start efficiency"),
    ("pursuit_angles", "movement", "Pursuit angle quality"),
    ("awareness", "mental", "Awareness"),
    ("processing_speed", "mental", "Processing speed"),
    ("recognition", "mental", "Recognition"),
    ("anticipation", "mental", "Anticipation"),
    ("discipline", "mental", "Discipline"),
    ("decision_quality", "mental", "Decision quality"),
    ("risk_tolerance", "mental", "Risk tolerance"),
    ("communication", "mental", "Communication"),
    ("composure", "mental", "Composure"),
    ("adaptability", "mental", "Adaptability"),
    ("memory", "mental", "Memory"),
    ("consistency", "mental", "Consistency"),
    ("short_accuracy", "qb", "Short throw accuracy"),
    ("intermediate_accuracy", "qb", "Intermediate throw accuracy"),
    ("deep_accuracy", "qb", "Deep throw accuracy"),
    ("throw_power", "qb", "Throw power"),
    ("release_quickness", "qb", "Release quickness"),
    ("platform_stability", "qb", "Platform stability"),
    ("pocket_sense", "qb", "Pocket sense"),
    ("progression_depth", "qb", "Progression depth"),
    ("pressure_response", "qb", "Pressure response"),
    ("timing_precision", "qb", "Timing precision"),
    ("hands", "ball", "Hands"),
    ("catch_radius", "ball", "Catch radius"),
    ("contested_catch", "ball", "Contested catch"),
    ("ball_tracking", "ball", "Ball tracking"),
    ("route_fidelity", "ball", "Route fidelity"),
    ("release_quality", "ball", "Release quality"),
    ("yac_vision", "ball", "YAC vision"),
    ("ball_security", "ball", "Ball security"),
    ("pass_set", "blocking", "Pass set quality"),
    ("hand_placement", "blocking", "Hand placement"),
    ("mirror_skill", "blocking", "Mirror skill"),
    ("anchor", "blocking", "Anchor"),
    ("recovery_blocking", "blocking", "Recovery blocking"),
    ("run_block_drive", "blocking", "Run-block drive"),
    ("run_block_positioning", "blocking", "Run-block positioning"),
    ("combo_coordination", "blocking", "Combo coordination"),
    ("second_level_targeting", "blocking", "Second-level targeting"),
    ("hold_risk_control", "blocking", "Hold risk control"),
    ("get_off", "front7", "Get-off"),
    ("hand_fighting", "front7", "Hand fighting"),
    ("rush_plan_diversity", "front7", "Rush plan diversity"),
    ("edge_contain", "front7", "Edge contain"),
    ("block_shed", "front7", "Block shed"),
    ("gap_integrity", "front7", "Gap integrity"),
    ("stack_shed", "front7", "Stack shed"),
    ("closing_speed", "front7", "Closing speed"),
    ("tackle_power", "front7", "Tackle power"),
    ("tackle_form", "front7", "Tackle form"),
    ("man_footwork", "coverage", "Man footwork"),
    ("zone_spacing", "coverage", "Zone spacing"),
    ("route_match_skill", "coverage", "Route match skill"),
    ("leverage_management", "coverage", "Leverage management"),
    ("transition_speed", "coverage", "Transition speed"),
    ("ball_skills_defense", "coverage", "Ball skills defense"),
    ("press_technique", "coverage", "Press technique"),
    ("recovery_speed", "coverage", "Recovery speed"),
    ("communication_secondary", "coverage", "Secondary communication"),
    ("dpi_risk_control", "coverage", "DPI risk control"),
    ("soft_tissue_risk", "availability", "Soft tissue risk"),
    ("contact_injury_risk", "availability", "Contact injury risk"),
    ("re_injury_risk", "availability", "Re-injury risk"),
    ("durability", "availability", "Durability"),
    ("pain_tolerance", "availability", "Pain tolerance"),
    ("load_tolerance", "availability", "Load tolerance"),
    ("recovery_rate", "availability", "Recovery rate"),
    ("volatility_profile", "availability", "Volatility profile"),
]

assert len(TRAIT_DEFS) == 90


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
            version=version,
        )
        for code, category, desc in TRAIT_DEFS
    ]


def required_trait_codes() -> list[str]:
    return [c for c, _, _ in TRAIT_DEFS]


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

    return max(1.0, min(99.0, round(base, 3)))
