from __future__ import annotations

from grs.contracts import Difficulty, DifficultyProfile


def default_difficulty_profiles() -> dict[Difficulty, DifficultyProfile]:
    return {
        Difficulty.ROOKIE: DifficultyProfile(
            name=Difficulty.ROOKIE,
            volatility_multiplier=0.85,
            injury_rate_multiplier=0.8,
            injury_severity_multiplier=0.85,
            scouting_noise_multiplier=0.8,
            negotiation_friction_multiplier=0.85,
            ownership_pressure_multiplier=0.85,
            aging_variance_multiplier=0.85,
            upset_tail_multiplier=0.8,
        ),
        Difficulty.PRO: DifficultyProfile(
            name=Difficulty.PRO,
            volatility_multiplier=1.0,
            injury_rate_multiplier=1.0,
            injury_severity_multiplier=1.0,
            scouting_noise_multiplier=1.0,
            negotiation_friction_multiplier=1.0,
            ownership_pressure_multiplier=1.0,
            aging_variance_multiplier=1.0,
            upset_tail_multiplier=1.0,
        ),
        Difficulty.ALL_PRO: DifficultyProfile(
            name=Difficulty.ALL_PRO,
            volatility_multiplier=1.1,
            injury_rate_multiplier=1.1,
            injury_severity_multiplier=1.1,
            scouting_noise_multiplier=1.2,
            negotiation_friction_multiplier=1.15,
            ownership_pressure_multiplier=1.15,
            aging_variance_multiplier=1.15,
            upset_tail_multiplier=1.1,
        ),
        Difficulty.ALL_MADDEN: DifficultyProfile(
            name=Difficulty.ALL_MADDEN,
            volatility_multiplier=1.2,
            injury_rate_multiplier=1.2,
            injury_severity_multiplier=1.2,
            scouting_noise_multiplier=1.3,
            negotiation_friction_multiplier=1.3,
            ownership_pressure_multiplier=1.25,
            aging_variance_multiplier=1.25,
            upset_tail_multiplier=1.2,
        ),
    }


def validate_global_only_config(config: dict[str, object]) -> None:
    if "per_team_modifiers" in config:
        raise ValueError("difficulty configuration cannot contain per-team modifiers")

    profile = config.get("difficulty_profile")
    if isinstance(profile, DifficultyProfile):
        profile.validate()
