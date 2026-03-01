from __future__ import annotations

from grs.contracts import ActionRequest, ActionType
from grs.core import make_id


DEFAULT_SETUP_PAYLOAD = {
    "conference_count": 2,
    "divisions_per_conference": [2, 2],
    "teams_per_division": [[2, 2], [2, 2]],
    "roster_policy": {
        "players_per_team": 53,
        "active_gameday_min": 22,
        "active_gameday_max": 53,
    },
    "cap_policy": {
        "cap_amount": 255_000_000,
        "dead_money_penalty_multiplier": 1.0,
    },
    "schedule_policy": {
        "policy_id": "balanced_round_robin",
        "regular_season_weeks": 18,
    },
    "ruleset_id": "nfl_standard_v1",
    "difficulty_profile_id": "pro",
    "talent_profile_id": "balanced_mid",
    "league_identity_profile_id": "generated_custom_v1",
    "user_mode": "owner",
    "capability_overrides": {},
    "league_format_id": "custom_flexible_v1",
    "league_format_version": "1.0.0",
}


def bootstrap_profile(runtime, profile_id: str = "profile_test", profile_name: str = "Test Profile", selected_team_id: str = "T01") -> None:
    runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.CREATE_PROFILE,
            {"profile_id": profile_id, "profile_name": profile_name},
            selected_team_id,
        )
    )
    result = runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.CREATE_NEW_FRANCHISE_SAVE,
            {
                "profile_id": profile_id,
                "profile_name": profile_name,
                "selected_user_team_id": selected_team_id,
                "setup": DEFAULT_SETUP_PAYLOAD,
            },
            selected_team_id,
        )
    )
    if not result.success:
        raise RuntimeError(f"bootstrap_profile failed: {result.message} data={result.data}")
