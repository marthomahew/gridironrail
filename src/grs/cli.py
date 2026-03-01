from __future__ import annotations

import argparse
from pathlib import Path

from grs.contracts import ActionRequest, ActionType
from grs.core import make_id
from grs.simulation import DynastyRuntime


def _default_setup_payload() -> dict:
    return {
        "conference_count": 2,
        "divisions_per_conference": [2, 2],
        "teams_per_division": [[2, 2], [2, 2]],
        "roster_policy": {"players_per_team": 53, "active_gameday_min": 22, "active_gameday_max": 53},
        "cap_policy": {"cap_amount": 255_000_000, "dead_money_penalty_multiplier": 1.0},
        "schedule_policy": {"policy_id": "balanced_round_robin", "regular_season_weeks": 18},
        "ruleset_id": "nfl_standard_v1",
        "difficulty_profile_id": "pro",
        "talent_profile_id": "balanced_mid",
        "league_identity_profile_id": "generated_custom_v1",
        "user_mode": "owner",
        "capability_overrides": {},
        "league_format_id": "custom_flexible_v1",
        "league_format_version": "1.0.0",
    }


def _ensure_cli_profile(runtime: DynastyRuntime) -> None:
    listed = runtime.handle_action(ActionRequest(make_id("req"), ActionType.LIST_PROFILES, {}, "T01"))
    if not listed.success:
        raise RuntimeError(listed.message)
    profiles = listed.data.get("profiles", [])
    if profiles:
        profile_id = profiles[0]["profile_id"]
        loaded = runtime.handle_action(ActionRequest(make_id("req"), ActionType.LOAD_PROFILE, {"profile_id": profile_id}, "T01"))
        if not loaded.success:
            raise RuntimeError(loaded.message)
        return
    created = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.CREATE_NEW_FRANCHISE_SAVE, {
            "profile_id": "cli_profile",
            "profile_name": "CLI Profile",
            "selected_user_team_id": "T01",
            "setup": _default_setup_payload(),
        }, "T01")
    )
    if not created.success:
        raise RuntimeError(created.message)


def main() -> None:
    parser = argparse.ArgumentParser(description="Gridiron Rail: Sundays 1.0 vertical slice")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="runtime root directory")
    parser.add_argument("--seed", type=int, default=None, help="seed for deterministic dev/testing runs")
    parser.add_argument("--weeks", type=int, default=2, help="weeks to auto-advance in CLI mode")
    parser.add_argument("--ui", action="store_true", help="launch Qt desktop UI")
    parser.add_argument("--debug", action="store_true", help="enable debug/ground-truth tools")
    parser.add_argument("--play-user-game", action="store_true", help="play user game before advancing each week")
    args = parser.parse_args()

    runtime = DynastyRuntime(root=args.root, seed=args.seed, dev_mode=args.debug)
    _ensure_cli_profile(runtime)

    if args.ui:
        from grs.ui import launch_ui

        launch_ui(runtime.handle_action, debug_mode=args.debug)
        return

    for _ in range(args.weeks):
        if args.play_user_game:
            played = runtime.handle_action(ActionRequest(make_id("req"), ActionType.PLAY_USER_GAME, {}, "T01"))
            print(played.message)
        advanced = runtime.handle_action(ActionRequest(make_id("req"), ActionType.ADVANCE_WEEK, {}, "T01"))
        print(advanced.message)
        if not advanced.success:
            print(advanced.data)
            break

    standings = runtime.handle_action(ActionRequest(make_id("req"), ActionType.GET_STANDINGS, {}, "T01"))
    print("Standings:")
    for row in standings.data.get("standings", []):
        print(f"- {row['team_id']}: {row['wins']}-{row['losses']}-{row['ties']} (pd={row['point_diff']})")

    try:
        outputs = runtime.export()
        print("Exported datasets:")
        for p in outputs:
            print(f"- {p}")
    except RuntimeError as exc:
        print(f"Export unavailable: {exc}")


if __name__ == "__main__":
    main()
