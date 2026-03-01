from __future__ import annotations

import copy
import re
from pathlib import Path

from grs.contracts import ActionRequest, ActionType
from grs.core import make_id
from grs.simulation import DynastyRuntime
from tests.helpers import DEFAULT_SETUP_PAYLOAD


EXPECTED_DEFAULT_TEAMS = {
    "Minnesota Norsemen",
    "Chicago Grizzlies",
    "Detroit Hyenas",
    "Green Bay Union",
    "Seattle Seagulls",
    "Arizona Finches",
    "LA Billies",
    "San Francisco Miners",
    "Tampa Bay Pirates",
    "Carolina Cougars",
    "New Orleans Sentinels",
    "Atlanta Raptors",
    "Philadelphia Turkeys",
    "New York Bigmen",
    "Washington Warriors",
    "Dallas Ranchers",
    "Pittsburgh Oremen",
    "Cincinnati Sabretooths",
    "Baltimore Crows",
    "Cleveland Bulldogs",
    "Kansas City Tomahawks",
    "LA Lightning",
    "LV Plunderers",
    "Denver Mustangs",
    "Houston Steers",
    "Indianapolis Stallions",
    "Jacksonville Pumas",
    "Tennessee Hoplites",
    "New England Minutemen",
    "New York Pilots",
    "Miami Belugas",
    "Buffalo Wings",
}


def _default_heritage_setup() -> dict[str, object]:
    payload = copy.deepcopy(DEFAULT_SETUP_PAYLOAD)
    payload["conference_count"] = 2
    payload["divisions_per_conference"] = [4, 4]
    payload["teams_per_division"] = [[4, 4, 4, 4], [4, 4, 4, 4]]
    payload["league_identity_profile_id"] = "heritage_frontier_32_v1"
    payload["schedule_policy"] = {"policy_id": "division_weighted", "regular_season_weeks": 18}
    return payload


def _create_default_world(runtime: DynastyRuntime, *, profile_id: str, profile_name: str) -> None:
    runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.CREATE_PROFILE,
            {"profile_id": profile_id, "profile_name": profile_name},
            "T01",
        )
    )
    setup = _default_heritage_setup()
    created = runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.CREATE_NEW_FRANCHISE_SAVE,
            {
                "profile_id": profile_id,
                "profile_name": profile_name,
                "selected_user_team_id": "T01",
                "setup": setup,
            },
            "T01",
        )
    )
    assert created.success, created.message


def test_default_identity_profile_is_applied_and_named(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=500)
    runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.CREATE_PROFILE, {"profile_id": "p_def", "profile_name": "Default"}, "T01")
    )
    setup = _default_heritage_setup()
    validated = runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.VALIDATE_LEAGUE_SETUP,
            {"profile_id": "p_def", "setup": setup},
            "T01",
        )
    )
    assert validated.success
    assert validated.data["ok"] is True
    options = validated.data["team_options"]
    assert len(options) == 32
    assert {row["team_name"] for row in options} == EXPECTED_DEFAULT_TEAMS

    created = runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.CREATE_NEW_FRANCHISE_SAVE,
            {
                "profile_id": "p_def",
                "profile_name": "Default",
                "selected_user_team_id": "T01",
                "setup": setup,
            },
            "T01",
        )
    )
    assert created.success

    structure = runtime.handle_action(ActionRequest(make_id("req"), ActionType.GET_LEAGUE_STRUCTURE, {}, "T01"))
    assert structure.success
    assert structure.data["team_count"] == 32
    names: set[str] = set()
    for conference in structure.data["conferences"]:
        assert conference["division_count"] == 4
        for division in conference["divisions"]:
            assert division["team_count"] == 4
            for team in division["teams"]:
                team_name = str(team["team_name"])
                names.add(team_name)
                assert not team_name.startswith("Team ")
    assert names == EXPECTED_DEFAULT_TEAMS


def test_generated_players_have_identity_and_full_77_traits(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=501)
    _create_default_world(runtime, profile_id="p_traits", profile_name="Traits")
    team = runtime._team("T01")
    assert len(team.roster) == 53
    placeholder_pattern = re.compile(r"^(player\d+|T\d+_Player\d+)$", re.IGNORECASE)
    for player in team.roster:
        assert player.display_name.strip() != ""
        assert not placeholder_pattern.match(player.display_name)
        assert len(player.traits) == 77


def test_schedule_set_game_and_play_flow_work(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=502)
    _create_default_world(runtime, profile_id="p_flow", profile_name="Flow")

    schedule = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.GET_WEEK_SCHEDULE, {"week": runtime.org_state.week}, "T01")
    )
    assert schedule.success
    games = schedule.data["games"]
    assert games
    user_game = next((g["game_id"] for g in games if g.get("is_user_game")), games[0]["game_id"])
    set_game = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.SET_USER_GAME, {"game_id": user_game}, "T01")
    )
    assert set_game.success

    set_playcall = runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.SET_PLAYCALL,
            {
                "personnel": "11",
                "formation": "gun_trips",
                "offensive_concept": "spacing",
                "defensive_concept": "cover3_match",
                "play_type": "pass",
                "tempo": "normal",
                "aggression": "balanced",
                "playbook_entry_id": "pass_spacing",
            },
            "T01",
        )
    )
    assert set_playcall.success

    play = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.PLAY_USER_GAME, {"retained": True}, "T01")
    )
    assert play.success


def test_missing_package_slot_hard_fails_pre_sim(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=503)
    _create_default_world(runtime, profile_id="p_pkg", profile_name="Pkg")
    team = runtime._team("T01")
    assert "off_11" in team.package_book
    assert "QB1" in team.package_book["off_11"]
    del team.package_book["off_11"]["QB1"]

    schedule = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.GET_WEEK_SCHEDULE, {"week": runtime.org_state.week}, "T01")
    )
    user_game = next((g["game_id"] for g in schedule.data["games"] if g.get("is_user_game")), schedule.data["games"][0]["game_id"])
    runtime.handle_action(ActionRequest(make_id("req"), ActionType.SET_USER_GAME, {"game_id": user_game}, "T01"))
    runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.SET_PLAYCALL,
            {
                "personnel": "11",
                "formation": "gun_trips",
                "offensive_concept": "spacing",
                "defensive_concept": "cover3_match",
                "play_type": "pass",
                "tempo": "normal",
                "aggression": "balanced",
                "playbook_entry_id": "pass_spacing",
            },
            "T01",
        )
    )
    play = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.PLAY_USER_GAME, {"retained": True}, "T01")
    )
    assert not play.success
    assert "PRE_SIM_VALIDATION_FAILED" in play.message
    assert play.data is not None
    assert "forensic_path" in play.data


def test_runtime_readiness_reports_required_marts(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=504)
    _create_default_world(runtime, profile_id="p_ready", profile_name="Ready")
    readiness = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.GET_RUNTIME_READINESS, {}, "T01")
    )
    assert readiness.success
    assert readiness.data["ready"] is True
    checks = readiness.data["checks"]
    assert checks["mart:mart_traditional_stats"] is True
    assert checks["mart:mart_game_summaries"] is True
    assert checks["mart:mart_transactions"] is True
    assert checks["mart:mart_cap_history"] is True
