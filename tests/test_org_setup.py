from __future__ import annotations

import copy
from pathlib import Path

from grs.contracts import ActionRequest, ActionType
from grs.core import make_id
from grs.simulation import DynastyRuntime
from tests.helpers import DEFAULT_SETUP_PAYLOAD, bootstrap_profile


def test_profile_lifecycle_create_save_load_delete(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=300)
    create_profile = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.CREATE_PROFILE, {"profile_id": "p1", "profile_name": "Alpha"}, "T01")
    )
    assert create_profile.success

    create_save = runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.CREATE_NEW_FRANCHISE_SAVE,
            {
                "profile_id": "p1",
                "profile_name": "Alpha",
                "selected_user_team_id": "T01",
                "setup": DEFAULT_SETUP_PAYLOAD,
            },
            "T01",
        )
    )
    assert create_save.success

    loaded = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.LOAD_PROFILE, {"profile_id": "p1"}, "T01")
    )
    assert loaded.success

    listed = runtime.handle_action(ActionRequest(make_id("req"), ActionType.LIST_PROFILES, {}, "T01"))
    assert listed.success
    assert any(item["profile_id"] == "p1" for item in listed.data["profiles"])

    deleted = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.DELETE_PROFILE, {"profile_id": "p1"}, "T01")
    )
    assert deleted.success


def test_setup_validation_rejects_out_of_range(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=301)
    runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.CREATE_PROFILE, {"profile_id": "p2", "profile_name": "Beta"}, "T01")
    )
    bad_setup = copy.deepcopy(DEFAULT_SETUP_PAYLOAD)
    bad_setup["conference_count"] = 0
    validation = runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.VALIDATE_LEAGUE_SETUP,
            {"profile_id": "p2", "setup": bad_setup},
            "T01",
        )
    )
    assert validation.success
    assert not validation.data["ok"]
    assert any(issue["code"] == "SETUP_CONFERENCE_COUNT_OUT_OF_RANGE" for issue in validation.data["issues"])


def test_schedule_supports_odd_team_count_and_byes(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=302)
    runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.CREATE_PROFILE, {"profile_id": "p3", "profile_name": "Gamma"}, "T01")
    )
    odd_setup = copy.deepcopy(DEFAULT_SETUP_PAYLOAD)
    odd_setup["conference_count"] = 1
    odd_setup["divisions_per_conference"] = [1]
    odd_setup["teams_per_division"] = [[5]]
    odd_setup["schedule_policy"] = {"policy_id": "division_weighted", "regular_season_weeks": 10}

    created = runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.CREATE_NEW_FRANCHISE_SAVE,
            {
                "profile_id": "p3",
                "profile_name": "Gamma",
                "selected_user_team_id": "T01",
                "setup": odd_setup,
            },
            "T01",
        )
    )
    assert created.success
    assert runtime.store is not None
    with runtime.store.connect() as conn:
        rows = conn.execute(
            "SELECT week, home_team_id, away_team_id FROM schedule WHERE season = ? ORDER BY week, game_id",
            (2026,),
        ).fetchall()
    assert rows
    by_week: dict[int, set[str]] = {}
    for week, home, away in rows:
        by_week.setdefault(int(week), set())
        assert home not in by_week[int(week)]
        assert away not in by_week[int(week)]
        by_week[int(week)].add(str(home))
        by_week[int(week)].add(str(away))


def test_mode_matrix_enforces_capability_denial_and_override(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=303)
    bootstrap_profile(runtime, profile_id="p4", profile_name="Delta")

    set_mode = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.SET_ACTIVE_MODE, {"mode": "gm", "reason": "test"}, "T01")
    )
    assert set_mode.success

    denied = runtime.handle_action(
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
    assert not denied.success
    assert "capability denied" in denied.message

    enable_override = runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.SET_CAPABILITY_OVERRIDE,
            {"domain": "playcall_override", "enabled": True, "reason": "test override"},
            "T01",
        )
    )
    assert enable_override.success

    allowed = runtime.handle_action(
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
    assert allowed.success


def test_org_dashboard_exposes_mode_and_capabilities(tmp_path: Path) -> None:
    runtime = DynastyRuntime(root=tmp_path, seed=304)
    bootstrap_profile(runtime, profile_id="p5", profile_name="Epsilon")
    dashboard = runtime.handle_action(
        ActionRequest(make_id("req"), ActionType.GET_ORG_DASHBOARD, {}, "T01")
    )
    assert dashboard.success
    assert dashboard.data["mode"] == "owner"
    assert "capabilities" in dashboard.data
