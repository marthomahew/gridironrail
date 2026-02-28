from __future__ import annotations

import hashlib
import json
from importlib import resources
from pathlib import Path

import pytest

from grs.contracts import ActionRequest, ActionType, ValidationError
from grs.core import make_id
from grs.football import PreSimValidator, ResourceResolver
from grs.simulation import DynastyRuntime


def _resource_payload(name: str) -> dict:
    package = resources.files("grs.resources.football")
    return json.loads((package / name).read_text(encoding="utf-8"))


def _checksum(resources_payload: list[dict]) -> str:
    canonical = json.dumps(resources_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _active_player(team, slot: str):
    assignment = next(d for d in team.depth_chart if d.active_flag and d.slot_role == slot)
    return next(p for p in team.roster if p.player_id == assignment.player_id)


def test_pre_sim_rejects_missing_formation_id(tmp_path: Path):
    runtime = DynastyRuntime(root=tmp_path, seed=101)
    result = runtime.handle_action(
        ActionRequest(
            make_id("req"),
            ActionType.SET_PLAYCALL,
            {
                "personnel": "11",
                "formation": "missing_formation",
                "offensive_concept": "spacing",
                "defensive_concept": "cover3_match",
                "play_type": "pass",
            },
            "T01",
        )
    )
    assert not result.success
    assert "pre-sim gate" in result.message


def test_pre_sim_hard_fails_incomplete_depth_chart(tmp_path: Path):
    runtime = DynastyRuntime(root=tmp_path, seed=102)
    team = next(t for t in runtime.org_state.teams if t.team_id == "T01")
    team.depth_chart = [d for d in team.depth_chart if d.slot_role != "QB1"]

    result = runtime.handle_action(ActionRequest(make_id("req"), ActionType.PLAY_USER_GAME, {}, "T01"))
    assert not result.success
    assert runtime.halted
    assert Path(result.data["forensic_path"]).exists()


def test_pre_sim_hard_fails_missing_trait(tmp_path: Path):
    runtime = DynastyRuntime(root=tmp_path, seed=103)
    team = next(t for t in runtime.org_state.teams if t.team_id == "T01")
    player = _active_player(team, "QB1")
    player.traits.pop("awareness", None)

    result = runtime.handle_action(ActionRequest(make_id("req"), ActionType.PLAY_USER_GAME, {}, "T01"))
    assert not result.success
    assert runtime.halted
    assert Path(result.data["forensic_path"]).exists()


def test_pre_sim_hard_fails_trait_out_of_range(tmp_path: Path):
    runtime = DynastyRuntime(root=tmp_path, seed=104)
    team = next(t for t in runtime.org_state.teams if t.team_id == "T01")
    player = _active_player(team, "QB1")
    player.traits["awareness"] = 200.0

    result = runtime.handle_action(ActionRequest(make_id("req"), ActionType.PLAY_USER_GAME, {}, "T01"))
    assert not result.success
    assert runtime.halted
    assert Path(result.data["forensic_path"]).exists()


def test_no_fallback_when_depth_chart_invalid(tmp_path: Path):
    runtime = DynastyRuntime(root=tmp_path, seed=105)
    team = next(t for t in runtime.org_state.teams if t.team_id == "T01")
    team.depth_chart = [d for d in team.depth_chart if d.slot_role != "WR1"]

    result = runtime.handle_action(ActionRequest(make_id("req"), ActionType.PLAY_USER_GAME, {}, "T01"))
    assert not result.success

    with runtime.store.connect() as conn:
        user_game = conn.execute(
            "SELECT game_id, status FROM schedule WHERE season = ? AND week = ? AND is_user_game = 1",
            (runtime.org_state.season, runtime.org_state.week),
        ).fetchone()
    assert user_game is not None
    assert user_game[1] != "final"


def test_resource_loader_rejects_schema_version_mismatch():
    formations = _resource_payload("formations.json")
    formations["manifest"]["schema_version"] = "9.9"
    with pytest.raises(ValidationError) as ex:
        ResourceResolver(bundle_overrides={"formations.json": formations})
    assert any(issue.code == "RESOURCE_SCHEMA_MISMATCH" for issue in ex.value.issues)


def test_resource_loader_rejects_referential_integrity_break():
    formations = _resource_payload("formations.json")
    formations["resources"][0]["allowed_personnel"] = ["11", "not_real"]
    formations["manifest"]["checksum"] = _checksum(formations["resources"])

    with pytest.raises(ValidationError) as ex:
        ResourceResolver(bundle_overrides={"formations.json": formations})
    assert any(issue.code == "FORMATION_PERSONNEL_REF_MISSING" for issue in ex.value.issues)


def test_trait_influence_loader_rejects_checksum_mismatch():
    influences = _resource_payload("trait_influences.json")
    influences["manifest"]["checksum"] = "invalid_checksum"
    with pytest.raises(ValidationError) as ex:
        ResourceResolver(bundle_overrides={"trait_influences.json": influences})
    assert any(issue.code == "RESOURCE_CHECKSUM_MISMATCH" for issue in ex.value.issues)


def test_trait_influence_profile_rejects_missing_required_family():
    influences = _resource_payload("trait_influences.json")
    run_profile = next(resource for resource in influences["resources"] if resource["id"] == "run")
    run_profile["families"] = [f for f in run_profile["families"] if f["family"] != "ball_security"]
    influences["manifest"]["checksum"] = _checksum(influences["resources"])

    with pytest.raises(ValidationError) as ex:
        resolver = ResourceResolver(bundle_overrides={"trait_influences.json": influences})
        PreSimValidator(resource_resolver=resolver)
    assert any(issue.code == "MISSING_INFLUENCE_FAMILY" for issue in ex.value.issues)


def test_trait_influence_profile_rejects_unknown_trait_code():
    influences = _resource_payload("trait_influences.json")
    pass_profile = next(resource for resource in influences["resources"] if resource["id"] == "pass")
    pass_profile["families"][0]["offense_weights"]["not_a_trait"] = 0.1
    influences["manifest"]["checksum"] = _checksum(influences["resources"])

    with pytest.raises(ValidationError) as ex:
        resolver = ResourceResolver(bundle_overrides={"trait_influences.json": influences})
        PreSimValidator(resource_resolver=resolver)
    assert any(issue.code == "UNKNOWN_INFLUENCE_TRAIT" for issue in ex.value.issues)


def test_active_players_have_complete_90_trait_vectors(tmp_path: Path):
    runtime = DynastyRuntime(root=tmp_path, seed=106)
    team = next(t for t in runtime.org_state.teams if t.team_id == "T01")
    active_ids = {d.player_id for d in team.depth_chart if d.active_flag}
    active_players = [p for p in team.roster if p.player_id in active_ids]
    assert active_players
    assert all(len(p.traits) == 90 for p in active_players)


def test_batch_hard_fails_if_any_scheduled_game_invalid(tmp_path: Path):
    runtime = DynastyRuntime(root=tmp_path, seed=107)
    invalid_team = runtime.org_state.teams[2]
    invalid_team.depth_chart = [d for d in invalid_team.depth_chart if d.slot_role != "QB1"]

    result = runtime.handle_action(ActionRequest(make_id("req"), ActionType.ADVANCE_WEEK, {}, "T01"))
    assert not result.success
    assert runtime.halted
    assert Path(result.data["forensic_path"]).exists()
