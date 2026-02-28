from __future__ import annotations

from pathlib import Path

from grs.contracts import ActionRequest, ActionType
from grs.core import make_id
from grs.simulation import DynastyRuntime, ReplayHarness
from tests.helpers import bootstrap_profile


def test_integrity_hard_stop_produces_forensic_artifact(tmp_path: Path):
    runtime = DynastyRuntime(root=tmp_path / "runtime", seed=44)
    bootstrap_profile(runtime)
    assert runtime.org_state is not None
    user_team = next(t for t in runtime.org_state.teams if t.team_id == "T01")
    user_team.depth_chart = []

    result = runtime.handle_action(ActionRequest(make_id("req"), ActionType.PLAY_USER_GAME, {}, "T01"))
    assert not result.success
    assert runtime.halted
    forensic_path = Path(result.data["forensic_path"])
    assert forensic_path.exists()


def test_replay_harness_determinism(tmp_path: Path):
    harness = ReplayHarness(seed=99)
    harness.record(ActionType.PLAY_USER_GAME.value, {})
    harness.record(ActionType.ADVANCE_WEEK.value, {})
    harness.record(ActionType.ADVANCE_WEEK.value, {})

    a, b = harness.replay(tmp_path)
    assert a == b
