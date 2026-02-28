from __future__ import annotations

from grs.contracts import ActionRequest
from grs.core import make_id
from grs.simulation import DynastyRuntime


def test_narrative_events_emitted_across_layers(tmp_path):
    runtime = DynastyRuntime(root=tmp_path, seed=123)

    runtime.handle_action(ActionRequest(make_id("req"), "play_snap", {}, "USER_TEAM"))

    assert runtime.event_bus.emitted_count("football") >= 1
    with runtime.store.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM narrative_events").fetchone()[0] >= 1
