from __future__ import annotations

from grs.contracts import ActionRequest, ActionType
from grs.core import make_id
from grs.simulation import DynastyRuntime


def test_narrative_events_emitted_across_layers(tmp_path):
    runtime = DynastyRuntime(root=tmp_path, seed=123)

    runtime.handle_action(ActionRequest(make_id("req"), ActionType.ADVANCE_WEEK, {}, "T01"))

    assert runtime.event_bus.emitted_count("football") >= 1
    with runtime.store.connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM narrative_events").fetchone()[0]
    assert total >= 1
