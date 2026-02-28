from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json

from grs.contracts import ActionRequest
from grs.core import make_id
from grs.simulation.dynasty import DynastyRuntime


@dataclass(slots=True)
class ReplayAction:
    action_type: str
    payload: dict
    actor_team_id: str


class ReplayHarness:
    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.actions: list[ReplayAction] = []

    def record(self, action_type: str, payload: dict, actor_team_id: str = "T01") -> None:
        self.actions.append(ReplayAction(action_type=action_type, payload=payload, actor_team_id=actor_team_id))

    def save(self, path: Path) -> None:
        path.write_text(json.dumps({"seed": self.seed, "actions": [a.__dict__ for a in self.actions]}, indent=2), encoding="utf-8")

    @staticmethod
    def load(path: Path) -> ReplayHarness:
        data = json.loads(path.read_text(encoding="utf-8"))
        harness = ReplayHarness(seed=int(data["seed"]))
        for raw in data["actions"]:
            harness.actions.append(ReplayAction(action_type=raw["action_type"], payload=raw["payload"], actor_team_id=raw["actor_team_id"]))
        return harness

    def replay(self, root: Path) -> tuple[dict, dict]:
        runtime_a = DynastyRuntime(root=root / "replay_a", seed=self.seed)
        runtime_b = DynastyRuntime(root=root / "replay_b", seed=self.seed)

        for action in self.actions:
            runtime_a.handle_action(ActionRequest(make_id("req"), action.action_type, action.payload, action.actor_team_id))
            runtime_b.handle_action(ActionRequest(make_id("req"), action.action_type, action.payload, action.actor_team_id))

        summary_a = self._fingerprint(runtime_a)
        summary_b = self._fingerprint(runtime_b)
        return summary_a, summary_b

    def _fingerprint(self, runtime: DynastyRuntime) -> dict:
        rows = runtime.store.get_latest_standings(runtime.org_state.season, max(1, runtime.org_state.week - 1))
        return {
            "season": runtime.org_state.season,
            "week": runtime.org_state.week,
            "standings": rows,
            "last_user_game": runtime.last_user_game_result.final_state.game_id if runtime.last_user_game_result else None,
        }
