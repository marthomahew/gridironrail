from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json

from grs.contracts import ActionRequest, ActionType
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
        self._bootstrap_runtime(runtime_a)
        self._bootstrap_runtime(runtime_b)

        for action in self.actions:
            runtime_a.handle_action(ActionRequest(make_id("req"), action.action_type, action.payload, action.actor_team_id))
            runtime_b.handle_action(ActionRequest(make_id("req"), action.action_type, action.payload, action.actor_team_id))

        summary_a = self._fingerprint(runtime_a)
        summary_b = self._fingerprint(runtime_b)
        return summary_a, summary_b

    def _fingerprint(self, runtime: DynastyRuntime) -> dict:
        if runtime.org_state is None or runtime.store is None:
            raise RuntimeError("replay runtime has no loaded profile state")
        standings_week = runtime.org_state.week - 1
        if standings_week < 1:
            standings_week = 1
        rows = runtime.store.get_latest_standings(runtime.org_state.season, standings_week)
        return {
            "season": runtime.org_state.season,
            "week": runtime.org_state.week,
            "standings": rows,
            "last_user_game": runtime.last_user_game_result.final_state.game_id if runtime.last_user_game_result else None,
        }

    def _bootstrap_runtime(self, runtime: DynastyRuntime) -> None:
        setup = {
            "conference_count": 2,
            "divisions_per_conference": [2, 2],
            "teams_per_division": [[2, 2], [2, 2]],
            "roster_policy": {"players_per_team": 53, "active_gameday_min": 22, "active_gameday_max": 53},
            "cap_policy": {"cap_amount": 255_000_000, "dead_money_penalty_multiplier": 1.0},
            "schedule_policy": {"policy_id": "balanced_round_robin", "regular_season_weeks": 18},
            "ruleset_id": "nfl_standard_v1",
            "difficulty_profile_id": "pro",
            "talent_profile_id": "balanced_mid",
            "user_mode": "owner",
            "capability_overrides": {},
            "league_format_id": "custom_flexible_v1",
            "league_format_version": "1.0.0",
        }
        runtime.handle_action(
            ActionRequest(
                make_id("req"),
                ActionType.CREATE_PROFILE,
                {"profile_id": "replay_profile", "profile_name": "Replay Profile"},
                "T01",
            )
        )
        result = runtime.handle_action(
            ActionRequest(
                make_id("req"),
                ActionType.CREATE_NEW_FRANCHISE_SAVE,
                {
                    "profile_id": "replay_profile",
                    "profile_name": "Replay Profile",
                    "selected_user_team_id": "T01",
                    "setup": setup,
                },
                "T01",
            )
        )
        if not result.success:
            raise RuntimeError(f"replay bootstrap failed: {result.message}")
