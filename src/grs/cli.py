from __future__ import annotations

import argparse
from pathlib import Path

from grs.contracts import ActionRequest, ActionType
from grs.core import make_id
from grs.simulation import DynastyRuntime


def main() -> None:
    parser = argparse.ArgumentParser(description="Gridiron Rail: Sundays 1.0 vertical slice")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="runtime root directory")
    parser.add_argument("--seed", type=int, default=None, help="seed for deterministic dev/testing runs")
    parser.add_argument("--weeks", type=int, default=2, help="weeks to auto-advance in CLI mode")
    parser.add_argument("--ui", action="store_true", help="launch Qt desktop UI")
    parser.add_argument("--debug", action="store_true", help="enable debug/ground-truth tools")
    parser.add_argument("--play-user-game", action="store_true", help="play user game before advancing each week")
    args = parser.parse_args()

    runtime = DynastyRuntime(root=args.root, seed=args.seed)

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
