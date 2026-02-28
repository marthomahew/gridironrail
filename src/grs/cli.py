from __future__ import annotations

import argparse
from pathlib import Path

from grs.contracts import ActionRequest
from grs.core import make_id
from grs.simulation import DynastyRuntime


def main() -> None:
    parser = argparse.ArgumentParser(description="Gridiron Rail: Sundays 1.0 skeleton")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="runtime root directory")
    parser.add_argument("--seed", type=int, default=None, help="seed for deterministic dev/testing runs")
    parser.add_argument("--weeks", type=int, default=2, help="weeks to auto-advance in CLI mode")
    parser.add_argument("--ui", action="store_true", help="launch Qt desktop UI")
    parser.add_argument("--debug", action="store_true", help="enable debug/ground-truth tools")
    args = parser.parse_args()

    runtime = DynastyRuntime(root=args.root, seed=args.seed)

    if args.ui:
        from grs.ui import launch_ui

        launch_ui(runtime.handle_action, debug_mode=args.debug)
        return

    for _ in range(args.weeks):
        print(runtime.handle_action(ActionRequest(make_id("req"), "play_snap", {}, "USER_TEAM")).message)
        try:
            print(runtime.handle_action(ActionRequest(make_id("req"), "advance_week", {}, "USER_TEAM")).message)
        except RuntimeError as exc:
            print(f"Advance week halted: {exc}")
            break

    try:
        outputs = runtime.export()
        print("Exported datasets:")
        for p in outputs:
            print(f"- {p}")
    except RuntimeError as exc:
        print(f"Export unavailable: {exc}")


if __name__ == "__main__":
    main()
