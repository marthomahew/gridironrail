from __future__ import annotations

import argparse
from pathlib import Path

from grs.simulation import DynastyRuntime
from grs.ui import launch_ui


def main() -> None:
    parser = argparse.ArgumentParser(description="Gridiron Rail: Sundays desktop launcher")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="runtime root directory")
    parser.add_argument("--seed", type=int, default=None, help="seed for deterministic dev/testing runs")
    parser.add_argument("--debug", action="store_true", help="enable debug/dev tools in UI")
    args = parser.parse_args()

    runtime = DynastyRuntime(root=args.root, seed=args.seed, dev_mode=args.debug)
    launch_ui(runtime.handle_action, debug_mode=args.debug)


if __name__ == "__main__":
    main()
