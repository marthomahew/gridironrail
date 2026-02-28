from __future__ import annotations

from importlib import resources
from pathlib import Path


def resource_path(package: str, name: str) -> Path:
    ref = resources.files(package).joinpath(name)
    with resources.as_file(ref) as p:
        return Path(p)
