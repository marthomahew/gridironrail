from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class ModValidationResult:
    valid: bool
    errors: list[str]


class DataPackValidator:
    REQUIRED_MANIFEST_KEYS = {"mod_id", "name", "version", "schema_version"}

    def validate_manifest(self, manifest: dict[str, Any]) -> ModValidationResult:
        errors: list[str] = []
        missing = sorted(self.REQUIRED_MANIFEST_KEYS - set(manifest.keys()))
        if missing:
            errors.append(f"manifest missing required keys: {', '.join(missing)}")
        if manifest.get("schema_version") != "1.0":
            errors.append("schema_version must be '1.0'")
        return ModValidationResult(valid=not errors, errors=errors)

    def validate_players_csv(self, csv_path: Path) -> ModValidationResult:
        errors: list[str] = []
        required_cols = {"player_id", "name", "position", "overall_truth"}
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            headers = set(reader.fieldnames or [])
            missing = sorted(required_cols - headers)
            if missing:
                errors.append(f"players.csv missing columns: {', '.join(missing)}")
        return ModValidationResult(valid=not errors, errors=errors)


class DataPackLoader:
    def __init__(self) -> None:
        self.validator = DataPackValidator()

    def load(self, pack_dir: Path) -> dict[str, Any]:
        manifest_path = pack_dir / "manifest.json"
        if not manifest_path.exists():
            raise ValueError("mod pack missing manifest.json")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        manifest_check = self.validator.validate_manifest(manifest)
        if not manifest_check.valid:
            raise ValueError("; ".join(manifest_check.errors))

        players_path = pack_dir / "players.csv"
        if players_path.exists():
            players_check = self.validator.validate_players_csv(players_path)
            if not players_check.valid:
                raise ValueError("; ".join(players_check.errors))

        return manifest
