from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, UTC
from pathlib import Path
from uuid import uuid4

from grs.contracts import ForensicArtifact


class EngineIntegrityError(RuntimeError):
    def __init__(self, artifact: ForensicArtifact) -> None:
        super().__init__(artifact.message)
        self.artifact = artifact


def build_forensic_artifact(
    engine_scope: str,
    error_code: str,
    message: str,
    state_snapshot: dict[str, object],
    context: dict[str, object],
    identifiers: dict[str, str],
    causal_fragment: list[str],
) -> ForensicArtifact:
    return ForensicArtifact(
        artifact_id=str(uuid4()),
        timestamp=datetime.now(UTC),
        engine_scope=engine_scope,
        error_code=error_code,
        message=message,
        state_snapshot=state_snapshot,
        context=context,
        identifiers=identifiers,
        causal_fragment=causal_fragment,
    )


def persist_forensic_artifact(artifact: ForensicArtifact, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"forensic_{artifact.artifact_id}.json"
    path.write_text(json.dumps(asdict(artifact), default=str, indent=2), encoding="utf-8")
    return path
