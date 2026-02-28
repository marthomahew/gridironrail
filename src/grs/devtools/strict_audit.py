from __future__ import annotations

import re
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from grs.contracts import StrictAuditFinding, StrictAuditReport, StrictAuditSection
from grs.core import make_id


class StrictAuditService:
    CLAMP_PATTERN = re.compile(r"\b(?:max|min)\(\s*[-]?\d+(?:\.\d+)?,")
    PAYLOAD_DEFAULT_PATTERN = re.compile(r"payload\.get\([^)]*,\s*[^)]+\)")
    RESCUE_CONSTRUCTION_PATTERN = re.compile(
        r"\bor\b\s*(?:ResourceResolver|PreSimValidator|FootballResolver|FootballEngine|PolicyDrivenCoachDecisionEngine|gameplay_random)\("
    )

    def run(self, *, repo_root: Path) -> StrictAuditReport:
        src_root = repo_root / "src" / "grs"
        findings_static: list[StrictAuditFinding] = []
        findings_resource: list[StrictAuditFinding] = []
        findings_import: list[StrictAuditFinding] = []

        for py in src_root.rglob("*.py"):
            rel = py.relative_to(repo_root).as_posix()
            text = py.read_text(encoding="utf-8")
            if "football" in rel or "org" in rel or "simulation" in rel:
                if self.CLAMP_PATTERN.search(text):
                    findings_static.append(
                        StrictAuditFinding(
                            finding_id=make_id("saf"),
                            scope="static",
                            severity="blocking",
                            summary="clamp/floor-ceiling pattern detected",
                            location=rel,
                        )
                    )
            if "simulation" in rel and self.PAYLOAD_DEFAULT_PATTERN.search(text):
                findings_static.append(
                    StrictAuditFinding(
                        finding_id=make_id("saf"),
                        scope="static",
                        severity="blocking",
                        summary="payload.get default rescue detected",
                        location=rel,
                    )
                )
            if self.RESCUE_CONSTRUCTION_PATTERN.search(text):
                findings_static.append(
                    StrictAuditFinding(
                        finding_id=make_id("saf"),
                        scope="static",
                        severity="blocking",
                        summary="constructor rescue fallback detected",
                        location=rel,
                    )
                )

        for resource in (src_root / "resources" / "football").glob("*.json"):
            rel = resource.relative_to(repo_root).as_posix()
            payload = json.loads(resource.read_text(encoding="utf-8"))
            resources = payload.get("resources", [])
            if not isinstance(resources, list):
                findings_resource.append(
                    StrictAuditFinding(
                        finding_id=make_id("saf"),
                        scope="resource",
                        severity="blocking",
                        summary="resource bundle payload is invalid",
                        location=rel,
                    )
                )
                continue
            for entry in resources:
                if not isinstance(entry, dict):
                    continue
                rid = str(entry.get("id", ""))
                if "_default" in rid:
                    findings_resource.append(
                        StrictAuditFinding(
                            finding_id=make_id("saf"),
                            scope="resource",
                            severity="blocking",
                            summary="default_* resource id naming found in active resource",
                            location=f"{rel}:{rid}",
                        )
                    )
                metadata = entry.get("metadata")
                if isinstance(metadata, dict) and metadata.get("placeholder") is True:
                    findings_resource.append(
                        StrictAuditFinding(
                            finding_id=make_id("saf"),
                            scope="resource",
                            severity="blocking",
                            summary="placeholder marker found in active resource metadata",
                            location=f"{rel}:{rid}",
                        )
                    )

        dynasty_py = src_root / "simulation" / "dynasty.py"
        dynasty_text = dynasty_py.read_text(encoding="utf-8")
        top_chunk = dynasty_text.split("class DynastyRuntime", maxsplit=1)[0]
        if "from grs.devtools" in top_chunk or "import grs.devtools" in top_chunk:
            findings_import.append(
                StrictAuditFinding(
                    finding_id=make_id("saf"),
                    scope="import_boundary",
                    severity="blocking",
                    summary="normal runtime imports devtools at module load",
                    location="src/grs/simulation/dynasty.py",
                )
            )

        sections = [
            StrictAuditSection(section="static", passed=not findings_static, findings=findings_static),
            StrictAuditSection(section="resource", passed=not findings_resource, findings=findings_resource),
            StrictAuditSection(section="import_boundary", passed=not findings_import, findings=findings_import),
        ]
        passed = all(section.passed for section in sections)
        return StrictAuditReport(
            report_id=make_id("strict"),
            generated_at=datetime.now(UTC),
            passed=passed,
            sections=sections,
        )

    def to_dict(self, report: StrictAuditReport) -> dict[str, object]:
        return asdict(report)
