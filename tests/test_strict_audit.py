from __future__ import annotations

from pathlib import Path

from grs.devtools.strict_audit import StrictAuditService


def test_strict_audit_passes_on_current_repo() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    report = StrictAuditService().run(repo_root=repo_root)
    assert report.passed, report
