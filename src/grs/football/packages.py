from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime

from grs.contracts import (
    DepthChartAssignment,
    PackageAssignmentValidationReport,
    PlayType,
    TeamPackageBook,
    ValidationError,
    ValidationIssue,
)
from grs.core import make_id

PACKAGE_SLOT_REQUIREMENTS: dict[str, list[str]] = {
    "off_11": ["QB1", "RB1", "WR1", "WR2", "WR3", "TE1", "LT", "LG", "C", "RG", "RT"],
    "off_12": ["QB1", "RB1", "WR1", "WR2", "TE1", "TE2", "LT", "LG", "C", "RG", "RT"],
    "off_21": ["QB1", "RB1", "RB2", "WR1", "WR2", "TE1", "LT", "LG", "C", "RG", "RT"],
    "off_two_point": ["QB1", "RB1", "WR1", "WR2", "WR3", "TE1", "LT", "LG", "C", "RG", "RT"],
    "def_base": ["DE1", "DT1", "DT2", "DE2", "LB1", "LB2", "LB3", "CB1", "CB2", "S1", "S2"],
    "st_punt": ["P", "LT", "LG", "C", "RG", "RT", "TE1", "WR1", "WR2", "CB1", "S1"],
    "st_punt_return": ["DE1", "DT1", "DT2", "DE2", "LB1", "LB2", "LB3", "CB1", "CB2", "S1", "RB1"],
    "st_kickoff": ["K", "LB1", "LB2", "LB3", "CB1", "CB2", "S1", "S2", "DE1", "DE2", "WR1"],
    "st_kick_return": ["RB1", "WR1", "WR2", "WR3", "TE1", "LB1", "LB2", "CB1", "S1", "S2", "DE1"],
    "st_field_goal": ["K", "LT", "LG", "C", "RG", "RT", "TE1", "LB1", "LB2", "DE1", "DE2"],
    "st_fg_block": ["DE1", "DE2", "DT1", "DT2", "LB1", "LB2", "LB3", "CB1", "CB2", "S1", "S2"],
    "st_hands": ["K", "WR1", "WR2", "WR3", "RB1", "RB2", "TE1", "CB1", "CB2", "S1", "S2"],
}


def required_package_ids_for_runtime() -> list[str]:
    return sorted(PACKAGE_SLOT_REQUIREMENTS.keys())


def resolve_package_ids(play_type: PlayType, personnel: str) -> tuple[str, str]:
    if play_type == PlayType.PUNT:
        return ("st_punt", "st_punt_return")
    if play_type == PlayType.KICKOFF:
        return ("st_kickoff", "st_kick_return")
    if play_type in {PlayType.FIELD_GOAL, PlayType.EXTRA_POINT}:
        return ("st_field_goal", "st_fg_block")
    if play_type == PlayType.TWO_POINT:
        return ("off_two_point", "def_base")
    offense_personnel_map = {
        "11": "off_11",
        "12": "off_12",
        "21": "off_21",
    }
    if personnel not in offense_personnel_map:
        raise ValidationError(
            [
                ValidationIssue(
                    code="UNSUPPORTED_PERSONNEL_PACKAGE",
                    severity="blocking",
                    field_path="playcall.personnel",
                    entity_id=personnel,
                    message=f"personnel '{personnel}' has no package mapping",
                )
            ]
        )
    return (offense_personnel_map[personnel], "def_base")


class PackageCompiler:
    def compile_team_package_book(
        self,
        *,
        team_id: str,
        season: int,
        week: int,
        depth_chart: list[DepthChartAssignment],
        roster_player_ids: set[str],
        source: str,
    ) -> TeamPackageBook:
        depth_map = self._depth_map(depth_chart)
        assignments: dict[str, dict[str, str]] = {}
        issues: list[ValidationIssue] = []
        for package_id, required_slots in PACKAGE_SLOT_REQUIREMENTS.items():
            package_assignment: dict[str, str] = {}
            for slot_role in required_slots:
                candidates = depth_map.get(slot_role)
                if not candidates:
                    issues.append(
                        ValidationIssue(
                            code="PACKAGE_SLOT_UNRESOLVED",
                            severity="blocking",
                            field_path=f"package_book.{package_id}.{slot_role}",
                            entity_id=team_id,
                            message=f"missing active depth slot '{slot_role}'",
                        )
                    )
                    continue
                player_id = candidates[0]
                if player_id not in roster_player_ids:
                    issues.append(
                        ValidationIssue(
                            code="PACKAGE_UNKNOWN_PLAYER",
                            severity="blocking",
                            field_path=f"package_book.{package_id}.{slot_role}",
                            entity_id=team_id,
                            message=f"slot '{slot_role}' references unknown player '{player_id}'",
                        )
                    )
                    continue
                package_assignment[slot_role] = player_id
            duplicate_players = self._duplicates(list(package_assignment.values()))
            if duplicate_players:
                issues.append(
                    ValidationIssue(
                        code="PACKAGE_DUPLICATE_PLAYER",
                        severity="blocking",
                        field_path=f"package_book.{package_id}",
                        entity_id=team_id,
                        message=f"package '{package_id}' contains duplicate players: {sorted(duplicate_players)}",
                    )
                )
            assignments[package_id] = package_assignment
        if issues:
            raise ValidationError(issues)
        return TeamPackageBook(
            team_id=team_id,
            season=season,
            week=week,
            assignments=assignments,
            source=source,
            updated_at=datetime.now(UTC),
        )

    def validate_team_package_book(
        self,
        *,
        team_id: str,
        season: int,
        week: int,
        package_book: dict[str, dict[str, str]],
        roster_player_ids: set[str],
    ) -> PackageAssignmentValidationReport:
        blocking: list[ValidationIssue] = []
        warnings: list[ValidationIssue] = []
        for package_id in required_package_ids_for_runtime():
            if package_id not in package_book:
                blocking.append(
                    ValidationIssue(
                        code="PACKAGE_MISSING",
                        severity="blocking",
                        field_path=f"package_book.{package_id}",
                        entity_id=team_id,
                        message=f"required package '{package_id}' is missing",
                    )
                )
                continue
            mapping = package_book[package_id]
            required_slots = PACKAGE_SLOT_REQUIREMENTS[package_id]
            for slot_role in required_slots:
                if slot_role not in mapping:
                    blocking.append(
                        ValidationIssue(
                            code="PACKAGE_SLOT_UNRESOLVED",
                            severity="blocking",
                            field_path=f"package_book.{package_id}.{slot_role}",
                            entity_id=team_id,
                            message=f"required slot '{slot_role}' is not assigned",
                        )
                    )
                    continue
                player_id = str(mapping[slot_role])
                if player_id not in roster_player_ids:
                    blocking.append(
                        ValidationIssue(
                            code="PACKAGE_UNKNOWN_PLAYER",
                            severity="blocking",
                            field_path=f"package_book.{package_id}.{slot_role}",
                            entity_id=team_id,
                            message=f"assigned player '{player_id}' is not on roster",
                        )
                    )
            duplicate_players = self._duplicates(list(mapping.values()))
            if duplicate_players:
                blocking.append(
                    ValidationIssue(
                        code="PACKAGE_DUPLICATE_PLAYER",
                        severity="blocking",
                        field_path=f"package_book.{package_id}",
                        entity_id=team_id,
                        message=f"duplicate players in package: {sorted(duplicate_players)}",
                    )
                )
            unknown_slots = sorted(set(mapping.keys()) - set(required_slots))
            if unknown_slots:
                warnings.append(
                    ValidationIssue(
                        code="PACKAGE_UNKNOWN_SLOT",
                        severity="warning",
                        field_path=f"package_book.{package_id}",
                        entity_id=team_id,
                        message=f"unknown assigned slots ignored: {unknown_slots}",
                    )
                )
        return PackageAssignmentValidationReport(
            report_id=make_id("pkgval"),
            team_id=team_id,
            season=season,
            week=week,
            blocking_issues=blocking,
            warning_issues=warnings,
            validated_at=datetime.now(UTC),
        )

    def update_assignment(
        self,
        *,
        team_id: str,
        package_book: dict[str, dict[str, str]],
        package_id: str,
        slot_role: str,
        player_id: str,
    ) -> dict[str, dict[str, str]]:
        if package_id not in PACKAGE_SLOT_REQUIREMENTS:
            raise ValidationError(
                [
                    ValidationIssue(
                        code="PACKAGE_MISSING",
                        severity="blocking",
                        field_path="package_id",
                        entity_id=team_id,
                        message=f"unknown package_id '{package_id}'",
                    )
                ]
            )
        if slot_role not in PACKAGE_SLOT_REQUIREMENTS[package_id]:
            raise ValidationError(
                [
                    ValidationIssue(
                        code="PACKAGE_SLOT_UNRESOLVED",
                        severity="blocking",
                        field_path="slot_role",
                        entity_id=team_id,
                        message=f"slot '{slot_role}' is not valid for package '{package_id}'",
                    )
                ]
            )
        updated = {key: dict(value) for key, value in package_book.items()}
        mapping = dict(updated.get(package_id, {}))
        mapping[slot_role] = player_id
        updated[package_id] = mapping
        return updated

    def _depth_map(self, depth_chart: list[DepthChartAssignment]) -> dict[str, list[str]]:
        by_slot: dict[str, list[tuple[int, str]]] = {}
        for assignment in depth_chart:
            if not assignment.active_flag:
                continue
            by_slot.setdefault(assignment.slot_role, []).append((assignment.priority, assignment.player_id))
        resolved: dict[str, list[str]] = {}
        for slot_role, entries in by_slot.items():
            ordered = sorted(entries, key=lambda row: row[0])
            resolved[slot_role] = [player_id for _, player_id in ordered]
        return resolved

    def _duplicates(self, values: list[str]) -> set[str]:
        seen: set[str] = set()
        dup: set[str] = set()
        for value in values:
            if value in seen:
                dup.add(value)
            else:
                seen.add(value)
        return dup

    def debug_snapshot(self, package_book: TeamPackageBook) -> dict[str, object]:
        return {
            "team_id": package_book.team_id,
            "season": package_book.season,
            "week": package_book.week,
            "source": package_book.source,
            "assignments": asdict(package_book)["assignments"],
        }

