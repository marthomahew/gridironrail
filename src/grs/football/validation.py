from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, UTC
from typing import Iterable

from grs.contracts import (
    GameSessionState,
    PlaycallRequest,
    RandomSource,
    SimulationReadinessReport,
    SnapContextPackage,
    TraitCatalogEntry,
    ValidationError,
    ValidationIssue,
    ValidationResult,
)
from grs.football.contest import parse_influence_profiles, required_influence_families
from grs.football.resources import ResourceResolver
from grs.football.traits import canonical_trait_catalog, validate_traits
from grs.org.entities import Franchise, Player

OFFENSE_REQUIRED_SLOTS = {"QB1", "RB1", "WR1", "WR2", "WR3", "TE1", "LT", "LG", "C", "RG", "RT"}
DEFENSE_REQUIRED_SLOTS = {"DE1", "DT1", "DT2", "DE2", "LB1", "LB2", "LB3", "CB1", "CB2", "S1", "S2"}
SPECIAL_TEAM_REQUIRED_SLOTS = {"K", "P"}
ALL_REQUIRED_SLOTS = OFFENSE_REQUIRED_SLOTS | DEFENSE_REQUIRED_SLOTS | SPECIAL_TEAM_REQUIRED_SLOTS


class PreSimValidator:
    def __init__(
        self,
        resource_resolver: ResourceResolver | None = None,
        trait_catalog: Iterable[TraitCatalogEntry] | None = None,
    ) -> None:
        self._resource_resolver = resource_resolver or ResourceResolver()
        self._trait_catalog = list(trait_catalog or canonical_trait_catalog())
        self._required_traits = {t.trait_code for t in self._trait_catalog if t.required}
        self._validate_trait_influence_resources()

    def validate_game_input(
        self,
        *,
        season: int,
        week: int,
        game_id: str,
        home: Franchise,
        away: Franchise,
        session_state: GameSessionState,
        random_source: RandomSource | None,
        coaching_policy_id: str = "balanced_default",
    ) -> ValidationResult:
        issues: list[ValidationIssue] = []
        if random_source is None:
            issues.append(
                ValidationIssue(
                    code="MISSING_RANDOM_SOURCE",
                    severity="blocking",
                    field_path="random_source",
                    entity_id=game_id,
                    message="random source must be injected for simulation context",
                )
            )

        if season != session_state.season or week != session_state.week or game_id != session_state.game_id:
            issues.append(
                ValidationIssue(
                    code="SESSION_STATE_MISMATCH",
                    severity="blocking",
                    field_path="session_state",
                    entity_id=game_id,
                    message="session identity does not match simulation entrypoint",
                )
            )
        if session_state.home_team_id != home.team_id or session_state.away_team_id != away.team_id:
            issues.append(
                ValidationIssue(
                    code="SESSION_TEAM_MISMATCH",
                    severity="blocking",
                    field_path="session_state.team_ids",
                    entity_id=game_id,
                    message="session team ids do not match scheduled teams",
                )
            )
        if session_state.possession_team_id not in {home.team_id, away.team_id}:
            issues.append(
                ValidationIssue(
                    code="INVALID_POSSESSION_TEAM",
                    severity="blocking",
                    field_path="session_state.possession_team_id",
                    entity_id=game_id,
                    message="possession team id must be one of scheduled teams",
                )
            )

        issues.extend(self._validate_team_readiness(home))
        issues.extend(self._validate_team_readiness(away))
        issues.extend(self._validate_policy(coaching_policy_id))

        return self._finalize(issues)

    def validate_playcall(self, playcall: PlaycallRequest) -> ValidationResult:
        issues: list[ValidationIssue] = []
        issues.extend(self._validate_playcall_fields(playcall))
        return self._finalize(issues)

    def validate_snap_context(
        self,
        scp: SnapContextPackage,
        *,
        player_lookup: dict[str, Player] | None = None,
    ) -> ValidationResult:
        issues: list[ValidationIssue] = []
        issues.extend(self._validate_situation(scp))
        issues.extend(self._validate_participants(scp))
        issues.extend(self._validate_snap_states(scp))

        playcall = PlaycallRequest(
            team_id=scp.situation.possession_team_id,
            personnel=scp.intent.personnel,
            formation=scp.intent.formation,
            offensive_concept=scp.intent.offensive_concept,
            defensive_concept=scp.intent.defensive_concept,
            tempo=scp.intent.tempo,
            aggression=scp.intent.aggression,
            play_type=scp.intent.play_type,
        )
        issues.extend(self._validate_playcall_fields(playcall))
        issues.extend(self._validate_snap_traits(scp, player_lookup))

        return self._finalize(issues)

    def readiness_report(
        self,
        *,
        season: int,
        week: int,
        game_id: str,
        home_team_id: str,
        away_team_id: str,
        issues: list[ValidationIssue],
    ) -> SimulationReadinessReport:
        blocking = [i for i in issues if i.severity == "blocking"]
        warnings = [i for i in issues if i.severity != "blocking"]
        return SimulationReadinessReport(
            season=season,
            week=week,
            game_id=game_id,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            blocking_issues=blocking,
            warning_issues=warnings,
            validated_at=datetime.now(UTC),
        )

    def trait_catalog(self) -> list[TraitCatalogEntry]:
        return list(self._trait_catalog)

    def _finalize(self, issues: list[ValidationIssue]) -> ValidationResult:
        ordered = sorted(issues, key=lambda x: (x.severity, x.code, x.entity_id, x.field_path))
        blocking = [i for i in ordered if i.severity == "blocking"]
        if blocking:
            raise ValidationError(blocking)
        return ValidationResult(ok=True, issues=ordered)

    def _validate_team_readiness(self, team: Franchise) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        by_player = {p.player_id for p in team.roster}
        active = [d for d in team.depth_chart if d.active_flag]
        slots = {d.slot_role for d in active}
        for slot in sorted(ALL_REQUIRED_SLOTS - slots):
            issues.append(
                ValidationIssue(
                    code="MISSING_DEPTH_SLOT",
                    severity="blocking",
                    field_path="depth_chart.slot_role",
                    entity_id=team.team_id,
                    message=f"missing required active slot '{slot}'",
                )
            )
        for assignment in active:
            if assignment.player_id not in by_player:
                issues.append(
                    ValidationIssue(
                        code="DEPTH_ASSIGNMENT_INVALID_PLAYER",
                        severity="blocking",
                        field_path="depth_chart.player_id",
                        entity_id=team.team_id,
                        message=f"slot {assignment.slot_role} references unknown player {assignment.player_id}",
                    )
                )

        for player in team.roster:
            issues.extend(validate_traits(player.player_id, player.traits, self._trait_catalog))
            missing = self._required_traits - set(player.traits.keys())
            if missing:
                issues.append(
                    ValidationIssue(
                        code="INCOMPLETE_TRAIT_VECTOR",
                        severity="blocking",
                        field_path="traits",
                        entity_id=player.player_id,
                        message=f"missing {len(missing)} required traits",
                    )
                )
        return issues

    def _validate_policy(self, policy_id: str) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        try:
            self._resource_resolver.resolve_policy(policy_id)
        except ValidationError as exc:
            issues.extend(exc.issues)
        return issues

    def _validate_playcall_fields(self, playcall: PlaycallRequest) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        try:
            self._resource_resolver.resolve_personnel(playcall.personnel)
        except ValidationError as exc:
            issues.extend(exc.issues)
        try:
            formation = self._resource_resolver.resolve_formation(playcall.formation)
            allowed = formation.get("allowed_personnel", [])
            if playcall.personnel not in allowed:
                issues.append(
                    ValidationIssue(
                        code="FORMATION_PERSONNEL_INCOMPATIBLE",
                        severity="blocking",
                        field_path="playcall.formation",
                        entity_id=playcall.team_id,
                        message=f"formation '{playcall.formation}' incompatible with personnel '{playcall.personnel}'",
                    )
                )
        except ValidationError as exc:
            issues.extend(exc.issues)
        try:
            offense = self._resource_resolver.resolve_concept(playcall.offensive_concept, "offense")
            if playcall.play_type.value not in offense.get("play_types", []):
                issues.append(
                    ValidationIssue(
                        code="OFFENSE_CONCEPT_PLAYTYPE_MISMATCH",
                        severity="blocking",
                        field_path="playcall.offensive_concept",
                        entity_id=playcall.team_id,
                        message=f"concept '{playcall.offensive_concept}' not valid for play type '{playcall.play_type.value}'",
                    )
                )
        except ValidationError as exc:
            issues.extend(exc.issues)
        try:
            defense = self._resource_resolver.resolve_concept(playcall.defensive_concept, "defense")
            if playcall.play_type.value not in defense.get("play_types", []):
                issues.append(
                    ValidationIssue(
                        code="DEFENSE_CONCEPT_PLAYTYPE_MISMATCH",
                        severity="blocking",
                        field_path="playcall.defensive_concept",
                        entity_id=playcall.team_id,
                        message=f"concept '{playcall.defensive_concept}' not valid for play type '{playcall.play_type.value}'",
                    )
                )
        except ValidationError as exc:
            issues.extend(exc.issues)
        return issues

    def _validate_situation(self, scp: SnapContextPackage) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        sit = scp.situation
        if sit.down < 1 or sit.down > 4:
            issues.append(
                ValidationIssue(
                    code="INVALID_DOWN",
                    severity="blocking",
                    field_path="situation.down",
                    entity_id=scp.play_id,
                    message="down must be within [1, 4]",
                )
            )
        if sit.distance < 1:
            issues.append(
                ValidationIssue(
                    code="INVALID_DISTANCE",
                    severity="blocking",
                    field_path="situation.distance",
                    entity_id=scp.play_id,
                    message="distance must be >= 1",
                )
            )
        if sit.yard_line < 1 or sit.yard_line > 99:
            issues.append(
                ValidationIssue(
                    code="INVALID_YARD_LINE",
                    severity="blocking",
                    field_path="situation.yard_line",
                    entity_id=scp.play_id,
                    message="yard line must be within [1, 99]",
                )
            )
        return issues

    def _validate_participants(self, scp: SnapContextPackage) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        if len(scp.participants) != 22:
            issues.append(
                ValidationIssue(
                    code="INVALID_PARTICIPANT_COUNT",
                    severity="blocking",
                    field_path="participants",
                    entity_id=scp.play_id,
                    message=f"expected 22 participants, got {len(scp.participants)}",
                )
            )
        ids = [p.actor_id for p in scp.participants]
        if len(ids) != len(set(ids)):
            issues.append(
                ValidationIssue(
                    code="DUPLICATE_PARTICIPANT",
                    severity="blocking",
                    field_path="participants.actor_id",
                    entity_id=scp.play_id,
                    message="participant actor ids must be unique",
                )
            )
        teams = {p.team_id for p in scp.participants}
        if len(teams) != 2:
            issues.append(
                ValidationIssue(
                    code="INVALID_TEAM_SPLIT",
                    severity="blocking",
                    field_path="participants.team_id",
                    entity_id=scp.play_id,
                    message="participants must include exactly two teams",
                )
            )
        return issues

    def _validate_snap_states(self, scp: SnapContextPackage) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        for participant in scp.participants:
            if participant.actor_id not in scp.in_game_states:
                issues.append(
                    ValidationIssue(
                        code="MISSING_INGAME_STATE",
                        severity="blocking",
                        field_path="in_game_states",
                        entity_id=participant.actor_id,
                        message="participant missing in-game state record",
                    )
                )
        return issues

    def _validate_snap_traits(
        self,
        scp: SnapContextPackage,
        player_lookup: dict[str, Player] | None,
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        for participant in scp.participants:
            traits = scp.trait_vectors.get(participant.actor_id)
            if traits is None:
                issues.append(
                    ValidationIssue(
                        code="MISSING_PARTICIPANT_TRAITS",
                        severity="blocking",
                        field_path="trait_vectors",
                        entity_id=participant.actor_id,
                        message="participant missing trait vector in snap context",
                    )
                )
                continue
            issues.extend(validate_traits(participant.actor_id, traits, self._trait_catalog))
            if player_lookup is not None:
                player = player_lookup.get(participant.actor_id)
                if player is None:
                    issues.append(
                        ValidationIssue(
                            code="PARTICIPANT_NOT_ON_ROSTER",
                            severity="blocking",
                            field_path="participants",
                            entity_id=participant.actor_id,
                            message="participant is not present in provided roster lookup",
                        )
                    )
                    continue
                if traits != player.traits:
                    issues.append(
                        ValidationIssue(
                            code="TRAIT_VECTOR_MISMATCH",
                            severity="blocking",
                            field_path="trait_vectors",
                            entity_id=participant.actor_id,
                            message="snap trait vector does not match authoritative player traits",
                        )
                    )
        return issues

    def _validate_trait_influence_resources(self) -> None:
        issues: list[ValidationIssue] = []
        trait_codes = {t.trait_code for t in self._trait_catalog}
        for play_type in ["run", "pass", "punt", "kickoff", "field_goal", "extra_point", "two_point"]:
            try:
                resource = self._resource_resolver.resolve_trait_influence(play_type)
            except ValidationError as exc:
                issues.extend(exc.issues)
                continue
            try:
                by_family, outcome = parse_influence_profiles(resource)
                required = required_influence_families(play_type)
                missing_families = required - set(by_family.keys())
                if missing_families:
                    issues.append(
                        ValidationIssue(
                            code="MISSING_INFLUENCE_FAMILY",
                            severity="blocking",
                            field_path=f"trait_influences.{play_type}.families",
                            entity_id=play_type,
                            message=f"missing required families: {sorted(missing_families)}",
                        )
                    )
                for family_name, profile in by_family.items():
                    for trait_code in set(profile.offense_weights) | set(profile.defense_weights):
                        if trait_code not in trait_codes:
                            issues.append(
                                ValidationIssue(
                                    code="UNKNOWN_INFLUENCE_TRAIT",
                                    severity="blocking",
                                    field_path=f"trait_influences.{play_type}.{family_name}",
                                    entity_id=play_type,
                                    message=f"unknown trait code '{trait_code}' in influence profile",
                                )
                            )
                    if not profile.offense_weights or not profile.defense_weights:
                        issues.append(
                            ValidationIssue(
                                code="INVALID_INFLUENCE_WEIGHTS",
                                severity="blocking",
                                field_path=f"trait_influences.{play_type}.{family_name}",
                                entity_id=play_type,
                                message="offense_weights and defense_weights must be non-empty",
                            )
                        )
                if outcome.clock_delta_min < 1 or outcome.clock_delta_max < outcome.clock_delta_min:
                    issues.append(
                        ValidationIssue(
                            code="INVALID_OUTCOME_PROFILE_CLOCK",
                            severity="blocking",
                            field_path=f"trait_influences.{play_type}.outcome_profile",
                            entity_id=play_type,
                            message="invalid clock delta bounds",
                        )
                    )
            except ValueError as exc:
                issues.append(
                    ValidationIssue(
                        code="INVALID_INFLUENCE_PROFILE",
                        severity="blocking",
                        field_path=f"trait_influences.{play_type}",
                        entity_id=play_type,
                        message=str(exc),
                    )
                )
        if issues:
            raise ValidationError(issues)

    def debug_snapshot(self) -> dict[str, object]:
        return {
            "required_slots": sorted(ALL_REQUIRED_SLOTS),
            "required_traits": len(self._required_traits),
            "resource_manifests": [asdict(m) for m in self._resource_resolver.resource_manifests()],
        }
