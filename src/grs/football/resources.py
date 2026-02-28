from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from importlib import resources
from typing import Any

from grs.contracts import (
    AssignmentTemplate,
    PlayType,
    PlaybookEntry,
    ResourceManifest,
    TraitRoleMappingEntry,
    TraitStatus,
    ValidationError,
    ValidationIssue,
)

EXPECTED_SCHEMA_VERSION = "1.0"


@dataclass(slots=True)
class ResourceBundle:
    manifest: ResourceManifest
    resources_by_id: dict[str, dict[str, Any]]


class ResourceResolver:
    """Data-pack backed registry adapter for football runtime resources."""

    def __init__(self, bundle_overrides: dict[str, dict[str, Any]] | None = None) -> None:
        self._bundle_overrides = bundle_overrides or {}
        self._personnel = self._load_bundle("personnel_packages.json", "personnel_package")
        self._formations = self._load_bundle("formations.json", "formation")
        self._offense = self._load_bundle("concepts_offense.json", "concept_offense")
        self._defense = self._load_bundle("concepts_defense.json", "concept_defense")
        self._policies = self._load_bundle("coaching_policies.json", "coaching_policy")
        self._trait_influences = self._load_bundle("trait_influences.json", "trait_influence_profile")
        self._playbook = self._load_bundle("playbook_entries.json", "playbook_entry")
        self._assignment_templates = self._load_bundle("assignment_templates.json", "assignment_template")
        self._trait_role_mapping = self._load_bundle("trait_role_mapping.json", "trait_role_mapping")
        self._rules_profiles = self._load_bundle("rules_profiles.json", "rules_profile")
        self._validate_cross_references()

    def resolve_personnel(self, personnel_id: str) -> dict[str, Any]:
        return self._resolve(self._personnel, personnel_id, "UNKNOWN_PERSONNEL")

    def resolve_formation(self, formation_id: str) -> dict[str, Any]:
        return self._resolve(self._formations, formation_id, "UNKNOWN_FORMATION")

    def resolve_concept(self, concept_id: str, side: str) -> dict[str, Any]:
        if side not in {"offense", "defense"}:
            raise ValueError("side must be 'offense' or 'defense'")
        bundle = self._offense if side == "offense" else self._defense
        error_code = "UNKNOWN_OFFENSE_CONCEPT" if side == "offense" else "UNKNOWN_DEFENSE_CONCEPT"
        return self._resolve(bundle, concept_id, error_code)

    def resolve_policy(self, policy_id: str) -> dict[str, Any]:
        return self._resolve(self._policies, policy_id, "UNKNOWN_COACHING_POLICY")

    def resolve_trait_influence(self, play_type: str) -> dict[str, Any]:
        return self._resolve(self._trait_influences, play_type, "UNKNOWN_TRAIT_INFLUENCE_PLAYTYPE")

    def resolve_playbook_entry(self, play_id: str) -> PlaybookEntry:
        raw = self._resolve(self._playbook, play_id, "UNKNOWN_PLAYBOOK_ENTRY")
        required_fields = {
            "family",
            "personnel_id",
            "formation_id",
            "offensive_concept_id",
            "defensive_concept_id",
            "assignment_template_id",
        }
        missing = sorted(required_fields - set(raw.keys()))
        if missing:
            issue = ValidationIssue(
                code="MISSING_REQUIRED_RUNTIME_CONFIG",
                severity="blocking",
                field_path=f"playbook.{play_id}",
                entity_id=play_id,
                message=f"missing required fields: {missing}",
            )
            raise ValidationError([issue])
        try:
            play_type = PlayType(str(raw["play_type"]))
        except ValueError as exc:
            issue = ValidationIssue(
                code="INVALID_PLAYBOOK_PLAY_TYPE",
                severity="blocking",
                field_path=f"playbook.{play_id}.play_type",
                entity_id=play_id,
                message=f"unsupported play_type '{raw.get('play_type')}'",
            )
            raise ValidationError([issue]) from exc
        return PlaybookEntry(
            play_id=play_id,
            play_type=play_type,
            family=str(raw["family"]),
            personnel_id=str(raw["personnel_id"]),
            formation_id=str(raw["formation_id"]),
            offensive_concept_id=str(raw["offensive_concept_id"]),
            defensive_concept_id=str(raw["defensive_concept_id"]),
            assignment_template_id=str(raw["assignment_template_id"]),
            branch_trigger_ids=[str(v) for v in raw.get("branch_trigger_ids", [])],
            tags=[str(v) for v in raw.get("tags", [])],
        )

    def resolve_playbook_entry_for_intent(
        self,
        *,
        play_type: PlayType,
        personnel_id: str,
        formation_id: str,
        offensive_concept_id: str,
        defensive_concept_id: str,
    ) -> PlaybookEntry:
        for play_id in self.playbook_ids():
            entry = self.resolve_playbook_entry(play_id)
            if (
                entry.play_type == play_type
                and entry.personnel_id == personnel_id
                and entry.formation_id == formation_id
                and entry.offensive_concept_id == offensive_concept_id
                and entry.defensive_concept_id == defensive_concept_id
            ):
                return entry
        issue = ValidationIssue(
            code="PLAYBOOK_INTENT_UNRESOLVABLE",
            severity="blocking",
            field_path="playcall",
            entity_id=f"{play_type.value}:{personnel_id}:{formation_id}:{offensive_concept_id}:{defensive_concept_id}",
            message="no playbook entry matches playcall intent",
        )
        raise ValidationError([issue])

    def resolve_assignment_template(self, template_id: str) -> AssignmentTemplate:
        raw = self._resolve(self._assignment_templates, template_id, "UNKNOWN_ASSIGNMENT_TEMPLATE")
        required_fields = {"offense_roles", "defense_roles", "pairing_hints", "default_technique"}
        missing = sorted(required_fields - set(raw.keys()))
        if missing:
            issue = ValidationIssue(
                code="MISSING_REQUIRED_RUNTIME_CONFIG",
                severity="blocking",
                field_path=f"assignment_template.{template_id}",
                entity_id=template_id,
                message=f"missing required fields: {missing}",
            )
            raise ValidationError([issue])
        return AssignmentTemplate(
            template_id=template_id,
            offense_roles=[str(v) for v in raw["offense_roles"]],
            defense_roles=[str(v) for v in raw["defense_roles"]],
            pairing_hints=[dict(item) for item in raw["pairing_hints"] if isinstance(item, dict)],
            default_technique=str(raw["default_technique"]),
        )

    def resolve_trait_role_mappings(self) -> list[TraitRoleMappingEntry]:
        rows: list[TraitRoleMappingEntry] = []
        for row in self._trait_role_mapping.resources_by_id.values():
            required_fields = {"trait_code", "status", "phase", "contest_family", "role_group", "evidence_tag"}
            missing = sorted(required_fields - set(row.keys()))
            if missing:
                issue = ValidationIssue(
                    code="MISSING_REQUIRED_RUNTIME_CONFIG",
                    severity="blocking",
                    field_path="trait_role_mapping",
                    entity_id=str(row.get("id", "unknown")),
                    message=f"missing required fields: {missing}",
                )
                raise ValidationError([issue])
            status = TraitStatus(str(row["status"]))
            rows.append(
                TraitRoleMappingEntry(
                    trait_code=str(row["trait_code"]),
                    status=status,
                    phase=str(row["phase"]),
                    contest_family=str(row["contest_family"]),
                    role_group=str(row["role_group"]),
                    evidence_tag=str(row["evidence_tag"]),
                )
            )
        return rows

    def resolve_rules_profile(self, rules_profile_id: str) -> dict[str, Any]:
        return self._resolve(self._rules_profiles, rules_profile_id, "UNKNOWN_RULES_PROFILE")

    def resource_manifests(self) -> list[ResourceManifest]:
        return [
            self._personnel.manifest,
            self._formations.manifest,
            self._offense.manifest,
            self._defense.manifest,
            self._policies.manifest,
            self._trait_influences.manifest,
            self._playbook.manifest,
            self._assignment_templates.manifest,
            self._trait_role_mapping.manifest,
            self._rules_profiles.manifest,
        ]

    def personnel_ids(self) -> list[str]:
        return sorted(self._personnel.resources_by_id.keys())

    def formation_ids(self) -> list[str]:
        return sorted(self._formations.resources_by_id.keys())

    def offense_concept_ids(self) -> list[str]:
        return sorted(self._offense.resources_by_id.keys())

    def defense_concept_ids(self) -> list[str]:
        return sorted(self._defense.resources_by_id.keys())

    def policy_ids(self) -> list[str]:
        return sorted(self._policies.resources_by_id.keys())

    def playbook_ids(self) -> list[str]:
        return sorted(self._playbook.resources_by_id.keys())

    def assignment_template_ids(self) -> list[str]:
        return sorted(self._assignment_templates.resources_by_id.keys())

    def rules_profile_ids(self) -> list[str]:
        return sorted(self._rules_profiles.resources_by_id.keys())

    def _resolve(self, bundle: ResourceBundle, key: str, error_code: str) -> dict[str, Any]:
        if key not in bundle.resources_by_id:
            issue = ValidationIssue(
                code=error_code,
                severity="blocking",
                field_path="resource_id",
                entity_id=key,
                message=f"{bundle.manifest.resource_type} id '{key}' is not registered",
            )
            raise ValidationError([issue])
        return bundle.resources_by_id[key]

    def _load_bundle(self, filename: str, expected_type: str) -> ResourceBundle:
        if filename in self._bundle_overrides:
            payload = self._bundle_overrides[filename]
        else:
            package = resources.files("grs.resources.football")
            payload = json.loads((package / filename).read_text(encoding="utf-8"))
        manifest_data = payload.get("manifest")
        resources_list = payload.get("resources")
        if not isinstance(manifest_data, dict) or not isinstance(resources_list, list):
            issue = ValidationIssue(
                code="INVALID_RESOURCE_BUNDLE",
                severity="blocking",
                field_path=filename,
                entity_id=expected_type,
                message="resource bundle must provide manifest and resources list",
            )
            raise ValidationError([issue])

        required_manifest_fields = {"resource_type", "schema_version", "resource_version", "generated_at", "checksum"}
        missing_manifest = sorted(required_manifest_fields - set(manifest_data.keys()))
        if missing_manifest:
            issue = ValidationIssue(
                code="MISSING_REQUIRED_RUNTIME_CONFIG",
                severity="blocking",
                field_path=f"{filename}.manifest",
                entity_id=expected_type,
                message=f"manifest missing required fields {missing_manifest}",
            )
            raise ValidationError([issue])

        manifest = ResourceManifest(
            resource_type=str(manifest_data["resource_type"]),
            schema_version=str(manifest_data["schema_version"]),
            resource_version=str(manifest_data["resource_version"]),
            generated_at=str(manifest_data["generated_at"]),
            checksum=str(manifest_data["checksum"]),
        )
        issues = self._validate_manifest(manifest, expected_type, resources_list)
        if issues:
            raise ValidationError(issues)

        by_id: dict[str, dict[str, Any]] = {}
        for entry in resources_list:
            if not isinstance(entry, dict):
                continue
            rid = str(entry.get("id", ""))
            if not rid:
                continue
            by_id[rid] = dict(entry)
        if not by_id:
            issue = ValidationIssue(
                code="EMPTY_RESOURCE_SET",
                severity="blocking",
                field_path=filename,
                entity_id=expected_type,
                message="resource bundle contains no usable resource ids",
            )
            raise ValidationError([issue])
        return ResourceBundle(manifest=manifest, resources_by_id=by_id)

    def _validate_manifest(
        self,
        manifest: ResourceManifest,
        expected_type: str,
        resources_list: list[dict[str, Any]],
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        if manifest.resource_type != expected_type:
            issues.append(
                ValidationIssue(
                    code="RESOURCE_TYPE_MISMATCH",
                    severity="blocking",
                    field_path="manifest.resource_type",
                    entity_id=expected_type,
                    message=f"expected '{expected_type}', got '{manifest.resource_type}'",
                )
            )
        if manifest.schema_version != EXPECTED_SCHEMA_VERSION:
            issues.append(
                ValidationIssue(
                    code="RESOURCE_SCHEMA_MISMATCH",
                    severity="blocking",
                    field_path="manifest.schema_version",
                    entity_id=expected_type,
                    message=f"expected schema {EXPECTED_SCHEMA_VERSION}, got {manifest.schema_version}",
                )
            )
        canonical = json.dumps(resources_list, sort_keys=True, separators=(",", ":")).encode("utf-8")
        checksum = hashlib.sha256(canonical).hexdigest()
        if manifest.checksum != checksum:
            issues.append(
                ValidationIssue(
                    code="RESOURCE_CHECKSUM_MISMATCH",
                    severity="blocking",
                    field_path="manifest.checksum",
                    entity_id=expected_type,
                    message=f"expected {checksum}, got {manifest.checksum}",
                )
            )
        return issues

    def _validate_cross_references(self) -> None:
        issues: list[ValidationIssue] = []

        for formation_id, formation in self._formations.resources_by_id.items():
            if "allowed_personnel" not in formation:
                issues.append(
                    ValidationIssue(
                        code="MISSING_REQUIRED_RUNTIME_CONFIG",
                        severity="blocking",
                        field_path=f"formation.{formation_id}.allowed_personnel",
                        entity_id=formation_id,
                        message="required field 'allowed_personnel' is missing",
                    )
                )
                continue
            allowed = formation["allowed_personnel"]
            if not isinstance(allowed, list):
                issues.append(
                    ValidationIssue(
                        code="INVALID_FORMATION_PERSONNEL",
                        severity="blocking",
                        field_path=f"formation.{formation_id}.allowed_personnel",
                        entity_id=formation_id,
                        message="allowed_personnel must be a list",
                    )
                )
                continue
            for pid in allowed:
                if pid not in self._personnel.resources_by_id:
                    issues.append(
                        ValidationIssue(
                            code="FORMATION_PERSONNEL_REF_MISSING",
                            severity="blocking",
                            field_path=f"formation.{formation_id}.allowed_personnel",
                            entity_id=formation_id,
                            message=f"references missing personnel '{pid}'",
                        )
                    )

        for policy_id, policy in self._policies.resources_by_id.items():
            defaults = policy["defaults"] if "defaults" in policy else None
            if defaults is not None and not isinstance(defaults, dict):
                issues.append(
                    ValidationIssue(
                        code="INVALID_POLICY_DEFAULTS",
                        severity="blocking",
                        field_path=f"policy.{policy_id}.defaults",
                        entity_id=policy_id,
                        message="defaults must be an object",
                    )
                )
            playbook_by_posture = policy["playbook_by_posture"] if "playbook_by_posture" in policy else None
            if not isinstance(playbook_by_posture, dict):
                issues.append(
                    ValidationIssue(
                        code="INVALID_POLICY_PLAYBOOK_MAP",
                        severity="blocking",
                        field_path=f"policy.{policy_id}.playbook_by_posture",
                        entity_id=policy_id,
                        message="playbook_by_posture must be an object",
                    )
                )
                continue
            for posture, play_ids in playbook_by_posture.items():
                if not isinstance(play_ids, list) or not play_ids:
                    issues.append(
                        ValidationIssue(
                            code="INVALID_POLICY_PLAYLIST",
                            severity="blocking",
                            field_path=f"policy.{policy_id}.playbook_by_posture.{posture}",
                            entity_id=policy_id,
                            message="posture playlist must be a non-empty list",
                        )
                    )
                    continue
                for play_id in play_ids:
                    if str(play_id) not in self._playbook.resources_by_id:
                        issues.append(
                            ValidationIssue(
                                code="POLICY_PLAYBOOK_REF_MISSING",
                                severity="blocking",
                                field_path=f"policy.{policy_id}.playbook_by_posture.{posture}",
                                entity_id=policy_id,
                                message=f"references unknown playbook id '{play_id}'",
                            )
                        )

        for play_id, play in self._playbook.resources_by_id.items():
            self._ensure_ref(
                issues,
                play_id,
                "personnel_id",
                play,
                self._personnel.resources_by_id,
                "PLAYBOOK_PERSONNEL_REF_MISSING",
            )
            self._ensure_ref(
                issues,
                play_id,
                "formation_id",
                play,
                self._formations.resources_by_id,
                "PLAYBOOK_FORMATION_REF_MISSING",
            )
            self._ensure_ref(
                issues,
                play_id,
                "offensive_concept_id",
                play,
                self._offense.resources_by_id,
                "PLAYBOOK_OFFENSE_CONCEPT_REF_MISSING",
            )
            self._ensure_ref(
                issues,
                play_id,
                "defensive_concept_id",
                play,
                self._defense.resources_by_id,
                "PLAYBOOK_DEFENSE_CONCEPT_REF_MISSING",
            )
            self._ensure_ref(
                issues,
                play_id,
                "assignment_template_id",
                play,
                self._assignment_templates.resources_by_id,
                "PLAYBOOK_TEMPLATE_REF_MISSING",
            )

        for template_id, template in self._assignment_templates.resources_by_id.items():
            if "offense_roles" not in template or "defense_roles" not in template:
                issues.append(
                    ValidationIssue(
                        code="MISSING_REQUIRED_RUNTIME_CONFIG",
                        severity="blocking",
                        field_path=f"assignment_template.{template_id}",
                        entity_id=template_id,
                        message="assignment template missing offense_roles or defense_roles",
                    )
                )
                continue
            offense_roles = template["offense_roles"]
            defense_roles = template["defense_roles"]
            if not isinstance(offense_roles, list) or not isinstance(defense_roles, list):
                issues.append(
                    ValidationIssue(
                        code="INVALID_ASSIGNMENT_TEMPLATE_ROLES",
                        severity="blocking",
                        field_path=f"assignment_template.{template_id}",
                        entity_id=template_id,
                        message="offense_roles and defense_roles must be lists",
                    )
                )
                continue
            if len(offense_roles) != 11 or len(defense_roles) != 11:
                issues.append(
                    ValidationIssue(
                        code="ASSIGNMENT_TEMPLATE_ROLE_COUNT",
                        severity="blocking",
                        field_path=f"assignment_template.{template_id}",
                        entity_id=template_id,
                        message="assignment templates must define 11 offense and 11 defense roles",
                    )
                )

        for mapping_id, row in self._trait_role_mapping.resources_by_id.items():
            if "trait_code" not in row or not row["trait_code"]:
                issues.append(
                    ValidationIssue(
                        code="INVALID_TRAIT_ROLE_MAPPING",
                        severity="blocking",
                        field_path=f"trait_role_mapping.{mapping_id}",
                        entity_id=mapping_id,
                        message="trait_code is required",
                    )
                )
            try:
                if "status" not in row:
                    raise ValueError("missing status")
                TraitStatus(str(row["status"]))
            except ValueError:
                issues.append(
                    ValidationIssue(
                        code="INVALID_TRAIT_ROLE_STATUS",
                        severity="blocking",
                        field_path=f"trait_role_mapping.{mapping_id}.status",
                        entity_id=mapping_id,
                        message="status must be core_now or reserved_phasal",
                    )
                )

        if not self._rules_profiles.resources_by_id:
            issues.append(
                ValidationIssue(
                    code="MISSING_RULES_PROFILES",
                    severity="blocking",
                    field_path="rules_profiles",
                    entity_id="rules_profiles",
                    message="at least one rules profile is required",
                )
            )

        if issues:
            raise ValidationError(issues)

    def _ensure_ref(
        self,
        issues: list[ValidationIssue],
        resource_id: str,
        field_name: str,
        payload: dict[str, Any],
        allowed: dict[str, dict[str, Any]],
        code: str,
    ) -> None:
        if field_name not in payload:
            issues.append(
                ValidationIssue(
                    code="MISSING_REQUIRED_RUNTIME_CONFIG",
                    severity="blocking",
                    field_path=f"playbook.{resource_id}.{field_name}",
                    entity_id=resource_id,
                    message=f"required field '{field_name}' missing",
                )
            )
            return
        value = str(payload[field_name])
        if value not in allowed:
            issues.append(
                ValidationIssue(
                    code=code,
                    severity="blocking",
                    field_path=f"playbook.{resource_id}.{field_name}",
                    entity_id=resource_id,
                    message=f"references unknown id '{value}'",
                )
            )
