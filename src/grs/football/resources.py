from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from importlib import resources
from typing import Any

from grs.contracts import ResourceManifest, ValidationError, ValidationIssue

EXPECTED_SCHEMA_VERSION = "1.0"


@dataclass(slots=True)
class ResourceBundle:
    manifest: ResourceManifest
    resources_by_id: dict[str, dict[str, Any]]


class ResourceResolver:
    def __init__(self, bundle_overrides: dict[str, dict[str, Any]] | None = None) -> None:
        self._bundle_overrides = bundle_overrides or {}
        self._personnel = self._load_bundle("personnel_packages.json", "personnel_package")
        self._formations = self._load_bundle("formations.json", "formation")
        self._offense = self._load_bundle("concepts_offense.json", "concept_offense")
        self._defense = self._load_bundle("concepts_defense.json", "concept_defense")
        self._policies = self._load_bundle("coaching_policies.json", "coaching_policy")
        self._trait_influences = self._load_bundle("trait_influences.json", "trait_influence_profile")
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

    def resource_manifests(self) -> list[ResourceManifest]:
        return [
            self._personnel.manifest,
            self._formations.manifest,
            self._offense.manifest,
            self._defense.manifest,
            self._policies.manifest,
            self._trait_influences.manifest,
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

    def trait_influence_play_types(self) -> list[str]:
        return sorted(self._trait_influences.resources_by_id.keys())

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

        manifest = ResourceManifest(
            resource_type=str(manifest_data.get("resource_type", "")),
            schema_version=str(manifest_data.get("schema_version", "")),
            resource_version=str(manifest_data.get("resource_version", "")),
            generated_at=str(manifest_data.get("generated_at", "")),
            checksum=str(manifest_data.get("checksum", "")),
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
            allowed = formation.get("allowed_personnel", [])
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
            defaults = policy.get("defaults")
            if not isinstance(defaults, dict):
                issues.append(
                    ValidationIssue(
                        code="INVALID_POLICY_DEFAULTS",
                        severity="blocking",
                        field_path=f"policy.{policy_id}.defaults",
                        entity_id=policy_id,
                        message="defaults must be an object",
                    )
                )
                continue
            for posture, config in defaults.items():
                if not isinstance(config, dict):
                    issues.append(
                        ValidationIssue(
                            code="INVALID_POLICY_POSTURE",
                            severity="blocking",
                            field_path=f"policy.{policy_id}.defaults.{posture}",
                            entity_id=policy_id,
                            message="posture config must be an object",
                        )
                    )
                    continue
                self._check_policy_reference(
                    issues,
                    policy_id,
                    posture,
                    config,
                    "personnel",
                    self._personnel.resources_by_id,
                )
                self._check_policy_reference(
                    issues,
                    policy_id,
                    posture,
                    config,
                    "formation_pass",
                    self._formations.resources_by_id,
                )
                self._check_policy_reference(
                    issues,
                    policy_id,
                    posture,
                    config,
                    "formation_run",
                    self._formations.resources_by_id,
                )
                self._check_policy_reference(
                    issues,
                    policy_id,
                    posture,
                    config,
                    "offense_pass",
                    self._offense.resources_by_id,
                )
                self._check_policy_reference(
                    issues,
                    policy_id,
                    posture,
                    config,
                    "offense_run",
                    self._offense.resources_by_id,
                )
                self._check_policy_reference(
                    issues,
                    policy_id,
                    posture,
                    config,
                    "defense_base",
                    self._defense.resources_by_id,
                )

        if issues:
            raise ValidationError(issues)

    def _check_policy_reference(
        self,
        issues: list[ValidationIssue],
        policy_id: str,
        posture: str,
        config: dict[str, Any],
        field_name: str,
        allowed: dict[str, dict[str, Any]],
    ) -> None:
        if field_name not in config:
            return
        value = str(config[field_name])
        if value not in allowed:
            issues.append(
                ValidationIssue(
                    code="POLICY_REFERENCE_MISSING",
                    severity="blocking",
                    field_path=f"policy.{policy_id}.defaults.{posture}.{field_name}",
                    entity_id=policy_id,
                    message=f"references unknown id '{value}'",
                )
            )
