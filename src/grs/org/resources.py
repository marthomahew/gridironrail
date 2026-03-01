from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from importlib import resources
from typing import Any

from grs.contracts import (
    LeagueIdentityProfile,
    PlayerIdentityRecord,
    ResourceManifest,
    TeamIdentityRecord,
    ValidationError,
    ValidationIssue,
)
from grs.football.traits import canonical_trait_catalog, generate_player_traits, validate_traits
from grs.org.entities import Player

EXPECTED_SCHEMA_VERSION = "1.0"


@dataclass(slots=True)
class OrgResourceBundle:
    manifest: ResourceManifest
    resources_by_id: dict[str, dict[str, Any]]


@dataclass(slots=True)
class PlayerIdentityPool:
    pool_id: str
    first_names: list[str]
    last_names: list[str]
    hometowns: list[dict[str, str]]


class OrgResourceResolver:
    def __init__(self, bundle_overrides: dict[str, dict[str, Any]] | None = None) -> None:
        self._bundle_overrides = bundle_overrides if bundle_overrides is not None else {}
        self._identity_profiles = self._load_bundle("league_identity_profiles.json", "league_identity_profile")
        self._identity_pools = self._load_bundle("player_identity_pools.json", "player_identity_pool")

    def resolve_league_identity_profile(self, profile_id: str) -> LeagueIdentityProfile:
        raw = self._resolve(self._identity_profiles, profile_id, "UNKNOWN_LEAGUE_IDENTITY_PROFILE")
        conference_names = [str(v) for v in raw.get("conference_names", [])]
        division_names = {
            str(key): [str(v) for v in values]
            for key, values in dict(raw.get("division_names", {})).items()
        }
        teams_raw = list(raw.get("teams", []))
        teams: list[dict[str, str]] = []
        typed_teams: list[TeamIdentityRecord] = []
        for entry in teams_raw:
            if not isinstance(entry, dict):
                continue
            conference_name = str(entry.get("conference_name", ""))
            division_name = str(entry.get("division_name", ""))
            conference_index = conference_names.index(conference_name) + 1 if conference_name in conference_names else 1
            division_candidates = division_names.get(conference_name, [])
            division_index = division_candidates.index(division_name) + 1 if division_name in division_candidates else 1
            teams.append(
                {
                    "team_id": str(entry.get("team_id", "")),
                    "team_name": str(entry.get("team_name", "")),
                    "conference_name": conference_name,
                    "division_name": division_name,
                }
            )
            typed_teams.append(
                TeamIdentityRecord(
                    team_id=str(entry.get("team_id", "")),
                    team_name=str(entry.get("team_name", "")),
                    conference_id=f"C{conference_index:02d}",
                    conference_name=conference_name,
                    division_id=f"C{conference_index:02d}D{division_index:02d}",
                    division_name=division_name,
                )
            )
        return LeagueIdentityProfile(
            profile_id=profile_id,
            conference_names=conference_names,
            division_names=division_names,
            teams=typed_teams,
            schema_version=self._identity_profiles.manifest.schema_version,
            resource_version=self._identity_profiles.manifest.resource_version,
            checksum=self._identity_profiles.manifest.checksum,
            kind=str(raw.get("kind", "fixed")),
            name_bank=dict(raw.get("name_bank", {})) if isinstance(raw.get("name_bank"), dict) else {},
            fixed_teams=teams,
        )

    def resolve_player_identity_pool(self, pool_id: str) -> PlayerIdentityPool:
        raw = self._resolve(self._identity_pools, pool_id, "UNKNOWN_PLAYER_IDENTITY_POOL")
        first_names = [str(v) for v in raw.get("first_names", [])]
        last_names = [str(v) for v in raw.get("last_names", [])]
        hometowns = [dict(v) for v in raw.get("hometowns", []) if isinstance(v, dict)]
        if not first_names or not last_names or not hometowns:
            issues = [
                ValidationIssue(
                    code="INVALID_PLAYER_IDENTITY_POOL",
                    severity="blocking",
                    field_path=f"player_identity_pools.{pool_id}",
                    entity_id=pool_id,
                    message="first_names, last_names, and hometowns must be non-empty",
                )
            ]
            raise ValidationError(issues)
        return PlayerIdentityPool(pool_id=pool_id, first_names=first_names, last_names=last_names, hometowns=hometowns)

    def list_league_identity_profiles(self) -> list[str]:
        return sorted(self._identity_profiles.resources_by_id.keys())

    def _resolve(self, bundle: OrgResourceBundle, key: str, error_code: str) -> dict[str, Any]:
        if key not in bundle.resources_by_id:
            raise ValidationError(
                [
                    ValidationIssue(
                        code=error_code,
                        severity="blocking",
                        field_path="resource_id",
                        entity_id=key,
                        message=f"{bundle.manifest.resource_type} id '{key}' is not registered",
                    )
                ]
            )
        return bundle.resources_by_id[key]

    def _load_bundle(self, filename: str, expected_type: str) -> OrgResourceBundle:
        if filename in self._bundle_overrides:
            payload = self._bundle_overrides[filename]
        else:
            package = resources.files("grs.resources.org")
            payload = json.loads((package / filename).read_text(encoding="utf-8"))
        manifest_raw = payload.get("manifest")
        resources_raw = payload.get("resources")
        if not isinstance(manifest_raw, dict) or not isinstance(resources_raw, list):
            raise ValidationError(
                [
                    ValidationIssue(
                        code="INVALID_RESOURCE_BUNDLE",
                        severity="blocking",
                        field_path=filename,
                        entity_id=expected_type,
                        message="resource bundle must include manifest and resources",
                    )
                ]
            )
        manifest_required = {"resource_type", "schema_version", "resource_version", "generated_at", "checksum"}
        missing = sorted(manifest_required - set(manifest_raw.keys()))
        if missing:
            raise ValidationError(
                [
                    ValidationIssue(
                        code="MISSING_REQUIRED_RUNTIME_CONFIG",
                        severity="blocking",
                        field_path=f"{filename}.manifest",
                        entity_id=expected_type,
                        message=f"manifest missing required fields: {missing}",
                    )
                ]
            )
        manifest = ResourceManifest(
            resource_type=str(manifest_raw["resource_type"]),
            schema_version=str(manifest_raw["schema_version"]),
            resource_version=str(manifest_raw["resource_version"]),
            generated_at=str(manifest_raw["generated_at"]),
            checksum=str(manifest_raw["checksum"]),
        )
        issues = self._validate_manifest(manifest, expected_type, resources_raw)
        if issues:
            raise ValidationError(issues)
        by_id: dict[str, dict[str, Any]] = {}
        for entry in resources_raw:
            if not isinstance(entry, dict):
                continue
            resource_id = str(entry.get("id", ""))
            if not resource_id:
                continue
            by_id[resource_id] = dict(entry)
        if not by_id:
            raise ValidationError(
                [
                    ValidationIssue(
                        code="EMPTY_RESOURCE_SET",
                        severity="blocking",
                        field_path=filename,
                        entity_id=expected_type,
                        message="resource bundle contains no usable ids",
                    )
                ]
            )
        return OrgResourceBundle(manifest=manifest, resources_by_id=by_id)

    def _validate_manifest(
        self,
        manifest: ResourceManifest,
        expected_type: str,
        resources_raw: list[dict[str, Any]],
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
        canonical = json.dumps(resources_raw, sort_keys=True, separators=(",", ":")).encode("utf-8")
        checksum = hashlib.sha256(canonical).hexdigest()
        if checksum != manifest.checksum:
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


class PlayerCreationEngine:
    AGE_RANGES: dict[str, tuple[int, int]] = {
        "QB": (21, 37),
        "RB": (20, 32),
        "WR": (20, 34),
        "TE": (21, 35),
        "OL": (21, 36),
        "DL": (21, 35),
        "LB": (21, 35),
        "CB": (20, 34),
        "S": (20, 35),
        "K": (21, 40),
        "P": (21, 40),
    }
    JERSEY_POOLS: dict[str, list[int]] = {
        "QB": list(range(1, 20)),
        "RB": list(range(1, 50)),
        "WR": list(range(1, 20)) + list(range(80, 90)),
        "TE": list(range(40, 50)) + list(range(80, 90)),
        "OL": list(range(50, 80)),
        "DL": list(range(50, 80)) + list(range(90, 100)),
        "LB": list(range(40, 60)) + list(range(90, 100)),
        "CB": list(range(1, 50)),
        "S": list(range(1, 50)),
        "K": list(range(1, 20)),
        "P": list(range(1, 20)),
    }
    ARCHETYPES: dict[str, list[str]] = {
        "QB": ["Field General", "Gunslinger", "Dual Threat"],
        "RB": ["Power Back", "One-Cut Runner", "Receiving Back"],
        "WR": ["Route Artist", "Deep Threat", "YAC Specialist"],
        "TE": ["Balanced TE", "Inline Blocker", "Move TE"],
        "OL": ["Technician", "Mauler", "Anchor"],
        "DL": ["Pocket Crusher", "Run Plugger", "Penetrator"],
        "LB": ["Mike General", "Edge Hunter", "Coverage Linebacker"],
        "CB": ["Press Corner", "Mirror Corner", "Ball Hawk"],
        "S": ["Centerfielder", "Box Safety", "Hybrid Safety"],
        "K": ["Power Kicker", "Precision Kicker"],
        "P": ["Directional Punter", "Hang-Time Punter"],
    }

    def __init__(self, *, resource_resolver: OrgResourceResolver, identity_pool_id: str = "default_player_identity_v1") -> None:
        self._resolver = resource_resolver
        self._identity_pool_id = identity_pool_id
        self._trait_catalog = canonical_trait_catalog()
        self._required_traits = {trait.trait_code for trait in self._trait_catalog if trait.required}

    def create_player(
        self,
        *,
        player_id: str,
        team_id: str,
        position: str,
        overall_truth: float,
        volatility_truth: float,
        injury_susceptibility_truth: float,
        hidden_dev_curve: float,
        rand,
        used_jerseys: set[int],
    ) -> Player:
        identity = self._create_identity(position=position, rand=rand, used_jerseys=used_jerseys)
        traits = generate_player_traits(
            player_id=player_id,
            position=position,
            overall_truth=overall_truth,
            volatility_truth=volatility_truth,
            injury_susceptibility_truth=injury_susceptibility_truth,
        )
        missing = self._required_traits - set(traits.keys())
        if missing:
            raise ValueError(f"player generation missing required traits for '{player_id}': {sorted(missing)}")
        issues = validate_traits(player_id, traits, self._trait_catalog)
        if issues:
            issue_text = "; ".join(f"{issue.code}:{issue.message}" for issue in issues)
            raise ValueError(f"invalid generated traits for '{player_id}': {issue_text}")
        return Player(
            player_id=player_id,
            team_id=team_id,
            name=identity.display_name,
            first_name=identity.first_name,
            last_name=identity.last_name,
            display_name=identity.display_name,
            archetype=identity.archetype,
            jersey_number=identity.jersey_number,
            hometown=identity.hometown,
            state_province=identity.state_province,
            position=position,
            age=self._generate_age(position=position, rand=rand),
            overall_truth=overall_truth,
            volatility_truth=volatility_truth,
            injury_susceptibility_truth=injury_susceptibility_truth,
            hidden_dev_curve=hidden_dev_curve,
            traits=traits,
        )

    def generate_identity(self, *, position: str, rand, used_jerseys: set[int]) -> PlayerIdentityRecord:
        return self._create_identity(position=position, rand=rand, used_jerseys=used_jerseys)

    def _create_identity(self, *, position: str, rand, used_jerseys: set[int]) -> PlayerIdentityRecord:
        pool = self._resolver.resolve_player_identity_pool(self._identity_pool_id)
        first_name = str(rand.choice(pool.first_names))
        last_name = str(rand.choice(pool.last_names))
        hometown_entry = dict(rand.choice(pool.hometowns))
        jersey_number = self._next_jersey(position=position, used_jerseys=used_jerseys, rand=rand)
        archetypes = self.ARCHETYPES.get(position)
        if not archetypes:
            raise ValueError(f"missing archetype profile for position '{position}'")
        return PlayerIdentityRecord(
            first_name=first_name,
            last_name=last_name,
            display_name=f"{first_name} {last_name}",
            archetype=str(rand.choice(archetypes)),
            jersey_number=jersey_number,
            hometown=str(hometown_entry.get("city", "")),
            state_province=str(hometown_entry.get("state", "")),
        )

    def _generate_age(self, *, position: str, rand) -> int:
        if position not in self.AGE_RANGES:
            raise ValueError(f"unsupported position '{position}' for age generation")
        low, high = self.AGE_RANGES[position]
        return rand.randint(low, high)

    def _next_jersey(self, *, position: str, used_jerseys: set[int], rand) -> int:
        pool = self.JERSEY_POOLS.get(position)
        if not pool:
            raise ValueError(f"missing jersey pool for position '{position}'")
        candidates = [number for number in pool if number not in used_jerseys]
        if not candidates:
            raise ValueError(f"no available jersey numbers left for position '{position}'")
        jersey = int(rand.choice(candidates))
        used_jerseys.add(jersey)
        return jersey
