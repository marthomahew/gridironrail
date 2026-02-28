from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Mapping, Protocol, Sequence


class PlayType(str, Enum):
    RUN = "run"
    PASS = "pass"
    PUNT = "punt"
    KICKOFF = "kickoff"
    FIELD_GOAL = "field_goal"
    EXTRA_POINT = "extra_point"
    TWO_POINT = "two_point"


class SimMode(str, Enum):
    PLAY = "play"
    SIM = "sim"
    OFFSCREEN = "offscreen"


class Difficulty(str, Enum):
    ROOKIE = "rookie"
    PRO = "pro"
    ALL_PRO = "all_pro"
    ALL_MADDEN = "all_madden"


class ActionType(str, Enum):
    ADVANCE_WEEK = "advance_week"
    PLAY_USER_GAME = "play_user_game"
    PLAY_SNAP = "play_snap"
    SIM_DRIVE = "sim_drive"
    LOAD_RETAINED = "load_retained"
    DEBUG_TRUTH = "debug_truth"
    GET_ORG_OVERVIEW = "get_org_overview"
    GET_STANDINGS = "get_standings"
    GET_GAME_STATE = "get_game_state"
    GET_RETAINED_GAMES = "get_retained_games"
    GET_FILM_ROOM_GAME = "get_film_room_game"
    GET_ANALYTICS_SERIES = "get_analytics_series"
    SET_PLAYCALL = "set_playcall"


class TraitStatus(str, Enum):
    CORE_NOW = "core_now"
    RESERVED_PHASAL = "reserved_phasal"


class RandomSource(Protocol):
    def rand(self) -> float: ...

    def randint(self, a: int, b: int) -> int: ...

    def choice(self, items: Sequence[Any]) -> Any: ...

    def shuffle(self, items: list[Any]) -> None: ...

    def spawn(self, substream_id: str) -> RandomSource: ...


@dataclass(slots=True)
class ActorRef:
    actor_id: str
    team_id: str
    role: str


@dataclass(slots=True)
class Situation:
    quarter: int
    clock_seconds: int
    down: int
    distance: int
    yard_line: int
    possession_team_id: str
    score_diff: int
    timeouts_offense: int
    timeouts_defense: int


@dataclass(slots=True)
class InGameState:
    fatigue: float
    acute_wear: float
    confidence_tilt: float
    injury_limitation: str = "none"
    discipline_risk: float = 0.0


@dataclass(slots=True)
class ParameterizedIntent:
    personnel: str
    formation: str
    offensive_concept: str
    defensive_concept: str
    playbook_entry_id: str | None = None
    assignment_template_id: str | None = None
    rules_profile_id: str = "nfl_placeholder_v1"
    tempo: str = "normal"
    aggression: str = "balanced"
    allows_audible: bool = True
    play_type: PlayType = PlayType.PASS


@dataclass(slots=True)
class PlaycallRequest:
    team_id: str
    personnel: str
    formation: str
    offensive_concept: str
    defensive_concept: str
    playbook_entry_id: str | None = None
    tempo: str = "normal"
    aggression: str = "balanced"
    play_type: PlayType = PlayType.PASS


@dataclass(slots=True)
class SnapContextPackage:
    game_id: str
    play_id: str
    mode: SimMode
    situation: Situation
    participants: list[ActorRef]
    in_game_states: dict[str, InGameState]
    intent: ParameterizedIntent
    trait_vectors: dict[str, dict[str, float]] = field(default_factory=dict)
    weather_flags: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PenaltyArtifact:
    code: str
    against_team_id: str
    yards: int
    enforcement_rationale: str


@dataclass(slots=True)
class PlayResult:
    play_id: str
    yards: int
    new_spot: int
    turnover: bool
    turnover_type: str | None
    score_event: str | None
    penalties: list[PenaltyArtifact]
    clock_delta: int
    next_down: int
    next_distance: int
    next_possession_team_id: str


@dataclass(slots=True)
class RepActor:
    actor_id: str
    team_id: str
    role: str
    assignment_tag: str


@dataclass(slots=True)
class RepLedgerEntry:
    rep_id: str
    play_id: str
    phase: str
    rep_type: str
    actors: list[RepActor]
    assignment_tags: list[str]
    outcome_tags: list[str]
    responsibility_weights: dict[str, float]
    context_tags: list[str]
    evidence_handles: list[str]

    def validate(self) -> None:
        if not self.responsibility_weights:
            raise ValueError("responsibility_weights must not be empty")
        total = round(sum(self.responsibility_weights.values()), 6)
        if abs(total - 1.0) > 0.001:
            raise ValueError(f"responsibility weights must sum to 1.0, got {total}")


@dataclass(slots=True)
class CausalityNode:
    source_type: str
    source_id: str
    weight: float
    description: str


@dataclass(slots=True)
class CausalityChain:
    terminal_event: str
    play_id: str
    nodes: list[CausalityNode]

    def validate(self) -> None:
        if not self.nodes:
            raise ValueError("causality chain must contain at least one node")
        total = round(sum(n.weight for n in self.nodes), 6)
        if abs(total - 1.0) > 0.001:
            raise ValueError(f"causality weights must sum to 1.0, got {total}")


@dataclass(slots=True)
class NarrativeEvent:
    event_id: str
    time: datetime
    scope: str
    event_type: str
    actors: list[str]
    claims: list[str]
    evidence_handles: list[str]
    severity: str
    confidentiality_tier: str


@dataclass(slots=True)
class ActionRequest:
    request_id: str
    action_type: ActionType | str
    payload: dict[str, Any]
    actor_team_id: str


@dataclass(slots=True)
class ActionResult:
    request_id: str
    success: bool
    message: str
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PerceivedMetric:
    label: str
    estimate: float
    lower_bound: float
    upper_bound: float
    confidence: float


@dataclass(slots=True)
class PerceivedPlayerCard:
    player_id: str
    team_id: str
    scout_metrics: list[PerceivedMetric]
    coach_metrics: list[PerceivedMetric]
    medical_metrics: list[PerceivedMetric]
    updated_at: datetime


@dataclass(slots=True)
class DifficultyProfile:
    name: Difficulty
    volatility_multiplier: float
    injury_rate_multiplier: float
    injury_severity_multiplier: float
    scouting_noise_multiplier: float
    negotiation_friction_multiplier: float
    ownership_pressure_multiplier: float
    aging_variance_multiplier: float
    upset_tail_multiplier: float

    def validate(self) -> None:
        values = [
            self.volatility_multiplier,
            self.injury_rate_multiplier,
            self.injury_severity_multiplier,
            self.scouting_noise_multiplier,
            self.negotiation_friction_multiplier,
            self.ownership_pressure_multiplier,
            self.aging_variance_multiplier,
            self.upset_tail_multiplier,
        ]
        if any(v <= 0 for v in values):
            raise ValueError("difficulty multipliers must be positive")


@dataclass(slots=True)
class RetentionPolicy:
    retain_playoffs: bool = True
    retain_championship: bool = True
    retain_instant_classics: bool = True
    retain_rivalry_games: bool = False
    retain_record_games: bool = False


@dataclass(slots=True)
class DepthChartAssignment:
    team_id: str
    player_id: str
    slot_role: str
    priority: int
    active_flag: bool


@dataclass(slots=True)
class ScheduleEntry:
    game_id: str
    season: int
    week: int
    home_team_id: str
    away_team_id: str
    status: str
    is_user_game: bool


@dataclass(slots=True)
class TeamStanding:
    team_id: str
    wins: int = 0
    losses: int = 0
    ties: int = 0
    points_for: int = 0
    points_against: int = 0

    @property
    def point_diff(self) -> int:
        return self.points_for - self.points_against


@dataclass(slots=True)
class GameSessionState:
    game_id: str
    season: int
    week: int
    home_team_id: str
    away_team_id: str
    quarter: int
    clock_seconds: int
    home_score: int
    away_score: int
    possession_team_id: str
    down: int
    distance: int
    yard_line: int
    drive_index: int
    timeouts_home: int
    timeouts_away: int
    active_injuries: dict[str, str] = field(default_factory=dict)
    active_penalties: list[PenaltyArtifact] = field(default_factory=list)
    is_overtime: bool = False
    completed: bool = False


@dataclass(slots=True)
class WeekSimulationResult:
    season: int
    week: int
    finalized_game_ids: list[str]
    standings_delta: dict[str, dict[str, int]]
    injuries: list[str]
    transactions: list[str]
    integrity_checks: list[str]


@dataclass(slots=True)
class LeagueSnapshotRef:
    snapshot_id: str
    season: int
    week: int
    created_at: datetime
    blob_path: str


@dataclass(slots=True)
class ValidationIssue:
    code: str
    severity: str
    field_path: str
    entity_id: str
    message: str


@dataclass(slots=True)
class ValidationResult:
    ok: bool
    issues: list[ValidationIssue]


class ValidationError(ValueError):
    def __init__(self, issues: list[ValidationIssue]) -> None:
        message = "; ".join(f"{i.code}:{i.entity_id}:{i.message}" for i in issues)
        super().__init__(message)
        self.issues = issues


@dataclass(slots=True)
class TraitCatalogEntry:
    trait_code: str
    dtype: str
    min_value: float
    max_value: float
    required: bool
    description: str
    category: str
    status: TraitStatus
    version: str


@dataclass(slots=True)
class PlayerTraitValue:
    player_id: str
    trait_code: str
    value: float


@dataclass(slots=True)
class ResourceManifest:
    resource_type: str
    schema_version: str
    resource_version: str
    generated_at: str
    checksum: str


@dataclass(slots=True)
class TraitInfluenceManifest:
    resource_type: str
    schema_version: str
    resource_version: str
    generated_at: str
    checksum: str


@dataclass(slots=True)
class PlayFamilyInfluenceProfile:
    play_type: str
    family: str
    offense_weights: dict[str, float]
    defense_weights: dict[str, float]
    fatigue_sensitivity: float
    wear_sensitivity: float
    context_modifiers: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class OutcomeResolutionProfile:
    play_type: str
    noise_scale: float
    explosive_threshold: int
    turnover_scale: float
    score_scale: float
    clock_delta_min: int
    clock_delta_max: int
    context_modifiers: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class ContestInput:
    contest_id: str
    play_id: str
    play_type: str
    family: str
    offense_actor_ids: list[str]
    defense_actor_ids: list[str]
    influence_profile: PlayFamilyInfluenceProfile
    situation: Situation
    in_game_states: dict[str, InGameState]


@dataclass(slots=True)
class ContestOutput:
    contest_id: str
    play_id: str
    play_type: str
    family: str
    score: float
    offense_score: float
    defense_score: float
    actor_contributions: dict[str, float]
    trait_contributions: dict[str, float]
    variance_hint: float
    evidence_handles: list[str]


@dataclass(slots=True)
class CausalityTemplate:
    terminal_event_family: str
    descriptions: list[str]


@dataclass(slots=True)
class ResolverEvidenceRef:
    handle: str
    source_type: str
    source_id: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PlaybookEntry:
    play_id: str
    play_type: PlayType
    family: str
    personnel_id: str
    formation_id: str
    offensive_concept_id: str
    defensive_concept_id: str
    assignment_template_id: str
    branch_trigger_ids: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)


@dataclass(slots=True)
class AssignmentTemplate:
    template_id: str
    offense_roles: list[str]
    defense_roles: list[str]
    pairing_hints: list[dict[str, str]] = field(default_factory=list)
    default_technique: str = "balanced"


@dataclass(slots=True)
class TraitRoleMappingEntry:
    trait_code: str
    status: TraitStatus
    phase: str
    contest_family: str
    role_group: str
    evidence_tag: str


@dataclass(slots=True)
class MatchupEdge:
    edge_id: str
    offense_actor_id: str
    defense_actor_id: str
    offense_role: str
    defense_role: str
    technique: str
    leverage: str
    responsibility_weight: float
    context_tags: list[str] = field(default_factory=list)


@dataclass(slots=True)
class MatchupGraph:
    graph_id: str
    play_id: str
    phase: str
    edges: list[MatchupEdge]


@dataclass(slots=True)
class PreSnapMatchupPlan:
    plan_id: str
    play_id: str
    playbook_entry_id: str
    assignment_template_id: str
    offense_team_id: str
    defense_team_id: str
    graph: MatchupGraph
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class LeverageState:
    edge_wins_offense: float
    edge_wins_defense: float
    contested_edges: int


@dataclass(slots=True)
class PocketState:
    integrity: float
    pressure_score: float
    qb_constraint: str


@dataclass(slots=True)
class SeparationState:
    window_score: float
    leverage_label: str
    busted_coverage: bool


@dataclass(slots=True)
class RunFitState:
    lane_score: float
    fit_label: str
    pursuit_eta: float


@dataclass(slots=True)
class PursuitState:
    convergence_score: float
    tackle_depth_estimate: float


@dataclass(slots=True)
class ContestResolution:
    contest_id: str
    play_id: str
    phase: str
    family: str
    score: float
    offense_score: float
    defense_score: float
    contributor_trace: dict[str, float]
    trait_trace: dict[str, float]
    evidence_handles: list[str]
    variance_hint: float


@dataclass(slots=True)
class RulesAdjudicationResult:
    penalties: list[PenaltyArtifact]
    score_event: str | None
    enforcement_notes: list[str]
    next_down: int
    next_distance: int
    next_possession_team_id: str
    clock_delta: int


@dataclass(slots=True)
class ResolvedSnapStateDelta:
    next_down: int
    next_distance: int
    next_possession_team_id: str
    new_spot: int
    clock_delta: int
    score_delta_by_team: dict[str, int] = field(default_factory=dict)
    drive_increment: bool = False
    injuries: dict[str, str] = field(default_factory=dict)
    fatigue_delta: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class SnapArtifactBundle:
    play_result: PlayResult
    pre_snap_plan: PreSnapMatchupPlan
    matchup_snapshots: list[MatchupGraph]
    phase_transitions: list[str]
    contest_resolutions: list[ContestResolution]
    rep_ledger: list[RepLedgerEntry]
    causality_chain: CausalityChain
    evidence_refs: list[ResolverEvidenceRef]
    rules_adjudication: RulesAdjudicationResult
    narrative_events: list[NarrativeEvent] = field(default_factory=list)


@dataclass(slots=True)
class SimulationReadinessReport:
    season: int
    week: int
    game_id: str
    home_team_id: str
    away_team_id: str
    blocking_issues: list[ValidationIssue] = field(default_factory=list)
    warning_issues: list[ValidationIssue] = field(default_factory=list)
    validated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass(slots=True)
class ForensicArtifact:
    artifact_id: str
    timestamp: datetime
    engine_scope: str
    error_code: str
    message: str
    state_snapshot: Mapping[str, Any]
    context: Mapping[str, Any]
    identifiers: Mapping[str, str]
    causal_fragment: Sequence[str]


@dataclass(slots=True)
class PlayIntentFrame:
    play_type: PlayType
    personnel_id: str
    formation_id: str
    offense_concept_id: str
    defense_concept_id: str
    playbook_entry_id: str
    posture: str
    tempo: str = "normal"
    aggression: str = "balanced"
    allows_audible: bool = True


@dataclass(slots=True)
class TeamGamePackage:
    team_id: str
    active_players: list[str]
    depth_slots: dict[str, str]
    perceived_inputs: dict[str, float]
    coaching_policy_id: str
    default_rules_profile_id: str = "nfl_placeholder_v1"


class RegistryRepository(Protocol):
    def resolve_personnel(self, personnel_id: str) -> dict[str, Any]: ...

    def resolve_formation(self, formation_id: str) -> dict[str, Any]: ...

    def resolve_concept(self, concept_id: str, side: str) -> dict[str, Any]: ...

    def resolve_policy(self, policy_id: str) -> dict[str, Any]: ...

    def resolve_playbook_entry(self, play_id: str) -> PlaybookEntry: ...

    def resolve_assignment_template(self, template_id: str) -> AssignmentTemplate: ...

    def resolve_trait_role_mappings(self) -> list[TraitRoleMappingEntry]: ...

    def resolve_rules_profile(self, rules_profile_id: str) -> dict[str, Any]: ...


class CoachDecisionEngine(Protocol):
    def decide_play_intent(
        self,
        *,
        session_state: GameSessionState,
        offense_package: TeamGamePackage,
        defense_package: TeamGamePackage,
        random_source: RandomSource,
    ) -> PlayIntentFrame: ...
