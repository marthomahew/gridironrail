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
