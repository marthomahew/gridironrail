from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from grs.contracts import DepthChartAssignment, TeamStanding


@dataclass(slots=True)
class Owner:
    owner_id: str
    name: str
    risk_tolerance: float
    patience: float
    spending_aggressiveness: float
    mandate: str


@dataclass(slots=True)
class TeamIdentityProfile:
    scheme_offense: str
    scheme_defense: str
    roster_strategy: str
    risk_posture: str


@dataclass(slots=True)
class StaffMember:
    staff_id: str
    name: str
    role: str
    evaluation: float
    development: float
    discipline: float
    adaptability: float


@dataclass(slots=True)
class ContractYear:
    year: int
    base_salary: int
    bonus_prorated: int
    guaranteed: int


@dataclass(slots=True)
class Contract:
    contract_id: str
    player_id: str
    team_id: str
    years: list[ContractYear]
    signed_date: date

    @property
    def total_value(self) -> int:
        return sum(y.base_salary + y.bonus_prorated for y in self.years)


@dataclass(slots=True)
class Player:
    player_id: str
    team_id: str
    name: str
    position: str
    age: int
    overall_truth: float
    volatility_truth: float
    injury_susceptibility_truth: float
    hidden_dev_curve: float
    morale: float = 0.5
    traits: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class Prospect:
    prospect_id: str
    name: str
    position: str
    age: int
    draft_grade_truth: float


@dataclass(slots=True)
class Franchise:
    team_id: str
    name: str
    owner: Owner
    identity: TeamIdentityProfile
    conference_id: str = ""
    division_id: str = ""
    staff: list[StaffMember] = field(default_factory=list)
    roster: list[Player] = field(default_factory=list)
    depth_chart: list[DepthChartAssignment] = field(default_factory=list)
    cap_space: int = 255_000_000
    coaching_policy_id: str = "balanced_base"
    rules_profile_id: str = "nfl_standard_v1"


@dataclass(slots=True)
class LeagueWeek:
    season: int
    week: int
    phase: str


@dataclass(slots=True)
class TransactionRecord:
    tx_id: str
    season: int
    week: int
    tx_type: str
    summary: str
    team_id: str
    causality_context: dict[str, float | str] = field(default_factory=dict)


@dataclass(slots=True)
class CapLedgerEntry:
    entry_id: str
    team_id: str
    season: int
    reason: str
    amount: int


@dataclass(slots=True)
class TradeRecord:
    trade_id: str
    season: int
    week: int
    from_team_id: str
    to_team_id: str
    assets_from: list[str]
    assets_to: list[str]


@dataclass(slots=True)
class LeagueStandingBook:
    entries: dict[str, TeamStanding] = field(default_factory=dict)

    def ensure_team(self, team_id: str) -> TeamStanding:
        if team_id not in self.entries:
            self.entries[team_id] = TeamStanding(team_id=team_id)
        return self.entries[team_id]
