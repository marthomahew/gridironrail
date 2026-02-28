from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import math

from grs.contracts import (
    CapabilityDomain,
    CapabilityPolicy,
    LeagueSetupConfig,
    ManagementMode,
    ScheduleEntry,
    ValidationIssue,
    ValidationResult,
)
from grs.core import default_difficulty_profiles
from grs.football.resources import ResourceResolver
from grs.football.traits import generate_player_traits
from grs.org.engine import LeagueState
from grs.org.entities import Franchise, LeagueStandingBook, Owner, Player, StaffMember, TeamIdentityProfile


LEAGUE_LIMITS = {
    "conference_count_min": 1,
    "conference_count_max": 4,
    "divisions_per_conference_min": 1,
    "divisions_per_conference_max": 8,
    "teams_per_division_min": 2,
    "teams_per_division_max": 16,
    "players_per_team_min": 30,
    "players_per_team_max": 75,
    "regular_season_weeks_min": 4,
    "regular_season_weeks_max": 24,
}


@dataclass(slots=True)
class TalentProfileSpec:
    profile_id: str
    description: str
    overall_center: float
    overall_spread: float
    volatility_center: float
    injury_center: float


TALENT_PROFILES: dict[str, TalentProfileSpec] = {
    "balanced_mid": TalentProfileSpec(
        profile_id="balanced_mid",
        description="Balanced fictional profile centered around league-average outcomes.",
        overall_center=57.0,
        overall_spread=10.0,
        volatility_center=0.42,
        injury_center=0.37,
    ),
    "narrow_parity": TalentProfileSpec(
        profile_id="narrow_parity",
        description="Tighter parity profile with reduced talent variance.",
        overall_center=56.0,
        overall_spread=6.0,
        volatility_center=0.40,
        injury_center=0.36,
    ),
    "top_heavy": TalentProfileSpec(
        profile_id="top_heavy",
        description="Higher-end talent concentration with a longer lower tail.",
        overall_center=61.0,
        overall_spread=14.0,
        volatility_center=0.48,
        injury_center=0.39,
    ),
    "rebuild_chaos": TalentProfileSpec(
        profile_id="rebuild_chaos",
        description="High variance setup with large roster quality swings.",
        overall_center=54.0,
        overall_spread=18.0,
        volatility_center=0.54,
        injury_center=0.42,
    ),
}

SCHEDULE_POLICIES = {"balanced_round_robin", "division_weighted"}


@dataclass(slots=True)
class TeamBlueprint:
    team_id: str
    team_name: str
    conference_id: str
    conference_name: str
    division_id: str
    division_name: str


class LeagueSetupValidator:
    def __init__(self, resource_resolver: ResourceResolver) -> None:
        self._resource_resolver = resource_resolver

    def validate(self, config: LeagueSetupConfig) -> ValidationResult:
        issues: list[ValidationIssue] = []
        limits = LEAGUE_LIMITS

        if not (limits["conference_count_min"] <= config.conference_count <= limits["conference_count_max"]):
            issues.append(
                ValidationIssue(
                    code="SETUP_CONFERENCE_COUNT_OUT_OF_RANGE",
                    severity="blocking",
                    field_path="conference_count",
                    entity_id="league_setup",
                    message=(
                        f"conference_count must be in "
                        f"[{limits['conference_count_min']}, {limits['conference_count_max']}]"
                    ),
                )
            )

        if len(config.divisions_per_conference) != config.conference_count:
            issues.append(
                ValidationIssue(
                    code="SETUP_DIVISION_VECTOR_LENGTH_MISMATCH",
                    severity="blocking",
                    field_path="divisions_per_conference",
                    entity_id="league_setup",
                    message="divisions_per_conference length must equal conference_count",
                )
            )

        if len(config.teams_per_division) != config.conference_count:
            issues.append(
                ValidationIssue(
                    code="SETUP_TEAMS_MATRIX_CONFERENCE_LENGTH_MISMATCH",
                    severity="blocking",
                    field_path="teams_per_division",
                    entity_id="league_setup",
                    message="teams_per_division outer length must equal conference_count",
                )
            )

        for conf_index, divisions in enumerate(config.divisions_per_conference):
            if not (
                limits["divisions_per_conference_min"]
                <= divisions
                <= limits["divisions_per_conference_max"]
            ):
                issues.append(
                    ValidationIssue(
                        code="SETUP_DIVISION_COUNT_OUT_OF_RANGE",
                        severity="blocking",
                        field_path=f"divisions_per_conference[{conf_index}]",
                        entity_id=f"conference_{conf_index + 1}",
                        message=(
                            "division count must be in "
                            f"[{limits['divisions_per_conference_min']}, {limits['divisions_per_conference_max']}]"
                        ),
                    )
                )
            if conf_index < len(config.teams_per_division):
                team_row = config.teams_per_division[conf_index]
                if len(team_row) != divisions:
                    issues.append(
                        ValidationIssue(
                            code="SETUP_TEAMS_MATRIX_DIVISION_LENGTH_MISMATCH",
                            severity="blocking",
                            field_path=f"teams_per_division[{conf_index}]",
                            entity_id=f"conference_{conf_index + 1}",
                            message=(
                                "teams_per_division row length must equal division count "
                                f"for conference {conf_index + 1}"
                            ),
                        )
                    )
                for div_index, teams in enumerate(team_row):
                    if not (
                        limits["teams_per_division_min"] <= teams <= limits["teams_per_division_max"]
                    ):
                        issues.append(
                            ValidationIssue(
                                code="SETUP_TEAMS_PER_DIVISION_OUT_OF_RANGE",
                                severity="blocking",
                                field_path=f"teams_per_division[{conf_index}][{div_index}]",
                                entity_id=f"conference_{conf_index + 1}_division_{div_index + 1}",
                                message=(
                                    "teams per division must be in "
                                    f"[{limits['teams_per_division_min']}, {limits['teams_per_division_max']}]"
                                ),
                            )
                        )

        players_per_team = config.roster_policy.players_per_team
        if not (
            limits["players_per_team_min"] <= players_per_team <= limits["players_per_team_max"]
        ):
            issues.append(
                ValidationIssue(
                    code="SETUP_PLAYERS_PER_TEAM_OUT_OF_RANGE",
                    severity="blocking",
                    field_path="roster_policy.players_per_team",
                    entity_id="roster_policy",
                    message=(
                        "players_per_team must be in "
                        f"[{limits['players_per_team_min']}, {limits['players_per_team_max']}]"
                    ),
                )
            )

        weeks = config.schedule_policy.regular_season_weeks
        if not (
            limits["regular_season_weeks_min"] <= weeks <= limits["regular_season_weeks_max"]
        ):
            issues.append(
                ValidationIssue(
                    code="SETUP_REGULAR_SEASON_WEEKS_OUT_OF_RANGE",
                    severity="blocking",
                    field_path="schedule_policy.regular_season_weeks",
                    entity_id="schedule_policy",
                    message=(
                        "regular_season_weeks must be in "
                        f"[{limits['regular_season_weeks_min']}, {limits['regular_season_weeks_max']}]"
                    ),
                )
            )

        if config.schedule_policy.policy_id not in SCHEDULE_POLICIES:
            issues.append(
                ValidationIssue(
                    code="SETUP_UNKNOWN_SCHEDULE_POLICY",
                    severity="blocking",
                    field_path="schedule_policy.policy_id",
                    entity_id="schedule_policy",
                    message=f"schedule policy '{config.schedule_policy.policy_id}' is not supported",
                )
            )

        if config.talent_profile_id not in TALENT_PROFILES:
            issues.append(
                ValidationIssue(
                    code="SETUP_UNKNOWN_TALENT_PROFILE",
                    severity="blocking",
                    field_path="talent_profile_id",
                    entity_id="talent_profile",
                    message=f"talent_profile_id '{config.talent_profile_id}' is not supported",
                )
            )

        if config.difficulty_profile_id not in {d.value for d in default_difficulty_profiles().keys()}:
            issues.append(
                ValidationIssue(
                    code="SETUP_UNKNOWN_DIFFICULTY_PROFILE",
                    severity="blocking",
                    field_path="difficulty_profile_id",
                    entity_id="difficulty_profile",
                    message=f"difficulty_profile_id '{config.difficulty_profile_id}' is not supported",
                )
            )

        try:
            self._resource_resolver.resolve_rules_profile(config.ruleset_id)
        except Exception:
            issues.append(
                ValidationIssue(
                    code="SETUP_UNKNOWN_RULESET_ID",
                    severity="blocking",
                    field_path="ruleset_id",
                    entity_id="ruleset",
                    message=f"ruleset_id '{config.ruleset_id}' is not registered",
                )
            )

        for domain in config.capability_overrides:
            if not isinstance(domain, CapabilityDomain):
                issues.append(
                    ValidationIssue(
                        code="SETUP_UNKNOWN_CAPABILITY_DOMAIN",
                        severity="blocking",
                        field_path="capability_overrides",
                        entity_id="capability_policy",
                        message=f"unknown capability domain '{domain}'",
                    )
                )

        return ValidationResult(ok=not issues, issues=issues)


class LeagueStructureCompiler:
    def compile(self, config: LeagueSetupConfig) -> list[TeamBlueprint]:
        teams: list[TeamBlueprint] = []
        team_counter = 1
        for conf_index in range(config.conference_count):
            conference_id = f"C{conf_index + 1:02d}"
            conference_name = f"Conference {conf_index + 1}"
            division_count = config.divisions_per_conference[conf_index]
            for div_index in range(division_count):
                division_id = f"{conference_id}D{div_index + 1:02d}"
                division_name = f"{conference_name} Division {div_index + 1}"
                team_count = config.teams_per_division[conf_index][div_index]
                for _ in range(team_count):
                    team_id = f"T{team_counter:02d}"
                    teams.append(
                        TeamBlueprint(
                            team_id=team_id,
                            team_name=f"Team {team_counter}",
                            conference_id=conference_id,
                            conference_name=conference_name,
                            division_id=division_id,
                            division_name=division_name,
                        )
                    )
                    team_counter += 1
        return teams


class TeamSelectionPlanner:
    def team_ids(self, blueprints: list[TeamBlueprint]) -> list[str]:
        return [b.team_id for b in blueprints]


class ScheduleGenerationService:
    def generate(
        self,
        *,
        blueprints: list[TeamBlueprint],
        season: int,
        user_team_id: str,
        weeks: int,
        policy_id: str,
        rand,
    ) -> list[ScheduleEntry]:
        if policy_id not in SCHEDULE_POLICIES:
            raise ValueError(f"unsupported schedule policy '{policy_id}'")
        if not blueprints:
            raise ValueError("cannot generate schedule for empty team list")

        team_ids = [b.team_id for b in blueprints]
        conf_of = {b.team_id: b.conference_id for b in blueprints}
        div_of = {b.team_id: b.division_id for b in blueprints}

        meetings: dict[tuple[str, str], int] = {}
        home_count = {tid: 0 for tid in team_ids}
        bye_count = {tid: 0 for tid in team_ids}
        schedules: list[ScheduleEntry] = []

        for week in range(1, weeks + 1):
            available = set(team_ids)
            bye_team: str | None = None
            if len(available) % 2 == 1:
                bye_team = self._choose_bye(available, bye_count, rand)
                available.remove(bye_team)
                bye_count[bye_team] += 1

            week_pairs: list[tuple[str, str]] = []
            while available:
                anchor = min(available, key=lambda team_id: (self._games_played(team_id, meetings), team_id))
                available.remove(anchor)
                candidates = sorted(available)
                if not candidates:
                    raise ValueError(f"schedule generation failed week {week}: no candidate for {anchor}")
                partner = self._pick_partner(
                    anchor=anchor,
                    candidates=candidates,
                    policy_id=policy_id,
                    conf_of=conf_of,
                    div_of=div_of,
                    meetings=meetings,
                    rand=rand,
                )
                available.remove(partner)
                home, away = self._pick_home_away(anchor, partner, home_count, meetings)
                week_pairs.append((home, away))
                key = self._meeting_key(anchor, partner)
                meetings[key] = meetings.get(key, 0) + 1
                home_count[home] += 1

            for game_index, (home, away) in enumerate(week_pairs, start=1):
                game_id = f"S{season}_W{week}_G{game_index:03d}"
                schedules.append(
                    ScheduleEntry(
                        game_id=game_id,
                        season=season,
                        week=week,
                        home_team_id=home,
                        away_team_id=away,
                        status="scheduled",
                        is_user_game=user_team_id in {home, away},
                    )
                )

            if bye_team is not None:
                # no-op marker for deterministic auditability through byes
                _ = bye_team

        self._validate_schedule(
            schedules=schedules,
            team_ids=team_ids,
            weeks=weeks,
        )
        return schedules

    def _pick_partner(
        self,
        *,
        anchor: str,
        candidates: list[str],
        policy_id: str,
        conf_of: dict[str, str],
        div_of: dict[str, str],
        meetings: dict[tuple[str, str], int],
        rand,
    ) -> str:
        best: list[tuple[float, str]] = []
        for candidate in candidates:
            key = self._meeting_key(anchor, candidate)
            prior = meetings.get(key, 0)
            if policy_id == "division_weighted":
                score = 0.0
                if div_of[anchor] == div_of[candidate]:
                    score += 4.0
                elif conf_of[anchor] == conf_of[candidate]:
                    score += 2.0
                score -= prior * 1.3
            else:
                score = -(prior * 1.8)
                if div_of[anchor] == div_of[candidate]:
                    score += 0.8
            jitter = (rand.rand() * 0.001)
            best.append((score + jitter, candidate))
        best.sort(key=lambda item: (-item[0], item[1]))
        return best[0][1]

    def _pick_home_away(
        self,
        a: str,
        b: str,
        home_count: dict[str, int],
        meetings: dict[tuple[str, str], int],
    ) -> tuple[str, str]:
        key = self._meeting_key(a, b)
        prior = meetings.get(key, 0)
        if home_count[a] < home_count[b]:
            return a, b
        if home_count[b] < home_count[a]:
            return b, a
        if prior % 2 == 0:
            return a, b
        return b, a

    def _choose_bye(self, available: set[str], bye_count: dict[str, int], rand) -> str:
        ranked = sorted(available, key=lambda team_id: (bye_count[team_id], team_id))
        tied = [team for team in ranked if bye_count[team] == bye_count[ranked[0]]]
        if len(tied) == 1:
            return tied[0]
        return tied[rand.randint(0, len(tied) - 1)]

    def _meeting_key(self, a: str, b: str) -> tuple[str, str]:
        if a < b:
            return (a, b)
        return (b, a)

    def _games_played(self, team_id: str, meetings: dict[tuple[str, str], int]) -> int:
        total = 0
        for (a, b), count in meetings.items():
            if team_id in {a, b}:
                total += count
        return total

    def _validate_schedule(self, *, schedules: list[ScheduleEntry], team_ids: list[str], weeks: int) -> None:
        seen: set[tuple[int, str]] = set()
        by_team_week: dict[str, set[int]] = {team_id: set() for team_id in team_ids}
        for entry in schedules:
            key_home = (entry.week, entry.home_team_id)
            key_away = (entry.week, entry.away_team_id)
            if key_home in seen or key_away in seen:
                raise ValueError(f"team scheduled multiple times in week {entry.week}")
            seen.add(key_home)
            seen.add(key_away)
            by_team_week[entry.home_team_id].add(entry.week)
            by_team_week[entry.away_team_id].add(entry.week)

        for team_id in team_ids:
            if not by_team_week[team_id]:
                raise ValueError(f"team {team_id} has zero scheduled games")
            if max(by_team_week[team_id]) > weeks:
                raise ValueError(f"team {team_id} has out-of-range scheduled week")


class RosterGenerationService:
    BASE_POSITION_COUNTS = {
        "QB": 1,
        "RB": 2,
        "WR": 4,
        "TE": 2,
        "OL": 7,
        "DL": 4,
        "LB": 3,
        "CB": 3,
        "S": 2,
        "K": 1,
        "P": 1,
    }
    EXTRA_POSITION_CYCLE = ["OL", "DL", "WR", "LB", "CB", "S", "RB", "TE", "QB"]

    def build_team(
        self,
        *,
        blueprint: TeamBlueprint,
        players_per_team: int,
        cap_amount: int,
        talent_profile: TalentProfileSpec,
        rand,
    ) -> Franchise:
        if players_per_team < sum(self.BASE_POSITION_COUNTS.values()):
            raise ValueError(
                f"players_per_team {players_per_team} is below required generation floor "
                f"{sum(self.BASE_POSITION_COUNTS.values())}"
            )
        owner = Owner(
            owner_id=f"OWN_{blueprint.team_id}",
            name=f"Owner {blueprint.team_id}",
            risk_tolerance=0.35 + (rand.rand() * 0.45),
            patience=0.30 + (rand.rand() * 0.55),
            spending_aggressiveness=0.40 + (rand.rand() * 0.55),
            mandate="playoffs",
        )
        identity = TeamIdentityProfile(
            scheme_offense="multiple",
            scheme_defense="hybrid",
            roster_strategy="balanced",
            risk_posture="moderate",
        )
        staff = [
            StaffMember(f"{blueprint.team_id}_STAFF_HC", f"HC {blueprint.team_id}", "HeadCoach", 0.58, 0.62, 0.59, 0.58),
            StaffMember(f"{blueprint.team_id}_STAFF_OC", f"OC {blueprint.team_id}", "OffensiveCoach", 0.57, 0.60, 0.55, 0.58),
            StaffMember(f"{blueprint.team_id}_STAFF_DC", f"DC {blueprint.team_id}", "DefensiveCoach", 0.57, 0.59, 0.56, 0.57),
            StaffMember(f"{blueprint.team_id}_STAFF_SCOUT", f"Scout {blueprint.team_id}", "Scout", 0.56, 0.50, 0.50, 0.50),
            StaffMember(f"{blueprint.team_id}_STAFF_MED", f"Medical {blueprint.team_id}", "Medical", 0.56, 0.50, 0.50, 0.50),
        ]

        position_counts = dict(self.BASE_POSITION_COUNTS)
        extra_slots = players_per_team - sum(position_counts.values())
        for index in range(extra_slots):
            position = self.EXTRA_POSITION_CYCLE[index % len(self.EXTRA_POSITION_CYCLE)]
            position_counts[position] = position_counts.get(position, 0) + 1

        roster: list[Player] = []
        index = 1
        for position, count in position_counts.items():
            for _ in range(count):
                signal = self._sample_centered(rand)
                overall_truth = self._bounded_scale(
                    center=talent_profile.overall_center,
                    spread=talent_profile.overall_spread,
                    signal=signal,
                    low=30.0,
                    high=98.0,
                )
                volatility_truth = self._bounded_scale(
                    center=talent_profile.volatility_center,
                    spread=0.22,
                    signal=self._sample_centered(rand),
                    low=0.05,
                    high=0.95,
                )
                injury_truth = self._bounded_scale(
                    center=talent_profile.injury_center,
                    spread=0.20,
                    signal=self._sample_centered(rand),
                    low=0.05,
                    high=0.95,
                )
                hidden_dev_curve = self._bounded_scale(
                    center=56.0,
                    spread=16.0,
                    signal=self._sample_centered(rand),
                    low=22.0,
                    high=99.0,
                )
                player_id = f"{blueprint.team_id}_P{index:03d}"
                player = Player(
                    player_id=player_id,
                    team_id=blueprint.team_id,
                    name=f"{blueprint.team_id} Player {index}",
                    position=position,
                    age=21 + rand.randint(0, 13),
                    overall_truth=overall_truth,
                    volatility_truth=volatility_truth,
                    injury_susceptibility_truth=injury_truth,
                    hidden_dev_curve=hidden_dev_curve,
                    traits=generate_player_traits(
                        player_id=player_id,
                        position=position,
                        overall_truth=overall_truth,
                        volatility_truth=volatility_truth,
                        injury_susceptibility_truth=injury_truth,
                    ),
                )
                roster.append(player)
                index += 1

        roster.sort(key=lambda player: (player.position, -player.overall_truth, player.player_id))
        franchise = Franchise(
            team_id=blueprint.team_id,
            name=blueprint.team_name,
            owner=owner,
            identity=identity,
            conference_id=blueprint.conference_id,
            division_id=blueprint.division_id,
            staff=staff,
            roster=roster,
            depth_chart=_build_depth_chart(blueprint.team_id, roster),
            cap_space=cap_amount,
            coaching_policy_id="balanced_base",
            rules_profile_id="nfl_standard_v1",
        )
        return franchise

    def _sample_centered(self, rand) -> float:
        # Box-Muller transform with bounded tails by logistic mapping later.
        u1 = rand.rand()
        while u1 <= 1e-12:
            u1 = rand.rand()
        u2 = rand.rand()
        return math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)

    def _bounded_scale(self, *, center: float, spread: float, signal: float, low: float, high: float) -> float:
        sigmoid = 1.0 / (1.0 + math.exp(-signal))
        centered = (sigmoid - 0.5) * 2.0
        raw = center + (centered * spread)
        span = high - low
        bounded = low + span * (1.0 / (1.0 + math.exp(-(raw - (low + span * 0.5)) * 0.08)))
        return bounded


class CapabilityEnforcementService:
    BASELINES: dict[ManagementMode, set[CapabilityDomain]] = {
        ManagementMode.OWNER: {
            CapabilityDomain.FINANCE,
            CapabilityDomain.CONTRACTS,
            CapabilityDomain.TRADES,
            CapabilityDomain.DRAFT,
            CapabilityDomain.FA,
            CapabilityDomain.STAFFING,
            CapabilityDomain.DEPTH_CHART,
            CapabilityDomain.GAMEPLAN,
            CapabilityDomain.PLAYCALL_OVERRIDE,
            CapabilityDomain.OWNER_POLICY,
        },
        ManagementMode.GM: {
            CapabilityDomain.CONTRACTS,
            CapabilityDomain.TRADES,
            CapabilityDomain.DRAFT,
            CapabilityDomain.FA,
            CapabilityDomain.STAFFING,
            CapabilityDomain.DEPTH_CHART,
            CapabilityDomain.GAMEPLAN,
        },
        ManagementMode.COACH: {
            CapabilityDomain.DEPTH_CHART,
            CapabilityDomain.GAMEPLAN,
            CapabilityDomain.PLAYCALL_OVERRIDE,
        },
    }

    def build_policy(
        self,
        *,
        mode: ManagementMode,
        overrides: dict[CapabilityDomain, bool],
        updated_by_team_id: str,
        reason: str,
    ) -> CapabilityPolicy:
        baseline = sorted(self.BASELINES[mode], key=lambda item: item.value)
        return CapabilityPolicy(
            mode=mode,
            baseline_capabilities=baseline,
            override_capabilities=overrides,
            updated_by_team_id=updated_by_team_id,
            updated_at=datetime.now(UTC),
            reason=reason,
        )

    def has_capability(self, policy: CapabilityPolicy, domain: CapabilityDomain) -> bool:
        base = domain in set(policy.baseline_capabilities)
        if domain in policy.override_capabilities:
            return bool(policy.override_capabilities[domain])
        return base


def build_league_from_setup(
    *,
    config: LeagueSetupConfig,
    season: int,
    compiler: LeagueStructureCompiler,
    roster_generator: RosterGenerationService,
    rand,
) -> LeagueState:
    blueprints = compiler.compile(config)
    talent_profile = TALENT_PROFILES[config.talent_profile_id]
    teams: list[Franchise] = []
    standings = LeagueStandingBook()
    for blueprint in blueprints:
        team_rand = rand.spawn(f"team:{blueprint.team_id}")
        team = roster_generator.build_team(
            blueprint=blueprint,
            players_per_team=config.roster_policy.players_per_team,
            cap_amount=config.cap_policy.cap_amount,
            talent_profile=talent_profile,
            rand=team_rand,
        )
        team.rules_profile_id = config.ruleset_id
        teams.append(team)
        standings.ensure_team(team.team_id)
    return LeagueState(
        season=season,
        week=1,
        phase="regular",
        teams=teams,
        standings=standings,
        profile_id="",
        league_config_id="",
        league_format_id=config.league_format_id,
        league_format_version=config.league_format_version,
        ruleset_id=config.ruleset_id,
        ruleset_version="1.0.0",
        schedule_policy_id=config.schedule_policy.policy_id,
        schedule_policy_version="1.0.0",
    )


def _build_depth_chart(team_id: str, roster: list[Player]) -> list:
    from grs.contracts import DepthChartAssignment

    role_map = {
        "QB": ["QB1"],
        "RB": ["RB1"],
        "WR": ["WR1", "WR2", "WR3"],
        "TE": ["TE1"],
        "OL": ["LT", "LG", "C", "RG", "RT"],
        "DL": ["DE1", "DT1", "DT2", "DE2"],
        "LB": ["LB1", "LB2", "LB3"],
        "CB": ["CB1", "CB2"],
        "S": ["S1", "S2"],
        "K": ["K"],
        "P": ["P"],
    }
    by_pos: dict[str, list[Player]] = {}
    for player in roster:
        by_pos.setdefault(player.position, []).append(player)
    assignments: list[DepthChartAssignment] = []
    for pos, slots in role_map.items():
        candidates = sorted(by_pos.get(pos, []), key=lambda p: p.overall_truth, reverse=True)
        for idx, slot in enumerate(slots):
            if idx >= len(candidates):
                continue
            assignments.append(
                DepthChartAssignment(
                    team_id=team_id,
                    player_id=candidates[idx].player_id,
                    slot_role=slot,
                    priority=idx + 1,
                    active_flag=True,
                )
            )
    return assignments
