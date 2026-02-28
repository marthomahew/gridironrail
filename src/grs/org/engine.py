from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import math

from grs.contracts import CapabilityPolicy, LeagueSnapshotRef, NarrativeEvent, ScheduleEntry, TeamStanding
from grs.core import make_id, now_utc
from grs.football.traits import generate_player_traits
from grs.org.entities import (
    CapLedgerEntry,
    Contract,
    ContractYear,
    Franchise,
    LeagueStandingBook,
    LeagueWeek,
    Player,
    Prospect,
    StaffMember,
    TradeRecord,
    TransactionRecord,
)
from grs.org.perception import build_perceived_card


@dataclass(slots=True)
class LeagueState:
    season: int
    week: int
    phase: str
    teams: list[Franchise]
    profile_id: str = ""
    league_config_id: str = ""
    league_format_id: str = "custom_flexible_v1"
    league_format_version: str = "1.0.0"
    ruleset_id: str = "nfl_standard_v1"
    ruleset_version: str = "1.0.0"
    schedule_policy_id: str = "balanced_round_robin"
    schedule_policy_version: str = "1.0.0"
    capability_policy: CapabilityPolicy | None = None
    standings: LeagueStandingBook = field(default_factory=LeagueStandingBook)
    schedule: list[ScheduleEntry] = field(default_factory=list)
    snapshots: list[LeagueSnapshotRef] = field(default_factory=list)
    transactions: list[TransactionRecord] = field(default_factory=list)
    cap_ledger: list[CapLedgerEntry] = field(default_factory=list)
    trades: list[TradeRecord] = field(default_factory=list)
    contracts: list[Contract] = field(default_factory=list)
    prospects: list[Prospect] = field(default_factory=list)
    narrative_events: list[NarrativeEvent] = field(default_factory=list)


class OrganizationalEngine:
    def __init__(
        self,
        rand,
        difficulty,
        regular_season_weeks: int = 18,
        postseason_weeks: int = 4,
        offseason_weeks: int = 12,
    ) -> None:
        self._rand = rand
        self._difficulty = difficulty
        self._regular_season_weeks = regular_season_weeks
        self._postseason_weeks = postseason_weeks
        self._offseason_weeks = offseason_weeks

    def current_week(self, state: LeagueState) -> LeagueWeek:
        return LeagueWeek(season=state.season, week=state.week, phase=state.phase)

    def advance_week(self, state: LeagueState) -> None:
        state.week += 1
        if state.phase == "regular" and state.week > self._regular_season_weeks:
            state.phase = "postseason"
            state.week = 1
        elif state.phase == "postseason" and state.week > self._postseason_weeks:
            state.phase = "offseason"
            state.week = 1
        elif state.phase == "offseason" and state.week > self._offseason_weeks:
            state.phase = "regular"
            state.week = 1
            state.season += 1

        state.narrative_events.append(
            NarrativeEvent(
                event_id=make_id("ne"),
                time=now_utc(),
                scope="org",
                event_type="week_advanced",
                actors=[],
                claims=[f"advanced to S{state.season} W{state.week} {state.phase}"],
                evidence_handles=[f"season:{state.season}", f"week:{state.week}"],
                severity="normal",
                confidentiality_tier="public",
            )
        )

    def offseason_gate(self, state: LeagueState) -> str:
        if state.phase != "offseason":
            return "in_season"
        if state.week <= 2:
            return "re_signing"
        if state.week <= 5:
            return "free_agency"
        if state.week <= 7:
            return "draft"
        return "post_draft"

    def ensure_depth_chart_valid(self, team: Franchise) -> None:
        active = [d for d in team.depth_chart if d.active_flag]
        required = {"QB1", "RB1", "WR1", "WR2", "WR3", "TE1", "LT", "LG", "C", "RG", "RT"}
        present = {d.slot_role for d in active}
        missing = required - present
        if missing:
            raise ValueError(f"team {team.team_id} missing required depth chart slots: {sorted(missing)}")

    def validate_franchise_constraints(self, team: Franchise) -> None:
        if len(team.roster) > 53:
            raise ValueError(f"team {team.team_id} violates roster limit: {len(team.roster)}")
        if team.cap_space < 0:
            raise ValueError(f"team {team.team_id} exceeds cap by {-team.cap_space}")

    def apply_game_result(self, state: LeagueState, home_team_id: str, away_team_id: str, home_score: int, away_score: int) -> None:
        home = state.standings.ensure_team(home_team_id)
        away = state.standings.ensure_team(away_team_id)

        home.points_for += home_score
        home.points_against += away_score
        away.points_for += away_score
        away.points_against += home_score

        if home_score > away_score:
            home.wins += 1
            away.losses += 1
        elif away_score > home_score:
            away.wins += 1
            home.losses += 1
        else:
            home.ties += 1
            away.ties += 1

    def generate_draft_class(self, state: LeagueState, size: int = 224) -> None:
        if state.phase != "offseason":
            return
        state.prospects = []
        positions = ["QB", "RB", "WR", "TE", "OL", "DL", "LB", "CB", "S", "K", "P"]
        for i in range(size):
            state.prospects.append(
                Prospect(
                    prospect_id=make_id("pros"),
                    name=f"Prospect {i + 1}",
                    position=self._rand.choice(positions),
                    age=self._rand.randint(21, 24),
                    draft_grade_truth=35.0 + (self._rand.rand() * 60.0),
                )
            )

    def run_draft_round(self, state: LeagueState, picks_per_round: int = 32) -> None:
        if state.phase != "offseason" or not state.prospects:
            return

        ordered = sorted(state.teams, key=lambda t: t.owner.patience)
        for team in ordered[:picks_per_round]:
            self.validate_franchise_constraints(team)
            scouts = [s for s in team.staff if s.role == "Scout"]
            cards = [(prospect, self._perceived_draft_score(prospect, scouts)) for prospect in state.prospects]
            cards.sort(key=lambda pair: pair[1], reverse=True)
            selected, perceived_score = cards[0]
            state.prospects.remove(selected)
            if len(team.roster) >= 53:
                raise ValueError(f"cannot draft for {team.team_id}: roster full")

            player = Player(
                player_id=make_id("ply"),
                team_id=team.team_id,
                name=selected.name,
                position=selected.position,
                age=selected.age,
                overall_truth=selected.draft_grade_truth,
                volatility_truth=0.1 + (self._rand.rand() * 0.9),
                injury_susceptibility_truth=0.1 + (self._rand.rand() * 0.8),
                hidden_dev_curve=30.0 + (
                    65.0 * self._unit_sigmoid(((selected.draft_grade_truth + (self._rand.rand() * 16.0 - 8.0)) - 62.5) * 0.08)
                ),
                traits={},
            )
            player.traits = generate_player_traits(
                player_id=player.player_id,
                position=player.position,
                overall_truth=player.overall_truth,
                volatility_truth=player.volatility_truth,
                injury_susceptibility_truth=player.injury_susceptibility_truth,
            )
            team.roster.append(player)

            contract = Contract(
                contract_id=make_id("ctr"),
                player_id=player.player_id,
                team_id=team.team_id,
                years=[
                    ContractYear(
                        year=state.season + i,
                        base_salary=1_200_000 + (i * 500_000),
                        bonus_prorated=250_000,
                        guaranteed=750_000,
                    )
                    for i in range(4)
                ],
                signed_date=date.today(),
            )
            state.contracts.append(contract)
            year_one = contract.years[0].base_salary + contract.years[0].bonus_prorated
            team.cap_space -= year_one
            self._book_cap(state, team.team_id, state.season, "rookie_contract", year_one)
            self.validate_franchise_constraints(team)
            state.transactions.append(
                TransactionRecord(
                    tx_id=make_id("tx"),
                    season=state.season,
                    week=state.week,
                    tx_type="draft",
                    summary=f"{team.name} selected {player.name} ({player.position})",
                    team_id=team.team_id,
                    causality_context={
                        "scout_quality": round(sum(s.evaluation for s in scouts) / len(scouts), 3) if scouts else 0.5,
                        "perceived_score": round(perceived_score, 3),
                    },
                )
            )

    def run_free_agency(self, state: LeagueState, free_agents: list[Player]) -> None:
        if state.phase != "offseason":
            return
        for player in list(free_agents):
            offers: list[tuple[Franchise, int, float]] = []
            for team in state.teams:
                self.validate_franchise_constraints(team)
                need_bonus = 1.0 + (0.2 if self._position_need(team, player.position) else 0.0)
                bid = int((player.overall_truth * 120_000) * need_bonus / self._difficulty.negotiation_friction_multiplier)
                leverage = (1.0 - team.owner.patience) * self._difficulty.ownership_pressure_multiplier
                if bid <= team.cap_space:
                    offers.append((team, bid, leverage))
            if not offers:
                continue
            offers.sort(key=lambda x: (x[1], x[2]), reverse=True)
            winner, bid, leverage = offers[0]
            if len(winner.roster) >= 53:
                raise ValueError(f"cannot sign free agent for {winner.team_id}: roster full")

            winner.roster.append(player)
            winner.cap_space -= bid
            player.team_id = winner.team_id
            self._book_cap(state, winner.team_id, state.season, "free_agency", bid)
            self.validate_franchise_constraints(winner)
            state.transactions.append(
                TransactionRecord(
                    tx_id=make_id("tx"),
                    season=state.season,
                    week=state.week,
                    tx_type="free_agency",
                    summary=f"{winner.name} signed {player.name} for {bid}",
                    team_id=winner.team_id,
                    causality_context={"offer": bid, "owner_leverage": round(leverage, 3)},
                )
            )
            free_agents.remove(player)

    def run_trade_window(self, state: LeagueState) -> None:
        if state.phase not in {"regular", "offseason"}:
            return
        if len(state.teams) < 2:
            return

        team_a = self._rand.choice(state.teams)
        team_b = self._rand.choice([t for t in state.teams if t.team_id != team_a.team_id])
        if not team_a.roster or not team_b.roster:
            return
        a_player = self._rand.choice(team_a.roster)
        b_player = self._rand.choice(team_b.roster)

        a_val = self._perceived_player_value(a_player)
        b_val = self._perceived_player_value(b_player)
        leverage = (1.0 - team_a.owner.patience) * self._difficulty.ownership_pressure_multiplier

        if abs(a_val - b_val) < 15 + (leverage * 20):
            team_a.roster.remove(a_player)
            team_b.roster.remove(b_player)
            team_a.roster.append(b_player)
            team_b.roster.append(a_player)
            a_player.team_id = team_b.team_id
            b_player.team_id = team_a.team_id
            state.trades.append(
                TradeRecord(
                    trade_id=make_id("trade"),
                    season=state.season,
                    week=state.week,
                    from_team_id=team_a.team_id,
                    to_team_id=team_b.team_id,
                    assets_from=[a_player.player_id],
                    assets_to=[b_player.player_id],
                )
            )
            state.transactions.append(
                TransactionRecord(
                    tx_id=make_id("tx"),
                    season=state.season,
                    week=state.week,
                    tx_type="trade",
                    summary=f"{team_a.name} traded {a_player.name} to {team_b.name} for {b_player.name}",
                    team_id=team_a.team_id,
                    causality_context={
                        "a_value": round(a_val, 2),
                        "b_value": round(b_val, 2),
                        "owner_pressure": round(leverage, 2),
                    },
                )
            )

    def develop_players(self, state: LeagueState) -> None:
        for team in state.teams:
            coaches = [s for s in team.staff if "Coach" in s.role]
            if not coaches:
                raise ValueError(f"team {team.team_id} is missing coach staff required for development")
            dev_quality = sum(c.development for c in coaches) / len(coaches)
            for player in team.roster:
                age_penalty = 0.0
                if player.age >= 30:
                    age_penalty = (player.age - 29) * 0.7 * self._difficulty.aging_variance_multiplier
                growth = (dev_quality * 2.0) - age_penalty + ((self._rand.rand() * 2.0 - 1.0) * 2.2)
                signal = player.overall_truth + growth
                player.overall_truth = 20.0 + (79.0 * self._unit_sigmoid((signal - 59.5) * 0.11))
                player.age += 1 if state.phase == "offseason" and state.week == 1 else 0

    def update_ownership_pressure(self, state: LeagueState, team_results: dict[str, float]) -> None:
        for team in state.teams:
            if team.team_id not in team_results:
                raise ValueError(f"missing team_result entry for {team.team_id}")
            perf = team_results[team.team_id]
            pressure = (1.0 - perf) * team.owner.spending_aggressiveness * self._difficulty.ownership_pressure_multiplier
            fire_chance = self._unit_sigmoid((pressure - 0.5) * 4.0)
            if pressure > 0.5 and self._rand.rand() < fire_chance:
                fired = next((s for s in team.staff if s.role == "HeadCoach"), None)
                if fired:
                    team.staff.remove(fired)
                    team.staff.append(
                        StaffMember(
                            staff_id=make_id("staff"),
                            name=f"HC Replacement {team.name}",
                            role="HeadCoach",
                            evaluation=0.45 + (self._rand.rand() * 0.4),
                            development=0.45 + (self._rand.rand() * 0.4),
                            discipline=0.45 + (self._rand.rand() * 0.4),
                            adaptability=0.45 + (self._rand.rand() * 0.4),
                        )
                    )
                    state.transactions.append(
                        TransactionRecord(
                            tx_id=make_id("tx"),
                            season=state.season,
                            week=state.week,
                            tx_type="coaching_change",
                        summary=f"{team.name} fired {fired.name}",
                        team_id=team.team_id,
                        causality_context={"pressure": round(pressure, 3), "fire_chance": round(fire_chance, 3)},
                    )
                )

    def perceived_cards_for_team(self, state: LeagueState, team_id: str) -> list:
        team = next(t for t in state.teams if t.team_id == team_id)
        scouts = [s for s in team.staff if s.role == "Scout"]
        coaches = [s for s in team.staff if "Coach" in s.role]
        medical = [s for s in team.staff if s.role == "Medical"]
        rand = self._rand.spawn(f"perceived:{state.season}:{state.week}:{team_id}")
        return [
            build_perceived_card(
                player=p,
                team_id=team_id,
                scouts=scouts,
                coaches=coaches,
                medical=medical,
                rand=rand,
                scouting_noise_multiplier=self._difficulty.scouting_noise_multiplier,
            )
            for p in team.roster
        ]

    def ai_rank_players(self, cards: list) -> list:
        return sorted(
            cards,
            key=lambda c: (
                c.scout_metrics[0].estimate * 0.6
                + c.coach_metrics[0].estimate * 0.25
                + c.medical_metrics[0].estimate * 0.15
            ),
            reverse=True,
        )

    def _position_need(self, team: Franchise, position: str) -> bool:
        return sum(1 for p in team.roster if p.position == position) < 3

    def _perceived_draft_score(self, prospect: Prospect, scouts: list[StaffMember]) -> float:
        if not scouts:
            raise ValueError("draft scoring requires at least one scout")
        quality = sum(s.evaluation for s in scouts) / len(scouts)
        noise = (self._rand.rand() * 2.0 - 1.0) * (1.0 - quality) * 25.0 * self._difficulty.scouting_noise_multiplier
        return prospect.draft_grade_truth + noise

    def _perceived_player_value(self, player: Player) -> float:
        noise = (self._rand.rand() * 2.0 - 1.0) * 14.0 * self._difficulty.scouting_noise_multiplier
        return player.overall_truth + noise

    def _book_cap(self, state: LeagueState, team_id: str, season: int, reason: str, amount: int) -> None:
        state.cap_ledger.append(
            CapLedgerEntry(
                entry_id=make_id("cap"),
                team_id=team_id,
                season=season,
                reason=reason,
                amount=amount,
            )
        )

    def _unit_sigmoid(self, signal: float) -> float:
        return 1.0 / (1.0 + math.exp(-signal))

    def standings_dict(self, state: LeagueState) -> dict[str, TeamStanding]:
        return dict(state.standings.entries)
