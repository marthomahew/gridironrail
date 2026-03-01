from __future__ import annotations

from grs.core import seeded_random
from grs.contracts import DepthChartAssignment
from grs.football.packages import PackageCompiler
from grs.org.engine import LeagueState
from grs.org.entities import Franchise, LeagueStandingBook, Owner, Player, StaffMember, TeamIdentityProfile
from grs.org.resources import OrgResourceResolver, PlayerCreationEngine


def _build_depth_chart(team_id: str, roster: list[Player]) -> list[DepthChartAssignment]:
    role_map = {
        "QB": ["QB1", "QB2"],
        "RB": ["RB1", "RB2"],
        "WR": ["WR1", "WR2", "WR3", "WR4"],
        "TE": ["TE1", "TE2"],
        "OL": ["LT", "LG", "C", "RG", "RT"],
        "DL": ["DE1", "DT1", "DT2", "DE2"],
        "LB": ["LB1", "LB2", "LB3", "LB4"],
        "CB": ["CB1", "CB2", "CB3"],
        "S": ["S1", "S2", "S3"],
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


def build_default_league(team_count: int = 8, season: int = 2026) -> LeagueState:
    default_names = [
        "Apex Guardians",
        "Harbor Storm",
        "Granite Wolves",
        "River Kings",
        "Summit Ravens",
        "Cedar Legion",
        "Liberty Riders",
        "Pioneer Titans",
    ]
    player_creator = PlayerCreationEngine(resource_resolver=OrgResourceResolver())
    package_compiler = PackageCompiler()
    teams: list[Franchise] = []
    standings = LeagueStandingBook()
    for idx in range(team_count):
        team_id = f"T{idx + 1:02d}"
        owner = Owner(
            owner_id=f"OWN_{team_id}",
            name=f"Owner {idx + 1}",
            risk_tolerance=0.35 + ((idx % 5) * 0.1),
            patience=0.3 + ((idx % 4) * 0.15),
            spending_aggressiveness=0.45 + ((idx % 3) * 0.2),
            mandate="playoffs",
        )
        identity = TeamIdentityProfile(
            scheme_offense="multiple",
            scheme_defense="hybrid",
            roster_strategy="balanced",
            risk_posture="moderate",
        )
        staff = [
            StaffMember(f"{team_id}_STAFF_HC", f"HC {idx + 1}", "HeadCoach", 0.6, 0.6, 0.6, 0.6),
            StaffMember(f"{team_id}_STAFF_OC", f"OC {idx + 1}", "OffensiveCoach", 0.58, 0.61, 0.55, 0.6),
            StaffMember(f"{team_id}_STAFF_DC", f"DC {idx + 1}", "DefensiveCoach", 0.58, 0.59, 0.58, 0.57),
            StaffMember(f"{team_id}_STAFF_SCOUT", f"Scout {idx + 1}", "Scout", 0.55, 0.5, 0.5, 0.5),
            StaffMember(f"{team_id}_STAFF_MED", f"Medical {idx + 1}", "Medical", 0.55, 0.5, 0.5, 0.5),
        ]
        roster: list[Player] = []
        used_jerseys: set[int] = set()
        team_rand = seeded_random((idx + 1) * 1000)
        positions = ["QB", "RB", "WR", "TE", "OL", "DL", "LB", "CB", "S", "K", "P"]
        for p_idx in range(53):
            position = positions[p_idx % len(positions)]
            player_id = f"{team_id}_P{p_idx + 1:02d}"
            overall_truth = 55 + ((p_idx % 30) * 1.2)
            volatility_truth = 0.25 + ((p_idx % 7) * 0.08)
            injury_susceptibility_truth = 0.2 + ((p_idx % 8) * 0.07)
            roster.append(
                player_creator.create_player(
                    player_id=player_id,
                    team_id=team_id,
                    position=position,
                    overall_truth=overall_truth,
                    volatility_truth=volatility_truth,
                    injury_susceptibility_truth=injury_susceptibility_truth,
                    hidden_dev_curve=50 + ((p_idx % 35) * 1.1),
                    rand=team_rand.spawn(f"player:{player_id}"),
                    used_jerseys=used_jerseys,
                )
            )
        depth_chart = _build_depth_chart(team_id, roster)
        package_book = package_compiler.compile_team_package_book(
            team_id=team_id,
            season=season,
            week=1,
            depth_chart=depth_chart,
            roster_player_ids={player.player_id for player in roster},
            source="auto_depth_chart",
        )
        teams.append(
            Franchise(
                team_id=team_id,
                name=default_names[idx % len(default_names)],
                owner=owner,
                identity=identity,
                staff=staff,
                roster=roster,
                depth_chart=depth_chart,
                package_book=package_book.assignments,
                coaching_policy_id="balanced_base",
                rules_profile_id="nfl_standard_v1",
            )
        )
        standings.ensure_team(team_id)

    return LeagueState(season=season, week=1, phase="regular", teams=teams, standings=standings)
