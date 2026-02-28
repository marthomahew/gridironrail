from __future__ import annotations

from grs.core import make_id
from grs.org.engine import LeagueState
from grs.org.entities import Franchise, Owner, Player, StaffMember, TeamIdentityProfile


def build_default_league(team_count: int = 8, season: int = 2026) -> LeagueState:
    teams: list[Franchise] = []
    for idx in range(team_count):
        team_id = f"T{idx + 1:02d}"
        owner = Owner(
            owner_id=make_id("own"),
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
            StaffMember(make_id("staff"), f"HC {idx + 1}", "HeadCoach", 0.6, 0.6, 0.6, 0.6),
            StaffMember(make_id("staff"), f"OC {idx + 1}", "OffensiveCoach", 0.58, 0.61, 0.55, 0.6),
            StaffMember(make_id("staff"), f"DC {idx + 1}", "DefensiveCoach", 0.58, 0.59, 0.58, 0.57),
            StaffMember(make_id("staff"), f"Scout {idx + 1}", "Scout", 0.55, 0.5, 0.5, 0.5),
            StaffMember(make_id("staff"), f"Medical {idx + 1}", "Medical", 0.55, 0.5, 0.5, 0.5),
        ]
        roster: list[Player] = []
        positions = ["QB", "RB", "WR", "TE", "OL", "DL", "LB", "CB", "S", "K", "P"]
        for p_idx in range(53):
            position = positions[p_idx % len(positions)]
            roster.append(
                Player(
                    player_id=make_id("ply"),
                    team_id=team_id,
                    name=f"{team_id} Player {p_idx + 1}",
                    position=position,
                    age=22 + (p_idx % 12),
                    overall_truth=55 + ((p_idx % 30) * 1.2),
                    volatility_truth=0.25 + ((p_idx % 7) * 0.08),
                    injury_susceptibility_truth=0.2 + ((p_idx % 8) * 0.07),
                    hidden_dev_curve=50 + ((p_idx % 35) * 1.1),
                )
            )
        teams.append(Franchise(team_id=team_id, name=f"Team {idx + 1}", owner=owner, identity=identity, staff=staff, roster=roster))

    return LeagueState(season=season, week=1, phase="regular", teams=teams)
