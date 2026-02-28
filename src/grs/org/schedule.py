from __future__ import annotations

from dataclasses import dataclass

from grs.contracts import ScheduleEntry, TeamStanding


@dataclass(slots=True)
class StandingRank:
    team_id: str
    wins: int
    losses: int
    ties: int
    point_diff: int


def generate_season_schedule(team_ids: list[str], season: int, user_team_id: str, weeks: int = 18) -> list[ScheduleEntry]:
    if len(team_ids) % 2 != 0:
        raise ValueError("team count must be even for schedule generation")

    ordered = sorted(team_ids)
    rotate_by = season % len(ordered)
    ordered = ordered[rotate_by:] + ordered[:rotate_by]

    rounds: list[list[tuple[str, str]]] = []
    teams = ordered[:]
    n = len(teams)
    for _ in range(n - 1):
        pairs = []
        for i in range(n // 2):
            home = teams[i]
            away = teams[n - 1 - i]
            pairs.append((home, away))
        rounds.append(pairs)
        teams = [teams[0]] + [teams[-1]] + teams[1:-1]

    weekly_games: list[list[tuple[str, str]]] = []
    weekly_games.extend(rounds)
    weekly_games.extend([[(away, home) for (home, away) in r] for r in rounds])

    idx = 0
    while len(weekly_games) < weeks:
        weekly_games.append(rounds[idx % len(rounds)])
        idx += 1

    schedule: list[ScheduleEntry] = []
    for week_no in range(1, weeks + 1):
        games = weekly_games[week_no - 1]
        for gidx, (home, away) in enumerate(games, start=1):
            game_id = f"S{season}_W{week_no}_G{gidx:02d}"
            schedule.append(
                ScheduleEntry(
                    game_id=game_id,
                    season=season,
                    week=week_no,
                    home_team_id=home,
                    away_team_id=away,
                    status="scheduled",
                    is_user_game=user_team_id in {home, away},
                )
            )
    return schedule


def rank_standings(standings: dict[str, TeamStanding]) -> list[StandingRank]:
    ranked = sorted(
        standings.values(),
        key=lambda s: (s.wins, -s.losses, s.point_diff, s.points_for),
        reverse=True,
    )
    return [
        StandingRank(team_id=s.team_id, wins=s.wins, losses=s.losses, ties=s.ties, point_diff=s.point_diff)
        for s in ranked
    ]
