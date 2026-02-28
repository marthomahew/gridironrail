from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable

from grs.contracts import (
    CausalityChain,
    LeagueSnapshotRef,
    NarrativeEvent,
    PlayResult,
    RepLedgerEntry,
    ScheduleEntry,
    TeamStanding,
)
from grs.football.models import GameSessionResult, SnapResolution
from grs.org.entities import CapLedgerEntry, Franchise, TransactionRecord
from grs.persistence.migrations import MigrationRunner


class AuthoritativeStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def initialize_schema(self) -> None:
        with self.connect() as conn:
            MigrationRunner(conn).apply()

    def save_league_state(self, season: int, week: int, phase: str, teams: list[Franchise], metadata: dict[str, Any] | None = None) -> None:
        with self.connect() as conn:
            for team in teams:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO teams(team_id, name, owner_name, cap_space, mandate)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (team.team_id, team.name, team.owner.name, team.cap_space, team.owner.mandate),
                )
                for player in team.roster:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO players(
                            player_id, team_id, name, position, age, overall_truth, volatility_truth,
                            injury_susceptibility_truth, hidden_dev_curve, morale
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            player.player_id,
                            team.team_id,
                            player.name,
                            player.position,
                            player.age,
                            player.overall_truth,
                            player.volatility_truth,
                            player.injury_susceptibility_truth,
                            player.hidden_dev_curve,
                            player.morale,
                        ),
                    )
                for staff in team.staff:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO staff(
                            staff_id, team_id, name, role, evaluation, development, discipline, adaptability
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            staff.staff_id,
                            team.team_id,
                            staff.name,
                            staff.role,
                            staff.evaluation,
                            staff.development,
                            staff.discipline,
                            staff.adaptability,
                        ),
                    )
                conn.execute("DELETE FROM depth_chart WHERE team_id = ?", (team.team_id,))
                for d in team.depth_chart:
                    conn.execute(
                        """
                        INSERT INTO depth_chart(team_id, player_id, slot_role, priority, active_flag)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (d.team_id, d.player_id, d.slot_role, d.priority, int(d.active_flag)),
                    )

            conn.execute(
                """
                INSERT OR REPLACE INTO season_state(season, phase, current_week, metadata_json)
                VALUES (?, ?, ?, ?)
                """,
                (season, phase, week, json.dumps(metadata or {})),
            )

    def save_contracts(self, contracts: Iterable[Any]) -> None:
        with self.connect() as conn:
            for c in contracts:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO contracts(contract_id, player_id, team_id, signed_date, years_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        c.contract_id,
                        c.player_id,
                        c.team_id,
                        c.signed_date.isoformat(),
                        json.dumps([asdict(y) for y in c.years]),
                    ),
                )

    def save_schedule_entries(self, entries: Iterable[ScheduleEntry]) -> None:
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO schedule(game_id, season, week, home_team_id, away_team_id, status, is_user_game)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        e.game_id,
                        e.season,
                        e.week,
                        e.home_team_id,
                        e.away_team_id,
                        e.status,
                        int(e.is_user_game),
                    )
                    for e in entries
                ],
            )

    def get_schedule_for_week(self, season: int, week: int) -> list[ScheduleEntry]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT game_id, season, week, home_team_id, away_team_id, status, is_user_game FROM schedule WHERE season = ? AND week = ? ORDER BY game_id",
                (season, week),
            ).fetchall()
        return [
            ScheduleEntry(
                game_id=r[0],
                season=r[1],
                week=r[2],
                home_team_id=r[3],
                away_team_id=r[4],
                status=r[5],
                is_user_game=bool(r[6]),
            )
            for r in rows
        ]

    def update_schedule_status(self, game_id: str, status: str) -> None:
        with self.connect() as conn:
            conn.execute("UPDATE schedule SET status = ? WHERE game_id = ?", (status, game_id))

    def register_game(
        self,
        game_id: str,
        season: int,
        week: int,
        phase: str,
        home_team_id: str,
        away_team_id: str,
        retained: bool,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO games(game_id, season, week, phase, home_team_id, away_team_id, retained, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (game_id, season, week, phase, home_team_id, away_team_id, int(retained), "scheduled"),
            )

    def set_game_status(self, game_id: str, status: str) -> None:
        with self.connect() as conn:
            conn.execute("UPDATE games SET status = ? WHERE game_id = ?", (status, game_id))

    def save_snap_resolution(self, game_id: str, resolution: SnapResolution, retained: bool) -> None:
        with self.connect() as conn:
            self._insert_play_result(conn, game_id, resolution.play_result, resolution.conditioned, resolution.attempts)
            self._insert_causality(conn, resolution.causality_chain)
            if retained:
                self._insert_rep_ledger(conn, resolution.rep_ledger)
            self._save_narrative_events_conn(conn, resolution.narrative_events)

    def save_game_session_result(
        self,
        season: int,
        week: int,
        mode: str,
        result: GameSessionResult,
        retained: bool,
        seed: int | None,
    ) -> None:
        state_json = json.dumps(asdict(result.final_state), default=str)
        action_stream_json = json.dumps(result.action_stream)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO game_state(
                    game_id, season, week, mode, state_json, action_stream_json, seed, retained, finalized
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.final_state.game_id,
                    season,
                    week,
                    mode,
                    state_json,
                    action_stream_json,
                    seed,
                    int(retained),
                    int(result.final_state.completed),
                ),
            )

    def load_game_session_state(self, game_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT state_json, action_stream_json, seed, retained, finalized FROM game_state WHERE game_id = ?", (game_id,)).fetchone()
        if not row:
            return None
        return {
            "state": json.loads(row[0]),
            "action_stream": json.loads(row[1]),
            "seed": row[2],
            "retained": bool(row[3]),
            "finalized": bool(row[4]),
        }

    def save_week_snapshot(self, snapshot: LeagueSnapshotRef, snapshot_payload: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO week_snapshots(snapshot_id, season, week, created_at, blob_path, snapshot_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.snapshot_id,
                    snapshot.season,
                    snapshot.week,
                    snapshot.created_at.isoformat(),
                    snapshot.blob_path,
                    json.dumps(snapshot_payload),
                ),
            )

    def save_standings_week(self, season: int, week: int, standings: dict[str, TeamStanding]) -> None:
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO standings_history(
                    season, week, team_id, wins, losses, ties, points_for, points_against
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (season, week, s.team_id, s.wins, s.losses, s.ties, s.points_for, s.points_against)
                    for s in standings.values()
                ],
            )

    def get_latest_standings(self, season: int, week: int) -> list[tuple]:
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT team_id, wins, losses, ties, points_for, points_against
                FROM standings_history
                WHERE season = ? AND week = ?
                ORDER BY wins DESC, losses ASC, (points_for - points_against) DESC
                """,
                (season, week),
            ).fetchall()

    def list_retained_games(self) -> list[tuple[str, int, int]]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT game_id, season, week FROM games WHERE retained = 1 AND status = 'final' ORDER BY season DESC, week DESC, game_id DESC"
            ).fetchall()

    def load_film_room_game(self, game_id: str) -> dict[str, Any]:
        with self.connect() as conn:
            plays = conn.execute(
                "SELECT play_id, yards, score_event, turnover_type FROM play_results WHERE game_id = ? ORDER BY play_id",
                (game_id,),
            ).fetchall()
            reps = conn.execute(
                "SELECT rep_id, play_id, phase, rep_type, context_tags_json FROM rep_ledger WHERE play_id IN (SELECT play_id FROM play_results WHERE game_id = ?)",
                (game_id,),
            ).fetchall()
            causality = conn.execute(
                "SELECT play_id, terminal_event, source_id, weight, description FROM causality_nodes WHERE play_id IN (SELECT play_id FROM play_results WHERE game_id = ?) ORDER BY node_id",
                (game_id,),
            ).fetchall()
        return {
            "plays": [
                {"play_id": p[0], "yards": p[1], "score_event": p[2], "turnover_type": p[3]}
                for p in plays
            ],
            "reps": [
                {
                    "rep_id": r[0],
                    "play_id": r[1],
                    "phase": r[2],
                    "rep_type": r[3],
                    "context_tags": json.loads(r[4]),
                }
                for r in reps
            ],
            "causality": [
                {"play_id": c[0], "terminal_event": c[1], "source_id": c[2], "weight": c[3], "description": c[4]}
                for c in causality
            ],
        }

    def save_game_summary(
        self,
        game_id: str,
        home_team_id: str,
        away_team_id: str,
        home_score: int,
        away_score: int,
        plays: int,
        turnovers: int,
        penalties: int,
        season: int | None = None,
        week: int | None = None,
    ) -> None:
        if season is None or week is None:
            with self.connect() as conn:
                row = conn.execute("SELECT season, week FROM games WHERE game_id = ?", (game_id,)).fetchone()
                if row:
                    season, week = row
                else:
                    season, week = 0, 0

        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO game_summaries(
                    game_id, season, week, home_team_id, away_team_id, home_score, away_score, plays, turnovers, penalties
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (game_id, season, week, home_team_id, away_team_id, home_score, away_score, plays, turnovers, penalties),
            )

    def save_transactions(self, txs: Iterable[TransactionRecord]) -> None:
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO transactions(tx_id, season, week, tx_type, summary, team_id, context_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (t.tx_id, t.season, t.week, t.tx_type, t.summary, t.team_id, json.dumps(t.causality_context))
                    for t in txs
                ],
            )

    def save_cap_ledger(self, entries: Iterable[CapLedgerEntry]) -> None:
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO cap_ledger(entry_id, team_id, season, reason, amount)
                VALUES (?, ?, ?, ?, ?)
                """,
                [(e.entry_id, e.team_id, e.season, e.reason, e.amount) for e in entries],
            )

    def save_narrative_events(self, events: Iterable[NarrativeEvent], conn: sqlite3.Connection | None = None) -> None:
        close_conn = False
        if conn is None:
            conn = self.connect()
            close_conn = True
        self._save_narrative_events_conn(conn, events)
        if close_conn:
            conn.close()

    def _save_narrative_events_conn(self, conn: sqlite3.Connection, events: Iterable[NarrativeEvent]) -> None:
        conn.executemany(
            """
            INSERT OR REPLACE INTO narrative_events(
                event_id, time, scope, event_type, actors_json, claims_json, evidence_json, severity, confidentiality_tier
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    e.event_id,
                    e.time.isoformat(),
                    e.scope,
                    e.event_type,
                    json.dumps(e.actors),
                    json.dumps(e.claims),
                    json.dumps(e.evidence_handles),
                    e.severity,
                    e.confidentiality_tier,
                )
                for e in events
            ],
        )

    def purge_non_retained_deep_logs(self, game_id: str) -> None:
        with self.connect() as conn:
            retained = conn.execute("SELECT retained FROM games WHERE game_id = ?", (game_id,)).fetchone()
            if not retained or retained[0] == 1:
                return
            conn.execute(
                """
                DELETE FROM rep_actors
                WHERE rep_id IN (
                    SELECT rep_id FROM rep_ledger
                    WHERE play_id IN (SELECT play_id FROM play_results WHERE game_id = ?)
                )
                """,
                (game_id,),
            )
            conn.execute(
                "DELETE FROM rep_ledger WHERE play_id IN (SELECT play_id FROM play_results WHERE game_id = ?)",
                (game_id,),
            )

    def season_rollover_integrity_check(self, season: int) -> None:
        with self.connect() as conn:
            missing_summaries = conn.execute(
                """
                SELECT COUNT(*)
                FROM games g
                LEFT JOIN game_summaries s ON s.game_id = g.game_id
                WHERE g.season = ? AND s.game_id IS NULL AND g.status = 'final'
                """,
                (season,),
            ).fetchone()[0]
            if missing_summaries:
                raise ValueError(f"integrity failure: {missing_summaries} finalized games missing summaries")

            bad_weights = conn.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT rep_id, ROUND(SUM(responsibility_weight), 4) AS total
                    FROM rep_actors
                    GROUP BY rep_id
                )
                WHERE ABS(total - 1.0) > 0.001
                """
            ).fetchone()[0]
            if bad_weights:
                raise ValueError(f"integrity failure: {bad_weights} reps have invalid responsibility weights")

            orphan_game_state = conn.execute(
                """
                SELECT COUNT(*)
                FROM game_state gs
                LEFT JOIN games g ON g.game_id = gs.game_id
                WHERE g.game_id IS NULL
                """
            ).fetchone()[0]
            if orphan_game_state:
                raise ValueError(f"integrity failure: {orphan_game_state} orphan game_state rows")

    def _insert_play_result(
        self,
        conn: sqlite3.Connection,
        game_id: str,
        play: PlayResult,
        conditioned: bool,
        attempts: int,
    ) -> None:
        penalties_json = json.dumps([asdict(p) for p in play.penalties])
        conn.execute(
            """
            INSERT OR REPLACE INTO play_results(
                play_id, game_id, yards, new_spot, turnover, turnover_type, score_event,
                penalties_json, clock_delta, next_down, next_distance, next_possession_team_id,
                conditioned, attempts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                play.play_id,
                game_id,
                play.yards,
                play.new_spot,
                int(play.turnover),
                play.turnover_type,
                play.score_event,
                penalties_json,
                play.clock_delta,
                play.next_down,
                play.next_distance,
                play.next_possession_team_id,
                int(conditioned),
                attempts,
            ),
        )

    def _insert_rep_ledger(self, conn: sqlite3.Connection, reps: list[RepLedgerEntry]) -> None:
        for rep in reps:
            conn.execute(
                """
                INSERT OR REPLACE INTO rep_ledger(
                    rep_id, play_id, phase, rep_type, assignment_tags_json,
                    outcome_tags_json, context_tags_json, evidence_handles_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rep.rep_id,
                    rep.play_id,
                    rep.phase,
                    rep.rep_type,
                    json.dumps(rep.assignment_tags),
                    json.dumps(rep.outcome_tags),
                    json.dumps(rep.context_tags),
                    json.dumps(rep.evidence_handles),
                ),
            )
            for actor in rep.actors:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO rep_actors(
                        rep_id, actor_id, team_id, role, assignment_tag, responsibility_weight
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rep.rep_id,
                        actor.actor_id,
                        actor.team_id,
                        actor.role,
                        actor.assignment_tag,
                        rep.responsibility_weights.get(actor.actor_id, 0.0),
                    ),
                )

    def _insert_causality(self, conn: sqlite3.Connection, chain: CausalityChain) -> None:
        for node in chain.nodes:
            conn.execute(
                """
                INSERT INTO causality_nodes(play_id, terminal_event, source_type, source_id, weight, description)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    chain.play_id,
                    chain.terminal_event,
                    node.source_type,
                    node.source_id,
                    node.weight,
                    node.description,
                ),
            )
