from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Iterable

from grs.contracts import CausalityChain, NarrativeEvent, PlayResult, RepLedgerEntry
from grs.football.models import SnapResolution
from grs.org.entities import CapLedgerEntry, TransactionRecord


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
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS games (
                    game_id TEXT PRIMARY KEY,
                    season INTEGER NOT NULL,
                    week INTEGER NOT NULL,
                    phase TEXT NOT NULL,
                    home_team_id TEXT NOT NULL,
                    away_team_id TEXT NOT NULL,
                    retained INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS play_results (
                    play_id TEXT PRIMARY KEY,
                    game_id TEXT NOT NULL,
                    yards INTEGER NOT NULL,
                    new_spot INTEGER NOT NULL,
                    turnover INTEGER NOT NULL,
                    turnover_type TEXT,
                    score_event TEXT,
                    penalties_json TEXT NOT NULL,
                    clock_delta INTEGER NOT NULL,
                    next_down INTEGER NOT NULL,
                    next_distance INTEGER NOT NULL,
                    next_possession_team_id TEXT NOT NULL,
                    conditioned INTEGER NOT NULL,
                    attempts INTEGER NOT NULL,
                    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS rep_ledger (
                    rep_id TEXT PRIMARY KEY,
                    play_id TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    rep_type TEXT NOT NULL,
                    assignment_tags_json TEXT NOT NULL,
                    outcome_tags_json TEXT NOT NULL,
                    context_tags_json TEXT NOT NULL,
                    evidence_handles_json TEXT NOT NULL,
                    FOREIGN KEY (play_id) REFERENCES play_results(play_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS rep_actors (
                    rep_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    team_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    assignment_tag TEXT NOT NULL,
                    responsibility_weight REAL NOT NULL,
                    PRIMARY KEY (rep_id, actor_id),
                    FOREIGN KEY (rep_id) REFERENCES rep_ledger(rep_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS causality_nodes (
                    node_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    play_id TEXT NOT NULL,
                    terminal_event TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    weight REAL NOT NULL,
                    description TEXT NOT NULL,
                    FOREIGN KEY (play_id) REFERENCES play_results(play_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS game_summaries (
                    game_id TEXT PRIMARY KEY,
                    home_team_id TEXT NOT NULL,
                    away_team_id TEXT NOT NULL,
                    home_score INTEGER NOT NULL,
                    away_score INTEGER NOT NULL,
                    plays INTEGER NOT NULL,
                    turnovers INTEGER NOT NULL,
                    penalties INTEGER NOT NULL,
                    exported INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS transactions (
                    tx_id TEXT PRIMARY KEY,
                    season INTEGER NOT NULL,
                    week INTEGER NOT NULL,
                    tx_type TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    team_id TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS cap_ledger (
                    entry_id TEXT PRIMARY KEY,
                    team_id TEXT NOT NULL,
                    season INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    amount INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS standings (
                    season INTEGER NOT NULL,
                    team_id TEXT NOT NULL,
                    wins INTEGER NOT NULL,
                    losses INTEGER NOT NULL,
                    ties INTEGER NOT NULL,
                    PRIMARY KEY (season, team_id)
                );

                CREATE TABLE IF NOT EXISTS awards (
                    award_id TEXT PRIMARY KEY,
                    season INTEGER NOT NULL,
                    award_type TEXT NOT NULL,
                    winner_id TEXT NOT NULL,
                    winner_name TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS narrative_events (
                    event_id TEXT PRIMARY KEY,
                    time TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    actors_json TEXT NOT NULL,
                    claims_json TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    confidentiality_tier TEXT NOT NULL
                );
                """
            )

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
            self.save_narrative_events(resolution.narrative_events, conn)

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
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO game_summaries(
                    game_id, home_team_id, away_team_id, home_score, away_score, plays, turnovers, penalties
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (game_id, home_team_id, away_team_id, home_score, away_score, plays, turnovers, penalties),
            )

    def save_transactions(self, txs: Iterable[TransactionRecord]) -> None:
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO transactions(tx_id, season, week, tx_type, summary, team_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [(t.tx_id, t.season, t.week, t.tx_type, t.summary, t.team_id) for t in txs],
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
        if close_conn:
            conn.close()

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
