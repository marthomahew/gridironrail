from __future__ import annotations

import sqlite3

MIGRATIONS: list[tuple[int, str]] = [
    (
        1,
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS teams (
            team_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            owner_name TEXT NOT NULL,
            cap_space INTEGER NOT NULL,
            mandate TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS players (
            player_id TEXT PRIMARY KEY,
            team_id TEXT NOT NULL,
            name TEXT NOT NULL,
            position TEXT NOT NULL,
            age INTEGER NOT NULL,
            overall_truth REAL NOT NULL,
            volatility_truth REAL NOT NULL,
            injury_susceptibility_truth REAL NOT NULL,
            hidden_dev_curve REAL NOT NULL,
            morale REAL NOT NULL,
            FOREIGN KEY (team_id) REFERENCES teams(team_id)
        );

        CREATE TABLE IF NOT EXISTS staff (
            staff_id TEXT PRIMARY KEY,
            team_id TEXT NOT NULL,
            name TEXT NOT NULL,
            role TEXT NOT NULL,
            evaluation REAL NOT NULL,
            development REAL NOT NULL,
            discipline REAL NOT NULL,
            adaptability REAL NOT NULL,
            FOREIGN KEY (team_id) REFERENCES teams(team_id)
        );

        CREATE TABLE IF NOT EXISTS depth_chart (
            team_id TEXT NOT NULL,
            player_id TEXT NOT NULL,
            slot_role TEXT NOT NULL,
            priority INTEGER NOT NULL,
            active_flag INTEGER NOT NULL,
            PRIMARY KEY (team_id, slot_role, priority),
            FOREIGN KEY (team_id) REFERENCES teams(team_id),
            FOREIGN KEY (player_id) REFERENCES players(player_id)
        );

        CREATE TABLE IF NOT EXISTS contracts (
            contract_id TEXT PRIMARY KEY,
            player_id TEXT NOT NULL,
            team_id TEXT NOT NULL,
            signed_date TEXT NOT NULL,
            years_json TEXT NOT NULL,
            FOREIGN KEY (player_id) REFERENCES players(player_id),
            FOREIGN KEY (team_id) REFERENCES teams(team_id)
        );

        CREATE TABLE IF NOT EXISTS schedule (
            game_id TEXT PRIMARY KEY,
            season INTEGER NOT NULL,
            week INTEGER NOT NULL,
            home_team_id TEXT NOT NULL,
            away_team_id TEXT NOT NULL,
            status TEXT NOT NULL,
            is_user_game INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS standings_history (
            season INTEGER NOT NULL,
            week INTEGER NOT NULL,
            team_id TEXT NOT NULL,
            wins INTEGER NOT NULL,
            losses INTEGER NOT NULL,
            ties INTEGER NOT NULL,
            points_for INTEGER NOT NULL,
            points_against INTEGER NOT NULL,
            PRIMARY KEY (season, week, team_id)
        );

        CREATE TABLE IF NOT EXISTS awards_history (
            award_id TEXT PRIMARY KEY,
            season INTEGER NOT NULL,
            award_type TEXT NOT NULL,
            winner_id TEXT NOT NULL,
            winner_name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS season_state (
            season INTEGER PRIMARY KEY,
            phase TEXT NOT NULL,
            current_week INTEGER NOT NULL,
            metadata_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS game_state (
            game_id TEXT PRIMARY KEY,
            season INTEGER NOT NULL,
            week INTEGER NOT NULL,
            mode TEXT NOT NULL,
            state_json TEXT NOT NULL,
            action_stream_json TEXT NOT NULL,
            seed INTEGER,
            retained INTEGER NOT NULL,
            finalized INTEGER NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS week_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            season INTEGER NOT NULL,
            week INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            blob_path TEXT NOT NULL,
            snapshot_json TEXT NOT NULL
        );

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
            season INTEGER NOT NULL,
            week INTEGER NOT NULL,
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
            team_id TEXT NOT NULL,
            context_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cap_ledger (
            entry_id TEXT PRIMARY KEY,
            team_id TEXT NOT NULL,
            season INTEGER NOT NULL,
            reason TEXT NOT NULL,
            amount INTEGER NOT NULL
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
        """,
    ),
    (
        2,
        """
        CREATE TABLE IF NOT EXISTS trait_catalog (
            trait_code TEXT PRIMARY KEY,
            dtype TEXT NOT NULL,
            min_value REAL NOT NULL,
            max_value REAL NOT NULL,
            required INTEGER NOT NULL,
            description TEXT NOT NULL,
            category TEXT NOT NULL,
            version TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS player_traits (
            player_id TEXT NOT NULL,
            trait_code TEXT NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY (player_id, trait_code),
            FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE,
            FOREIGN KEY (trait_code) REFERENCES trait_catalog(trait_code)
        );

        CREATE TABLE IF NOT EXISTS simulation_validation_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            season INTEGER NOT NULL,
            week INTEGER NOT NULL,
            game_id TEXT NOT NULL,
            home_team_id TEXT NOT NULL,
            away_team_id TEXT NOT NULL,
            status TEXT NOT NULL,
            blocking_issues_json TEXT NOT NULL,
            warning_issues_json TEXT NOT NULL,
            validated_at TEXT NOT NULL
        );
        """,
    ),
    (
        3,
        """
        ALTER TABLE trait_catalog ADD COLUMN status TEXT NOT NULL DEFAULT 'core_now';

        CREATE TABLE IF NOT EXISTS matchup_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            play_id TEXT NOT NULL,
            phase TEXT NOT NULL,
            graph_json TEXT NOT NULL,
            FOREIGN KEY (play_id) REFERENCES play_results(play_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS phase_transitions (
            transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
            play_id TEXT NOT NULL,
            phase TEXT NOT NULL,
            FOREIGN KEY (play_id) REFERENCES play_results(play_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS contest_resolutions (
            contest_id TEXT PRIMARY KEY,
            play_id TEXT NOT NULL,
            phase TEXT NOT NULL,
            family TEXT NOT NULL,
            score REAL NOT NULL,
            offense_score REAL NOT NULL,
            defense_score REAL NOT NULL,
            contributor_json TEXT NOT NULL,
            trait_json TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            variance_hint REAL NOT NULL,
            FOREIGN KEY (play_id) REFERENCES play_results(play_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS rules_adjudications (
            play_id TEXT PRIMARY KEY,
            score_event TEXT,
            notes_json TEXT NOT NULL,
            next_down INTEGER NOT NULL,
            next_distance INTEGER NOT NULL,
            next_possession_team_id TEXT NOT NULL,
            clock_delta INTEGER NOT NULL,
            FOREIGN KEY (play_id) REFERENCES play_results(play_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS evidence_refs (
            handle TEXT PRIMARY KEY,
            play_id TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            FOREIGN KEY (play_id) REFERENCES play_results(play_id) ON DELETE CASCADE
        );
        """,
    ),
]


class MigrationRunner:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def apply(self) -> None:
        self.conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT DEFAULT CURRENT_TIMESTAMP)")
        applied = {
            row[0]
            for row in self.conn.execute("SELECT version FROM schema_migrations").fetchall()
        }
        for version, sql in MIGRATIONS:
            if version in applied:
                continue
            self.conn.executescript(sql)
            self.conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", (version,))
        self.conn.commit()
