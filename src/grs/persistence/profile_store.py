from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from grs.contracts import (
    CapabilityDomain,
    CapabilityPolicy,
    FranchiseProfile,
    LeagueSetupConfig,
    LeagueSetupValidationReport,
    ManagementMode,
)


class ProfileStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def initialize_schema(self) -> None:
        sql = """
        CREATE TABLE IF NOT EXISTS franchise_profiles (
            profile_id TEXT PRIMARY KEY,
            profile_name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_opened_at TEXT NOT NULL,
            league_config_ref TEXT NOT NULL,
            selected_user_team_id TEXT NOT NULL,
            active_mode TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS league_config (
            league_config_id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            conference_count INTEGER NOT NULL,
            divisions_per_conference_json TEXT NOT NULL,
            teams_per_division_json TEXT NOT NULL,
            ruleset_id TEXT NOT NULL,
            difficulty_profile_id TEXT NOT NULL,
            talent_profile_id TEXT NOT NULL,
            league_identity_profile_id TEXT NOT NULL,
            league_format_id TEXT NOT NULL,
            league_format_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (profile_id) REFERENCES franchise_profiles(profile_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS league_conferences (
            conference_id TEXT PRIMARY KEY,
            league_config_id TEXT NOT NULL,
            conference_name TEXT NOT NULL,
            FOREIGN KEY (league_config_id) REFERENCES league_config(league_config_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS league_divisions (
            division_id TEXT PRIMARY KEY,
            league_config_id TEXT NOT NULL,
            conference_id TEXT NOT NULL,
            division_name TEXT NOT NULL,
            FOREIGN KEY (league_config_id) REFERENCES league_config(league_config_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS league_teams (
            team_id TEXT PRIMARY KEY,
            league_config_id TEXT NOT NULL,
            conference_id TEXT NOT NULL,
            division_id TEXT NOT NULL,
            team_name TEXT NOT NULL,
            FOREIGN KEY (league_config_id) REFERENCES league_config(league_config_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS mode_policy (
            profile_id TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            baseline_json TEXT NOT NULL,
            overrides_json TEXT NOT NULL,
            updated_by_team_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            reason TEXT NOT NULL,
            FOREIGN KEY (profile_id) REFERENCES franchise_profiles(profile_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS cap_policy (
            profile_id TEXT PRIMARY KEY,
            cap_amount INTEGER NOT NULL,
            dead_money_penalty_multiplier REAL NOT NULL,
            FOREIGN KEY (profile_id) REFERENCES franchise_profiles(profile_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS roster_policy (
            profile_id TEXT PRIMARY KEY,
            players_per_team INTEGER NOT NULL,
            active_gameday_min INTEGER NOT NULL,
            active_gameday_max INTEGER NOT NULL,
            FOREIGN KEY (profile_id) REFERENCES franchise_profiles(profile_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS talent_profile (
            profile_id TEXT PRIMARY KEY,
            talent_profile_id TEXT NOT NULL,
            FOREIGN KEY (profile_id) REFERENCES franchise_profiles(profile_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS schedule_policy (
            profile_id TEXT PRIMARY KEY,
            schedule_policy_id TEXT NOT NULL,
            regular_season_weeks INTEGER NOT NULL,
            schedule_policy_version TEXT NOT NULL,
            FOREIGN KEY (profile_id) REFERENCES franchise_profiles(profile_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS league_setup_validation_runs (
            report_id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            setup_config_ref TEXT NOT NULL,
            blocking_issues_json TEXT NOT NULL,
            warning_issues_json TEXT NOT NULL,
            validated_at TEXT NOT NULL,
            FOREIGN KEY (profile_id) REFERENCES franchise_profiles(profile_id) ON DELETE CASCADE
        );
        """
        with self.connect() as conn:
            conn.executescript(sql)
            columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(league_config)").fetchall()
            }
            if "league_identity_profile_id" not in columns:
                conn.execute(
                    "ALTER TABLE league_config ADD COLUMN league_identity_profile_id TEXT NOT NULL DEFAULT ''"
                )

    def list_profiles(self) -> list[FranchiseProfile]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT profile_id, profile_name, created_at, last_opened_at, league_config_ref, selected_user_team_id, active_mode
                FROM franchise_profiles
                ORDER BY last_opened_at DESC
                """
            ).fetchall()
        return [
            FranchiseProfile(
                profile_id=row[0],
                profile_name=row[1],
                created_at=datetime.fromisoformat(row[2]),
                last_opened_at=datetime.fromisoformat(row[3]),
                league_config_ref=row[4],
                selected_user_team_id=row[5],
                active_mode=ManagementMode(row[6]),
            )
            for row in rows
        ]

    def save_profile(self, profile: FranchiseProfile) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO franchise_profiles(
                    profile_id, profile_name, created_at, last_opened_at, league_config_ref, selected_user_team_id, active_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    profile.profile_id,
                    profile.profile_name,
                    profile.created_at.isoformat(),
                    profile.last_opened_at.isoformat(),
                    profile.league_config_ref,
                    profile.selected_user_team_id,
                    profile.active_mode.value,
                ),
            )

    def load_profile(self, profile_id: str) -> FranchiseProfile | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT profile_id, profile_name, created_at, last_opened_at, league_config_ref, selected_user_team_id, active_mode
                FROM franchise_profiles
                WHERE profile_id = ?
                """,
                (profile_id,),
            ).fetchone()
        if row is None:
            return None
        return FranchiseProfile(
            profile_id=row[0],
            profile_name=row[1],
            created_at=datetime.fromisoformat(row[2]),
            last_opened_at=datetime.fromisoformat(row[3]),
            league_config_ref=row[4],
            selected_user_team_id=row[5],
            active_mode=ManagementMode(row[6]),
        )

    def delete_profile(self, profile_id: str) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM franchise_profiles WHERE profile_id = ?", (profile_id,))

    def touch_profile(self, profile_id: str, opened_at: datetime) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE franchise_profiles SET last_opened_at = ? WHERE profile_id = ?",
                (opened_at.isoformat(), profile_id),
            )

    def save_league_setup(
        self,
        *,
        profile_id: str,
        league_config_id: str,
        config: LeagueSetupConfig,
        blueprints: list[dict[str, str]],
    ) -> None:
        created_at = datetime.now(UTC).isoformat()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO league_config(
                    league_config_id, profile_id, conference_count, divisions_per_conference_json, teams_per_division_json,
                    ruleset_id, difficulty_profile_id, talent_profile_id, league_identity_profile_id,
                    league_format_id, league_format_version, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    league_config_id,
                    profile_id,
                    config.conference_count,
                    json.dumps(config.divisions_per_conference),
                    json.dumps(config.teams_per_division),
                    config.ruleset_id,
                    config.difficulty_profile_id,
                    config.talent_profile_id,
                    config.league_identity_profile_id,
                    config.league_format_id,
                    config.league_format_version,
                    created_at,
                ),
            )
            conferences = sorted({(b["conference_id"], b["conference_name"]) for b in blueprints})
            divisions = sorted(
                {
                    (b["division_id"], b["conference_id"], b["division_name"])
                    for b in blueprints
                }
            )
            conn.execute("DELETE FROM league_conferences WHERE league_config_id = ?", (league_config_id,))
            conn.execute("DELETE FROM league_divisions WHERE league_config_id = ?", (league_config_id,))
            conn.execute("DELETE FROM league_teams WHERE league_config_id = ?", (league_config_id,))
            conn.executemany(
                """
                INSERT INTO league_conferences(conference_id, league_config_id, conference_name)
                VALUES (?, ?, ?)
                """,
                [(cid, league_config_id, cname) for cid, cname in conferences],
            )
            conn.executemany(
                """
                INSERT INTO league_divisions(division_id, league_config_id, conference_id, division_name)
                VALUES (?, ?, ?, ?)
                """,
                [(did, league_config_id, cid, dname) for did, cid, dname in divisions],
            )
            conn.executemany(
                """
                INSERT INTO league_teams(team_id, league_config_id, conference_id, division_id, team_name)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        b["team_id"],
                        league_config_id,
                        b["conference_id"],
                        b["division_id"],
                        b["team_name"],
                    )
                    for b in blueprints
                ],
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO cap_policy(profile_id, cap_amount, dead_money_penalty_multiplier)
                VALUES (?, ?, ?)
                """,
                (
                    profile_id,
                    config.cap_policy.cap_amount,
                    config.cap_policy.dead_money_penalty_multiplier,
                ),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO roster_policy(profile_id, players_per_team, active_gameday_min, active_gameday_max)
                VALUES (?, ?, ?, ?)
                """,
                (
                    profile_id,
                    config.roster_policy.players_per_team,
                    config.roster_policy.active_gameday_min,
                    config.roster_policy.active_gameday_max,
                ),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO talent_profile(profile_id, talent_profile_id)
                VALUES (?, ?)
                """,
                (profile_id, config.talent_profile_id),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO schedule_policy(profile_id, schedule_policy_id, regular_season_weeks, schedule_policy_version)
                VALUES (?, ?, ?, ?)
                """,
                (
                    profile_id,
                    config.schedule_policy.policy_id,
                    config.schedule_policy.regular_season_weeks,
                    "1.0.0",
                ),
            )

    def save_mode_policy(self, profile_id: str, policy: CapabilityPolicy) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO mode_policy(
                    profile_id, mode, baseline_json, overrides_json, updated_by_team_id, updated_at, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    profile_id,
                    policy.mode.value,
                    json.dumps([d.value for d in policy.baseline_capabilities]),
                    json.dumps({k.value: v for k, v in policy.override_capabilities.items()}),
                    policy.updated_by_team_id,
                    policy.updated_at.isoformat(),
                    policy.reason,
                ),
            )

    def load_mode_policy(self, profile_id: str) -> CapabilityPolicy | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT mode, baseline_json, overrides_json, updated_by_team_id, updated_at, reason
                FROM mode_policy
                WHERE profile_id = ?
                """,
                (profile_id,),
            ).fetchone()
        if row is None:
            return None
        baseline = [CapabilityDomain(item) for item in json.loads(row[1])]
        overrides = {
            CapabilityDomain(key): bool(value)
            for key, value in dict(json.loads(row[2])).items()
        }
        return CapabilityPolicy(
            mode=ManagementMode(row[0]),
            baseline_capabilities=baseline,
            override_capabilities=overrides,
            updated_by_team_id=row[3],
            updated_at=datetime.fromisoformat(row[4]),
            reason=row[5],
        )

    def save_validation_report(self, report: LeagueSetupValidationReport) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO league_setup_validation_runs(
                    report_id, profile_id, setup_config_ref, blocking_issues_json, warning_issues_json, validated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    report.report_id,
                    report.profile_id,
                    report.setup_config_ref,
                    json.dumps([asdict(issue) for issue in report.blocking_issues]),
                    json.dumps([asdict(issue) for issue in report.warning_issues]),
                    report.validated_at.isoformat(),
                ),
            )

    def load_setup_config_row(self, league_config_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT conference_count, divisions_per_conference_json, teams_per_division_json,
                       ruleset_id, difficulty_profile_id, talent_profile_id, league_identity_profile_id,
                       league_format_id, league_format_version
                FROM league_config
                WHERE league_config_id = ?
                """,
                (league_config_id,),
            ).fetchone()
            sched_row = conn.execute(
                "SELECT regular_season_weeks, schedule_policy_id FROM schedule_policy WHERE profile_id = (SELECT profile_id FROM league_config WHERE league_config_id = ?)",
                (league_config_id,),
            ).fetchone()
        if row is None:
            return None
        if sched_row is None:
            raise ValueError(
                f"league_config '{league_config_id}' is missing required schedule_policy linkage"
            )
        return {
            "conference_count": row[0],
            "divisions_per_conference": json.loads(row[1]),
            "teams_per_division": json.loads(row[2]),
            "ruleset_id": row[3],
            "difficulty_profile_id": row[4],
            "talent_profile_id": row[5],
            "league_identity_profile_id": row[6],
            "league_format_id": row[7],
            "league_format_version": row[8],
            "regular_season_weeks": int(sched_row[0]),
            "schedule_policy_id": str(sched_row[1]),
        }

    def load_team_topology(self, league_config_id: str) -> dict[str, dict[str, str]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT team_id, conference_id, division_id, team_name
                FROM league_teams
                WHERE league_config_id = ?
                """,
                (league_config_id,),
            ).fetchall()
        return {
            str(row[0]): {
                "conference_id": str(row[1]),
                "division_id": str(row[2]),
                "team_name": str(row[3]),
            }
            for row in rows
        }
