from __future__ import annotations

from dataclasses import dataclass, field

from grs.contracts import CausalityChain, GameSessionState, NarrativeEvent, PlayResult, RepLedgerEntry


@dataclass(slots=True)
class SnapResolution:
    play_result: PlayResult
    rep_ledger: list[RepLedgerEntry]
    causality_chain: CausalityChain
    narrative_events: list[NarrativeEvent] = field(default_factory=list)
    conditioned: bool = False
    attempts: int = 1


@dataclass(slots=True)
class GameSessionResult:
    final_state: GameSessionState
    snaps: list[SnapResolution]
    home_team_id: str
    away_team_id: str
    home_score: int
    away_score: int
    action_stream: list[dict[str, str | int | float]] = field(default_factory=list)
