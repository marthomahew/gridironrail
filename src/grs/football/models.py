from __future__ import annotations

from dataclasses import dataclass, field

from grs.contracts import CausalityChain, NarrativeEvent, PlayResult, RepLedgerEntry


@dataclass(slots=True)
class SnapResolution:
    play_result: PlayResult
    rep_ledger: list[RepLedgerEntry]
    causality_chain: CausalityChain
    narrative_events: list[NarrativeEvent] = field(default_factory=list)
    conditioned: bool = False
    attempts: int = 1
