from __future__ import annotations

from dataclasses import dataclass, field

from grs.contracts import (
    CausalityChain,
    ContestResolution,
    GameSessionState,
    NarrativeEvent,
    PlayResult,
    RepLedgerEntry,
    ResolvedSnapStateDelta,
    ResolverEvidenceRef,
    SnapArtifactBundle,
)


@dataclass(slots=True)
class SnapResolution:
    artifact_bundle: SnapArtifactBundle
    state_delta: ResolvedSnapStateDelta
    conditioned: bool = False
    attempts: int = 1

    @property
    def play_result(self) -> PlayResult:
        return self.artifact_bundle.play_result

    @property
    def rep_ledger(self) -> list[RepLedgerEntry]:
        return self.artifact_bundle.rep_ledger

    @property
    def causality_chain(self) -> CausalityChain:
        return self.artifact_bundle.causality_chain

    @property
    def contest_outputs(self) -> list[ContestResolution]:
        return self.artifact_bundle.contest_resolutions

    @property
    def evidence_refs(self) -> list[ResolverEvidenceRef]:
        return self.artifact_bundle.evidence_refs

    @property
    def narrative_events(self) -> list[NarrativeEvent]:
        return self.artifact_bundle.narrative_events


@dataclass(slots=True)
class GameSessionResult:
    final_state: GameSessionState
    snaps: list[SnapResolution]
    home_team_id: str
    away_team_id: str
    home_score: int
    away_score: int
    action_stream: list[dict[str, str | int | float]] = field(default_factory=list)
