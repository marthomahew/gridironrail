from __future__ import annotations

from dataclasses import dataclass

from grs.contracts import (
    CoachDecisionEngine,
    GameSessionState,
    PlayIntentFrame,
    PlaycallRequest,
    TeamGamePackage,
    RandomSource,
)
from grs.football.resources import ResourceResolver


@dataclass(slots=True)
class PolicyDrivenCoachDecisionEngine(CoachDecisionEngine):
    repository: ResourceResolver

    def decide_play_intent(
        self,
        *,
        session_state: GameSessionState,
        offense_package: TeamGamePackage,
        defense_package: TeamGamePackage,
        random_source: RandomSource,
    ) -> PlayIntentFrame:
        policy = self.repository.resolve_policy(offense_package.coaching_policy_id)
        posture = self._posture_for_state(session_state)

        playbook_by_posture = policy.get("playbook_by_posture")
        if not isinstance(playbook_by_posture, dict):
            raise ValueError(f"coaching policy '{offense_package.coaching_policy_id}' missing playbook_by_posture")
        candidates = playbook_by_posture.get(posture)
        if not isinstance(candidates, list) or not candidates:
            raise ValueError(
                f"coaching policy '{offense_package.coaching_policy_id}' has no playbook entries for posture '{posture}'"
            )

        play_id = str(random_source.choice(candidates))
        entry = self.repository.resolve_playbook_entry(play_id)
        return PlayIntentFrame(
            play_type=entry.play_type,
            personnel_id=entry.personnel_id,
            formation_id=entry.formation_id,
            offense_concept_id=entry.offensive_concept_id,
            defense_concept_id=entry.defensive_concept_id,
            playbook_entry_id=entry.play_id,
            posture=posture,
            tempo="normal",
            aggression="balanced",
            allows_audible=True,
        )

    def _posture_for_state(self, state: GameSessionState) -> str:
        if state.down == 4 and state.yard_line >= 88:
            return "field_goal_try"
        if state.down == 4:
            return "fourth_and_long"
        if state.down >= 3 and state.distance >= 7:
            return "third_and_long"
        if state.distance <= 2:
            return "short_yardage"
        return "normal"


def intent_to_playcall(team_id: str, intent: PlayIntentFrame) -> PlaycallRequest:
    return PlaycallRequest(
        team_id=team_id,
        personnel=intent.personnel_id,
        formation=intent.formation_id,
        offensive_concept=intent.offense_concept_id,
        defensive_concept=intent.defense_concept_id,
        playbook_entry_id=intent.playbook_entry_id,
        tempo=intent.tempo,
        aggression=intent.aggression,
        play_type=intent.play_type,
    )
