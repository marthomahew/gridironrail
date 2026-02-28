from __future__ import annotations

import pytest

from grs.contracts import ActorRef, InGameState, ParameterizedIntent, PlayType, SimMode, Situation, SnapContextPackage
from grs.core import EngineIntegrityError, seeded_random
from grs.football import FootballEngine, FootballResolver


def test_engine_hard_stop_on_invalid_state():
    scp = SnapContextPackage(
        game_id="G1",
        play_id="P1",
        mode=SimMode.PLAY,
        situation=Situation(1, 800, 1, 10, 25, "A", 0, 3, 3),
        participants=[],
        in_game_states={},
        intent=ParameterizedIntent("11", "gun_trips", "inside_zone", "cover3_match", play_type=PlayType.RUN),
    )
    engine = FootballEngine(FootballResolver(seeded_random(1)))
    with pytest.raises(EngineIntegrityError) as ex:
        engine.run_snap(scp)
    assert ex.value.artifact.error_code == "PRE_SIM_VALIDATION_FAILED"


def test_force_outcome_dev_mode():
    participants = [ActorRef(actor_id=f"A{i}", team_id="A", role="O") for i in range(11)] + [
        ActorRef(actor_id=f"B{i}", team_id="B", role="D") for i in range(11)
    ]
    states = {p.actor_id: InGameState(0.2, 0.2, 0.0, discipline_risk=0.3) for p in participants}
    scp = SnapContextPackage(
        game_id="G1",
        play_id="PFORCE",
        mode=SimMode.PLAY,
        situation=Situation(2, 500, 2, 7, 50, "A", 0, 2, 2),
        participants=participants,
        in_game_states=states,
        intent=ParameterizedIntent("11", "gun_trips", "spacing", "cover2", play_type=PlayType.PASS),
    )

    engine = FootballEngine(FootballResolver(seeded_random(77)))
    res = engine.run_snap(scp, dev_mode=True, force_target="first_down", max_attempts=300)
    assert res.conditioned
    assert res.causality_chain.terminal_event == "first_down"


def test_force_outcome_not_available_without_dev_mode():
    participants = [ActorRef(actor_id=f"A{i}", team_id="A", role="O") for i in range(11)] + [
        ActorRef(actor_id=f"B{i}", team_id="B", role="D") for i in range(11)
    ]
    states = {p.actor_id: InGameState(0.2, 0.2, 0.0, discipline_risk=0.3) for p in participants}
    scp = SnapContextPackage(
        game_id="G1",
        play_id="PFORCE2",
        mode=SimMode.PLAY,
        situation=Situation(2, 500, 2, 7, 50, "A", 0, 2, 2),
        participants=participants,
        in_game_states=states,
        intent=ParameterizedIntent("11", "gun_trips", "spacing", "cover2", play_type=PlayType.PASS),
    )
    engine = FootballEngine(FootballResolver(seeded_random(77)))
    with pytest.raises(ValueError):
        engine.run_snap(scp, dev_mode=False, force_target="first_down", max_attempts=3)
