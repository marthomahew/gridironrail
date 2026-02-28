from __future__ import annotations

from grs.contracts import (
    ActorRef,
    InGameState,
    ParameterizedIntent,
    PlayType,
    SimMode,
    Situation,
    SnapContextPackage,
)
from grs.core import gameplay_random, seeded_random
from grs.football import FootballEngine, FootballResolver


def build_context(play_id: str, mode: SimMode = SimMode.PLAY) -> SnapContextPackage:
    participants = []
    for i in range(11):
        participants.append(ActorRef(actor_id=f"A_O_{i}", team_id="A", role=f"O{i}"))
    for i in range(11):
        participants.append(ActorRef(actor_id=f"B_D_{i}", team_id="B", role=f"D{i}"))
    states = {
        p.actor_id: InGameState(fatigue=0.3, acute_wear=0.2, confidence_tilt=0.0, discipline_risk=0.5)
        for p in participants
    }
    return SnapContextPackage(
        game_id="G1",
        play_id=play_id,
        mode=mode,
        situation=Situation(
            quarter=2,
            clock_seconds=720,
            down=2,
            distance=8,
            yard_line=45,
            possession_team_id="A",
            score_diff=0,
            timeouts_offense=3,
            timeouts_defense=3,
        ),
        participants=participants,
        in_game_states=states,
        intent=ParameterizedIntent(
            personnel="11",
            formation="gun",
            offensive_concept="spacing",
            defensive_concept="cover3",
            play_type=PlayType.PASS,
        ),
    )


def test_seeded_determinism_same_inputs():
    r1 = seeded_random(99)
    r2 = seeded_random(99)
    e1 = FootballEngine(FootballResolver(r1))
    e2 = FootballEngine(FootballResolver(r2))

    scp = build_context("P1")
    a = e1.run_snap(scp)
    b = e2.run_snap(scp)

    assert a.play_result.yards == b.play_result.yards
    assert a.causality_chain.terminal_event == b.causality_chain.terminal_event
    assert [n.description for n in a.causality_chain.nodes] == [n.description for n in b.causality_chain.nodes]


def test_gameplay_random_is_non_deterministic_distribution():
    engine = FootballEngine(FootballResolver(gameplay_random()))
    outcomes = set()
    for i in range(25):
        res = engine.run_snap(build_context(f"P{i}"))
        outcomes.add((res.play_result.yards, res.causality_chain.terminal_event))
    assert len(outcomes) > 1


def test_mode_invariance_uses_same_resolver_api():
    engine = FootballEngine(FootballResolver(seeded_random(42)))
    base = build_context("P_MODE")

    play = engine.run_mode_invariant(base, SimMode.PLAY)
    sim = engine.run_mode_invariant(base, SimMode.SIM)
    off = engine.run_mode_invariant(base, SimMode.OFFSCREEN)

    assert play.rep_ledger
    assert sim.rep_ledger
    assert off.rep_ledger
    assert play.play_result.play_id == sim.play_result.play_id == off.play_result.play_id


def test_multi_actor_rep_present_and_valid_weights():
    engine = FootballEngine(FootballResolver(seeded_random(12)))
    res = engine.run_snap(build_context("PMULTI"))
    multi = [r for r in res.rep_ledger if r.rep_type == "double_team_or_bracket"]
    assert multi
    for rep in multi:
        assert "double_team" in rep.context_tags
        assert abs(sum(rep.responsibility_weights.values()) - 1.0) < 0.001
