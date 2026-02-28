from __future__ import annotations

from grs.contracts import ActorRef, GameSessionState, InGameState, ParameterizedIntent, PlayType, SimMode, Situation, SnapContextPackage
from grs.core import gameplay_random, seeded_random
from grs.football import FootballEngine, FootballResolver, GameSessionEngine
from grs.org import build_default_league


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
            formation="gun_trips",
            offensive_concept="spacing",
            defensive_concept="cover3_match",
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


def test_gameplay_random_is_non_deterministic_distribution():
    engine = FootballEngine(FootballResolver(gameplay_random()))
    outcomes = set()
    for i in range(25):
        res = engine.run_snap(build_context(f"P{i}"))
        outcomes.add((res.play_result.yards, res.causality_chain.terminal_event))
    assert len(outcomes) > 1


def test_mode_invariance_same_snap_inputs():
    engine = FootballEngine(FootballResolver(seeded_random(42)))
    base = build_context("P_MODE")

    play = engine.run_mode_invariant(base, SimMode.PLAY)
    sim = engine.run_mode_invariant(base, SimMode.SIM)
    off = engine.run_mode_invariant(base, SimMode.OFFSCREEN)

    assert play.play_result.yards == sim.play_result.yards == off.play_result.yards
    assert play.causality_chain.terminal_event == sim.causality_chain.terminal_event == off.causality_chain.terminal_event


def test_game_session_completes_with_real_lineups():
    league = build_default_league(team_count=2)
    home = league.teams[0]
    away = league.teams[1]
    state = GameSessionState(
        game_id="S2026_W1_G01",
        season=2026,
        week=1,
        home_team_id=home.team_id,
        away_team_id=away.team_id,
        quarter=1,
        clock_seconds=900,
        home_score=0,
        away_score=0,
        possession_team_id=home.team_id,
        down=1,
        distance=10,
        yard_line=25,
        drive_index=1,
        timeouts_home=3,
        timeouts_away=3,
    )

    engine = GameSessionEngine(FootballEngine(FootballResolver(seeded_random(5))))
    result = engine.run_game(state, home, away, mode=SimMode.SIM)

    assert result.final_state.completed
    assert len(result.snaps) > 0
    assert result.home_score >= 0 and result.away_score >= 0
