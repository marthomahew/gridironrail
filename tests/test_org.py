from __future__ import annotations

import pytest

from grs.contracts import Difficulty
from grs.core import default_difficulty_profiles, seeded_random, validate_global_only_config
from grs.org import OrganizationalEngine, build_default_league


def test_difficulty_guard_blocks_per_team_cheats():
    with pytest.raises(ValueError):
        validate_global_only_config({"per_team_modifiers": {"USER": 1.5}})


def test_ai_uses_perceived_cards_only():
    league = build_default_league(team_count=2)
    diff = default_difficulty_profiles()[Difficulty.PRO]
    engine = OrganizationalEngine(seeded_random(3), diff)
    cards = engine.perceived_cards_for_team(league, league.teams[0].team_id)
    ranked = engine.ai_rank_players(cards)
    assert ranked
    assert all(hasattr(card, "scout_metrics") for card in ranked)


def test_ownership_pressure_can_trigger_coaching_change():
    league = build_default_league(team_count=2)
    diff = default_difficulty_profiles()[Difficulty.ALL_MADDEN]
    engine = OrganizationalEngine(seeded_random(9), diff)

    before = len(league.teams[0].staff)
    for _ in range(20):
        engine.update_ownership_pressure(league, {league.teams[0].team_id: 0.1, league.teams[1].team_id: 0.8})
    after = len(league.teams[0].staff)

    assert after >= before - 1
    assert league.transactions


def test_cap_and_roster_constraints_raise():
    league = build_default_league(team_count=2)
    team = league.teams[0]
    team.cap_space = -1

    diff = default_difficulty_profiles()[Difficulty.PRO]
    engine = OrganizationalEngine(seeded_random(7), diff)

    with pytest.raises(ValueError):
        engine.validate_franchise_constraints(team)
