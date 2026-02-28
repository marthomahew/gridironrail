from __future__ import annotations

from dataclasses import dataclass

from grs.contracts import RetentionPolicy


@dataclass(slots=True)
class GameRetentionContext:
    game_id: str
    phase: str
    is_championship: bool
    is_playoff: bool
    is_rivalry: bool
    is_record_game: bool
    tagged_instant_classic: bool


def should_retain_game(policy: RetentionPolicy, context: GameRetentionContext) -> bool:
    if context.tagged_instant_classic and policy.retain_instant_classics:
        return True
    if context.is_championship and policy.retain_championship:
        return True
    if context.is_playoff and policy.retain_playoffs:
        return True
    if context.is_rivalry and policy.retain_rivalry_games:
        return True
    if context.is_record_game and policy.retain_record_games:
        return True
    return False
