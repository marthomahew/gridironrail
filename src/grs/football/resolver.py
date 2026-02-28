from __future__ import annotations

from dataclasses import replace
from typing import Iterable

from grs.contracts import (
    ActorRef,
    CausalityChain,
    CausalityNode,
    NarrativeEvent,
    PenaltyArtifact,
    PlayResult,
    PlayType,
    RandomSource,
    RepActor,
    RepLedgerEntry,
    SimMode,
    SnapContextPackage,
)
from grs.core import (
    EngineIntegrityError,
    build_forensic_artifact,
    gameplay_random,
    make_id,
    now_utc,
)
from grs.football.models import SnapResolution


class FootballResolver:
    PHASES = ["pre_snap", "early", "engagement", "decision", "terminal", "aftermath"]

    def __init__(self, random_source: RandomSource | None = None) -> None:
        self._random_source = random_source or gameplay_random()

    def resolve_snap(self, scp: SnapContextPackage, *, conditioned: bool = False, attempt: int = 1) -> SnapResolution:
        if len(scp.participants) < 2:
            artifact = build_forensic_artifact(
                engine_scope="football",
                error_code="INVALID_PARTICIPANTS",
                message="snap has insufficient participants",
                state_snapshot={"participants": len(scp.participants)},
                context={"play_id": scp.play_id},
                identifiers={"game_id": scp.game_id, "play_id": scp.play_id},
                causal_fragment=["pre_snap_validation"],
            )
            raise EngineIntegrityError(artifact)

        offense, defense = self._infer_teams(scp.participants, scp.situation.possession_team_id)
        reps = self._build_rep_ledger(scp, offense, defense)
        penalties = self._maybe_penalties(scp, offense, defense)
        play_result = self._resolve_play_result(scp, offense, defense, penalties)
        causality = self._build_causality_chain(scp, play_result, reps)

        for rep in reps:
            rep.validate()
        causality.validate()

        narrative_events = [
            NarrativeEvent(
                event_id=make_id("ne"),
                time=now_utc(),
                scope="football",
                event_type="snap_resolved",
                actors=[offense, defense],
                claims=[f"Play {scp.play_id} resolved as {self._terminal_event(play_result)}"],
                evidence_handles=[play_result.play_id] + [r.rep_id for r in reps[:2]],
                severity="normal",
                confidentiality_tier="public",
            )
        ]
        if conditioned:
            narrative_events.append(
                NarrativeEvent(
                    event_id=make_id("ne"),
                    time=now_utc(),
                    scope="football",
                    event_type="conditioned_play",
                    actors=[offense],
                    claims=["Play outcome was conditioned in dev/casual mode"],
                    evidence_handles=[play_result.play_id],
                    severity="high",
                    confidentiality_tier="internal",
                )
            )

        return SnapResolution(
            play_result=play_result,
            rep_ledger=reps,
            causality_chain=causality,
            narrative_events=narrative_events,
            conditioned=conditioned,
            attempts=attempt,
        )

    def _infer_teams(self, participants: Iterable[ActorRef], possession_team_id: str) -> tuple[str, str]:
        teams = sorted({p.team_id for p in participants})
        if possession_team_id not in teams:
            teams.insert(0, possession_team_id)
        if len(teams) == 1:
            teams.append(f"opp_{teams[0]}")
        offense = possession_team_id
        defense = next(t for t in teams if t != offense)
        return offense, defense

    def _build_rep_ledger(self, scp: SnapContextPackage, offense: str, defense: str) -> list[RepLedgerEntry]:
        rep_types = {
            PlayType.RUN: ["run_fit", "block", "pursuit", "tackle"],
            PlayType.PASS: ["release", "coverage", "pass_pro", "read", "contest"],
            PlayType.PUNT: ["protection", "rush", "gunners", "return"],
            PlayType.KICKOFF: ["coverage_lane", "wedge", "return", "tackle"],
            PlayType.FIELD_GOAL: ["snap_hold_kick", "edge_rush", "interior_push"],
            PlayType.EXTRA_POINT: ["snap_hold_kick", "block_attempt"],
            PlayType.TWO_POINT: ["redzone_concept", "coverage", "pressure"],
        }
        selected = rep_types.get(scp.intent.play_type, ["generic"])
        offense_actors = [p for p in scp.participants if p.team_id == offense][:11]
        defense_actors = [p for p in scp.participants if p.team_id == defense][:11]

        if not offense_actors or not defense_actors:
            artifact = build_forensic_artifact(
                engine_scope="football",
                error_code="TEAM_ASSIGNMENT_MISSING",
                message="unable to split offensive/defensive participants",
                state_snapshot={"participants": [p.team_id for p in scp.participants]},
                context={"play_id": scp.play_id},
                identifiers={"game_id": scp.game_id, "play_id": scp.play_id},
                causal_fragment=["roster_split"],
            )
            raise EngineIntegrityError(artifact)

        reps: list[RepLedgerEntry] = []
        for idx, rep_type in enumerate(selected):
            phase = self.PHASES[min(idx + 1, len(self.PHASES) - 1)]
            trio = offense_actors[idx % len(offense_actors) : (idx % len(offense_actors)) + 2]
            duo = defense_actors[idx % len(defense_actors) : (idx % len(defense_actors)) + 2]
            actors: list[RepActor] = [
                RepActor(a.actor_id, a.team_id, a.role, assignment_tag=f"{rep_type}_assignment") for a in trio + duo
            ]
            weights = self._weights_for_actors(actors)
            reps.append(
                RepLedgerEntry(
                    rep_id=make_id("rep"),
                    play_id=scp.play_id,
                    phase=phase,
                    rep_type=rep_type,
                    actors=actors,
                    assignment_tags=[f"{scp.intent.offensive_concept}", f"{scp.intent.defensive_concept}"],
                    outcome_tags=["win_contested", f"mode_{scp.mode.value}"],
                    responsibility_weights=weights,
                    context_tags=[scp.intent.play_type.value, scp.intent.formation, scp.intent.personnel],
                    evidence_handles=[scp.play_id, f"phase:{phase}", f"concept:{scp.intent.offensive_concept}"],
                )
            )

        # Explicit multi-actor constructs required by spec.
        reps.append(
            RepLedgerEntry(
                rep_id=make_id("rep"),
                play_id=scp.play_id,
                phase="engagement",
                rep_type="double_team_or_bracket",
                actors=[
                    RepActor(offense_actors[0].actor_id, offense, offense_actors[0].role, "combo_primary"),
                    RepActor(offense_actors[1].actor_id, offense, offense_actors[1].role, "combo_help"),
                    RepActor(defense_actors[0].actor_id, defense, defense_actors[0].role, "target_defender"),
                ],
                assignment_tags=["multi_actor"],
                outcome_tags=["supported"],
                responsibility_weights={
                    offense_actors[0].actor_id: 0.45,
                    offense_actors[1].actor_id: 0.25,
                    defense_actors[0].actor_id: 0.30,
                },
                context_tags=["double_team", "bracket", "chip_release", "stunt_exchange", "pursuit_convergence"],
                evidence_handles=[scp.play_id, "multi_actor_rep"],
            )
        )
        return reps

    def _weights_for_actors(self, actors: list[RepActor]) -> dict[str, float]:
        share = round(1.0 / len(actors), 6)
        weights: dict[str, float] = {a.actor_id: share for a in actors}
        correction = round(1.0 - sum(weights.values()), 6)
        first = actors[0].actor_id
        weights[first] = round(weights[first] + correction, 6)
        return weights

    def _maybe_penalties(self, scp: SnapContextPackage, offense: str, defense: str) -> list[PenaltyArtifact]:
        avg_risk = 0.0
        if scp.in_game_states:
            avg_risk = sum(s.discipline_risk for s in scp.in_game_states.values()) / len(scp.in_game_states)

        penalties: list[PenaltyArtifact] = []
        if scp.intent.play_type in {PlayType.PASS, PlayType.TWO_POINT} and avg_risk > 0.6:
            penalties.append(
                PenaltyArtifact(
                    code="DPI",
                    against_team_id=defense,
                    yards=15,
                    enforcement_rationale="defender lost leverage in contested catch window",
                )
            )
        if scp.intent.play_type in {PlayType.RUN, PlayType.PUNT} and avg_risk > 0.7:
            penalties.append(
                PenaltyArtifact(
                    code="HOLD",
                    against_team_id=offense,
                    yards=10,
                    enforcement_rationale="blocker technique collapse under leverage loss",
                )
            )
        return penalties

    def _resolve_play_result(
        self,
        scp: SnapContextPackage,
        offense: str,
        defense: str,
        penalties: list[PenaltyArtifact],
    ) -> PlayResult:
        rand = self._random_from_scp(scp)
        play_type = scp.intent.play_type

        yards = 0
        turnover = False
        turnover_type: str | None = None
        score_event: str | None = None
        clock_delta = rand.randint(4, 15)

        if play_type == PlayType.RUN:
            yards = rand.randint(-3, 18)
            turnover = rand.rand() < 0.02
            turnover_type = "FUMBLE" if turnover else None
        elif play_type == PlayType.PASS:
            yards = rand.randint(-10, 45)
            pick_roll = rand.rand()
            if pick_roll < 0.04:
                turnover = True
                turnover_type = "INT"
            elif pick_roll < 0.06:
                turnover = True
                turnover_type = "FUMBLE"
        elif play_type == PlayType.PUNT:
            gross = rand.randint(35, 62)
            ret = rand.randint(-2, 25)
            yards = gross - ret
            score_event = "PUNT_RETURN_TD" if ret > 22 and rand.rand() < 0.15 else None
        elif play_type == PlayType.KICKOFF:
            ret = rand.randint(15, 42)
            yards = 65 - ret
            score_event = "KICK_RETURN_TD" if ret > 37 and rand.rand() < 0.1 else None
        elif play_type == PlayType.FIELD_GOAL:
            dist = max(18, 100 - scp.situation.yard_line)
            make_prob = max(0.05, 0.97 - (dist / 80))
            made = rand.rand() < make_prob
            score_event = "FG_GOOD" if made else "FG_MISS"
            yards = 0
        elif play_type == PlayType.EXTRA_POINT:
            made = rand.rand() < 0.94
            score_event = "XP_GOOD" if made else "XP_MISS"
        elif play_type == PlayType.TWO_POINT:
            made = rand.rand() < 0.47
            score_event = "TWO_PT_GOOD" if made else "TWO_PT_FAIL"
            yards = 2 if made else 0

        for penalty in penalties:
            if penalty.against_team_id == offense:
                yards -= penalty.yards
            else:
                yards += penalty.yards

        current_spot = scp.situation.yard_line
        new_spot = max(1, min(99, current_spot + yards))

        if score_event in {"FG_GOOD", "XP_GOOD", "TWO_PT_GOOD", "PUNT_RETURN_TD", "KICK_RETURN_TD"}:
            next_possession = defense
            next_down = 1
            next_distance = 10
        elif turnover:
            next_possession = defense
            next_down = 1
            next_distance = 10
        else:
            gained = scp.situation.distance - yards
            first_down = gained <= 0
            if first_down:
                next_down = 1
                next_distance = 10
            else:
                next_down = min(4, scp.situation.down + 1)
                next_distance = max(1, gained)
            next_possession = offense

        return PlayResult(
            play_id=scp.play_id,
            yards=yards,
            new_spot=new_spot,
            turnover=turnover,
            turnover_type=turnover_type,
            score_event=score_event,
            penalties=penalties,
            clock_delta=clock_delta,
            next_down=next_down,
            next_distance=next_distance,
            next_possession_team_id=next_possession,
        )

    def _build_causality_chain(
        self,
        scp: SnapContextPackage,
        play_result: PlayResult,
        reps: list[RepLedgerEntry],
    ) -> CausalityChain:
        terminal = self._terminal_event(play_result)

        if play_result.turnover and play_result.turnover_type == "INT":
            descriptions = [
                "pressure timing compressed window",
                "qb decision risk exceeded threshold",
                "route landmark drift",
                "coverage leverage won",
                "catch point lost",
            ]
        elif play_result.turnover and play_result.turnover_type == "FUMBLE":
            descriptions = [
                "collision quality high",
                "ball security degraded under contact",
                "pursuit convergence amplified hit stack",
                "fatigue and wear increased susceptibility",
            ]
        else:
            descriptions = [
                "pre-snap leverage set",
                "engagement outcomes shaped windows",
                "decision timing matched available lane",
                "terminal execution converted opportunity",
            ]

        node_count = min(len(descriptions), len(reps))
        base_weight = round(1.0 / node_count, 6)
        nodes: list[CausalityNode] = []
        for idx in range(node_count):
            weight = base_weight
            if idx == 0:
                weight = round(weight + (1.0 - (base_weight * node_count)), 6)
            nodes.append(
                CausalityNode(
                    source_type="rep",
                    source_id=reps[idx].rep_id,
                    weight=weight,
                    description=descriptions[idx],
                )
            )

        return CausalityChain(terminal_event=terminal, play_id=scp.play_id, nodes=nodes)

    def _terminal_event(self, play_result: PlayResult) -> str:
        if play_result.turnover_type == "INT":
            return "interception"
        if play_result.turnover_type == "FUMBLE":
            return "fumble"
        if play_result.score_event:
            return play_result.score_event.lower()
        if play_result.next_down == 1 and play_result.yards >= 0:
            return "first_down" if play_result.yards >= 10 else "positive_play"
        return "normal_play"

    def _random_from_scp(self, scp: SnapContextPackage):
        # Explicit substream split keeps replay/testing deterministic while allowing mode invariance.
        return self._random_source.spawn(f"{scp.game_id}:{scp.play_id}:{scp.mode.value}")


class FootballEngine:
    def __init__(self, resolver: FootballResolver | None = None) -> None:
        self._resolver = resolver or FootballResolver()

    def run_snap(
        self,
        scp: SnapContextPackage,
        *,
        dev_mode: bool = False,
        force_target: str | None = None,
        max_attempts: int = 1000,
    ) -> SnapResolution:
        if force_target and not dev_mode:
            raise ValueError("force_target is only available in dev mode")

        if not force_target:
            return self._resolver.resolve_snap(scp, conditioned=False, attempt=1)

        for attempt in range(1, max_attempts + 1):
            resolution = self._resolver.resolve_snap(scp, conditioned=True, attempt=attempt)
            terminal = resolution.causality_chain.terminal_event
            if terminal == force_target:
                return resolution

        artifact = build_forensic_artifact(
            engine_scope="football",
            error_code="FORCE_OUTCOME_FAIL",
            message=f"force outcome target '{force_target}' not reached in {max_attempts} attempts",
            state_snapshot={"play_id": scp.play_id, "mode": scp.mode.value},
            context={"force_target": force_target, "max_attempts": max_attempts},
            identifiers={"game_id": scp.game_id, "play_id": scp.play_id},
            causal_fragment=["dev_force_outcome"],
        )
        raise EngineIntegrityError(artifact)

    def run_mode_invariant(self, scp: SnapContextPackage, mode: SimMode) -> SnapResolution:
        variant = replace(scp, mode=mode)
        return self.run_snap(variant)
