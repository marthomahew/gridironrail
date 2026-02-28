from __future__ import annotations

from dataclasses import asdict, replace
from typing import Iterable

from grs.contracts import (
    ActorRef,
    CausalityChain,
    CausalityNode,
    CausalityTemplate,
    ContestInput,
    ContestOutput,
    NarrativeEvent,
    OutcomeResolutionProfile,
    PenaltyArtifact,
    PlayFamilyInfluenceProfile,
    PlayResult,
    PlayType,
    RandomSource,
    RepActor,
    RepLedgerEntry,
    ResolverEvidenceRef,
    SimMode,
    SnapContextPackage,
    ValidationError,
)
from grs.core import (
    EngineIntegrityError,
    build_forensic_artifact,
    gameplay_random,
    make_id,
    now_utc,
)
from grs.football.contest import ContestEvaluator, parse_influence_profiles, required_influence_families
from grs.football.models import SnapResolution
from grs.football.resources import ResourceResolver
from grs.football.validation import PreSimValidator


class FootballResolver:
    PHASES = ["pre_snap", "early", "engagement", "decision", "terminal", "aftermath"]
    TERMINAL_TEMPLATES: dict[str, CausalityTemplate] = {
        "score": CausalityTemplate(
            terminal_event_family="score",
            descriptions=[
                "advantage creation succeeded in core contest",
                "decision timing aligned with available leverage",
                "terminal execution converted scoring chance",
                "supporting rep context reinforced outcome",
            ],
        ),
        "turnover_interception": CausalityTemplate(
            terminal_event_family="turnover_interception",
            descriptions=[
                "pressure constrained platform and timing",
                "decision quality dropped under contested window",
                "coverage/catch-point leverage won by defense",
                "upstream contest deficits compounded into interception",
            ],
        ),
        "turnover_fumble": CausalityTemplate(
            terminal_event_family="turnover_fumble",
            descriptions=[
                "collision leverage and tackle finish favored defense",
                "ball security contest deteriorated under contact",
                "pursuit convergence amplified disruption",
                "upstream trait-context pressure drove fumble risk",
            ],
        ),
        "explosive": CausalityTemplate(
            terminal_event_family="explosive",
            descriptions=[
                "primary contest generated separation/lane advantage",
                "second-order continuation contest sustained gain",
                "terminal finish outpaced recovery",
                "supporting reps prevented containment",
            ],
        ),
        "first_down": CausalityTemplate(
            terminal_event_family="first_down",
            descriptions=[
                "core contest tilted slightly toward offense",
                "decision and execution held enough edge",
                "terminal finish crossed line-to-gain",
                "supporting reps prevented negative swing",
            ],
        ),
        "negative_play": CausalityTemplate(
            terminal_event_family="negative_play",
            descriptions=[
                "defensive leverage won primary contest",
                "offense failed to recover in continuation phase",
                "terminal contact finished behind baseline",
                "supporting context reinforced loss",
            ],
        ),
        "short_play": CausalityTemplate(
            terminal_event_family="short_play",
            descriptions=[
                "contest balanced near neutral",
                "execution produced limited advancement",
                "terminal tackle/contact capped gain",
                "supporting reps prevented explosive branch",
            ],
        ),
        "normal_play": CausalityTemplate(
            terminal_event_family="normal_play",
            descriptions=[
                "contests produced moderate advantage",
                "decision and execution stayed stable",
                "terminal event reflected bounded outcome",
                "supporting context maintained continuity",
            ],
        ),
        "failed_conversion": CausalityTemplate(
            terminal_event_family="failed_conversion",
            descriptions=[
                "conversion contest failed to clear threshold",
                "defensive leverage constrained terminal option",
                "execution could not recover margin",
                "supporting reps preserved defensive stop",
            ],
        ),
        "return_td": CausalityTemplate(
            terminal_event_family="return_td",
            descriptions=[
                "return/coverage contest created breakaway lane",
                "containment integrity collapsed downstream",
                "terminal finish outran pursuit convergence",
                "supporting blocks/tracking amplified return",
            ],
        ),
    }

    def __init__(
        self,
        random_source: RandomSource | None = None,
        resource_resolver: ResourceResolver | None = None,
        trait_weighted_enabled: bool = True,
    ) -> None:
        self._random_source = random_source or gameplay_random()
        self._resource_resolver = resource_resolver or ResourceResolver()
        self._trait_weighted_enabled = trait_weighted_enabled
        self._contest_evaluator = ContestEvaluator()
        self._influence_profiles: dict[str, dict[str, PlayFamilyInfluenceProfile]] = {}
        self._outcome_profiles: dict[str, OutcomeResolutionProfile] = {}
        self._load_trait_influence_profiles()

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
        contests = self._evaluate_contests(scp, offense, defense) if self._trait_weighted_enabled else []
        reps = self._build_rep_ledger(scp, offense, defense, contests)
        penalties = self._maybe_penalties(scp, offense, defense, contests)
        play_result = self._resolve_play_result(scp, offense, defense, penalties, contests)
        causality = self._build_causality_chain(scp, play_result, reps, contests)

        for rep in reps:
            rep.validate()
        causality.validate()

        evidence_refs = [
            ResolverEvidenceRef(
                handle=f"contest:{contest.contest_id}",
                source_type="contest",
                source_id=contest.contest_id,
                metadata={
                    "family": contest.family,
                    "score": contest.score,
                    "play_type": contest.play_type,
                },
            )
            for contest in contests
        ]

        narrative_events = [
            NarrativeEvent(
                event_id=make_id("ne"),
                time=now_utc(),
                scope="football",
                event_type="snap_resolved",
                actors=[offense, defense],
                claims=[f"Play {scp.play_id} resolved as {self._terminal_event(play_result)}"],
                evidence_handles=[play_result.play_id] + [r.rep_id for r in reps[:2]] + [e.handle for e in evidence_refs[:2]],
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
            contest_outputs=contests,
            evidence_refs=evidence_refs,
            narrative_events=narrative_events,
            conditioned=conditioned,
            attempts=attempt,
        )

    def _infer_teams(self, participants: Iterable[ActorRef], possession_team_id: str) -> tuple[str, str]:
        teams = sorted({p.team_id for p in participants})
        if possession_team_id not in teams:
            artifact = build_forensic_artifact(
                engine_scope="football",
                error_code="POSSESSION_TEAM_NOT_ON_FIELD",
                message="possession team not present among participants",
                state_snapshot={"teams": teams, "possession_team_id": possession_team_id},
                context={},
                identifiers={"possession_team_id": possession_team_id},
                causal_fragment=["participant_team_resolution"],
            )
            raise EngineIntegrityError(artifact)
        if len(teams) != 2:
            artifact = build_forensic_artifact(
                engine_scope="football",
                error_code="INVALID_TEAM_PARTITION",
                message="snap must include exactly two teams",
                state_snapshot={"teams": teams},
                context={},
                identifiers={"possession_team_id": possession_team_id},
                causal_fragment=["participant_team_resolution"],
            )
            raise EngineIntegrityError(artifact)
        offense = possession_team_id
        defense = next(t for t in teams if t != offense)
        return offense, defense

    def _build_rep_ledger(
        self,
        scp: SnapContextPackage,
        offense: str,
        defense: str,
        contests: list[ContestOutput],
    ) -> list[RepLedgerEntry]:
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

        contest_map = {c.family: c for c in contests}
        reps: list[RepLedgerEntry] = []
        for idx, rep_type in enumerate(selected):
            phase = self.PHASES[min(idx + 1, len(self.PHASES) - 1)]
            trio = offense_actors[idx % len(offense_actors) : (idx % len(offense_actors)) + 2]
            duo = defense_actors[idx % len(defense_actors) : (idx % len(defense_actors)) + 2]
            actors: list[RepActor] = [
                RepActor(a.actor_id, a.team_id, a.role, assignment_tag=f"{rep_type}_assignment") for a in trio + duo
            ]
            weights = self._weights_for_actors(actors)
            family = self._rep_family(rep_type, scp.intent.play_type.value)
            family_contest = contest_map.get(family) if family else None
            evidence_handles = [scp.play_id, f"phase:{phase}", f"concept:{scp.intent.offensive_concept}"]
            outcome_tags = ["win_contested", f"mode_{scp.mode.value}"]
            if family_contest:
                evidence_handles += family_contest.evidence_handles
                outcome_tags.append(f"contest_{family}:{family_contest.score:.3f}")
            reps.append(
                RepLedgerEntry(
                    rep_id=make_id("rep"),
                    play_id=scp.play_id,
                    phase=phase,
                    rep_type=rep_type,
                    actors=actors,
                    assignment_tags=[f"{scp.intent.offensive_concept}", f"{scp.intent.defensive_concept}"],
                    outcome_tags=outcome_tags,
                    responsibility_weights=weights,
                    context_tags=[scp.intent.play_type.value, scp.intent.formation, scp.intent.personnel],
                    evidence_handles=evidence_handles,
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
                evidence_handles=[scp.play_id, "multi_actor_rep"] + [h for c in contests for h in c.evidence_handles[:1]],
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

    def _maybe_penalties(
        self,
        scp: SnapContextPackage,
        offense: str,
        defense: str,
        contests: list[ContestOutput],
    ) -> list[PenaltyArtifact]:
        contest_map = {c.family: c for c in contests}
        penalties: list[PenaltyArtifact] = []
        rand = self._random_from_scp(scp).spawn("penalties")

        offense_actor_ids = {p.actor_id for p in scp.participants if p.team_id == offense}
        defense_actor_ids = {p.actor_id for p in scp.participants if p.team_id == defense}
        offense_states = [s for aid, s in scp.in_game_states.items() if aid in offense_actor_ids]
        defense_states = [s for aid, s in scp.in_game_states.items() if aid in defense_actor_ids]
        if not offense_states or not defense_states:
            raise ValueError("missing in-game states required for penalty evaluation")
        offense_discipline = sum(s.discipline_risk for s in offense_states) / len(offense_states)
        defense_discipline = sum(s.discipline_risk for s in defense_states) / len(defense_states)

        if scp.intent.play_type in {PlayType.PASS, PlayType.TWO_POINT}:
            leverage_loss = 1.0 - self._contest_score(contest_map, "catch_point_contest")
            dpi_prob = (leverage_loss * 0.35) + (defense_discipline * 0.15)
            if rand.rand() < dpi_prob:
                penalties.append(
                    PenaltyArtifact(
                        code="DPI",
                        against_team_id=defense,
                        yards=15,
                        enforcement_rationale="coverage leverage was lost at catch point under contest pressure",
                    )
                )

        if scp.intent.play_type in {PlayType.RUN, PlayType.PUNT, PlayType.KICKOFF}:
            if scp.intent.play_type == PlayType.RUN:
                leverage_loss = 1.0 - self._contest_score(contest_map, "lane_creation")
            else:
                leverage_loss = 1.0 - self._contest_score(contest_map, "coverage_lane_integrity")
            hold_prob = (leverage_loss * 0.3) + (offense_discipline * 0.12)
            if rand.rand() < hold_prob:
                penalties.append(
                    PenaltyArtifact(
                        code="HOLD",
                        against_team_id=offense,
                        yards=10,
                        enforcement_rationale="block leverage deteriorated under contest load and technique stress",
                    )
                )
        return penalties

    def _resolve_play_result(
        self,
        scp: SnapContextPackage,
        offense: str,
        defense: str,
        penalties: list[PenaltyArtifact],
        contests: list[ContestOutput],
    ) -> PlayResult:
        if self._trait_weighted_enabled:
            return self._resolve_play_result_trait_weighted(scp, offense, defense, penalties, contests)
        return self._resolve_play_result_legacy(scp, offense, defense, penalties)

    def _resolve_play_result_trait_weighted(
        self,
        scp: SnapContextPackage,
        offense: str,
        defense: str,
        penalties: list[PenaltyArtifact],
        contests: list[ContestOutput],
    ) -> PlayResult:
        rand = self._random_from_scp(scp).spawn("trait_weighted")
        play_type = scp.intent.play_type
        profile = self._outcome_profiles[play_type.value]
        if not contests:
            raise ValueError(f"missing contest outputs for trait-weighted play_type '{play_type.value}'")
        contest_map = {c.family: c for c in contests}
        variance = sum(c.variance_hint for c in contests) / len(contests)

        yards = 0
        turnover = False
        turnover_type: str | None = None
        score_event: str | None = None

        if play_type == PlayType.RUN:
            lane = self._contest_score(contest_map, "lane_creation")
            fit = self._contest_score(contest_map, "fit_integrity")
            tackle = self._contest_score(contest_map, "tackle_finish")
            security = self._contest_score(contest_map, "ball_security")
            noise = (rand.rand() * 2.0 - 1.0) * (profile.noise_scale * (4.0 + variance * 6.0))
            yards = int(round(((lane - fit) * 16.0) + ((1.0 - tackle) * 3.0) + noise))
            turnover_prob = profile.turnover_scale * (((1.0 - security) * 1.1) + ((1.0 - tackle) * 0.7))
            if rand.rand() < turnover_prob:
                turnover = True
                turnover_type = "FUMBLE"

        elif play_type == PlayType.PASS:
            pressure = self._contest_score(contest_map, "pressure_emergence")
            window = self._contest_score(contest_map, "separation_window")
            decision = self._contest_score(contest_map, "decision_risk")
            catch = self._contest_score(contest_map, "catch_point_contest")
            yac = self._contest_score(contest_map, "yac_continuation")
            security = self._contest_score(contest_map, "ball_security")

            sack_prob = ((1.0 - pressure) * 0.45) + ((1.0 - window) * 0.15)
            int_prob = profile.turnover_scale * (
                ((1.0 - decision) * 1.1) + ((1.0 - window) * 0.8) + ((1.0 - catch) * 0.7)
            )
            fumble_prob = profile.turnover_scale * (((1.0 - security) * 1.0) + ((1.0 - pressure) * 0.8))
            completion_prob = (window * 0.35) + (decision * 0.25) + (catch * 0.25) + (pressure * 0.15)

            roll = rand.rand()
            if roll < sack_prob:
                yards = -int(round(2 + ((1.0 - pressure) * 8) + rand.rand() * 2))
            elif roll < sack_prob + int_prob:
                turnover = True
                turnover_type = "INT"
                yards = 0
            elif roll < sack_prob + int_prob + fumble_prob:
                turnover = True
                turnover_type = "FUMBLE"
                yards = int(round((rand.rand() * 2.0) - 1.0))
            elif roll < sack_prob + int_prob + fumble_prob + completion_prob:
                noise = (rand.rand() * 2.0 - 1.0) * (profile.noise_scale * (5.0 + variance * 8.0))
                yards = int(round((window * 17.0) + (yac * 10.0) + (decision * 6.0) - 8.0 + noise))
            else:
                yards = int(round((rand.rand() * 2.0) - 1.0))

        elif play_type == PlayType.PUNT:
            kick = self._contest_score(contest_map, "kick_quality")
            block = self._contest_score(contest_map, "block_pressure")
            coverage = self._contest_score(contest_map, "coverage_lane_integrity")
            ret = self._contest_score(contest_map, "return_vision_convergence")

            blocked_prob = (1.0 - block) * 0.08
            if rand.rand() < blocked_prob:
                turnover = True
                turnover_type = "FUMBLE"
                yards = -5
            else:
                noise = (rand.rand() * 2.0 - 1.0) * (profile.noise_scale * 7.0)
                gross = int(round(34.0 + (kick * 28.0) + noise))
                return_noise = (rand.rand() * 2.0 - 1.0) * (profile.noise_scale * 5.0)
                return_yards = int(round((((ret * (1.0 - coverage)) + 0.12) * 24.0) + return_noise))
                yards = gross - return_yards
                if return_yards >= profile.explosive_threshold and rand.rand() < ret * 0.15:
                    score_event = "PUNT_RETURN_TD"

        elif play_type == PlayType.KICKOFF:
            kick = self._contest_score(contest_map, "kick_quality")
            coverage = self._contest_score(contest_map, "coverage_lane_integrity")
            ret = self._contest_score(contest_map, "return_vision_convergence")
            touchback_prob = (kick * 0.7) + 0.15
            if rand.rand() < touchback_prob:
                yards = 40
            else:
                return_noise = (rand.rand() * 2.0 - 1.0) * (profile.noise_scale * 6.0)
                return_yards = int(round((((ret * (1.0 - coverage)) + 0.2) * 28.0) + return_noise))
                yards = 65 - return_yards
                if return_yards >= profile.explosive_threshold and rand.rand() < ret * 0.12:
                    score_event = "KICK_RETURN_TD"

        elif play_type in {PlayType.FIELD_GOAL, PlayType.EXTRA_POINT}:
            kick = self._contest_score(contest_map, "kick_quality")
            block = self._contest_score(contest_map, "block_pressure")
            if play_type == PlayType.FIELD_GOAL:
                distance = max(18, 100 - scp.situation.yard_line)
                distance_penalty = distance / 95.0
                make_prob = (kick * 0.78) + (block * 0.22) - distance_penalty
                made = rand.rand() < make_prob
                score_event = "FG_GOOD" if made else "FG_MISS"
            else:
                make_prob = (kick * 0.82) + (block * 0.18)
                made = rand.rand() < make_prob
                score_event = "XP_GOOD" if made else "XP_MISS"
            yards = 0

        elif play_type == PlayType.TWO_POINT:
            pressure = self._contest_score(contest_map, "pressure_emergence")
            window = self._contest_score(contest_map, "separation_window")
            decision = self._contest_score(contest_map, "decision_risk")
            catch = self._contest_score(contest_map, "catch_point_contest")
            tackle = self._contest_score(contest_map, "tackle_finish")
            security = self._contest_score(contest_map, "ball_security")
            success_prob = (window * 0.28) + (decision * 0.24) + (catch * 0.2) + (pressure * 0.14) + (tackle * 0.14)
            int_prob = profile.turnover_scale * (((1.0 - decision) * 1.2) + ((1.0 - catch) * 0.8))
            fumble_prob = profile.turnover_scale * (((1.0 - security) * 1.2) + ((1.0 - tackle) * 0.7))
            roll = rand.rand()
            if roll < int_prob:
                turnover = True
                turnover_type = "INT"
                score_event = "TWO_PT_FAIL"
                yards = 0
            elif roll < int_prob + fumble_prob:
                turnover = True
                turnover_type = "FUMBLE"
                score_event = "TWO_PT_FAIL"
                yards = 0
            else:
                made = roll < int_prob + fumble_prob + success_prob
                score_event = "TWO_PT_GOOD" if made else "TWO_PT_FAIL"
                yards = 2 if made else 0

        for penalty in penalties:
            if penalty.against_team_id == offense:
                yards -= penalty.yards
            else:
                yards += penalty.yards

        current_spot = scp.situation.yard_line
        new_spot = max(1, min(99, current_spot + yards))
        clock_delta = self._clock_delta(rand, profile)
        return self._finalize_play_result(
            scp=scp,
            offense=offense,
            defense=defense,
            yards=yards,
            new_spot=new_spot,
            turnover=turnover,
            turnover_type=turnover_type,
            score_event=score_event,
            penalties=penalties,
            clock_delta=clock_delta,
        )

    def _resolve_play_result_legacy(
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

        return self._finalize_play_result(
            scp=scp,
            offense=offense,
            defense=defense,
            yards=yards,
            new_spot=new_spot,
            turnover=turnover,
            turnover_type=turnover_type,
            score_event=score_event,
            penalties=penalties,
            clock_delta=clock_delta,
        )

    def _build_causality_chain(
        self,
        scp: SnapContextPackage,
        play_result: PlayResult,
        reps: list[RepLedgerEntry],
        contests: list[ContestOutput],
    ) -> CausalityChain:
        terminal = self._terminal_event(play_result)
        family = self._terminal_family(play_result, scp, contests)
        template = self.TERMINAL_TEMPLATES[family]
        contest_candidates = self._relevant_contests(family, contests)
        if not contest_candidates:
            contest_candidates = contests[:3]
        contributors: list[tuple[str, str, float]] = []
        for contest in contest_candidates[:3]:
            contest_weight = abs(contest.score - 0.5) + 0.05
            contributors.append(("contest", contest.contest_id, contest_weight))
        if reps:
            contributors.append(("rep", reps[0].rep_id, 0.2))

        total = sum(c[2] for c in contributors)
        if total <= 0:
            raise ValueError("causality contributors must have positive total weight")
        nodes: list[CausalityNode] = []
        for idx, (source_type, source_id, raw_weight) in enumerate(contributors):
            nodes.append(
                CausalityNode(
                    source_type=source_type,
                    source_id=source_id,
                    weight=round(raw_weight / total, 6),
                    description=template.descriptions[idx % len(template.descriptions)],
                )
            )
        correction = round(1.0 - sum(n.weight for n in nodes), 6)
        nodes[0].weight = round(nodes[0].weight + correction, 6)
        return CausalityChain(terminal_event=terminal, play_id=scp.play_id, nodes=nodes)

    def _terminal_event(self, play_result: PlayResult) -> str:
        if play_result.turnover_type == "INT":
            return "interception"
        if play_result.turnover_type == "FUMBLE":
            return "fumble"
        if play_result.score_event:
            return play_result.score_event.lower()
        if play_result.next_down == 1 and play_result.yards >= 0:
            return "explosive_play" if play_result.yards >= 20 else "first_down"
        if play_result.yards < 0:
            return "negative_play"
        if 0 <= play_result.yards <= 3:
            return "short_play"
        return "normal_play"

    def _load_trait_influence_profiles(self) -> None:
        for play_type in [
            PlayType.RUN.value,
            PlayType.PASS.value,
            PlayType.PUNT.value,
            PlayType.KICKOFF.value,
            PlayType.FIELD_GOAL.value,
            PlayType.EXTRA_POINT.value,
            PlayType.TWO_POINT.value,
        ]:
            resource = self._resource_resolver.resolve_trait_influence(play_type)
            families, outcome_profile = parse_influence_profiles(resource)
            required = required_influence_families(play_type)
            missing = required - set(families.keys())
            if missing:
                raise ValueError(f"trait influence profile for '{play_type}' missing families {sorted(missing)}")
            self._influence_profiles[play_type] = families
            self._outcome_profiles[play_type] = outcome_profile

    def _evaluate_contests(self, scp: SnapContextPackage, offense: str, defense: str) -> list[ContestOutput]:
        if not scp.trait_vectors:
            raise ValueError("snap context missing trait_vectors for trait-weighted resolution")

        offense_actors = [p for p in scp.participants if p.team_id == offense]
        defense_actors = [p for p in scp.participants if p.team_id == defense]
        play_type = scp.intent.play_type.value
        profiles = self._influence_profiles[play_type]

        outputs: list[ContestOutput] = []
        for family, profile in profiles.items():
            offense_ids, defense_ids = self._actors_for_family(family, offense_actors, defense_actors, play_type)
            contest_input = ContestInput(
                contest_id=make_id("ct"),
                play_id=scp.play_id,
                play_type=play_type,
                family=family,
                offense_actor_ids=offense_ids,
                defense_actor_ids=defense_ids,
                influence_profile=profile,
                situation=scp.situation,
                in_game_states=scp.in_game_states,
            )
            output = self._contest_evaluator.evaluate(
                contest_input,
                trait_vectors=scp.trait_vectors,
                random_source=self._random_from_scp(scp).spawn(f"contest:{family}"),
            )
            outputs.append(output)
        return outputs

    def _actors_for_family(
        self,
        family: str,
        offense_actors: list[ActorRef],
        defense_actors: list[ActorRef],
        play_type: str,
    ) -> tuple[list[str], list[str]]:
        offense_roles_map: dict[str, list[str]] = {
            "lane_creation": ["OL", "TE", "RB"],
            "fit_integrity": ["RB", "TE", "WR"],
            "tackle_finish": ["RB", "WR", "TE", "QB"],
            "ball_security": ["QB", "RB", "WR", "TE"],
            "pressure_emergence": ["OL", "RB", "TE", "QB"],
            "separation_window": ["WR", "TE", "RB"],
            "decision_risk": ["QB", "WR", "TE", "RB"],
            "catch_point_contest": ["WR", "TE", "RB"],
            "yac_continuation": ["WR", "TE", "RB"],
            "kick_quality": ["K", "P", "QB"],
            "block_pressure": ["OL", "TE", "LB", "DE"],
            "coverage_lane_integrity": ["LB", "CB", "S", "DE", "WR"],
            "return_vision_convergence": ["RB", "WR", "CB", "S", "LB"],
        }
        defense_roles_map: dict[str, list[str]] = {
            "lane_creation": ["DL", "LB", "S"],
            "fit_integrity": ["LB", "S", "CB", "DL"],
            "tackle_finish": ["LB", "S", "CB", "DL"],
            "ball_security": ["DL", "LB", "S", "CB"],
            "pressure_emergence": ["DL", "LB", "S"],
            "separation_window": ["CB", "S", "LB"],
            "decision_risk": ["S", "CB", "LB"],
            "catch_point_contest": ["CB", "S", "LB"],
            "yac_continuation": ["LB", "S", "CB"],
            "kick_quality": ["DE", "LB", "CB", "S"],
            "block_pressure": ["DE", "LB", "CB", "S"],
            "coverage_lane_integrity": ["RB", "WR", "TE", "CB", "S"],
            "return_vision_convergence": ["LB", "CB", "S", "DE", "WR"],
        }
        offense_roles = offense_roles_map.get(family, [])
        defense_roles = defense_roles_map.get(family, [])
        target_size = 4
        if play_type in {PlayType.FIELD_GOAL.value, PlayType.EXTRA_POINT.value}:
            target_size = 3
        offense_ids = self._select_actor_ids(offense_actors, offense_roles, target_size)
        defense_ids = self._select_actor_ids(defense_actors, defense_roles, target_size)
        return offense_ids, defense_ids

    def _select_actor_ids(self, actors: list[ActorRef], preferred_roles: list[str], target_size: int) -> list[str]:
        selected: list[str] = []
        preferred = [a for a in actors if a.role in preferred_roles]
        remainder = [a for a in actors if a.actor_id not in {p.actor_id for p in preferred}]
        for actor in preferred + remainder:
            if actor.actor_id not in selected:
                selected.append(actor.actor_id)
            if len(selected) >= target_size:
                break
        if len(selected) < target_size:
            raise ValueError(f"unable to select {target_size} actors for contest group")
        return selected

    def _contest_score(self, contest_map: dict[str, ContestOutput], family: str) -> float:
        if family not in contest_map:
            raise ValueError(f"missing contest output for required family '{family}'")
        return contest_map[family].score

    def _clock_delta(self, rand: RandomSource, profile: OutcomeResolutionProfile) -> int:
        return rand.randint(profile.clock_delta_min, profile.clock_delta_max)

    def _finalize_play_result(
        self,
        *,
        scp: SnapContextPackage,
        offense: str,
        defense: str,
        yards: int,
        new_spot: int,
        turnover: bool,
        turnover_type: str | None,
        score_event: str | None,
        penalties: list[PenaltyArtifact],
        clock_delta: int,
    ) -> PlayResult:
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

    def _terminal_family(self, play_result: PlayResult, scp: SnapContextPackage, contests: list[ContestOutput]) -> str:
        if play_result.score_event in {"PUNT_RETURN_TD", "KICK_RETURN_TD"}:
            return "return_td"
        if play_result.turnover and play_result.turnover_type == "INT":
            return "turnover_interception"
        if play_result.turnover and play_result.turnover_type == "FUMBLE":
            return "turnover_fumble"
        if play_result.score_event in {"FG_GOOD", "XP_GOOD", "TWO_PT_GOOD"}:
            return "score"
        if play_result.score_event in {"FG_MISS", "XP_MISS", "TWO_PT_FAIL"}:
            return "failed_conversion"
        profile = self._outcome_profiles.get(scp.intent.play_type.value)
        if profile and play_result.yards >= profile.explosive_threshold > 0:
            return "explosive"
        if play_result.next_down == 1 and not play_result.turnover:
            return "first_down"
        if play_result.yards < 0:
            return "negative_play"
        if 0 <= play_result.yards <= 3:
            return "short_play"
        return "normal_play"

    def _relevant_contests(self, family: str, contests: list[ContestOutput]) -> list[ContestOutput]:
        mapping = {
            "score": {"separation_window", "decision_risk", "catch_point_contest", "kick_quality"},
            "turnover_interception": {"pressure_emergence", "decision_risk", "catch_point_contest"},
            "turnover_fumble": {"ball_security", "tackle_finish", "return_vision_convergence"},
            "explosive": {"lane_creation", "separation_window", "yac_continuation", "return_vision_convergence"},
            "first_down": {"lane_creation", "fit_integrity", "separation_window", "decision_risk"},
            "negative_play": {"fit_integrity", "pressure_emergence", "tackle_finish"},
            "short_play": {"tackle_finish", "fit_integrity", "catch_point_contest"},
            "normal_play": {"lane_creation", "separation_window", "coverage_lane_integrity"},
            "failed_conversion": {"kick_quality", "block_pressure", "decision_risk"},
            "return_td": {"coverage_lane_integrity", "return_vision_convergence", "kick_quality"},
        }
        required = mapping.get(family, set())
        selected = [c for c in contests if c.family in required]
        if selected:
            return selected
        return contests

    def _rep_family(self, rep_type: str, play_type: str) -> str | None:
        mapping: dict[str, dict[str, str]] = {
            PlayType.RUN.value: {
                "run_fit": "fit_integrity",
                "block": "lane_creation",
                "pursuit": "tackle_finish",
                "tackle": "tackle_finish",
            },
            PlayType.PASS.value: {
                "release": "separation_window",
                "coverage": "separation_window",
                "pass_pro": "pressure_emergence",
                "read": "decision_risk",
                "contest": "catch_point_contest",
            },
            PlayType.PUNT.value: {
                "protection": "block_pressure",
                "rush": "block_pressure",
                "gunners": "coverage_lane_integrity",
                "return": "return_vision_convergence",
            },
            PlayType.KICKOFF.value: {
                "coverage_lane": "coverage_lane_integrity",
                "wedge": "block_pressure",
                "return": "return_vision_convergence",
                "tackle": "tackle_finish",
            },
            PlayType.FIELD_GOAL.value: {
                "snap_hold_kick": "kick_quality",
                "edge_rush": "block_pressure",
                "interior_push": "block_pressure",
            },
            PlayType.EXTRA_POINT.value: {
                "snap_hold_kick": "kick_quality",
                "block_attempt": "block_pressure",
            },
            PlayType.TWO_POINT.value: {
                "redzone_concept": "separation_window",
                "coverage": "catch_point_contest",
                "pressure": "pressure_emergence",
            },
        }
        return mapping.get(play_type, {}).get(rep_type)

    def _random_from_scp(self, scp: SnapContextPackage):
        # Mode is intentionally excluded so play/sim/off-screen share the same physics distribution.
        return self._random_source.spawn(f"{scp.game_id}:{scp.play_id}")


class FootballEngine:
    def __init__(
        self,
        resolver: FootballResolver | None = None,
        validator: PreSimValidator | None = None,
    ) -> None:
        self._resolver = resolver or FootballResolver()
        self._validator = validator or PreSimValidator()

    def run_snap(
        self,
        scp: SnapContextPackage,
        *,
        dev_mode: bool = False,
        force_target: str | None = None,
        max_attempts: int = 1000,
    ) -> SnapResolution:
        try:
            self._validator.validate_snap_context(scp)
        except ValidationError as exc:
            artifact = build_forensic_artifact(
                engine_scope="football",
                error_code="PRE_SIM_VALIDATION_FAILED",
                message="snap context failed pre-sim validation",
                state_snapshot={
                    "play_id": scp.play_id,
                    "game_id": scp.game_id,
                    "mode": scp.mode.value,
                    "issue_count": len(exc.issues),
                },
                context={"issues": [asdict(issue) for issue in exc.issues]},
                identifiers={"game_id": scp.game_id, "play_id": scp.play_id},
                causal_fragment=["pre_sim_gate"],
            )
            raise EngineIntegrityError(artifact) from exc

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
