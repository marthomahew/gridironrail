from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable

from grs.contracts import (
    ActorRef,
    AssignmentTemplate,
    CausalityChain,
    CausalityNode,
    ContestInput,
    ContestResolution,
    MatchupEdge,
    MatchupGraph,
    NarrativeEvent,
    OutcomeResolutionProfile,
    PenaltyArtifact,
    PlaybookEntry,
    PlayFamilyInfluenceProfile,
    PlayResult,
    PlayType,
    PreSnapMatchupPlan,
    RandomSource,
    RepActor,
    RepLedgerEntry,
    ResolvedSnapStateDelta,
    ResolverEvidenceRef,
    RulesAdjudicationResult,
    SimMode,
    SnapArtifactBundle,
    SnapContextPackage,
    ValidationError,
)
from grs.core import EngineIntegrityError, build_forensic_artifact, gameplay_random, make_id, now_utc
from grs.football.contest import ContestEvaluator, parse_influence_profiles, required_influence_families
from grs.football.matchup import MatchupCompileError, MatchupCompiler
from grs.football.models import SnapResolution
from grs.football.resources import ResourceResolver
from grs.football.validation import PreSimValidator


@dataclass(slots=True)
class _TerminalOutcome:
    play_id: str
    yards: int
    new_spot: int
    turnover: bool
    turnover_type: str | None
    score_event: str | None
    penalties: list[PenaltyArtifact]
    clock_delta: int


class FootballResolver:
    UNIVERSAL_FLOW = [
        "pre_snap_compile",
        "early_leverage",
        "disposition",
        "primary_contest",
        "branch_resolution",
        "terminal_event",
        "adjudication",
        "aftermath",
    ]
    MANDATORY_RECHECKS = 4
    CONDITIONAL_RECHECKS: dict[PlayType, int] = {
        PlayType.RUN: 1,
        PlayType.PASS: 2,
        PlayType.PUNT: 2,
        PlayType.KICKOFF: 3,
        PlayType.FIELD_GOAL: 1,
        PlayType.EXTRA_POINT: 1,
        PlayType.TWO_POINT: 2,
    }

    def __init__(self, random_source: RandomSource | None = None, resource_resolver: ResourceResolver | None = None) -> None:
        self._random_source = random_source or gameplay_random()
        self._resource_resolver = resource_resolver or ResourceResolver()
        self._contest = ContestEvaluator()
        self._matchup_compiler = MatchupCompiler()
        self._families: dict[str, dict[str, PlayFamilyInfluenceProfile]] = {}
        self._outcome_profiles: dict[str, OutcomeResolutionProfile] = {}
        self._load_profiles()

    def resolve_snap(self, scp: SnapContextPackage, *, conditioned: bool = False, attempt: int = 1) -> SnapResolution:
        offense, defense = self._infer_teams(scp.participants, scp.situation.possession_team_id)
        playbook_entry = self._resolve_playbook_entry(scp)
        assignment = self._resource_resolver.resolve_assignment_template(playbook_entry.assignment_template_id)
        pre_snap = self._compile_matchups(scp, playbook_entry, assignment, offense, defense)

        contests, snapshots, transitions = self._run_phasal_rechecks(scp, pre_snap, offense, defense)
        penalties = self._resolve_penalties(scp, offense, defense, contests)
        terminal = self._resolve_terminal_outcome(scp, offense, defense, contests, penalties)
        rules = self._adjudicate(scp, offense, defense, terminal)
        play_result = PlayResult(
            play_id=terminal.play_id,
            yards=terminal.yards,
            new_spot=terminal.new_spot,
            turnover=terminal.turnover,
            turnover_type=terminal.turnover_type,
            score_event=rules.score_event,
            penalties=rules.penalties,
            clock_delta=rules.clock_delta,
            next_down=rules.next_down,
            next_distance=rules.next_distance,
            next_possession_team_id=rules.next_possession_team_id,
        )
        reps = self._build_rep_ledger(scp, pre_snap, contests)
        causality = self._build_causality(play_result, reps, contests)
        for rep in reps:
            rep.validate()
        causality.validate()

        evidence_refs = [
            ResolverEvidenceRef(
                handle=f"contest:{c.contest_id}",
                source_type="contest",
                source_id=c.contest_id,
                metadata={"phase": c.phase, "family": c.family, "score": c.score},
            )
            for c in contests
        ]
        narrative = [
            NarrativeEvent(
                event_id=make_id("ne"),
                time=now_utc(),
                scope="football",
                event_type="snap_resolved",
                actors=[offense, defense],
                claims=[f"{scp.play_id} resolved via {playbook_entry.play_id}"],
                evidence_handles=[scp.play_id, pre_snap.plan_id],
                severity="normal",
                confidentiality_tier="public",
            )
        ]
        if conditioned:
            narrative.append(
                NarrativeEvent(
                    event_id=make_id("ne"),
                    time=now_utc(),
                    scope="football",
                    event_type="conditioned_play",
                    actors=[offense],
                    claims=["Outcome selected through force-outcome dev loop"],
                    evidence_handles=[scp.play_id],
                    severity="high",
                    confidentiality_tier="internal",
                )
            )

        bundle = SnapArtifactBundle(
            play_result=play_result,
            pre_snap_plan=pre_snap,
            matchup_snapshots=snapshots,
            phase_transitions=transitions,
            contest_resolutions=contests,
            rep_ledger=reps,
            causality_chain=causality,
            evidence_refs=evidence_refs,
            rules_adjudication=rules,
            narrative_events=narrative,
        )
        delta = ResolvedSnapStateDelta(
            next_down=rules.next_down,
            next_distance=rules.next_distance,
            next_possession_team_id=rules.next_possession_team_id,
            new_spot=bundle.play_result.new_spot,
            clock_delta=rules.clock_delta,
            score_delta_by_team=self._score_delta(rules.score_event, offense, defense),
            drive_increment=rules.next_down == 1,
            injuries={},
            fatigue_delta={p.actor_id: 0.01 for p in scp.participants},
        )
        return SnapResolution(artifact_bundle=bundle, state_delta=delta, conditioned=conditioned, attempts=attempt)

    def _resolve_playbook_entry(self, scp: SnapContextPackage):
        if scp.intent.playbook_entry_id:
            try:
                return self._resource_resolver.resolve_playbook_entry(scp.intent.playbook_entry_id)
            except ValidationError as exc:
                raise EngineIntegrityError(
                    build_forensic_artifact(
                        engine_scope="football",
                        error_code="PLAYBOOK_INTENT_UNRESOLVABLE",
                        message="explicit playbook_entry_id failed to resolve",
                        state_snapshot={
                            "game_id": scp.game_id,
                            "play_id": scp.play_id,
                            "playbook_entry_id": scp.intent.playbook_entry_id,
                        },
                        context={"issues": [asdict(i) for i in exc.issues]},
                        identifiers={"game_id": scp.game_id, "play_id": scp.play_id},
                        causal_fragment=["intent_resolution", "playbook_lookup"],
                    )
                ) from exc
        try:
            return self._resource_resolver.resolve_playbook_entry_for_intent(
                play_type=scp.intent.play_type,
                personnel_id=scp.intent.personnel,
                formation_id=scp.intent.formation,
                offensive_concept_id=scp.intent.offensive_concept,
                defensive_concept_id=scp.intent.defensive_concept,
            )
        except ValidationError as exc:
            raise EngineIntegrityError(
                build_forensic_artifact(
                    engine_scope="football",
                    error_code="PLAYBOOK_INTENT_UNRESOLVABLE",
                    message="unable to resolve playbook entry for snap intent",
                    state_snapshot={
                        "game_id": scp.game_id,
                        "play_id": scp.play_id,
                        "play_type": scp.intent.play_type.value,
                    },
                    context={"issues": [asdict(i) for i in exc.issues]},
                    identifiers={"game_id": scp.game_id, "play_id": scp.play_id},
                    causal_fragment=["intent_resolution", "playbook_lookup"],
                )
            ) from exc

    def _compile_matchups(
        self,
        scp: SnapContextPackage,
        playbook_entry: PlaybookEntry,
        assignment_template: AssignmentTemplate,
        offense: str,
        defense: str,
    ) -> PreSnapMatchupPlan:
        try:
            return self._matchup_compiler.compile(
                play_id=scp.play_id,
                playbook_entry_id=playbook_entry.play_id,
                assignment_template=assignment_template,
                offense_team_id=offense,
                defense_team_id=defense,
                participants=scp.participants,
            )
        except MatchupCompileError as exc:
            raise EngineIntegrityError(
                build_forensic_artifact(
                    engine_scope="football",
                    error_code="MATCHUP_COMPILE_INCOMPLETE",
                    message=str(exc),
                    state_snapshot={
                        "game_id": scp.game_id,
                        "play_id": scp.play_id,
                        "assignment_template_id": assignment_template.template_id,
                    },
                    context={"playbook_entry_id": playbook_entry.play_id},
                    identifiers={"game_id": scp.game_id, "play_id": scp.play_id},
                    causal_fragment=["pre_snap_compile", "matchup_graph"],
                )
            ) from exc

    def _run_phasal_rechecks(
        self,
        scp: SnapContextPackage,
        pre_snap: PreSnapMatchupPlan,
        offense: str,
        defense: str,
    ) -> tuple[list[ContestResolution], list[MatchupGraph], list[str]]:
        families = sorted(required_influence_families(scp.intent.play_type.value))
        checks = self.MANDATORY_RECHECKS + self.CONDITIONAL_RECHECKS[scp.intent.play_type]
        contests: list[ContestResolution] = []
        snapshots = [pre_snap.graph]
        transitions = ["pre_snap_compile"]
        for idx in range(1, checks + 1):
            phase = "branch_resolution" if idx > 4 else self.UNIVERSAL_FLOW[min(idx, 4)]
            transitions.append(f"{phase}:check_{idx}")
            family = families[(idx - 1) % len(families)]
            profile = self._families[scp.intent.play_type.value][family]
            o_ids, d_ids = self._actors_for_family(family, [p for p in scp.participants if p.team_id == offense], [p for p in scp.participants if p.team_id == defense], scp.intent.play_type.value)
            raw = self._contest.evaluate(
                ContestInput(
                    contest_id=make_id("ct"),
                    play_id=scp.play_id,
                    play_type=scp.intent.play_type.value,
                    family=family,
                    offense_actor_ids=o_ids,
                    defense_actor_ids=d_ids,
                    influence_profile=profile,
                    situation=scp.situation,
                    in_game_states=scp.in_game_states,
                ),
                trait_vectors=scp.trait_vectors,
                random_source=self._random_from_scp(scp).spawn(f"{phase}:{family}:{idx}"),
            )
            contest = ContestResolution(
                contest_id=raw.contest_id,
                play_id=raw.play_id,
                phase=phase,
                family=raw.family,
                score=raw.score,
                offense_score=raw.offense_score,
                defense_score=raw.defense_score,
                contributor_trace=raw.actor_contributions,
                trait_trace=raw.trait_contributions,
                evidence_handles=raw.evidence_handles,
                variance_hint=raw.variance_hint,
            )
            contests.append(contest)
            snapshots.append(
                MatchupGraph(
                    graph_id=make_id("graph"),
                    play_id=pre_snap.play_id,
                    phase=f"{phase}:{idx}",
                    edges=[
                        MatchupEdge(
                            edge_id=e.edge_id,
                            offense_actor_id=e.offense_actor_id,
                            defense_actor_id=e.defense_actor_id,
                            offense_role=e.offense_role,
                            defense_role=e.defense_role,
                            technique=e.technique,
                            leverage="offense" if contest.score >= 0.5 else "defense",
                            responsibility_weight=e.responsibility_weight,
                            context_tags=e.context_tags + [f"check:{idx}", f"family:{contest.family}"],
                        )
                        for e in pre_snap.graph.edges
                    ],
                )
            )
        transitions.extend(["terminal_event", "adjudication", "aftermath"])
        return contests, snapshots, transitions

    def _resolve_penalties(
        self,
        scp: SnapContextPackage,
        offense: str,
        defense: str,
        contests: list[ContestResolution],
    ) -> list[PenaltyArtifact]:
        rand = self._random_from_scp(scp).spawn("penalties")
        by_family = {c.family: c for c in contests}
        off_disc = sum(s.discipline_risk for aid, s in scp.in_game_states.items() if any(p.actor_id == aid and p.team_id == offense for p in scp.participants)) / 11.0
        def_disc = sum(s.discipline_risk for aid, s in scp.in_game_states.items() if any(p.actor_id == aid and p.team_id == defense for p in scp.participants)) / 11.0
        penalties: list[PenaltyArtifact] = []
        catch = by_family.get("catch_point_contest")
        if catch and rand.rand() < ((1.0 - catch.score) * 0.25 + def_disc * 0.1):
            penalties.append(PenaltyArtifact(code="DPI", against_team_id=defense, yards=15, enforcement_rationale="defender lost leverage at catch point"))
        lane = by_family.get("lane_creation")
        hold_stress = 1.0 - (lane.score if lane else 0.5)
        if rand.rand() < (hold_stress * 0.22 + off_disc * 0.1):
            penalties.append(PenaltyArtifact(code="HOLD", against_team_id=offense, yards=10, enforcement_rationale="blocker reached while losing leverage"))
        return penalties

    def _resolve_terminal_outcome(
        self,
        scp: SnapContextPackage,
        offense: str,
        defense: str,
        contests: list[ContestResolution],
        penalties: list[PenaltyArtifact],
    ) -> _TerminalOutcome:
        rand = self._random_from_scp(scp).spawn("terminal")
        profile = self._outcome_profiles[scp.intent.play_type.value]
        by_family = {c.family: c for c in contests}
        yards = 0
        turnover = False
        turnover_type: str | None = None
        score_event: str | None = None
        if scp.intent.play_type == PlayType.RUN:
            lane = by_family["lane_creation"].score
            fit = by_family["fit_integrity"].score
            tackle = by_family["tackle_finish"].score
            security = by_family["ball_security"].score
            yards = int(round(((lane - fit) * 16.0) + ((1.0 - tackle) * 5.0) + ((rand.rand() - 0.5) * 4.0)))
            turnover = rand.rand() < (profile.turnover_scale * (1.0 - security))
            turnover_type = "FUMBLE" if turnover else None
        elif scp.intent.play_type in {PlayType.PASS, PlayType.TWO_POINT}:
            pressure = by_family["pressure_emergence"].score
            separation = by_family["separation_window"].score
            decision = by_family["decision_risk"].score
            catch = by_family["catch_point_contest"].score
            comp_prob = max(0.03, min(0.97, 0.25 + separation * 0.3 + decision * 0.25 + catch * 0.2 - pressure * 0.2))
            complete = rand.rand() < comp_prob
            yards = int(round(((separation - pressure) * 14.0) + ((rand.rand() - 0.5) * 8.0))) if complete else -rand.randint(0, 5)
            int_prob = profile.turnover_scale * (1.0 - decision) * (1.0 - catch) * (0.7 + pressure)
            fum_prob = profile.turnover_scale * (1.0 - by_family["ball_security"].score) * 0.35
            roll = rand.rand()
            if roll < int_prob:
                turnover = True
                turnover_type = "INT"
            elif roll < int_prob + fum_prob:
                turnover = True
                turnover_type = "FUMBLE"
            if scp.intent.play_type == PlayType.TWO_POINT:
                score_event = "TWO_PT_GOOD" if (not turnover and yards >= 2) else "TWO_PT_FAIL"
                yards = 2 if score_event == "TWO_PT_GOOD" else max(-2, min(1, yards))
        elif scp.intent.play_type in {PlayType.PUNT, PlayType.KICKOFF}:
            kick = by_family["kick_quality"].score
            cover = by_family["coverage_lane_integrity"].score
            ret = by_family["return_vision_convergence"].score
            gross = int(round(28 + kick * 36 + ((rand.rand() - 0.5) * 8.0)))
            ret_yards = int(round(8 + ret * 24 - cover * 14 + ((rand.rand() - 0.5) * 8.0)))
            yards = gross - max(0, ret_yards)
            if rand.rand() < (0.01 + max(0.0, (ret - cover) * 0.18)):
                score_event = "PUNT_RETURN_TD" if scp.intent.play_type == PlayType.PUNT else "KICK_RETURN_TD"
        else:
            kick = by_family["kick_quality"].score
            block = by_family["block_pressure"].score
            dist = max(18, 100 - scp.situation.yard_line)
            make_prob = max(0.02, min(0.99, kick * 0.85 + (1.0 - block) * 0.3 - (dist / 80.0)))
            made = rand.rand() < make_prob
            score_event = "FG_GOOD" if scp.intent.play_type == PlayType.FIELD_GOAL and made else "FG_MISS" if scp.intent.play_type == PlayType.FIELD_GOAL else "XP_GOOD" if made else "XP_MISS"
        for penalty in penalties:
            yards += penalty.yards if penalty.against_team_id != offense else -penalty.yards
        new_spot = max(1, min(99, scp.situation.yard_line + yards))
        if scp.intent.play_type in {PlayType.RUN, PlayType.PASS} and not turnover and new_spot >= 99:
            score_event = "OFF_TD"
        return _TerminalOutcome(
            play_id=scp.play_id,
            yards=yards,
            new_spot=new_spot,
            turnover=turnover,
            turnover_type=turnover_type,
            score_event=score_event,
            penalties=penalties,
            clock_delta=self._clock_delta(scp),
        )

    def _adjudicate(
        self,
        scp: SnapContextPackage,
        offense: str,
        defense: str,
        terminal: _TerminalOutcome,
    ) -> RulesAdjudicationResult:
        score_event = terminal.score_event
        if score_event in {"OFF_TD", "PUNT_RETURN_TD", "KICK_RETURN_TD"}:
            next_possession, next_down, next_distance = defense, 1, 10
            notes = ["touchdown scored"]
        elif score_event in {"FG_GOOD", "FG_MISS", "XP_GOOD", "XP_MISS", "TWO_PT_GOOD", "TWO_PT_FAIL"}:
            next_possession, next_down, next_distance = defense, 1, 10
            notes = [f"conversion/kick attempt result: {score_event}"]
        elif terminal.turnover:
            next_possession, next_down, next_distance = defense, 1, 10
            notes = [f"turnover {terminal.turnover_type}"]
        else:
            remaining = scp.situation.distance - terminal.yards
            if remaining <= 0:
                next_down, next_distance = 1, 10
                next_possession = offense
                notes = ["first down achieved"]
            elif scp.situation.down >= 4:
                next_down, next_distance = 1, 10
                next_possession = defense
                notes = ["turnover on downs"]
            else:
                next_down, next_distance = scp.situation.down + 1, max(1, remaining)
                next_possession = offense
                notes = ["normal progression"]
        return RulesAdjudicationResult(
            penalties=terminal.penalties,
            score_event=score_event,
            enforcement_notes=notes,
            next_down=next_down,
            next_distance=next_distance,
            next_possession_team_id=next_possession,
            clock_delta=terminal.clock_delta,
        )

    def _build_rep_ledger(self, scp: SnapContextPackage, pre_snap: PreSnapMatchupPlan, contests: list[ContestResolution]) -> list[RepLedgerEntry]:
        reps: list[RepLedgerEntry] = []
        participant_by_id = {p.actor_id: p for p in scp.participants}
        for contest in contests:
            ranked_actor_ids = sorted(
                contest.contributor_trace.keys(),
                key=lambda aid: abs(contest.contributor_trace.get(aid, 0.0)),
                reverse=True,
            )
            selected_ids = ranked_actor_ids[:6]
            actors: list[RepActor] = []
            for actor_id in selected_ids:
                actor = participant_by_id.get(actor_id)
                if actor is None:
                    continue
                actors.append(
                    RepActor(
                        actor_id=actor.actor_id,
                        team_id=actor.team_id,
                        role=actor.role,
                        assignment_tag=self._assignment_tag_for_actor(pre_snap, actor.actor_id),
                    )
                )
            if not actors:
                continue
            reps.append(
                RepLedgerEntry(
                    rep_id=make_id("rep"),
                    play_id=scp.play_id,
                    phase=contest.phase,
                    rep_type=contest.family,
                    actors=actors,
                    assignment_tags=[pre_snap.assignment_template_id, contest.family],
                    outcome_tags=[f"contest_score:{contest.score:.4f}"],
                    responsibility_weights=self._contributor_weight_map(actors, contest.contributor_trace),
                    context_tags=[scp.intent.play_type.value, scp.intent.formation, scp.intent.personnel],
                    evidence_handles=contest.evidence_handles,
                )
            )
        reps.extend(self._build_group_reps(scp, pre_snap, participant_by_id))
        return reps

    def _build_causality(self, play_result: PlayResult, reps: list[RepLedgerEntry], contests: list[ContestResolution]) -> CausalityChain:
        terminal = (
            "interception"
            if play_result.turnover_type == "INT"
            else "fumble"
            if play_result.turnover_type == "FUMBLE"
            else play_result.score_event.lower()
            if play_result.score_event
            else "negative_play"
            if play_result.yards < 0
            else "first_down"
            if play_result.next_down == 1 and not play_result.turnover and play_result.score_event is None
            else "normal_play"
        )
        nodes = [CausalityNode("contest", c.contest_id, 0.0, f"{c.family} in {c.phase}") for c in sorted(contests, key=lambda x: abs(x.score - 0.5), reverse=True)[:3]]
        if reps:
            nodes.append(CausalityNode("rep", reps[0].rep_id, 0.0, "upstream assignment execution"))
        if not nodes:
            raise ValueError("causality chain requires nodes")
        share = round(1.0 / len(nodes), 6)
        for node in nodes:
            node.weight = share
        nodes[0].weight = round(nodes[0].weight + (1.0 - sum(n.weight for n in nodes)), 6)
        return CausalityChain(terminal_event=terminal, play_id=play_result.play_id, nodes=nodes)

    def _weight_map(self, actors: list[RepActor]) -> dict[str, float]:
        ids = list(dict.fromkeys(a.actor_id for a in actors))
        share = round(1.0 / len(ids), 6)
        out = {actor_id: share for actor_id in ids}
        out[ids[0]] = round(out[ids[0]] + (1.0 - sum(out.values())), 6)
        return out

    def _contributor_weight_map(self, actors: list[RepActor], contributor_trace: dict[str, float]) -> dict[str, float]:
        ids = list(dict.fromkeys(actor.actor_id for actor in actors))
        abs_scores = {actor_id: abs(float(contributor_trace.get(actor_id, 0.0))) for actor_id in ids}
        total = sum(abs_scores.values())
        if total <= 0.0:
            return self._weight_map(actors)
        weights = {actor_id: round(score / total, 6) for actor_id, score in abs_scores.items()}
        first = ids[0]
        weights[first] = round(weights[first] + (1.0 - sum(weights.values())), 6)
        return weights

    def _assignment_tag_for_actor(self, pre_snap: PreSnapMatchupPlan, actor_id: str) -> str:
        for edge in pre_snap.graph.edges:
            if edge.offense_actor_id == actor_id or edge.defense_actor_id == actor_id:
                return edge.technique
        return "involved"

    def _build_group_reps(
        self,
        scp: SnapContextPackage,
        pre_snap: PreSnapMatchupPlan,
        participant_by_id: dict[str, ActorRef],
    ) -> list[RepLedgerEntry]:
        grouped: dict[str, list[MatchupEdge]] = {}
        for edge in pre_snap.graph.edges:
            for tag in edge.context_tags:
                if tag.startswith("group:"):
                    grouped.setdefault(tag, []).append(edge)
        reps: list[RepLedgerEntry] = []
        for group_id, edges in grouped.items():
            group_name = group_id.split(":")[1]
            actor_ids: list[str] = []
            for edge in edges:
                actor_ids.append(edge.offense_actor_id)
                actor_ids.append(edge.defense_actor_id)
            actor_ids = list(dict.fromkeys(actor_ids))
            actors: list[RepActor] = []
            for actor_id in actor_ids:
                actor = participant_by_id.get(actor_id)
                if actor is None:
                    continue
                actors.append(
                    RepActor(
                        actor_id=actor.actor_id,
                        team_id=actor.team_id,
                        role=actor.role,
                        assignment_tag=self._assignment_tag_for_actor(pre_snap, actor.actor_id),
                    )
                )
            if len(actors) < 2:
                continue
            reps.append(
                RepLedgerEntry(
                    rep_id=make_id("rep"),
                    play_id=scp.play_id,
                    phase="branch_resolution",
                    rep_type=group_name,
                    actors=actors,
                    assignment_tags=[pre_snap.assignment_template_id, group_name],
                    outcome_tags=["group_interaction"],
                    responsibility_weights=self._weight_map(actors),
                    context_tags=["multi_actor", group_id],
                    evidence_handles=[pre_snap.plan_id, f"group:{group_id}"],
                )
            )
        return reps

    def _score_delta(self, score_event: str | None, offense: str, defense: str) -> dict[str, int]:
        if score_event == "OFF_TD":
            return {offense: 6}
        if score_event == "FG_GOOD":
            return {offense: 3}
        if score_event == "XP_GOOD":
            return {offense: 1}
        if score_event == "TWO_PT_GOOD":
            return {offense: 2}
        if score_event in {"PUNT_RETURN_TD", "KICK_RETURN_TD"}:
            return {defense: 6}
        return {}

    def _clock_delta(self, scp: SnapContextPackage) -> int:
        profile = self._outcome_profiles[scp.intent.play_type.value]
        return self._random_from_scp(scp).spawn("clock").randint(profile.clock_delta_min, profile.clock_delta_max)

    def _load_profiles(self) -> None:
        for play_type in [p.value for p in PlayType]:
            resource = self._resource_resolver.resolve_trait_influence(play_type)
            families, outcome = parse_influence_profiles(resource)
            missing = required_influence_families(play_type) - set(families.keys())
            if missing:
                raise ValueError(f"trait influence profile for '{play_type}' missing families {sorted(missing)}")
            self._families[play_type] = families
            self._outcome_profiles[play_type] = outcome

    def _actors_for_family(self, family: str, offense: list[ActorRef], defense: list[ActorRef], play_type: str) -> tuple[list[str], list[str]]:
        off_roles = {
            "lane_creation": ["OL", "TE", "RB"],
            "fit_integrity": ["RB", "TE", "WR"],
            "tackle_finish": ["RB", "WR", "TE", "QB"],
            "ball_security": ["QB", "RB", "WR", "TE"],
            "pressure_emergence": ["OL", "RB", "TE", "QB"],
            "separation_window": ["WR", "TE", "RB"],
            "decision_risk": ["QB", "WR", "TE", "RB"],
            "catch_point_contest": ["QB", "WR", "TE", "RB"],
            "yac_continuation": ["WR", "TE", "RB"],
            "kick_quality": ["K", "P", "QB"],
            "block_pressure": ["OL", "TE", "LB", "DE"],
            "coverage_lane_integrity": ["LB", "CB", "S", "DE", "WR"],
            "return_vision_convergence": ["RB", "WR", "CB", "S", "LB"],
        }
        def_roles = {
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
        target = 3 if play_type in {PlayType.FIELD_GOAL.value, PlayType.EXTRA_POINT.value} else 4
        return self._select_actor_ids(offense, off_roles.get(family, []), target), self._select_actor_ids(defense, def_roles.get(family, []), target)

    def _select_actor_ids(self, actors: list[ActorRef], preferred_roles: list[str], target: int) -> list[str]:
        selected = [a.actor_id for a in actors if a.role in preferred_roles]
        for actor in actors:
            if actor.actor_id not in selected:
                selected.append(actor.actor_id)
            if len(selected) >= target:
                break
        if len(selected) < target:
            raise ValueError(f"unable to select {target} actors for contest group")
        return selected[:target]

    def _infer_teams(self, participants: Iterable[ActorRef], possession: str) -> tuple[str, str]:
        teams = sorted({p.team_id for p in participants})
        if possession not in teams or len(teams) != 2:
            raise EngineIntegrityError(
                build_forensic_artifact(
                    engine_scope="football",
                    error_code="INVALID_TEAM_CONTEXT",
                    message="unable to infer offense/defense teams",
                    state_snapshot={"teams": teams, "possession_team_id": possession},
                    context={},
                    identifiers={"possession_team_id": possession},
                    causal_fragment=["team_partition"],
                )
            )
        return possession, next(t for t in teams if t != possession)

    def _random_from_scp(self, scp: SnapContextPackage):
        return self._random_source.spawn(f"{scp.game_id}:{scp.play_id}")


class FootballEngine:
    def __init__(self, resolver: FootballResolver | None = None, validator: PreSimValidator | None = None) -> None:
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
            raise EngineIntegrityError(
                build_forensic_artifact(
                    engine_scope="football",
                    error_code="PRE_SIM_VALIDATION_FAILED",
                    message="snap context failed pre-sim validation",
                    state_snapshot={"play_id": scp.play_id, "game_id": scp.game_id, "mode": scp.mode.value, "issue_count": len(exc.issues)},
                    context={"issues": [asdict(issue) for issue in exc.issues]},
                    identifiers={"game_id": scp.game_id, "play_id": scp.play_id},
                    causal_fragment=["pre_sim_gate"],
                )
            ) from exc
        if force_target and not dev_mode:
            raise ValueError("force_target is only available in dev mode")
        if not force_target:
            return self._resolver.resolve_snap(scp, conditioned=False, attempt=1)
        for attempt in range(1, max_attempts + 1):
            trial = SnapContextPackage(
                game_id=scp.game_id,
                play_id=f"{scp.play_id}_TRY{attempt:04d}",
                mode=scp.mode,
                situation=scp.situation,
                participants=list(scp.participants),
                in_game_states=dict(scp.in_game_states),
                intent=scp.intent,
                trait_vectors=dict(scp.trait_vectors),
                weather_flags=list(scp.weather_flags),
            )
            result = self._resolver.resolve_snap(trial, conditioned=True, attempt=attempt)
            if result.causality_chain.terminal_event == force_target:
                return result
        raise EngineIntegrityError(
            build_forensic_artifact(
                engine_scope="football",
                error_code="FORCE_OUTCOME_FAIL",
                message=f"force outcome target '{force_target}' not reached in {max_attempts} attempts",
                state_snapshot={"play_id": scp.play_id, "mode": scp.mode.value},
                context={"force_target": force_target, "max_attempts": max_attempts},
                identifiers={"game_id": scp.game_id, "play_id": scp.play_id},
                causal_fragment=["dev_force_outcome"],
            )
        )

    def run_mode_invariant(self, scp: SnapContextPackage, mode: SimMode) -> SnapResolution:
        return self.run_snap(
            SnapContextPackage(
                game_id=scp.game_id,
                play_id=scp.play_id,
                mode=mode,
                situation=scp.situation,
                participants=list(scp.participants),
                in_game_states=dict(scp.in_game_states),
                intent=scp.intent,
                trait_vectors=dict(scp.trait_vectors),
                weather_flags=list(scp.weather_flags),
            )
        )
