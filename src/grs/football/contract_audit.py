from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime

from grs.contracts import (
    ActorRef,
    ContractAuditCheck,
    ContractAuditReport,
    InGameState,
    ParameterizedIntent,
    PlayType,
    SimMode,
    Situation,
    SnapContextPackage,
    ValidationError,
)
from grs.core import make_id, seeded_random
from grs.football.resolver import FootballEngine, FootballResolver
from grs.football.traits import required_trait_codes
from grs.football.validation import PreSimValidator


class FootballContractAuditor:
    def __init__(self) -> None:
        self._validator = PreSimValidator()
        self._engine = FootballEngine(FootballResolver(seeded_random(2026)), validator=self._validator)

    def run(self) -> ContractAuditReport:
        checks: list[ContractAuditCheck] = []
        checks.append(self._check_input_validation_gate())
        checks.extend(self._check_play_family_contracts())
        checks.append(self._check_mode_invariance())
        return ContractAuditReport(
            report_id=make_id("audit"),
            generated_at=datetime.now(UTC),
            scope="football_layer",
            checks=checks,
        )

    def _check_input_validation_gate(self) -> ContractAuditCheck:
        bad = SnapContextPackage(
            game_id="AUDIT",
            play_id="AUDIT_BAD",
            mode=SimMode.SIM,
            situation=Situation(quarter=1, clock_seconds=900, down=1, distance=10, yard_line=25, possession_team_id="A", score_diff=0, timeouts_offense=3, timeouts_defense=3),
            participants=[],
            in_game_states={},
            intent=ParameterizedIntent(personnel="11", formation="gun_trips", offensive_concept="spacing", defensive_concept="cover3_match", play_type=PlayType.PASS),
        )
        try:
            self._validator.validate_snap_context(bad)
        except ValidationError as exc:
            return ContractAuditCheck(
                check_id="input_validation_gate",
                description="Pre-sim validator blocks incomplete snap packages.",
                passed=any(issue.code == "INVALID_PARTICIPANT_COUNT" for issue in exc.issues),
                evidence=f"issues={len(exc.issues)}",
            )
        return ContractAuditCheck(
            check_id="input_validation_gate",
            description="Pre-sim validator blocks incomplete snap packages.",
            passed=False,
            evidence="validator accepted invalid snap context",
        )

    def _check_play_family_contracts(self) -> list[ContractAuditCheck]:
        checks: list[ContractAuditCheck] = []
        for play_type in PlayType:
            scp = self._build_context(play_type)
            result = self._engine.run_snap(scp)
            transitions = result.artifact_bundle.phase_transitions
            expected_min = 1 + (4 + self._engine._resolver.CONDITIONAL_RECHECKS[play_type]) + 3
            checks.append(
                ContractAuditCheck(
                    check_id=f"{play_type.value}_phase_flow",
                    description=f"{play_type.value} uses universal phasal flow with re-check budget.",
                    passed=transitions[0] == "pre_snap_compile" and transitions[-3:] == ["terminal_event", "adjudication", "aftermath"] and len(transitions) >= expected_min,
                    evidence=f"transition_count={len(transitions)}; transitions={transitions[:4]}...{transitions[-3:]}",
                )
            )
            checks.append(
                ContractAuditCheck(
                    check_id=f"{play_type.value}_matchup_graph",
                    description=f"{play_type.value} includes matchup graph and snapshots.",
                    passed=len(result.artifact_bundle.pre_snap_plan.graph.edges) == 11 and len(result.artifact_bundle.matchup_snapshots) >= 2,
                    evidence=f"pre_edges={len(result.artifact_bundle.pre_snap_plan.graph.edges)} snapshots={len(result.artifact_bundle.matchup_snapshots)}",
                )
            )
            checks.append(
                ContractAuditCheck(
                    check_id=f"{play_type.value}_causality",
                    description=f"{play_type.value} terminal event has normalized causality chain.",
                    passed=bool(result.causality_chain.nodes) and abs(sum(n.weight for n in result.causality_chain.nodes) - 1.0) < 0.001,
                    evidence=f"terminal={result.causality_chain.terminal_event} nodes={len(result.causality_chain.nodes)}",
                )
            )
            checks.append(
                ContractAuditCheck(
                    check_id=f"{play_type.value}_penalty_rationale",
                    description=f"{play_type.value} penalties include explicit enforcement rationale when present.",
                    passed=all(bool(p.enforcement_rationale) for p in result.play_result.penalties),
                    evidence=f"penalty_count={len(result.play_result.penalties)}",
                )
            )
            checks.append(
                ContractAuditCheck(
                    check_id=f"{play_type.value}_contest_presence",
                    description=f"{play_type.value} emits contest evidence and rep artifacts.",
                    passed=bool(result.artifact_bundle.contest_resolutions) and bool(result.rep_ledger),
                    evidence=f"contests={len(result.artifact_bundle.contest_resolutions)} reps={len(result.rep_ledger)}",
                )
            )
            if play_type in {PlayType.PUNT, PlayType.KICKOFF, PlayType.FIELD_GOAL, PlayType.EXTRA_POINT}:
                families = {c.family for c in result.artifact_bundle.contest_resolutions}
                checks.append(
                    ContractAuditCheck(
                        check_id=f"{play_type.value}_special_teams_contests",
                        description=f"{play_type.value} traverses special-teams contest path.",
                        passed=bool({"kick_quality", "block_pressure"} & families),
                        evidence=f"families={sorted(families)}",
                    )
                )
            if play_type == PlayType.TWO_POINT:
                families = {c.family for c in result.artifact_bundle.contest_resolutions}
                checks.append(
                    ContractAuditCheck(
                        check_id="two_point_scrimmage_contests",
                        description="two_point traverses scrimmage conversion contest path.",
                        passed={"pressure_emergence", "separation_window", "decision_risk", "catch_point_contest", "ball_security"}.issubset(families) and "kick_quality" not in families,
                        evidence=f"families={sorted(families)}",
                    )
                )
        return checks

    def _check_mode_invariance(self) -> ContractAuditCheck:
        scp = self._build_context(PlayType.PASS)
        play = self._engine.run_mode_invariant(scp, SimMode.PLAY)
        sim = self._engine.run_mode_invariant(scp, SimMode.SIM)
        off = self._engine.run_mode_invariant(scp, SimMode.OFFSCREEN)
        aligned = (
            play.play_result.yards == sim.play_result.yards == off.play_result.yards
            and play.causality_chain.terminal_event == sim.causality_chain.terminal_event == off.causality_chain.terminal_event
        )
        evidence = {"play": asdict(play.play_result), "sim": asdict(sim.play_result), "off": asdict(off.play_result)}
        return ContractAuditCheck(
            check_id="mode_invariance",
            description="Play/Sim/Offscreen use identical football physics path for same seeded input.",
            passed=aligned,
            evidence=str(evidence),
        )

    def _build_context(self, play_type: PlayType) -> SnapContextPackage:
        offense_roles, defense_roles = self._roles_for_play_type(play_type)
        participants: list[ActorRef] = []
        for idx, role in enumerate(offense_roles):
            participants.append(ActorRef(actor_id=f"A_{idx}", team_id="A", role=role))
        for idx, role in enumerate(defense_roles):
            participants.append(ActorRef(actor_id=f"B_{idx}", team_id="B", role=role))
        states = {p.actor_id: InGameState(fatigue=0.2, acute_wear=0.15, confidence_tilt=0.0, discipline_risk=0.45) for p in participants}
        traits = {p.actor_id: {code: 50.0 for code in required_trait_codes()} for p in participants}
        personnel, formation, offense, defense = self._intent_for_play_type(play_type)
        return SnapContextPackage(
            game_id="AUDIT",
            play_id=f"AUDIT_{play_type.value}",
            mode=SimMode.SIM,
            situation=Situation(quarter=2, clock_seconds=700, down=2, distance=7, yard_line=55, possession_team_id="A", score_diff=0, timeouts_offense=3, timeouts_defense=3),
            participants=participants,
            in_game_states=states,
            trait_vectors=traits,
            intent=ParameterizedIntent(
                personnel=personnel,
                formation=formation,
                offensive_concept=offense,
                defensive_concept=defense,
                play_type=play_type,
            ),
            weather_flags=["clear"],
        )

    def _roles_for_play_type(self, play_type: PlayType) -> tuple[list[str], list[str]]:
        if play_type == PlayType.PUNT:
            return (
                ["P", "OL", "OL", "OL", "OL", "OL", "TE", "WR", "WR", "CB", "S"],
                ["DE", "DT", "DT", "DE", "LB", "LB", "LB", "CB", "CB", "S", "RB"],
            )
        if play_type == PlayType.KICKOFF:
            return (
                ["K", "LB", "LB", "LB", "CB", "CB", "S", "S", "DE", "DE", "WR"],
                ["RB", "WR", "WR", "WR", "TE", "LB", "LB", "CB", "S", "S", "DE"],
            )
        if play_type in {PlayType.FIELD_GOAL, PlayType.EXTRA_POINT}:
            return (
                ["K", "OL", "OL", "OL", "OL", "OL", "TE", "LB", "LB", "DE", "DE"],
                ["DE", "DE", "DT", "DT", "LB", "LB", "LB", "CB", "CB", "S", "S"],
            )
        return (
            ["QB", "RB", "WR", "WR", "WR", "TE", "OL", "OL", "OL", "OL", "OL"],
            ["DE", "DT", "DT", "DE", "LB", "LB", "LB", "CB", "CB", "S", "S"],
        )

    def _intent_for_play_type(self, play_type: PlayType) -> tuple[str, str, str, str]:
        if play_type == PlayType.RUN:
            return ("11", "singleback", "inside_zone", "base_over")
        if play_type == PlayType.PASS:
            return ("11", "gun_trips", "spacing", "cover3_match")
        if play_type == PlayType.PUNT:
            return ("punt", "punt_spread", "punt_safe", "punt_return_safe")
        if play_type == PlayType.KICKOFF:
            return ("kickoff", "kickoff_standard", "kickoff_sky", "kickoff_return")
        if play_type == PlayType.FIELD_GOAL:
            return ("field_goal", "field_goal_heavy", "field_goal_unit", "field_goal_block")
        if play_type == PlayType.EXTRA_POINT:
            return ("extra_point", "field_goal_heavy", "field_goal_unit", "field_goal_block")
        return ("two_point", "gun_trips", "two_point_mesh", "cover3_match")


def run_football_contract_audit() -> ContractAuditReport:
    return FootballContractAuditor().run()
