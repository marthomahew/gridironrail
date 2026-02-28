from __future__ import annotations

from collections import Counter

from grs.contracts import ActorRef, AssignmentTemplate, MatchupEdge, MatchupGraph, PreSnapMatchupPlan
from grs.core import make_id


class MatchupCompileError(ValueError):
    pass


class MatchupCompiler:
    ROLE_ALIASES: dict[str, tuple[str, ...]] = {
        "DE": ("DE", "DL"),
        "DT": ("DT", "DL"),
    }

    ROLE_COMPATIBILITY: dict[str, list[str]] = {
        "QB": ["S", "LB", "DE", "DT", "CB"],
        "RB": ["LB", "S", "CB", "DE", "DT"],
        "WR": ["CB", "S", "LB"],
        "TE": ["LB", "S", "DE", "CB"],
        "OL": ["DE", "DT", "LB"],
        "K": ["DE", "LB", "CB", "S", "RB"],
        "P": ["DE", "LB", "CB", "S", "RB"],
        "LB": ["LB", "S", "CB", "DE", "DT"],
        "DE": ["DE", "DT", "LB", "S"],
        "DT": ["DT", "DE", "LB", "S"],
        "CB": ["CB", "S", "LB", "WR"],
        "S": ["S", "CB", "LB", "WR", "TE"],
    }

    def compile(
        self,
        *,
        play_id: str,
        playbook_entry_id: str,
        assignment_template: AssignmentTemplate,
        offense_team_id: str,
        defense_team_id: str,
        participants: list[ActorRef],
    ) -> PreSnapMatchupPlan:
        offense = sorted(
            [p for p in participants if p.team_id == offense_team_id],
            key=lambda p: (p.role, p.actor_id),
        )
        defense = sorted(
            [p for p in participants if p.team_id == defense_team_id],
            key=lambda p: (p.role, p.actor_id),
        )
        if len(offense) != 11 or len(defense) != 11:
            raise MatchupCompileError("matchup compile requires 11v11 participants")

        self._validate_role_counts(offense, assignment_template.offense_roles, "offense")
        self._validate_role_counts(defense, assignment_template.defense_roles, "defense")

        off_used: set[str] = set()
        def_used: set[str] = set()
        edges: list[MatchupEdge] = []

        # Apply declared pairing hints first when candidates are available.
        for hint_idx, hint in enumerate(assignment_template.pairing_hints, start=1):
            off_role = str(hint.get("offense_role", ""))
            def_role = str(hint.get("defense_role", ""))
            technique = str(hint.get("technique", assignment_template.default_technique))
            off_actor = self._pick_by_role(offense, off_used, off_role)
            def_actor = self._pick_by_role(defense, def_used, def_role)
            if off_actor is None or def_actor is None:
                continue
            off_used.add(off_actor.actor_id)
            def_used.add(def_actor.actor_id)
            edges.append(
                MatchupEdge(
                    edge_id=make_id("edge"),
                    offense_actor_id=off_actor.actor_id,
                    defense_actor_id=def_actor.actor_id,
                    offense_role=off_actor.role,
                    defense_role=def_actor.role,
                    technique=technique,
                    leverage="neutral",
                    responsibility_weight=0.0,
                    context_tags=[
                        "primary",
                        f"hint:{hint_idx}",
                        f"off_role:{off_actor.role}",
                        f"def_role:{def_actor.role}",
                    ],
                )
            )

        for off_actor in offense:
            if off_actor.actor_id in off_used:
                continue
            def_actor = self._pick_compatible_defender(off_actor, defense, def_used)
            if def_actor is None:
                raise MatchupCompileError(f"unable to pair offense actor '{off_actor.actor_id}' ({off_actor.role})")
            off_used.add(off_actor.actor_id)
            def_used.add(def_actor.actor_id)
            edges.append(
                MatchupEdge(
                    edge_id=make_id("edge"),
                    offense_actor_id=off_actor.actor_id,
                    defense_actor_id=def_actor.actor_id,
                    offense_role=off_actor.role,
                    defense_role=def_actor.role,
                    technique=assignment_template.default_technique,
                    leverage="neutral",
                    responsibility_weight=0.0,
                    context_tags=[
                        "primary",
                        f"off_role:{off_actor.role}",
                        f"def_role:{def_actor.role}",
                    ],
                )
            )

        if len(edges) != 11:
            raise MatchupCompileError(f"expected 11 matchup edges, got {len(edges)}")

        self._tag_groups(edges)
        share = round(1.0 / len(edges), 6)
        for edge in edges:
            edge.responsibility_weight = share
        edges[0].responsibility_weight = round(edges[0].responsibility_weight + (1.0 - sum(e.responsibility_weight for e in edges)), 6)

        plan = PreSnapMatchupPlan(
            plan_id=make_id("plan"),
            play_id=play_id,
            playbook_entry_id=playbook_entry_id,
            assignment_template_id=assignment_template.template_id,
            offense_team_id=offense_team_id,
            defense_team_id=defense_team_id,
            graph=MatchupGraph(
                graph_id=make_id("graph"),
                play_id=play_id,
                phase="pre_snap_compile",
                edges=edges,
            ),
            warnings=[],
        )
        return plan

    def _validate_role_counts(self, actors: list[ActorRef], required_roles: list[str], side: str) -> None:
        pool = Counter(actor.role for actor in actors)
        for required_role in required_roles:
            chosen = self._take_from_pool(pool, required_role)
            if chosen is None:
                have = sum(pool.get(r, 0) for r in self._candidate_roles(required_role))
                raise MatchupCompileError(f"{side} role '{required_role}' requires 1, have {have}")

    def _pick_by_role(self, actors: list[ActorRef], used: set[str], role: str) -> ActorRef | None:
        candidates = self._candidate_roles(role)
        for candidate_role in candidates:
            for actor in actors:
                if actor.actor_id in used:
                    continue
                if actor.role == candidate_role:
                    return actor
        return None

    def _pick_compatible_defender(self, off_actor: ActorRef, defenders: list[ActorRef], used: set[str]) -> ActorRef | None:
        preferred = self.ROLE_COMPATIBILITY.get(off_actor.role, [])
        for role in preferred:
            candidate = self._pick_by_role(defenders, used, role)
            if candidate is not None:
                return candidate
        for candidate in defenders:
            if candidate.actor_id not in used:
                return candidate
        return None

    def _tag_groups(self, edges: list[MatchupEdge]) -> None:
        def add_group(group_id: str, selected: list[MatchupEdge]) -> None:
            if len(selected) < 2:
                return
            for edge in selected:
                if group_id not in edge.context_tags:
                    edge.context_tags.append(group_id)

        ol_edges = [e for e in edges if e.offense_role == "OL" and e.defense_role in {"DE", "DT", "DL", "LB"}]
        add_group("group:double_team:1", ol_edges[:2])

        bracket_edges = [e for e in edges if e.offense_role in {"WR", "TE", "RB"} and e.defense_role in {"CB", "S", "LB"}]
        add_group("group:bracket:1", bracket_edges[:2])

        chip_primary = [e for e in edges if e.offense_role in {"TE", "RB"}]
        if chip_primary and ol_edges:
            add_group("group:chip_release:1", [chip_primary[0], ol_edges[0]])

        stunt_edges = [e for e in edges if e.defense_role in {"DE", "DT", "DL"} and e.offense_role == "OL"]
        add_group("group:stunt_exchange:1", stunt_edges[:2])

    def _candidate_roles(self, role: str) -> list[str]:
        aliases = self.ROLE_ALIASES.get(role)
        if aliases is None:
            return [role]
        return list(dict.fromkeys(aliases))

    def _take_from_pool(self, pool: Counter[str], required_role: str) -> str | None:
        for candidate_role in self._candidate_roles(required_role):
            if pool.get(candidate_role, 0) > 0:
                pool[candidate_role] -= 1
                return candidate_role
        return None
