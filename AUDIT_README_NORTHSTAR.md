# Audit: Codebase vs README + North Star Documents

Date: 2026-02-28

## Update: Zero-Rescue hardening pass

This audit file predates the zero-rescue hardening pass now in code. Current implementation updates include:

- Dev tuning/calibration isolation behind a devtools gateway loaded only in `dev_mode`.
- Strict constructor injection for runtime-critical football/session/validation paths (no rescue construction fallbacks).
- Runtime clamp/floor/ceiling removal in football/session/org/perception simulation paths with domain guards and hard-fail behavior.
- Placeholder/default-rescue semantic removal from active rules/policy resources (`nfl_standard_v1`, `balanced_base`).
- New strict audit service and runtime action (`RUN_STRICT_AUDIT`) with CI test coverage.

Notes in this document that reference a 90-trait baseline are stale; current canonical target is 77 with current implementation lock documented in North Star files.

## Scope and method

This audit compares the current implementation against:

- `README.md` (current status + near-term goals)
- `NORTHSTAR.md` (project-wide 1.0 charter)
- `NORTHSTAR_FOOTBALL.md` (football-layer contract)

I used static review of the runtime, football, org, persistence, and UI modules plus the test suite.

## Executive summary

The repository is **strongly aligned with the 1.0 foundation goals** around strict validation, hard-fail integrity behavior, single resolver path, external resources, trait contracts, dual-store persistence, retention policy, and replay/determinism harness support.

The main gaps are exactly the ones already called out as in-progress in the docs: deeper trait-weighted football internals, broader phasal fidelity, and richer UI/planning surfaces. Narrative generation remains out of scope for 1.0 and is not implemented.

## Alignment matrix

### Implemented and consistent with stated goals

1. **Single football resolver path across modes**
   - Runtime routes user-play and simulation through the same football/session engines.
   - `run_mode_invariant` is used by session execution and is tested for Play/Sim/Off-screen invariance.

2. **Strict pre-sim gate + hard-fail + forensic artifacts**
   - Pre-sim validation checks game/session identity, roster/depth integrity, traits, playcall/resource compatibility, and snap context.
   - Runtime hard-stops on integrity failures and persists forensic artifacts.
   - Tests cover missing formation/depth slots/traits/out-of-range traits and hard-stop behavior.

3. **Externalized, versioned football resources with compatibility checks**
   - Resource bundles (personnel/formations/concepts/policies) are loaded from data files.
   - Manifest type/schema/checksum validation is enforced.
   - Cross-resource referential integrity is validated and blocking on failure.

4. **Canonical atomic trait contract enforced in runtime paths**
   - Trait catalog + required trait completeness checks are applied in pre-sim validation.
   - Tests assert active players have complete 90-trait vectors.

5. **Dual-store persistence and export pipeline present**
   - SQLite authoritative store and DuckDB analytics ETL are wired in runtime.
   - CSV/Parquet export parity is tested.

6. **Retention policy behavior aligns with docs**
   - Retained games keep deep rep data; non-retained games purge deep logs after derivation.
   - Tests validate retained vs non-retained behavior.

7. **Organizational layer baseline is in place**
   - Ownership pressure, roster/cap constraints, draft/FA/trade windows, development, standings, and transactions exist.
   - Perceived-data-oriented ranking is tested.

8. **Technical/tooling baseline is healthy**
   - Test, lint, and type-check commands pass (`pytest`, `ruff`, `mypy`).

### Partially implemented / in progress

1. **Trait weighting depth in football math (known in-progress item)**
   - The resolver currently relies heavily on random-range logic by play type.
   - Atomic traits are validated for completeness but are not yet deeply integrated into snap outcome math.

2. **Phasal causality depth and outcome branch richness**
   - Phases and multi-actor reps exist, but outcome modeling remains relatively coarse compared to the full North Star ambition (broader shared-responsibility and contextual branches).

3. **UI scope vs required 1.0 screens**
   - UI currently has broad tabs (org/game/history/film/analytics), but the North Star's detailed management surfaces (contracts/cap/scouting/draft/FA/trades/coaching/staff and ownership pressure screens) are represented at a higher-level summary rather than dedicated deep workflows.

### Not yet implemented (or intentionally out of 1.0 scope)

1. **Narrative Engine 2.0**
   - Narrative event emission exists, but no dedicated narrative interpretation engine is implemented (expected for 2.0 per docs).

## Risk notes

1. **Most material execution risk**: football outcome fidelity could lag design intent if deeper trait usage remains shallow for too long.
2. **Product risk**: UI may bottleneck adoption/testing of org systems if deeper management flows are delayed.
3. **Positive risk posture**: integrity guarantees are already strong and reduce long-term simulation debt.

## Recommended next milestones (ordered)

1. **Trait-weighted resolver pass**
   - Introduce explicit trait-to-phase influence maps and phase-specific weighted contests.
   - Add tests proving trait sensitivity (same seed + altered traits => shifted distributions).

2. **Phasal branch expansion with auditability constraints**
   - Expand causality node taxonomies and shared-responsibility evidence links while preserving derivability.

3. **UI deepening against existing contracts (no new sim logic in UI)**
   - Add dedicated org subviews for contracts/cap, scouting confidence surfaces, draft board, and transaction tooling.

4. **Quality-gate expansion**
   - Add explicit tests for perception-separation boundaries in additional org flows and season rollover transactionality.

## Bottom line

The codebase is **on-charter for the current 1.0 vertical-slice foundation** and meaningfully conforms to README/North Star commitments already marked as "implemented now." The principal deltas are **depth and breadth expansions**, not architectural misalignment.


## Direct answer: schedule/league/rules modularity

Short answer: **partially modular today, but not yet modular enough for broad format/rule variability without code changes**.

Why it is modular already:

- Football intent/resources are externalized through schema-validated resource packs (`personnel`, `formations`, `concepts`, `coaching_policies`) and strict ID resolution, which is a good base for evolving tactical content without resolver rewrites.
- `GameSessionEngine` accepts injected validator/resource resolver and a `playcall_provider`, which provides useful extension points for alternate control policies.

Where it is currently hard-coded (limits future format/rule flexibility):

- Runtime bootstraps a fixed 8-team default league and fixed 18-week scheduling in the core runtime flow.
- Schedule generation assumes even-team round-robin construction and a single `weeks` integer rather than pluggable league-format templates (e.g., conferences/divisions, byes, unbalanced schedules).
- Depth-chart and on-field slot requirements are static constants in validator/session code, so major roster/ruleset variants require code edits.
- Overtime and quarter/clock behavior are hard-coded in session state transitions (e.g., quarter 5 tie check + 600-second OT), rather than policy-driven rulesets.
- Special-team and scoring behaviors (kickoff/punt/XP/2pt/FG probabilities and effects) are currently encoded in resolver logic, not a versioned rules module.

Practical conclusion:

- The architecture is **good for content modularity** (plays/personnel/formations/policies) and integrity enforcement.
- It is **not yet fully modular for competition-format and ruleset modularity** (league topology, schedule templates, overtime variants, kickoff era changes).
- To reach that target, introduce first-class `LeagueFormat` + `Ruleset` contracts and inject them into runtime/scheduler/session/resolver paths the same way resource packs are injected today.
