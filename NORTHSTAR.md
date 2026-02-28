# Gridiron Rail: Sundays (GR:S) - North Star + Implementation Charter

Last updated: 2026-02-28

## One-sentence definition

GR:S is a data-first pro football dynasty simulation where on-field outcomes emerge from phasal, multi-actor rep interactions and off-field outcomes emerge from ownership-driven organizational pressure, with no soft fails, no outcome smoothing, and deep PFF-style accountability.

## Product targets

### 1.0 (MVP)

Ship everything except narrative generation:

- Football Layer (phasal + multi-actor matchups, rep ledger, causality)
- Organizational Layer (ownership + roster/cap/draft/FA/trades/coaching/dev/injuries)
- UI Glue Layer (Qt/PySide6 desktop, Windows/macOS)
- Stats + exports (derivable, analysis-ready)
- Modding-first data model (fictional default + realism mod support)
- Non-deterministic gameplay (different believable outcomes on re-run)
- Seeded determinism only for tests/dev harnesses

### 2.0

Add Narrative Engine as an interpretation layer that consumes events from 1.0 systems.

## 1.0 status snapshot (current implementation)

Completed and enforced now:

1. Single football resolver path for Play/Sim/Off-screen.
2. Strict pre-sim validation gate on football simulation entrypoints.
3. Immediate hard-fail enforcement (no warning phase) with forensic artifacts.
4. Externalized and versioned football resources (personnel/formations/concepts/coaching policies) with checksum + schema checks.
5. Canonical capability-driven trait contract at schema/storage/validation level (no arbitrary fixed count).
6. Atomic trait persistence keyed by (`player_id`, `trait_code`) in authoritative SQLite.
7. Validation audit persistence for simulation readiness runs.
8. CI coverage for strict pre-sim rejection scenarios.
9. Org-first franchise setup baseline:
- profile picker flow (`create/load/delete`) and profile-scoped save roots
- configurable league topology + schedule policy + cap/roster policy + talent profile
- owner/gm/coach mode matrix with explicit capability overrides and audit events
10. Runtime startup no longer auto-bootstraps a fixed league; simulation paths require loaded/created franchise profile.

Still in-progress toward full North Star:

1. Deeper phasal weighting integration using the canonical atomic trait catalog.
2. Broader football outcome fidelity expansion while preserving strict contracts and no-fallback behavior.
3. Continued UI functionalization and planning surfaces against the same runtime contracts.
4. Narrative generation itself (2.0 only), while keeping event emission active in 1.0.

## Global non-negotiables

1. No guardrails: no soft failures, silent fallbacks, parity stabilizers, rubber-banding, or score smoothing.
2. No dual sim engines: Play/Sim/Off-screen all use one football resolver path; only presentation changes.
3. Player is not a puppeteer: only parameter control (playcall, personnel, formation, posture, timeouts, etc.).
4. Perception separation is mandatory: AI/player decisions use perceived data, not ground truth.
5. Hard-stop on integrity failures: stop immediately and emit forensic artifact.
6. Outcome-focused specs: define outputs and guarantees, not rigid implementation scripts.
7. Modular boundaries by contract: systems interact through explicit interfaces only.
8. No fallback rating defaults in simulation: no `OVR` defaulting and no substitute-path calculations when required inputs are missing/invalid.
9. Missing/invalid calculation inputs are integrity failures: fail hard and emit forensic artifact; never continue with guessed values.
10. Tuning must be modular and externalized: constants/rates/weights are data-driven and editable without introducing hidden guardrails/floors/ceilings in resolver code.
11. Strict execution policy applies repo-wide:
- no runtime rescue defaults
- no active placeholder resources
- no runtime clamp/floor/ceiling rescue logic
- hard-fail + forensic artifact on contract/domain violations
12. Allowed defaults carveout:
- dataclass `default_factory` container initialization
- explicit test fixture setup in test-only code

## Difficulty philosophy

Allowed (global rates/variance/information quality only):

- Volatility width
- Injury frequency/severity
- Aging predictability variance
- Upset tail frequency
- Scouting noise/confidence width
- Negotiation friction
- Ownership pressure patience

Not allowed:

- Per-team resolution cheats
- Comeback boosts
- Post-hoc balancing
- User-team-only physics differences

## Persistence and retention

Always persist:

- Season/career/team/player aggregates
- Standings, awards, records
- Transactions, cap history, contracts

Conditional deep retention:

- Keep full play/rep/causality logs for retained games (playoffs/championships/instant classics; optional rivalry/record games)
- Non-retained games: derive summaries first, then purge deep logs

## Football layer north-star outcomes

Each snap must produce:

1. Official play result (yards/turnover/score/penalty/clock/next state)
2. Rep ledger entries (PFF-depth accountability)
3. Causality chain (machine-traceable "why")

Required properties:

- Parameterized intent -> plausible bounded outcomes
- Multi-actor support (double teams, brackets, chip-release, stunts, pursuit convergence)
- Phasal outcome structure (pre-snap -> early leverage -> engagement -> decision -> terminal -> aftermath)
- Shared responsibility weights and evidence handles
- Turnovers, penalties, and injuries attributable to represented contexts
- Mode invariance across Play/Sim/Off-screen

### Snap context package (SCP)

Must include:

- Situation (score/down-distance/field/clock/timeouts/phase)
- Participants (valid 22-man field state)
- In-game states (fatigue/wear/injury limitations/discipline risk)
- Parameterized intent (personnel/formation/concepts/posture)
- Random source handle

### Optional dev/casual feature

Force Outcome (dev/casual mode only):

- Re-sim same pre-snap context until target outcome or max attempts
- On failure, explicit fail state
- Full audit stamp (conditioned/forced)

## Organizational layer north-star outcomes

Must produce a credible multi-season ecosystem with:

- League calendar and phase gates (regular/postseason/offseason)
- Ownership as first-class pressure actor
- Roster construction under cap and roster constraints
- Contracts/dead money/restructures/extensions
- Draft/FA/trades under uncertainty and imperfect information
- Coaching/staff effects on evaluation/development/discipline
- Player lifecycle (development/peak/decline/retirement)
- Long-term history archives

Hard limits:

- No outcome overrides after games finalize
- No privileged truth access for AI decisions
- Same rules for user and AI teams

## Required entity model (1.0)

League/competition:

- League, Conference, Division, Season, Week, Schedule, Game, Play/Drive refs, Award, RecordBook

Franchise/org:

- Team/Franchise, Owner, Front Office, Coaches, Scouts, Medical

People:

- Player, Prospect (Agent/Referee optional in 1.0)

Assets/finance:

- Contract, ContractYear, bonus/guarantee, cap ledger entries, draft picks, trade records

State/events:

- Injury records, transaction records, coaching changes, scouting/evaluation snapshots, identity profile

## Real vs perceived data rule

Ground truth (hidden):

- True attributes, dev curves, volatility, injury susceptibility, fit multipliers

Perceived (decision surface):

- Scouting/coaching/medical reports + confidence bands
- Performance samples and derived analytics
- Minimal rumors/signals in 1.0

AI and user decisions must consume perceived artifacts only.

## UI glue layer (PySide6 Qt Widgets)

Role:

- Submit action requests
- Render perceived views and derived analytics
- Drill down season -> game -> play -> rep

Hard limits:

- UI never resolves simulation outcomes
- UI never mutates authoritative state directly
- UI never sees ground truth except explicit debug/dev mode

Required 1.0 screens:

- Org: roster, depth chart, contracts/cap, scouting, draft, FA, trades, coaching/staff, ownership pressure
- Game: play parameters, drive/game summary, diagnostics
- Film room (retained games): play list -> rep ledger -> causality
- History: standings/archives

## Statistics vision

Core rule:

- Stats are derivable from retained atomic snap/rep records and non-retained summaries; no synthetic orphan stats.

Must support:

- Traditional box stats
- Efficiency/situational splits
- Trench, coverage, QB decision, run-fit outcomes
- Shared responsibility grading rollups

## Narrative 2.0 future-proofing (required in 1.0)

Every subsystem emits normalized Narrative Event records even if unused in 1.0:

- event_id, time, scope, type, actors, claims, evidence_handles, severity, confidentiality_tier

Narrative in 2.0 changes perception/pressure, never football physics.

## Technical constraints and implementation guardrails

- Python baseline target: 3.12+ (current dev environment may be newer)
- Qt binding policy: PySide6 only (no PyQt/PySide2 mixed bindings)
- Packaging-safe practices: avoid runtime writes into install tree and avoid dynamic import sprawl
- Charting: Matplotlib via Qt backend behind adapter (swap-friendly)
- Storage strategy: dual-store (SQLite authoritative + DuckDB analytics)
- RNG correctness: injected random source with substream spawning per simulation context
- Modding 1.0: data-pack only (schema-validated JSON/CSV), no arbitrary Python plugin execution
- Scope 1.0: single-player local franchise
- Tuning rule: tuning values live in versioned data resources; gameplay math cannot silently clamp to rescue invalid model states.
- Failure rule: engine code must not \"default and continue\" when contract-required inputs are absent.

## Reliability and integrity requirements

- Engine integrity failures must hard-stop and persist forensic artifact snapshot
- Season rollover integrity checks are transactional
- No silent fallback when constraints are violated (cap/roster/depth chart/state integrity)
- No silent fallback when football/org calculation inputs are incomplete; hard-stop with explicit error code and context

## Locked sequencing for upcoming implementation

1. Keep strict pre-sim gate as non-bypassable for all sim-triggering paths.
2. Expand trait usage inside resolver/session internals without introducing any rescue defaults.
3. Grow external resource depth (formations/concepts/policies) through schema-versioned data, never hardcoded fallback branches.
4. Add deeper phasal causality coverage only after all required input contracts stay green at runtime.

## Trait Canon Lock (Approved)

This project now treats trait canon as locked by football capability, not arbitrary count targets.

1. Canon target is 77 atomic traits.
2. Current implementation has 72 active canonical traits.
3. Five approved additions are locked for phasal retrofit:
- `zone_spacing`
- `throw_on_move`
- `eye_discipline`
- `open_field_tackle`
- `strip_ball_skill`
4. Trait status tags are mandatory:
- `core_now`: actively wired in resolver/session contests.
- `reserved_phasal`: required in schema and validation now; wiring lands in phasal integration.
5. No removals/additions/renames without trait catalog version bump.
6. All required traits remain hard-fail validated; no fallback/default rescue paths.
7. Trait overlap rules stay explicit:
- `burst` vs `acceleration`
- `balance` vs `body_control` vs `momentum_management`
- `throw_power` vs `throw_touch` vs depth accuracy traits
- `communication` vs `communication_secondary`

## Schedule/League/Rules Modularity Plan (added)

Current state: football content resources are modular (personnel/formations/concepts/policies), but competition format and in-game rules are still partly hardcoded.

Goal: make schedule formats, league topology, and gameplay rulesets (OT, kickoff eras, etc.) pluggable by contract without creating dual simulation engines.

### New contracts to add

1. `LeagueFormat`
   - Defines conference/division topology, team-count constraints, phase calendar, and schedule template strategy.
   - Supports multiple schedule policies (balanced round-robin, division-heavy, custom historical templates).

2. `Ruleset`
   - Defines quarter length, overtime flow, kickoff touchback/placement behavior, PAT/2PT policy, and other officiating/scoring constraints.
   - Versioned and data-backed, with strict compatibility checks and no silent defaults.

3. `SeasonPlan`
   - Materialized output for one season: phase gates, week map, and schedule artifacts generated by a chosen `LeagueFormat`.

### Integration boundaries

- Runtime chooses a `LeagueFormat` + `Ruleset` at league initialization and persists their IDs/version metadata with season state.
- Scheduler consumes `LeagueFormat` only; it must not encode football rules.
- Session/resolver consume `Ruleset` only; they must not encode league topology assumptions.
- Validation gates enforce contract completeness/version compatibility before season start and before each game session.

### Persistence and migration rules

- Every season stores immutable `league_format_id`, `league_format_version`, `ruleset_id`, and `ruleset_version`.
- Save headers store active format/ruleset identifiers so reload/replay always resolve the exact contract versions used at creation time.
- Migration policy is forward-only with explicit migration IDs; no silent in-place mutation of historical season artifacts.
- Loading a save with missing or incompatible format/ruleset versions is a hard-fail with forensic artifact output.
- Rule changes apply at explicit season boundaries only unless a rule explicitly declares a safe mid-season effective window.
- Exports and forensic artifacts must include format/ruleset identifiers for auditability.

### Sequenced rollout

1. Extract current hardcoded schedule parameters into default `LeagueFormat` implementation that preserves existing behavior.
2. Extract overtime/clock/special-teams/scoring constants into default `Ruleset` implementation that preserves existing behavior.
3. Add persistence + forensic context for format/ruleset identifiers on each game/session artifact.
4. Add regression fixtures proving behavior parity for default contracts, then add at least one alternate format and one alternate ruleset fixture.
5. Expose read-only format/ruleset metadata in UI and exports before adding user-selectable presets.

### Non-negotiables for this plan

- No hidden rescue defaults when format/rules fields are missing.
- No mode-specific physics divergence (Play/Sim/Off-screen invariance remains mandatory).
- No direct UI mutation of authoritative rules/runtime state.

## Testing and quality gates

Required validation areas:

- Determinism harness (seeded reproducibility)
- Non-deterministic gameplay distributions in normal mode
- Mode invariance (Play/Sim/Off-screen)
- Retention behavior correctness (kept vs purged deep logs)
- Perception separation enforcement
- Cap/roster constraint enforcement
- Replay determinism from seed + action stream
- Export parity (CSV/Parquet row consistency)

Tooling baseline:

- `pytest`
- `ruff`
- `mypy`
- CI smoke run of CLI flow

## Working intent for all future iterations

- Keep advancing toward a credible dynasty loop first (vertical slices over speculative breadth)
- Preserve contract boundaries and auditability over quick hacks
- Prefer explicit failure with forensic context over hidden recovery
- Add complexity only when it increases explanatory power, accountability, and derivability

## Pre-Phasal Engine Gate (Mandatory)

Before full phasal engine implementation continues, the team must lock and review:

1. Football input contract completeness
2. Trait taxonomy and coverage requirements
3. Externalized resource libraries (formations/personnel/play concepts/coaching decisions)
4. Data quality and schema validation rules
5. Interactive command/response contract for play sim

The authoritative source for this gate is `NORTHSTAR_FOOTBALL.md`.
