# NORTHSTAR_FOOTBALL.md

Last updated: 2026-02-28

## Purpose

This document is the authoritative 1.0 North Star for the Football Layer.

It defines:

- Required inputs to the play simulation engine
- Data quality and schema expectations
- Trait coverage requirements
- External resource model (personnel, formations, plays, coaching)
- Interactive engine contract (requests/responses/audit trail)

This must be complete and stable before full phasal-engine expansion.

## Current implementation status

Implemented now:

1. Pre-sim validation gate exists and is wired into runtime/session/snap paths.
2. Validation failures are immediate hard-fails with forensic artifact output.
3. Capability-driven trait catalog exists with schema/range/completeness validation.
4. Atomic trait persistence is in authoritative storage (`player_id`, `trait_code`).
5. Externalized resource libraries are loaded from versioned manifests with checksum and schema checks.
6. Unknown or incompatible resource IDs are blocking failures.

Next football-layer goals:

1. Integrate canonical atomic trait weighting into deeper phasal internals (without fallback/default rescue).
2. Increase explanatory depth of causality chains and shared responsibility across expanded outcome branches.
3. Extend resource catalogs (formations/concepts/policies) while keeping compatibility/version enforcement strict.

Trait-canon lock now approved:

1. Canon target is 77 atomic traits.
2. 72 traits are currently implemented and validated.
3. Five additional traits are locked for phasal retrofit:
- `zone_spacing`
- `throw_on_move`
- `eye_discipline`
- `open_field_tackle`
- `strip_ball_skill`
4. Trait status tags are locked:
- `core_now` = wired today.
- `reserved_phasal` = schema-required now, wired in next phasal passes.

## Hard Rules (Football Layer)

1. No fallback/default simulation paths.
2. No `OVR` fallback in calculations.
3. If any required input is missing/invalid, engine hard-fails and emits forensic artifact.
4. No hidden floors/ceilings/guardrails in runtime sim math.
5. All tuning is externalized and versioned (data-driven, not hardcoded rescue logic).
6. Play/Sim/Off-screen must call identical football physics/resolution contracts.
7. Runtime math may not rely on clamp/floor/ceiling rescue behavior; invalid domains hard-fail.
8. Active football resources may not carry placeholder/default-rescue semantics.

## Football Layer Boundary

Football Layer owns:

- Snap/game resolution
- In-game mutable state (score/clock/down-distance/possession/fatigue/in-game injuries)
- Rep ledger and causality outputs

Football Layer does not own:

- Contract/cap decisions
- Long-term progression math
- UI rendering decisions

## Pre-Phasal Build Gate

No deeper phasal engine implementation proceeds until all items are true:

1. Input schemas are versioned and validated.
2. Trait library is complete and mapped to positions/archetypes.
3. Formation/personnel/play/coaching libraries exist as external data resources.
4. Interactive playcall contract is locked.
5. Validation harness blocks bad data before simulation start.

## Required Inputs to Simulation

## 1) Game Context Input

Required fields:

- game_id, season, week, phase
- quarter, clock_seconds, overtime flags
- score_home, score_away
- possession team, down, distance, yard_line
- timeout counts per team
- weather/environment flags

## 2) On-Field Participants Input

Required fields:

- 22 active participants (11 offense, 11 defense)
- player_id, team_id, role, alignment slot
- eligibility flags where relevant

Invalid participant state (hard-fail examples):

- fewer/more than 22 active players
- duplicate player in multiple slots
- missing required position/slot constraints

## 3) In-Game State Input

Required per active participant:

- fatigue
- acute wear
- injury limitation tag
- discipline risk
- confidence/tilt state

## 4) Intent Input (Parameterized, no actor puppeteering)

Required fields:

- personnel group
- formation identifier
- offensive concept
- defensive concept
- strategic posture (tempo/aggression/conservatism etc.)
- play type

## 5) Randomness Input

Required:

- injected random source handle
- substream spawn ID policy for deterministic replay/testing

## Trait Taxonomy (1.0 Baseline)

All players have the full atomic trait vector. Position/archetype determines weight usage, not trait existence.

### Trait catalog policy: capability-driven, not count-driven

Trait canon lock:

1. Target catalog size is 77 atomic traits.
2. Current active implementation is 72 traits.
3. Approved additions (reserved_phasal until wired):
- `zone_spacing`
- `throw_on_move`
- `eye_discipline`
- `open_field_tackle`
- `strip_ball_skill`

Coverage breakdown:

1. Athletic base (7)
- strength, burst, top_speed, acceleration, agility, balance, stamina

2. Movement control (4)
- body_control, leverage_control, momentum_management, pursuit_angles

3. Cognition and discipline (9)
- awareness, processing_speed, recognition, anticipation, discipline, decision_quality, communication, communication_secondary, composure

4. QB ball placement and timing (8)
- short_accuracy, intermediate_accuracy, deep_accuracy, throw_power, throw_touch, release_quickness, pocket_sense, timing_precision

5. Ball and receiving (8)
- hands, catch_radius, contested_catch, ball_tracking, route_fidelity, release_quality, yac_vision, ball_security

6. Blocking and protection (8)
- pass_set, hand_placement, mirror_skill, anchor, recovery_blocking, run_block_drive, run_block_positioning, combo_coordination

7. Front-seven pressure/run defense (10)
- get_off, hand_fighting, rush_plan_diversity, edge_contain, block_shed, gap_integrity, stack_shed, closing_speed, tackle_power, tackle_form

8. Coverage (8)
- man_footwork, route_match_skill, leverage_management, transition_speed, ball_skills_defense, press_technique, recovery_speed, dpi_risk_control

9. Special teams execution (3)
- kick_power, kick_accuracy, hang_time_control

10. Availability and volatility (7)
- soft_tissue_risk, contact_injury_risk, re_injury_risk, durability, pain_tolerance, recovery_rate, volatility_profile

Locked additions for phasal integration (5):

1. Coverage spacing and zone integrity
- `zone_spacing`

2. QB movement-platform throwing quality
- `throw_on_move`

3. QB manipulation/discipline under disguise
- `eye_discipline`

4. Space tackling quality
- `open_field_tackle`

5. Forced-fumble skill expression
- `strip_ball_skill`

Notes:

- No single synthetic `OVR` may replace atomic trait usage in football calculations.
- Derived metrics are allowed for analytics and UI, but cannot replace missing atomic inputs.
- Trait status tags are required on each catalog entry (`core_now` or `reserved_phasal`).
- No trait add/remove/rename without explicit catalog version bump and migration notes.
- Overlap constraints are explicit and audited:
- `burst` != `acceleration`
- `balance` != `body_control` != `momentum_management`
- `throw_power` != `throw_touch` != `deep_accuracy`
- `communication` != `communication_secondary`

## External Resource Libraries (Not Hardcoded in Resolver)

Football engine must consume external versioned resources for:

1. Personnel packages
- identifiers (e.g., 11, 12, 21, nickel, dime)
- allowed slot compositions

2. Formations
- alignment slots and geometry definitions
- motion/shift allowances

3. Offensive concepts
- route trees, run schemes, protection families
- assignment templates by slot

4. Defensive concepts
- fronts, pressure packages, coverage families, disguise intent
- assignment templates by slot

5. Coaching decision policies
- down-distance tendency maps
- aggression/tempo/posture presets
- timeout/challenge/clock strategy policies

All resources are loaded via schema-validated data packs, with strict version IDs.

## Data Quality Requirements

Each simulation run must pass quality checks before first snap.

### Required quality dimensions

1. Completeness: all required fields present
2. Validity: value domains/ranges respected
3. Referential integrity: IDs resolve across datasets
4. Consistency: no conflicting assignments/duplicate active roles
5. Version compatibility: schema/resource versions compatible with runtime

### Hard-fail examples

- Play references formation not in active formation catalog
- Depth chart slot references unknown player
- Player missing required trait fields
- Trait value outside allowed schema range
- Unknown coaching policy key

## Data Formatting and Schema Guidelines

Primary formats:

- JSON for structured catalogs (formations, concepts, coaching policies, trait metadata)
- CSV allowed for bulk entity tables (players, depth charts, tuning tables)

Required standards:

- UTF-8 encoding
- explicit schema_version in every top-level data file
- stable primary keys
- explicit enums for constrained fields
- no implicit defaults for required fields

Example top-level metadata contract (required in each resource pack):

- resource_type
- schema_version
- resource_version
- generated_at
- checksum

## Interactive Engine Contract (UI/AI -> Football)

UI/AI sends `PlaycallRequest` only with parameterized controls:

- personnel
- formation
- offensive/defensive concepts
- posture knobs
- play type

UI/AI never sends direct actor movement commands.

Engine returns per snap:

- official play result
- rep ledger entries
- causality chain
- integrity warnings/errors (if any)

Engine returns per game:

- final game session state
- action stream (for replay)
- retained/deep-log status metadata

## Audit and Forensics

Every hard-fail must emit:

- error_code
- scope
- message
- identifiers (game/play/team/request)
- state snapshot excerpt
- causal fragment

Conditioned dev-mode outcomes must be explicitly stamped in ledger/events.

## Football Data Validation Checklist (Pre-Sim)

1. Schema versions resolved and supported.
2. Player trait vectors complete (all required canonical traits present).
3. Depth charts valid for both teams and required slots.
4. Personnel package allowed by roster/depth chart.
5. Formation exists and matches personnel constraints.
6. Offensive/defensive concepts exist and are compatible with formation.
7. Coaching decision profile exists for both teams.
8. All IDs resolve with no orphan references.
9. Random source initialized and substream policy available.
10. No missing required fields in runtime SCP payload.

Failing any checklist item aborts simulation start.

## Build Order After This Document

1. Implement schema validators and pre-sim gate enforcement.
2. Implement external resource loaders and version compatibility checks.
3. Implement trait ingestion pipeline and completeness checks.
4. Expand phasal engine internals only after inputs/resources are guaranteed valid.
5. Add regression fixtures for malformed data to prove hard-fail behavior.

## Build order status

1. Completed: schema validators and pre-sim gate enforcement.
2. Completed: external resource loaders and compatibility checks.
3. Completed: trait ingestion, atomic storage, and completeness checks.
4. In progress: deeper phasal engine internals.
5. Completed: malformed-data regression fixtures for hard-fail behavior.

## Success Criteria for Football Layer 1.0 Readiness

1. No fallback/default paths triggered in normal execution.
2. Any invalid input state is rejected before or at resolution with forensic artifact.
3. All modes (play/sim/off-screen) produce consistent statistical physics under same inputs.
4. Rep and causality outputs remain derivable and auditable for retained games.
5. Input/data model is stable enough that tuning changes do not require resolver rewrites.
