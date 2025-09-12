# Package it.uniud.newbestsub.problem

Search space, objective evaluation, and genetic operators used by **NSGA-II** for the “best subset” optimization.

## Responsibilities
- Define the **problem** over a binary mask of topics and produce a 2-objective vector `(K, objectiveCorr)`.
- Keep the **solution** representation efficient and stream-friendly (bitset + compact serializers).
- Provide **operators** (mutation/crossover) with clear invariants so runs remain valid and reproducible.

## Key types
- `BestSubsetProblem`
  - Evaluates the objectives with a pluggable **correlation strategy** (Pearson/Kendall) and a **target encoder**.
  - Internal objectives (minimization in jMetal):
    - `obj[0] = K` (cardinality = number of selected topics)
    - `obj[1] = ±corr` where:
      - **BEST** → minimize `-corr` (so higher NATURAL corr is better)
      - **WORST** → minimize `+corr` (so lower NATURAL corr is better)
  - Emits **NATURAL** correlations to the view layer (no sign flips in output).

- `BestSubsetSolution`
  - Binary mask over topics (backed by jMetal `BinarySet`, sparse-aware via `nextSetBit`).
  - Convenience accessors: `numberOfSelectedTopics`, `getCardinality()`, `getCorrelation()`.
  - Streaming helpers and caches:
    - `packMaskToBase64()` → unpadded Base64 using LSB-first bits, little-endian 64-bit words.
    - `toVarLinePackedBase64()` → `"B64:<payload>"` form used by CSV.
    - Reusable buffers and delta-eval scaffolding: `reusableMaskBuffer`, `lastEvaluatedMask`, `cachedSumsBySystem`.
    - Operator hooks: `lastSwapOutIndex`, `lastSwapInIndex`, `lastMutationWasFixedKSwap`.
  - Value semantics: `copy()` produces an equal (value-based) clone.

- Operators
  - `BitFlipMutation` — flips exactly one bit (Hamming distance = 1) while keeping **≥ 1** topic selected.
  - `BinaryPruningCrossover` — uniform per-bit mixing with **non-empty repair** for children.
  - (Any fixed-K operator honors `forcedCardinality`, when present.)

## Invariants
- Masks are always sized to `numberOfTopics`.
- **BitFlipMutation** changes cardinality by **±1** and never yields an empty mask.
- When `forcedCardinality` is set, solutions must have **exactly K** selected topics.
- Views consume **NATURAL** correlations; internal sign handling is confined to the problem’s objective encoder.

## Performance & serialization
- Bit operations use `BinarySet` and `nextSetBit` for **O(K)** scans.
- Genotype key / Base64 payloads are **cached** and invalidated only when the mask changes.
- Base64 packer avoids Boolean-array round-trips and writes little-endian 64-bit words directly.

## Reproducibility
- All randomness flows through jMetal’s RNG; **deterministic** mode installs a seeded adapter (see `RandomBridge`).
- Child seeds can be derived per sub-run label to obtain stable, labeled streams.

## Interactions
- **DatasetModel** calls the problem to evaluate objectives; operators are plugged into NSGA-II.
- **Views** (CSV/Parquet) receive NATURAL correlations and serialized masks; global ordering and selection rules
  (e.g., BEST/WORST) are enforced at the model/view layer.
