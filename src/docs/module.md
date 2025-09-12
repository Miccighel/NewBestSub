# Module NewBestSub

Efficient topic-set reduction for IR evaluation with NSGA-II.

## What this module provides
- Evolutionary search over topic subsets that preserve **system rankings**.
- BEST / WORST / AVERAGE run modes with clear **NATURAL correlation** semantics.
- CSV/Parquet streaming views for FUN/VAR/TOP artifacts (final global rewrite on close).
- Correlation strategies (Pearson/Kendall) over per-system AP rows.
- Deterministic execution via a `SplittableRandom` adapter bridged into jMetal.

## Workflow
1. Load AP data with `DatasetController.load(...)`.
2. Prepare `Parameters` (dataset, correlation, target, iterations/repetitions, percentiles).
3. Call `DatasetModel.solve(parameters)` for **BEST**, **WORST**, or **AVERAGE**.
4. Inspect outputs under the generated CSV/Parquet paths (FUN/VAR/TOP).

## Run modes & selection rules (NATURAL corr)
- **BEST**: higher correlation is better.  
  - FUN (after final rewrite): grouped by `K`, sorted **ascending** → pick **last** per `K`.  
  - TOP: per-`K` blocks sorted **descending** → pick **first** per `K`.
- **WORST**: lower correlation is better.  
  - FUN (after final rewrite): grouped by `K`, sorted **descending** → pick **last** per `K`.  
  - TOP: per-`K` blocks sorted **ascending** → pick **first** per `K`.

## Artifacts (CSV / Parquet)
- **FUN**: `K,correlation` (NATURAL scale).
- **VAR**: `"B64:<payload>"` where payload packs the boolean mask as 64-bit words  
  (LSB-first within a word), serialized **little-endian**, Base64 **without padding**.
- **TOP**: `Cardinality,Correlation,TopicsB64` with `TopicsB64` encoded as above.

## Key types
- `DatasetController` – loads the dataset and exposes one or more `DatasetModel`s.
- `DatasetModel` – orchestrates runs, caches per-`K` stats/masks, streams progress.
- `DatasetView` – CSV/Parquet writers (lazy open; canonical global rewrite on close).
- `BestSubsetProblem` / `BestSubsetSolution` – evaluation and binary mask representation.

## Determinism
- Use CLI flags **`--deterministic`** (or `--seed <long>`) to fix the master seed.
- Internally, `RandomBridge.installDeterministic(seed)` swaps jMetal’s RNG with a
  `SplittableRandom` adapter; `Tools.stableSeedFrom(Parameters)` derives a stable seed
  from the parameter snapshot.

## Notes & tips
- Ensure **`-po ≥ #topics`** (population at least the topic count) for BEST/WORST/All.
- **AVERAGE** requires **`-r`** (repetitions) and **`-pe a,b`** (percentile bounds).
- Logs are parameterized with a token and the run timestamp; the `baseLogFileName`
  system property is switched once parameters are known.
- For very large runs, if you hit GC pressure, consider increasing `-Xmx` and, if needed,
  disabling the GC overhead limit: `-XX:-UseGCOverheadLimit`.
