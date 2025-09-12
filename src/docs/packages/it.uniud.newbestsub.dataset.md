# Package it.uniud.newbestsub.dataset

Loading, representing, and **streaming** results to CSV/Parquet.

## Key types
- `DatasetController`: loads input AP data and exposes `models`.
- `DatasetModel`: runs BEST/WORST/AVERAGE; caches per-K correlation and one representative mask; computes percentiles in AVERAGE.
- `DatasetView`: streaming writers for FUN/VAR/TOP with a canonical **final rewrite** on close.
- `Parameters`: run configuration (dataset, correlation, target, iterations, repetitions, percentiles, population, execution index).

## Outputs
- **FUN**: `K,correlation` (NATURAL scale)
- **VAR**: `"B64:<payload>"` — packed selection mask (see Encoding)
- **TOP**: `Cardinality,Correlation,TopicsB64` — topics encoded as `"B64:<payload>"`

## Encoding
- Mask packs bits LSB-first within each 64-bit word (bit 0 → topic 0).
- Words are serialized as little-endian bytes.
- Base64 is emitted **without padding**.
- CSV uses the `"B64:"` prefix; Parquet stores the **bare** Base64 payload.

## Ordering and selection rules
Correlations exposed by views are on the **NATURAL** scale (no sign flips).

- **FUN/VAR (final file order)**  
  - BEST  → sorted by `(K asc, corr asc)`; to select the per-K best, pick the **LAST** row of each K block (max).  
  - WORST → sorted by `(K asc, corr desc)`; to select the per-K best-of-worst, pick the **LAST** row (min).
- **TOP (as provided by the model)**  
  - BEST  → blocks are **descending** per K; pick the **FIRST** row (max).  
  - WORST → blocks are **ascending** per K; pick the **FIRST** row (min).  
  - `NaN` correlations, when present, are placed as a **contiguous suffix** (ranked after finite values).

## AVERAGE mode
- Streams one **representative per K** (`K = 1..N`) with a finite correlation and a size-N mask of weight K.
- Percentiles are computed over repeats; for each requested percentile `p`, the view exposes **one value per K**.

## Streaming and rewrite
- Streams open lazily; callers can append out-of-order.
- On `close()`, FUN and VAR are globally rewritten in their canonical per-target order (see above) and kept aligned row-by-row.
- TOP uses **replace-batch** semantics: a newer batch for a K **replaces** the previous block for that K; unrelated K blocks are preserved.

## Determinism
- When enabled, the run installs a deterministic RNG bridge; identical parameters and call order produce identical streams and files.
- Container/log names include a stable parameter token and a run timestamp to separate executions.
