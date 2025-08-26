# Package it.uniud.newbestsub.dataset

Loading, representing, and **streaming** results to CSV/Parquet.

## Key types
- `DatasetController`: loads input AP data and exposes `models`.
- `DatasetModel`: orchestrates runs (BEST/WORST/AVERAGE), caching per‑K stats and masks.
- `DatasetView`: streaming writers for FUN/VAR/TOP (+ canonical rewrite on close).
- `Parameters`: run configuration (dataset name, correlation, iterations, reps, percentiles).

## Outputs
- **FUN**: `K,correlation`
- **VAR**: `"B64:<payload>"` packed LSB‑first, little‑endian words, **no padding**
- **TOP**: `Cardinality,Correlation,TopicsB64` (topics encoded with `"B64:"`)

## Notes
- Streams open lazily and are globally rewritten sorted by `(K asc, corr asc)` on close.
- Representative masks are stored per K for quick lookups and exports.
