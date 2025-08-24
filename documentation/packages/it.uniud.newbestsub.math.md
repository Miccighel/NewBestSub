# Package it.uniud.newbestsub.math

Correlation primitives and related utilities.

## Responsibilities
- Single source of truth for Pearson/Kendall computations.
- Operates on **precomputed** per‑system means where applicable.
- Allocation‑aware helpers for streaming AVERAGE branch (reuse buffers).

## Invariants
- Inputs must be same length and finite.
- Correlation results are finite for valid inputs; `NaN`/`Inf` rejected upstream.
