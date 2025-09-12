# Package it.uniud.newbestsub.math

Correlation primitives and small numeric utilities used across the project.

## Responsibilities
- Single source of truth for **Pearson** and **Kendall** correlation computations.
- Works on per-system series and (where available) **precomputed means** to avoid recomputation.
- Allocation-aware helpers for streaming/AVERAGE paths (reuse scratch buffers when possible).
- Returns correlations on the **NATURAL** scale (no sign flips). Target-specific handling (BEST/WORST) is done upstream.

## API shape (conceptual)
- `pearson(x: DoubleArray, y: DoubleArray): Double` — standard Pearson on finite inputs.
- `kendall(x: DoubleArray, y: DoubleArray): Double` — Kendall correlation on finite inputs.
- Optional variants may accept reusable buffers to minimize allocation in tight loops.

> Exact function names/types may differ; this package guarantees consistent formulas and semantics wherever called.

## Numerical notes
- Computations use `Double` throughout.
- Implementations favor **numerical stability** (e.g., mean/variance computed in a single pass when safe).
- The math layer does **not** apply any target-specific sign transformation; callers interpret NATURAL values.

## Performance
- Hot-path helpers are written to minimize temporary allocations.
- When provided, caller-owned scratch buffers are reused across evaluations (important in AVERAGE streaming).

## Invariants & validation
- Input arrays must have the **same length** and contain **finite** values.
- If preconditions are violated, callers are expected to sanitize upstream; this package may return `NaN` which is then
  handled by the higher layers (e.g., sorted as a suffix in TOP, filtered in AVERAGE).

## Interactions
- **Program/Model** select `Pearson` or `Kendall` via CLI/params and pass series into this package.
- **Views** consume NATURAL correlations as-is; ordering/selection rules are enforced at the view/model level.
