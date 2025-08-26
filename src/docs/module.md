# Module NewBestSub

Efficient topic‑set reduction for IR evaluation using NSGA‑II.

## What this module provides
- Evolutionary search over topic subsets that preserve **system rankings**.
- CSV/Parquet streaming views for FUN/VAR/TOP artifacts.
- Correlation strategies (Pearson/Kendall) over per‑system AP rows.

## Workflow
1. Load AP data with `DatasetController`.
2. Configure `Parameters` (dataset, correlation, repetitions, percentiles).
3. Call `DatasetModel.solve(parameters)` for BEST/WORST/AVERAGE.
4. Inspect outputs under the generated CSV/Parquet paths.

## Terminology
- **Cardinality K**: number of selected topics.
- **Targets**: BEST, WORST, AVERAGE.
- **Correlations**: Pearson / Kendall based on averaged per‑system scores.
