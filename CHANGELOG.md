# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Nothing yet.

## [2.0.6] - 2025-09-12

### Added
- README refresh: clarified deterministic runs (`--deterministic` / `--seed`), file outputs (CSV/Parquet, FUN/VAR/TOP), streaming cadence, and run‑container naming.
- Python helper: **`parse_run_name.py`** to parse container folder names; documented in README.
- Shields: grouped and sorted badges; added Maven Central, build, tests, logging, and DOI where relevant.

### Changed
- **NATURAL vs INTERNAL correlation** clarified end‑to‑end (README, tests, and view docs). Views always receive **NATURAL** values; selection rules are explicit:
  - **BEST** → FUN final file is `(K asc, corr asc)` → pick **last** per `K`; TOP is **descending** → pick **first** per `K`.
  - **WORST** → FUN final file is `(K asc, corr desc)` → pick **last** per `K`; TOP is **ascending** → pick **first** per `K`.
- Tests enhanced with deeper comments and selection explanations (CSVView & ParquetView smoke).

### Fixed
- Early runtime validation: enforce `-po ≥ #topics` with clearer messages; improved distinction between CLI parse errors and runtime validation errors.

## [2.0.4] - 2025-09-03

### Changed
- Internal maintenance and minor documentation tweaks.

## [2.0.3] - 2025-08-29

### Changed
- Container folder names now include the iteration token when provided by CLI.
- AVERAGE CSV/Parquet filenames omit the iteration token by design.
- `numberOfIterations` is assigned at the start of `DatasetModel.solve()` to ensure consistent naming across modes.

## [2.0.0] - 2025-08-24

### Added
- Comprehensive KDoc for test classes:
    - `DatasetModelTest`: Base64 helpers, AVERAGE path semantics, percentiles, round-trips, edge cases, token/log path helpers.
    - `BestSubsetSolutionTest`: fixture setup, bit toggle semantics, bit capacity, copy value-semantics.
    - `BitFlipMutationTest`: operator semantics, single-bit flip invariants, Hamming distance checks, capacity preservation.
    - `DatasetViewTest`: FUN/VAR global sort, TOP replace-batch behavior, Base64 normalization, cleanup helper.

### Changed
- Unified documentation style across tests:
    - Section dividers preserved for readability.
    - Converted inline comments into structured KDoc.
    - Emphasized invariants and expected outcomes in test docs.

### Fixed
- CSV/Parquet test assertions now explicitly cover:
    - Global FUN/VAR rewrite order by `(K asc, corr asc)`.
    - Base64 prefix checks for VAR/TOP lines.
    - Percentile completeness in AVERAGE path tests.

## [1.0.0]

### Added
- Initial public release of **NewBestSub**.

[Unreleased]: https://github.com/Miccighel/NewBestSub/compare/v2.0.6...HEAD
[2.0.6]: https://github.com/Miccighel/NewBestSub/compare/v2.0.4...v2.0.6
[2.0.4]: https://github.com/Miccighel/NewBestSub/compare/v2.0.3...v2.0.4
[2.0.3]: https://github.com/Miccighel/NewBestSub/compare/v2.0.0...v2.0.3
[2.0.0]: https://github.com/Miccighel/NewBestSub/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/Miccighel/NewBestSub/releases/tag/v1.0.0
