# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),  
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Nothing yet.

---

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

---

## [1.0.0] - Initial release
- First public release of **NewBestSub**.
