# NewBestSub

<!-- Project -->
[![Release](https://img.shields.io/github/v/release/Miccighel/NewBestSub)](https://github.com/Miccighel/NewBestSub/releases)
[![Docs](https://img.shields.io/badge/docs-website-blue)](https://miccighel.github.io/NewBestSub/)
[![License](https://img.shields.io/github/license/Miccighel/NewBestSub)](LICENSE)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.16950389.svg)](https://doi.org/10.5281/zenodo.16950389)

<!-- Repo stats -->
![Last Commit](https://img.shields.io/github/last-commit/Miccighel/NewBestSub)
![Open Issues](https://img.shields.io/github/issues/Miccighel/NewBestSub)
![Pull Requests](https://img.shields.io/github/issues-pr/Miccighel/NewBestSub)
![Contributors](https://img.shields.io/github/contributors/Miccighel/NewBestSub)
![GitHub stars](https://img.shields.io/github/stars/Miccighel/NewBestSub?style=social)
[![Downloads](https://img.shields.io/github/downloads/Miccighel/NewBestSub/total)](https://github.com/Miccighel/NewBestSub/releases)

<!-- Tech stack -->
![Java 22](https://img.shields.io/badge/Java-22-007396?logo=openjdk&logoColor=white)
![Kotlin 2.2](https://img.shields.io/badge/Kotlin-2.2-7F52FF?logo=kotlin&logoColor=white)
![jMetal 6.9.1](https://img.shields.io/badge/jMetal-6.9.1-0A7BBB)
![Parquet 1.15.2](https://img.shields.io/badge/Parquet-1.15.2-50A9E5)
![Reproducible](https://img.shields.io/badge/Reproducible-Deterministic%20mode-brightgreen)

<!-- Build & Quality -->
[![CI](https://github.com/Miccighel/NewBestSub/actions/workflows/ci.yml/badge.svg)](https://github.com/Miccighel/NewBestSub/actions/workflows/ci.yml)
[![CodeQL](https://github.com/Miccighel/NewBestSub/actions/workflows/codeql.yml/badge.svg)](https://github.com/Miccighel/NewBestSub/actions/workflows/codeql.yml)


> Efficient topic‑set reduction for IR evaluation using NSGA‑II.

---

## Table of contents

- [Overview](#overview)
- [Installation](#installation)
- [Quick start (CLI)](#quick-start-cli)
- [Input format](#input-format)
- [CLI reference](#cli-reference)
- [Outputs & folder layout](#outputs--folder-layout)
- [Natural vs. internal correlation](#natural-vs-internal-correlation)
- [Deterministic & reproducible runs](#deterministic--reproducible-runs)
- [Logging](#logging)
- [Performance notes & tips](#performance-notes--tips)
- [Developing](#developing)
- [Citation](#citation)
- [Changelog](#changelog)
- [License](#license)

## Overview

**NewBestSub** searches for small subsets of topics that preserve the ranking of retrieval systems as measured by a correlation metric (Pearson or Kendall). It supports three targets:

- **BEST** – maximize correlation (pick strongest subsets).
- **WORST** – minimize correlation (stress test robustness).
- **AVERAGE** – sample random subsets per cardinality *K* and compute percentiles.

Results are streamed to CSV (always) and optionally Parquet, with careful attention to deterministic execution and logging for large runs.

## Installation

### CLI (recommended for experiments)

1. Download the shaded jar from **GitHub Releases**: `NewBestSub-2.0-jar-with-dependencies.jar`.
2. Run with Java 22+:

```bash
java -Xms32g -Xmx32g -jar NewBestSub-2.0-jar-with-dependencies.jar \
  -fi "mmlu" -c "Pearson" -t "Best" -po 20000 -i 100000 -l Limited --det
```

### As a library (Maven Central)

Add the core artifact if you want to integrate programmatically:

**Maven**

```xml
<dependency>
  <groupId>it.uniud.newbestsub</groupId>
  <artifactId>NewBestSub</artifactId>
  <version>2.0.6</version>
</dependency>
```

**Gradle (Kotlin DSL)**

```kts
implementation("it.uniud.newbestsub:NewBestSub:2.0.6")
```

## Quick start (CLI)

**BEST (Pearson):**

```bash
java -Xms32g -Xmx32g -jar NewBestSub-2.0-jar-with-dependencies.jar \
  -fi "mmlu" -c "Pearson" -t "Best" -po 20000 -i 100000 -l Limited --det
```

**AVERAGE (1..100 percentiles):**

```bash
java -Xms32g -Xmx32g -jar NewBestSub-2.0-jar-with-dependencies.jar \
  -fi "mmlu" -c "Pearson" -t "Average" -r 2000 -pe 1,100 -l Limited --det
```

**ALL (Best/Worst + Average):**

```bash
java -Xms32g -Xmx32g -jar NewBestSub-2.0-jar-with-dependencies.jar \
  -fi "mmlu" -c "Pearson" -t "All" -po 20000 -i 100000 -r 2000 -pe 1,100 -l Limited
```

## Input format

CSV with systems as rows and topics as columns. First header cell is `sys`.

```csv
sys,t1,t2,t3,t4,t5
BM25,0.31,0.45,0.22,0.18,0.40
QL,0.28,0.48,0.19,0.21,0.36
RM3,0.40,0.51,0.26,0.17,0.42
```

## CLI reference

Required flags vary by target:

- **Common required**: `-fi/--fileIn`, `-c/--corr`, `-t/--targ`, `-l/--log`
- **Best/Worst/All**: also `-i/--iter`, `-po/--pop` (and `-po ≥ #topics`)
- **Average/All**: also `-r/--rep`, `-pe/--perc A,B` (two integers; range normalized)

Key options (abridged):

| Option | Long        | Meaning |
|-------:|-------------|---------|
| `-fi`  | `--fileIn`  | Dataset name (CSV under `data/`, without `.csv`). |
| `-c`   | `--corr`    | Correlation method: `Pearson` or `Kendall`. |
| `-t`   | `--targ`    | Target: `Best`, `Worst`, `Average`, `All`. |
| `-l`   | `--log`     | Logging: `Verbose`, `Limited`, `Off`. |
| `-i`   | `--iter`    | Iterations (Best/Worst/All). |
| `-po`  | `--pop`     | Population size (Best/Worst/All); must be ≥ number of topics. |
| `-r`   | `--rep`     | Repetitions per K (Average/All). |
| `-pe`  | `--perc`    | Percentiles (two ints, e.g., `1,100`). |
| `-et`  | `--expt`    | Topic expansion step. |
| `-es`  | `--exps`    | System expansion step. |
| `-mx`  | `--max`     | Maximum size for expansions. |
| `-mr`  | `--mrg`     | Repeat program N times and merge. |
| `--det`| `--deterministic` | Enable deterministic mode. |
| `--sd` | `--seed`    | Master seed (implies deterministic). |
| `--copy` |           | Copy outputs into `NewBestSub-Experiments/` if present. |

## Outputs & folder layout

Base paths (relative to repo):  
- Input: `./data/`  
- Output: `./res/`  
- Logs: `./log/`

Each run is parameterized by a token and timestamp, e.g.:

```
res/<dataset>/<token>/FUN.csv
res/<dataset>/<token>/VAR.csv
res/<dataset>/<token>/TOP.csv
log/<token>-time<YYYY-MM-DD-HH-mm-ss>.log
```

### CSV formats

- **FUN**: `K,correlation` (one or more lines per `K`)
- **VAR**: `B64:<payload>` where the bitmask is packed as 64‑bit words (LSB‑first), serialized **little‑endian**, and Base64‑encoded **without padding**
- **TOP**: `Cardinality,Correlation,TopicsB64` with topics encoded via `B64:<payload>`

### Parquet (optional)

Parquet files mirror CSV content; the packed mask uses the same bit‑layout as `VAR` without the `B64:` prefix in the field payload.

## Natural vs internal correlation

Views operate on **natural** correlation (no sign flips). Selection rules:

- **FUN (after final rewrite)**  
  - **BEST**: file sorted `(K asc, corr asc)` → pick **LAST** line per K (max).  
  - **WORST**: file sorted `(K asc, corr desc)` → pick **LAST** line per K (min).

- **TOP (already target‑ordered when provided to the view)**  
  - **BEST**: **descending** per K → pick **FIRST** line (max).  
  - **WORST**: **ascending** per K → pick **FIRST** line (min).

`NaN` correlations, if any, are relegated to a suffix to keep deterministic ordering.

## Deterministic & reproducible runs

Use `--det` (or `--seed <long>`) to install a deterministic RNG bridge over jMetal:

```bash
... --det
# or
... --seed 1234567890
```

- All randomness flows through the `JMetalRandom` singleton.  
- Seeds are derived from parameters to make runs reproducible (when `--det` is enabled).

## Logging

Logs are written via Log4j2. On startup a bootstrap file is created; once parameters are known, logging switches to a parameterized filename based on the token and `-time<timestamp>` marker. The configuration expects the `baseLogFileName` system property (see `log4j2.xml`).

## Performance notes & tips

- Large experiments: prefer **G1** GC and high heap (e.g., `-Xms32g -Xmx32g`).  
- If you hit “GC overhead limit exceeded”, you can disable it with `-XX:-UseGCOverheadLimit`, but increasing `-Xmx` is usually better.  
- For **Best/Worst/All**, ensure `-po ≥ #topics`; the program validates early and provides hints.  
- CSV streaming throttles generation logging using `GENERATION_WARMUP` and `GENERATION_STRIDE` to reduce I/O.

## Developing

Build & test:

```bash
mvn -q -DskipTests=false clean verify
```

Generate API docs (Dokka) locally:

```bash
mvn -q -Ppages -DskipTests=true verify
```

Run unit tests only:

```bash
mvn -q -DskipITs=true -Dtest='*Test' test
```

## Citation

If you use **NewBestSub** in your research, please cite the Zenodo DOI:

> Soprano, M., Roitero, K., Lunardi, R., & Mizzaro, S. (2025). *NewBestSub* (v2.0.6). Zenodo. https://doi.org/10.5281/zenodo.16950389

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for detailed release notes. Highlights for **2.0.6**:

- NATURAL vs INTERNAL correlation clarified end‑to‑end (docs & tests).  
- Deterministic mode and seed plumbing consolidated.  
- CSV/Parquet formats documented, including Base64 packing details.  
- Readme shields grouped and sorted; examples expanded.

## License

This project is licensed under the terms of the [MIT License](LICENSE).
