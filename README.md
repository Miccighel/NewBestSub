# NewBestSub

<!-- Release & Docs -->
[![Release](https://img.shields.io/github/v/release/Miccighel/NewBestSub)](https://github.com/Miccighel/NewBestSub/releases)
[![Docs](https://img.shields.io/badge/docs-website-blue)](https://miccighel.github.io/NewBestSub/)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.16950389.svg)](https://doi.org/10.5281/zenodo.16950389)
[![License](https://img.shields.io/github/license/Miccighel/NewBestSub)](LICENSE)

<!-- CI & Quality -->
[![CI](https://github.com/Miccighel/NewBestSub/actions/workflows/ci.yml/badge.svg)](https://github.com/Miccighel/NewBestSub/actions/workflows/ci.yml)
[![CodeQL](https://github.com/Miccighel/NewBestSub/actions/workflows/codeql.yml/badge.svg)](https://github.com/Miccighel/NewBestSub/actions/workflows/codeql.yml)
[![Build](https://img.shields.io/badge/build-Maven-C71A36?logo=apache-maven)](#)
[![Tests](https://img.shields.io/badge/JUnit-5-25A162?logo=junit5)](#)
[![Logging](https://img.shields.io/badge/Log4j-2-CC0000?logo=apache)](#)

<!-- Usage & Repo Stats -->
[![Downloads](https://img.shields.io/github/downloads/Miccighel/NewBestSub/total)](https://github.com/Miccighel/NewBestSub/releases)
![Last Commit](https://img.shields.io/github/last-commit/Miccighel/NewBestSub)
![Open Issues](https://img.shields.io/github/issues/Miccighel/NewBestSub)
![Pull Requests](https://img.shields.io/github/issues-pr/Miccighel/NewBestSub)
![Contributors](https://img.shields.io/github/contributors/Miccighel/NewBestSub)
![GitHub stars](https://img.shields.io/github/stars/Miccighel/NewBestSub?style=social)

<!-- Tech stack -->
![Java 22](https://img.shields.io/badge/Java-22-007396?logo=openjdk&logoColor=white)
![Kotlin 2.2](https://img.shields.io/badge/Kotlin-2.2-7F52FF?logo=kotlin&logoColor=white)
![jMetal 6.9.1](https://img.shields.io/badge/jMetal-6.9.1-0A7BBB)
![Parquet 1.15.2](https://img.shields.io/badge/Parquet-1.15.2-50A9E5)
![Deterministic mode](https://img.shields.io/badge/Reproducible-Deterministic%20mode-brightgreen)

NewBestSub is a research tool to explore “best/worst subset of topics” under different correlation criteria (Pearson or Kendall). It provides a fast, streaming NSGA-II implementation over two objectives — cardinality `K` and **natural** correlation — plus convenient CSV/Parquet outputs and a deterministic mode for reproducible runs.

---

## Contents

- [Quick start](#quick-start)
- [CLI](#cli)
- [Deterministic execution (reproducibility)](#deterministic-execution-reproducibility)
- [Outputs & file layout](#outputs--file-layout)
- [CSV & TOP ordering (how to read the files)](#csv--top-ordering-how-to-read-the-files)
- [Run-name parser (helper)](#runname-parser-helper)
- [Build](#build)
- [Known constraints & common errors](#known-constraints--common-errors)
---

## Quick start

```bash
# Build a fat jar
mvn -q -DskipTests package

# BEST (Pearson), limited logging, deterministic
java -Xms32g -Xmx32g \
  -jar target/NewBestSub-2.0.6-jar-with-dependencies.jar \
  -fi "mmlu" -c "Pearson" -t "Best" -po 20000 -i 100000 -l Limited --det

# AVERAGE (Kendall), percentiles 1..100, 2000 repetitions
java -Xms32g -Xmx32g \
  -jar target/NewBestSub-2.0.6-jar-with-dependencies.jar \
  -fi "mmlu" -c "Kendall" -t "Average" -r 2000 -pe 1,100 -l Limited --det
```

Input CSVs are expected under `./data/` (e.g., `data/mmlu.csv`). Results go to `./res/` and logs to `./log/`.

---

## CLI

> Short flags are shown first; the long equivalents are in parentheses.

### Required core options

- `-fi, --fileIn <name>`: dataset file **without extension**, looked up in `data/` (e.g., `-fi mmlu`).
- `-c, --corr {Pearson|Kendall}`: correlation method.
- `-t, --targ {Best|Worst|Average|All}`: experiment target.
- `-l, --log {Verbose|Limited|Off}`: log level.

### Required depending on target

- `-po, --pop <int>`: population size (needed for `Best`, `Worst`, `All`).
- `-i, --iter <int>`: iterations (needed for `Best`, `Worst`, `All`).
- `-r, --rep <int>`: repetitions (needed for `Average`, `All`).
- `-pe, --perc <lo,hi>`: two comma-separated integers (needed for `Average`, `All`).

### Optional workflow controls

- `-mr, --mrg <int>`: repeat whole program N times and merge.
- `-et, --expt <int>`: expand topics by +N each step; see `-mx`.
- `-es, --exps <int>`: expand systems by +N each step; see `-mx`.
- `-mx, --max <int>`: maximum expanded size for `--expt/--exps`.
- `--copy`: after finishing, copy outputs into `../NewBestSub-Experiments/data/NewBestSub/<timestamp>/`.
- `--det, --deterministic`: enable deterministic execution.
- `--sd, --seed <long>`: explicit master seed (implies `--det`).

### Examples

```bash
# BEST, Pearson
java -jar target/NewBestSub-2.0.6-jar-with-dependencies.jar \
  -fi "mmlu" -c "Pearson" -t "Best" -po 5000 -i 20000 -l Limited --det

# WORST, Kendall
java -jar target/NewBestSub-2.0.6-jar-with-dependencies.jar \
  -fi "mmlu" -c "Kendall" -t "Worst" -po 5000 -i 20000 -l Verbose --sd 123456789

# ALL (Best/Worst + Average)
java -jar target/NewBestSub-2.0.6-jar-with-dependencies.jar \
  -fi "mmlu" -c "Pearson" -t "All" -po 20000 -i 100000 -r 2000 -pe 1,100 -l Limited

# Topic expansion (true data run, then +ET until MX)
java -jar target/NewBestSub-2.0.6-jar-with-dependencies.jar \
  -fi "mmlu" -c "Pearson" -t "Best" -po 12000 -i 20000 -et 500 -mx 20000 -l Limited

# Multi-run merge (repeat 'Best' 10 times)
java -jar target/NewBestSub-2.0.6-jar-with-dependencies.jar \
  -fi "mmlu" -c "Pearson" -t "Best" -po 20000 -i 100000 -mr 10 -l Limited
```

---

## Deterministic execution (reproducibility)

NewBestSub centralizes randomness through `RandomBridge`, which adapts a `SplittableRandom` to jMetal’s RNG. Use:

- `--det` to enable deterministic mode. If you omit `--seed`, a **stable** seed is derived from key parameters (dataset, correlation, target, iterations, repetitions, population, percentiles).
- `--seed <long>` to pin the **master seed** explicitly (implies `--det`).

**What becomes reproducible?**

- NSGA-II evolution, mutation/crossover, and all helper sampling that relies on jMetal’s RNG — **as long as** call order is the same.
- We recommend sequential runs when determinism matters.

Logs will include:
```
Deterministic mode: ON (masterSeed=<value>)
```

---

## Outputs & file layout

Default paths (relative to the project root):

- Input CSVs: `./data/`
- Output files: `./res/`
- Logs: `./log/`
- Experiments mirror (when `--copy` is used): `../NewBestSub-Experiments/data/NewBestSub/<RUN_TIMESTAMP>/`

**Log files**  
- Logging is backed by Log4j2. The active file is controlled by the system property `baseLogFileName` (used inside `log4j2.xml`).
- On start we write to a **bootstrap** log, then promote to a **parameterized** log name that includes the dataset and run parameters.

**Jar names**  
- Application: `target/NewBestSub-2.0.6-jar-with-dependencies.jar`
- (Tests) A separate assembly may build `*-test-jar-with-dependencies.jar` for test-only utilities and fixtures.

**Parquet outputs**  
- The Parquet view mirrors the CSV content but buffers in memory before writing. We don’t guarantee a stable row order beyond the documented ordering rules below.

---

## CSV & TOP ordering (how to read the files)

**Correlations are emitted on the NATURAL scale** (no sign flip). “Higher is better” for **Best**; “lower is better” for **Worst**.

**`FUN` (function values — `K,corr`)**
- **BEST:** after final rewrite, rows are globally sorted by `(K asc, corr asc)`.
  - To pick the **best** per `K`, take the **LAST** row of each `K` group (max).
- **WORST:** after final rewrite, rows are globally sorted by `(K asc, corr desc)`.
  - To pick the **worst** per `K`, take the **LAST** row of each `K` group (min).

**`VAR` (variable values — bitmasks)**  
- Each row corresponds 1:1 to `FUN`.
- The mask is serialized as `"B64:<payload>"` where `<payload>` is:
  - bits packed **LSB-first** into 64-bit words,
  - words written as **little-endian** bytes,
  - Base64-encoded **without padding**.

**`TOP` (top-solutions per `K`)**
- Already grouped by `K` and **target-ordered**:
  - **BEST:** descending within each `K`. Pick the **FIRST** row (max).
  - **WORST:** ascending within each `K`. Pick the **FIRST** row (min).
- When present, `NaN` correlations are ranked **last** (suffix) within the group to keep ordering deterministic.
- Topics are encoded as `"B64:<payload>"` using the same mask packing rules as `VAR`.

---

## Run-name parser (helper)

A small utility is included to parse container/run folder names used in logs and experiment copies.

**Pattern**
```
<DATASET>-<CORR>-top<Topics>-sys<Systems>-po<Population>-i<Iterations>[-r<Repetitions>][-exec<Executions>][-seed<Seed>][-det]-time<YYYY-MM-DD-HH-mm-ss>
```

**Example**
```
AH99-Pearson-top50-sys129-po1000-i10000-r2000-time2025-08-22-19-19-40
```

**Usage**
```bash
# Pretty JSON
./scripts/parse_run_name.py "AH99-Pearson-top50-sys129-po1000-i10000-r2000-time2025-08-22-19-19-40" --pretty

# Compact JSON
./scripts/parse_run_name.py "DS-Kendall-top256-sys15-po512-i100000-exec10-seed123456-det-time2025-08-24-13-00-00"
```

**Output (example)**
```json
{
  "dataset": "AH99",
  "correlation": "Pearson",
  "topics": 50,
  "systems": 129,
  "population": 1000,
  "iterations": 10000,
  "repetitions": 2000,
  "executions": null,
  "seed": null,
  "deterministic": false,
  "timestamp": "2025-08-22-19-19-40"
}
```

---

## Build

**Requirements**
- JDK 11+ (recommended)  
- Maven 3.8+

**Commands**
```bash
# Compile + test
mvn -q test

# Package runnable jar (fat jar)
mvn -q -DskipTests package

# Run
java -Xms32g -Xmx32g \
  -jar target/NewBestSub-2.0.6-jar-with-dependencies.jar <options...>
```

---

## Known constraints & common errors

- **Population must be ≥ number of topics.**  
  If you see messages like “Value for the option `<<p>>` or `<<po>>` must be greater or equal than/to …”, increase `-po`.
- **Required flags by target**  
  - `Best/Worst`: need `-po` and `-i`.  
  - `Average`: need `-r` and `-pe lo,hi`.  
  - `All`: needs **both** sets.
- **Negative or zero values**  
  Options like `-po`, `-i`, `-r`, `-et`, `-es`, `-mx` must be positive integers.
- **OutOfMemoryError**  
  Increase `-Xmx`, reduce `-po`/`-i`/`-r`, or tune GC. The program prints a friendly hint and example flags on OOM.

<sub>© NewBestSub authors. All trademarks and logos are the property of their respective owners.</sub>
