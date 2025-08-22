# NewBestSub

Efficient topic-set reduction for IR evaluation using a multi-objective evolutionary algorithm (NSGA-II). This repo reproduces and extends results on selecting small topic subsets that preserve the system ranking induced by the full set.

[![CI](https://github.com/Miccighel/NewBestSub/actions/workflows/ci.yml/badge.svg)](https://github.com/Miccighel/NewBestSub/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Miccighel/NewBestSub)](https://github.com/Miccighel/NewBestSub/releases)
[![License](https://img.shields.io/github/license/Miccighel/NewBestSub)](LICENSE)
[![Java 22](https://img.shields.io/badge/Java-22-007396?logo=openjdk&logoColor=white)]()
[![Kotlin 2.2](https://img.shields.io/badge/Kotlin-2.2-7F52FF?logo=kotlin&logoColor=white)]()
[![jMetal 6.9.1](https://img.shields.io/badge/jMetal-6.9.1-0A7BBB)]()
[![Parquet 1.15.2](https://img.shields.io/badge/Parquet-1.15.2-50A9E5)]()
[![Deterministic mode](https://img.shields.io/badge/Reproducible-Deterministic%20mode-brightgreen)]()

> JDIQ 2018: <https://doi.org/10.1145/3239573>  
> SIGIR 2018: <https://dl.acm.org/citation.cfm?doid=3209978.3210108>

---

## ✨ What’s inside

- **BEST / WORST / AVERAGE** experiments with **NSGA-II (jMetal 6.9.x)**
- **Streaming NSGA-II wrapper**  
  Per-generation callback to stream progress:
  - **FUN / VAR:** append only *improvements per K* (kept sorted per batch for stable growth); final global sort is handled on close.
  - **TOP:** exact 10-line blocks per K; replace a block only when it changes; skip partial blocks (<10 rows).
- **Environmental selection via MNDS**  
  NSGA-II replacement uses **MergeNonDominatedSortRanking (MNDS)** plus local crowding distance to fill the last partial front.
- **Step 3 incremental evaluation**  
  O(S) delta-updates of per-system subset means using cached sums and operator swap hints (fixed-K swap), with a cold-start fallback.
- **Dual outputs**  
  - **CSV** (streaming-first)  
  - **Parquet** (streaming-first, Snappy, independent of CSV)
- **Consistent formatting**  
  - Correlations serialized with **6-digit precision** in both CSV and Parquet  
  - **VAR** stores a **contiguous bitstring** (e.g., `101001…`) matching the streaming writer  
  - **TOP** topics are **semicolon-delimited** (`label1;label2;…`), no brackets
- **Clean layout**: per-run subfolders `.../CSV/` and `.../Parquet/`
- **Deterministic mode**  
  Reproducible runs via a master seed or auto-derived stable seed

---

## 🚀 Quick start

### Requirements
- **Java 22**
- Maven 3.9+
- Internet access to Maven Central

### Build
```bash
mvn -DskipTests=false clean package
```

Artifacts:
```
target/NewBestSub-2.0-jar-with-dependencies.jar
target/NewBestSub-2.0-test-jar-with-dependencies.jar
```

### Run
```bash
java -Xmx4g -jar target/NewBestSub-2.0-jar-with-dependencies.jar --help
```

Example:
```bash
java -Xmx4g -jar target/NewBestSub-2.0-jar-with-dependencies.jar \
  -fi AH99 -c Pearson -t All -po 2000 -i 10000 -r 2000 -pe 1,100 -log Limited
```

---

## 🧪 Minimal samples

You can reproduce a complete run with the tiny toy dataset below.

1) Save this as `samples/toy.csv`:
```csv
, t1, t2, t3, t4, t5
BM25, 0.31, 0.45, 0.22, 0.18, 0.40
QL,   0.28, 0.48, 0.19, 0.21, 0.36
RM3,  0.40, 0.51, 0.26, 0.17, 0.42
```

2) Build:
```bash
mvn clean package -DskipTests=false
```

3) Run a few representative commands (reduce `-i/-po/-r` if needed):

- **BEST (Pearson)**  
```bash
java -Xmx1g -jar target/NewBestSub-2.0-jar-with-dependencies.jar \
  -fi samples/toy -c Pearson -t Best -po 50 -i 5000 -log Limited
```

- **WORST (Pearson)**  
```bash
java -Xmx1g -jar target/NewBestSub-2.0-jar-with-dependencies.jar \
  -fi samples/toy -c Pearson -t Worst -po 50 -i 5000 -log Limited
```

- **AVERAGE (Kendall, 5–95th percentiles, 100 reps)**  
```bash
java -Xmx1g -jar target/NewBestSub-2.0-jar-with-dependencies.jar \
  -fi samples/toy -c Kendall -t Average -r 100 -pe 5,95 -log Limited
```

- **ALL (combine Best/Worst/Average)**  
```bash
java -Xmx1g -jar target/NewBestSub-2.0-jar-with-dependencies.jar \
  -fi samples/toy -c Pearson -t All -po 50 -i 5000 -r 100 -pe 1,100 -log Limited
```

Outputs will be written under a per-run container with `CSV/` and `Parquet/` subfolders. The `-Top-10-Solutions.*` tables will show the top solutions per K with topics **semicolon-delimited**.

---

## 🛠️ Usage: expected outputs

After a run, you will get a per-run container that includes `CSV/` and `Parquet/` subfolders. Typical files:

**CSV**
- `...-Fun.csv` - space separated: `K corr`  
  Example:
  ```
  2 0.873421
  3 0.901112
  4 0.915287
  ```
- `...-Var.csv` - contiguous bitstring for each representative solution  
  Example:
  ```
  2 10101
  3 11001
  4 11101
  ```
- `...-Top-10-Solutions.csv` - comma separated: `Cardinality,Correlation,Topics`  
  Example:
  ```csv
  3,0.912345,t2;t4;t5
  3,0.910112,t1;t3;t5
  3,0.905678,t1;t4;t5
  ```

**Parquet**
- `...-Fun.parquet` with schema `{ K:int, Correlation:double }`
- `...-Var.parquet` with schema `{ K:int, Bits:string }`
- `...-Top-10-Solutions.parquet` with schema `{ K:int, Correlation:double, Topics:string }`

File names are derived from: dataset id, correlation, number of topics and systems, iterations, population, repetitions or executions, and target.

---

## 🗂️ Outputs

Per-run container folder (derived from dataset, correlation, topics, systems, iterations, population, repetitions or executions, and target) containing:

```
.../<run-container>/CSV/
.../<run-container>/Parquet/
```

### CSV
- `...-Fun.csv`: `K corr` (space-separated; `K` int; `corr` with 6 digits; BEST and WORST print the true correlation)  
- `...-Var.csv`: contiguous bitstring for variables  
- `...-Top-10-Solutions.csv`: `Cardinality,Correlation,Topics` (topics **semicolon-delimited**)  
- `...-Final.csv`, `...-Info.csv`: final summary and metadata

### Parquet
- `...-Fun.parquet`: schema `{ K:int, Correlation:double }`  
- `...-Var.parquet`: schema `{ K:int, Bits:string }` (`Bits` is the contiguous bitstring)  
- `...-Top-10-Solutions.parquet`: schema `{ K:int, Correlation:double, Topics:string }` (topics **semicolon-delimited**)  
- `...-Aggregated.parquet`, `...-Info.parquet`: final tables

---

## 📁 Expected folder name pattern

Real runs produce container names like:
```
AH99-Pearson-top50-sys129-po1000-i10000-r2000-time2025-08-22-19-19-40
```

**Pattern (canonical)**
```
<DATASET>-<CORR>-top<Topics>-sys<Systems>-po<Population>-i<Iterations>[-r<Repetitions>][-exec<Executions>][-seed<seed>][-det]-time<YYYY-MM-DD-HH-mm-ss>
```

### Keys

| Token            | Meaning                                | Example                     |
|------------------|----------------------------------------|-----------------------------|
| `<DATASET>`      | Input CSV basename                     | `AH99`                      |
| `<CORR>`         | Correlation method                     | `Pearson`                   |
| `top<Topics>`    | Number of topics (columns)             | `top50`                     |
| `sys<Systems>`   | Number of systems (rows)               | `sys129`                    |
| `po<Population>` | Population size                        | `po1000`                    |
| `i<Iterations>`  | Iterations (Best/Worst/All)            | `i10000`                    |
| `r<Repetitions>` | Repetitions per K (Average/All)        | `r2000`                     |
| `exec<N>`        | Executions merged (if used)            | `exec3`                     |
| `seed<val>`      | Explicit seed (deterministic)          | `seed42`                    |
| `det`            | Deterministic mode (auto-seed)         | `det`                       |
| `time<stamp>`    | Timestamp `YYYY-MM-DD-HH-mm-ss`        | `time2025-08-22-19-19-40`   |

### Examples

- **BEST / Pearson**
  ```
  AH99-Pearson-top50-sys129-po1000-i10000-time2025-08-22-19-19-40
  ```

- **WORST / Pearson / merged, seeded**
  ```
  AH99-Pearson-top50-sys129-po1000-i10000-exec3-seed42-time2025-08-22-19-20-01
  ```

- **AVERAGE / Kendall**
  ```
  AH99-Kendall-top50-sys129-po1000-i0-r2000-time2025-08-22-19-21-12
  ```

**Regex hint** (scan results programmatically):
```
^(.+)-(Pearson|Kendall)-top(\d+)-sys(\d+)-po(\d+)-i(\d+)(?:-r(\d+))?(?:-exec(\d+))?(?:-seed(\d+))?(?:-det)?-time(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})$
```

### Helper: parse run names

Use the CLI helper to parse container names into JSON:
```bash
python tools/parse_run_name.py "AH99-Pearson-top50-sys129-po1000-i10000-r2000-time2025-08-22-19-19-40" --pretty
```

Programmatic usage:
```python
# add repo root to sys.path or place tools/ on PYTHONPATH
from tools.parse_run_name import parse_run_name
info = parse_run_name("AH99-Pearson-top50-sys129-po1000-i10000-r2000-time2025-08-22-19-19-40").to_dict()
```

The helper lives at `tools/parse_run_name.py`.

---

## 🖥️ CLI options

**Required**
- `-fi, --fileIn <file>` input CSV basename (no extension)  
- `-c, --corr <Pearson|Kendall>` correlation method  
- `-t, --targ <Best|Worst|Average|All>` target  
- `-l, --log <Verbose|Limited|Off>` logging level

**Optional (general)**
- `--copy` copy results into `NewBestSub-Experiments` sibling folder  
- `-det, --deterministic` enable deterministic mode  
- `-sd, --seed <long>` master seed (implies deterministic mode)  
- `-mr, --mrg <int>` merge N executions

**Optional (target-specific)**
- `-i, --iter <int>` iterations (Best/Worst/All)  
- `-po, --pop <int>` population size (≥ topics; Best/Worst/All)  
- `-r, --rep <int>` repetitions per K (Average/All)  
- `-pe, --perc <a,b>` percentile range (Average/All)  
- `-et, --expt <int>` fake topics to add per step  
- `-es, --exps <int>` fake systems to add per step  
- `-mx, --max <int>` cap for expansions

---

## 🧷 Deterministic execution

- `--seed <long>` sets the master seed explicitly  
- `--deterministic` enables deterministic mode; if `--seed` is absent, a stable seed is derived from key parameters  
- Effective seed is logged and embedded in the output folder name

---

## 🧩 Architecture overview

- **`DatasetModel`**  
  Loads data, wires correlation and target strategies, runs NSGA-II, and emits streaming events:  
  - `CardinalityResult` → append to FUN/VAR  
  - `TopKReplaceBatch` → replace-block write for TOP  
  - `RunCompleted` → finalize and close writers

- **Streaming NSGA-II wrapper**  
  Subclasses classic jMetal NSGA-II to:
  - call a per-generation hook (`onGen`) at init and after each generation
  - override `replacement(...)` to use **MNDS + local crowding distance**

- **Step 3 incremental evaluation**  
  - Maintains `cachedSumsBySystem` and `lastEvaluatedMask` per solution.  
  - On fixed-K swap mutation, applies one or two topic-column deltas; otherwise computes a mask diff and adjusts sums.  
  - Converts updated sums to subset means and computes correlation (Pearson or Kendall) vs full-set means.

- **Operators**  
  - `BinaryPruningCrossover`: length-safe pruning crossover; guarantees at least one selected topic; uses jMetal RNG  
  - `FixedKSwapMutation`: fixed-cardinality swap with hints used by incremental evaluation

- **Views**  
  `DatasetView` drives `CSVView` and `ParquetView`. Both are streaming-first; on close they flush, globally sort when needed, and write final tables.  
  CSV `-Fun/-Var` canonicalization is handled by a final rewrite step to ensure stable global order.

- **Paths**  
  `ViewPaths` builds canonical per-run folders with `CSV/` and `Parquet/` subdirs.

---

## ⚙️ CI & Releases

- **CI**: GitHub Actions builds on pushes and PRs to `main` and `master` using Java 22 and uploads shaded JARs as workflow artifacts. See [`.github/workflows/ci.yml`](.github/workflows/ci.yml).
- **Releases**: push a tag like `v0.2.0` to trigger a build that **publishes a GitHub Release** and **attaches** the shaded JARs.

Create a release:
```bash
git tag -a v0.2.0 -m "Release v0.2.0"
git push origin v0.2.0
```

The release workflow uses the built-in `GITHUB_TOKEN` with `contents: write` permission; no extra secrets are required.

---

## 🧪 Testing

JUnit 5 with Surefire 3.x:

```bash
mvn -DskipTests=false -Dmaven.test.skip=false -Dsurefire.printSummary=true test
```

Notes:
- Import `org.uma.jmetal.solution.binarysolution.BinarySolution` (jMetal 6.x).  
- Surefire picks up `**/*Test.class`; module path disabled.

---

## ⚙️ Build and logging

Key versions (see `pom.xml`):
- **Kotlin** 2.2.0  
- **Java** 22  
- **jMetal** 6.9.1  
- **Parquet** 1.15.2 / **Hadoop** 3.3.6  
- **Log4j2** 2.24.3  
- **JUnit** 5.13.4

Logging:
- Console and rolling files (`log4j2.xml`)  
- Uses `baseLogFileName` system property for destinations

---

## 🩺 Troubleshooting

- **AIOOBE in crossover**  
  Fixed by a length-safe `BinaryPruningCrossover` that caps loops to the smallest bit-vector length and repairs empty children by forcing one selected topic.
- **`.crc` sidecars with Parquet**  
  Local writes use Hadoop `RawLocalFileSystem` to avoid `.crc`.
- **Multiple SLF4J bindings**  
  Avoid `slf4j-log4j12`; this project uses Log4j2 bindings.
- **BEST sign confusion**  
  Internal negatives are flipped back on output; reported correlations are always the true values.
- **TOP seems incomplete with few iterations**  
  TOP blocks are replaced only when at least 10 entries exist for a given K; with small runs some Ks will not emit a block yet.

---

## 🧭 Changelog (2025-08-22)

- Migrated to **jMetal 6.9.x** and Kotlin 2.2, Java 22 toolchain  
- Added streaming NSGA-II wrapper with per-generation progress  
- Integrated **MNDS** in environmental selection with local crowding distance  
- Implemented **Step 3 incremental evaluation** (subset-mean delta updates with swap hints)  
- Hardened operators: length-safe pruning crossover; fixed-K swap mutation with feasibility repair  
- Unified **VAR** format to a contiguous bitstring (CSV and Parquet)  
- Standardized **6-digit correlation precision** across CSV and Parquet  
- **TOP** topics now semicolon-delimited in all outputs  
- Polished logging, output paths, and finalization order

---

## 📚 Citation

If you use this software in academic work, please cite:
- Kevin Roitero, Michael Soprano, **Andrea Brunello**, and Stefano Mizzaro. *Reproduce and Improve: An Evolutionary Approach to Select a Few Good Topics for Information Retrieval Evaluation*. **ACM Journal of Data and Information Quality (JDIQ)** 10(3), Article 12, 12:1–12:21, September 2018. https://doi.org/10.1145/3239573
- Kevin Roitero, Michael Soprano, and Stefano Mizzaro. *Effectiveness Evaluation with a Subset of Topics: A Practical Approach*. In **Proceedings of the 41st International ACM SIGIR Conference on Research and Development in Information Retrieval (SIGIR ’18)**, Ann Arbor, MI, USA, July 8–12, 2018, pp. 1145–1148. https://doi.org/10.1145/3209978.3210108

---

## 📝 License

© University of Udine. See `LICENSE`.
