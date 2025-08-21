# NewBestSub

Efficient topic-set reduction for IR evaluation using a multi-objective evolutionary algorithm (NSGA-II). This repo reproduces and extends results on selecting small topic subsets that preserve the system ranking induced by the full set.

> JDIQ 2018: <https://doi.org/10.1145/3239573>  
> SIGIR 2018: <https://dl.acm.org/citation.cfm?doid=3209978.3210108>

---

## ✨ What’s inside

- **BEST / WORST / AVERAGE** experiments with **NSGA-II (jMetal 6.9.x)**
- **Streaming NSGA-II wrapper**  
  Per-generation callback to stream progress:
  - **FUN / VAR:** append only *improvements per K* (kept sorted per batch for stable growth); final global sort is handled at close by the view layer.
  - **TOP:** exact 10-line blocks per K; replace a block only when it changes.
- **Environmental selection via MNDS**  
  NSGA-II replacement uses **MergeNonDominatedSortRanking (MNDS)** plus local crowding distance to fill the last partial front.
- **Dual outputs**  
  - **CSV** (streaming-first)  
  - **Parquet** (streaming-first, Snappy, no CSV coupling)
- **Consistent formatting**  
  - Correlations serialized with 6-digit precision in both CSV and Parquet  
  - **VAR** stores a **contiguous bitstring** (e.g., `101001…`) matching the streaming writer  
  - **TOP** topics are pipe-delimited (`label1|label2|…`), no brackets
- **Clean layout**: per-run subfolders `.../CSV/` and `.../Parquet/`
- **Deterministic mode**  
  Reproducible runs via a master seed or auto-derived stable seed

---

## 🚀 Quick start

### Requirements
- Java 21
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
java -Xmx4g -jar target/NewBestSub-2.0-jar-with-dependencies.jar   -fi AH99 -c Pearson -t All -po 2000 -i 10000 -r 2000 -pe 1,100 -log Limited
```

---

## 🧩 Architecture overview

- **`DatasetModel`**  
  Loads data, wires correlation/target strategies, runs NSGA-II, and emits streaming events:  
  - `CardinalityResult` → append to FUN/VAR  
  - `TopKReplaceBatch` → replace-block write for TOP  
  - `RunCompleted` → finalize/close writers

- **Streaming NSGA-II wrapper**  
  Subclasses classic jMetal NSGA-II to:
  - call a per-generation hook (`onGen`) at init and after each generation
  - override `replacement(...)` to use **MNDS + local crowding distance**

- **Operators**  
  - `BinaryPruningCrossover`: length-safe AND/OR crossover; guarantees at least one selected topic; uses jMetal RNG  
  - `BitFlipMutation`: length-safe single-bit toggle with the same feasibility guarantee

- **Views**  
  `DatasetView` drives `CSVView` and `ParquetView`. Both are streaming-first; on close they flush, globally sort when needed, and write final tables.

- **Paths**  
  `ViewPaths` builds canonical per-run folders with `CSV/` and `Parquet/` subdirs.

### Streaming details

- **FUN / VAR**  
  During the run, only improved representatives per K are appended. Each generation’s batch is sorted by **K asc** and by **correlation** (BEST: asc for external view; WORST: desc for external view). Final global sort occurs on close.

- **TOP**  
  Always exactly 10 rows per K (corr asc). On change, the corresponding K block is atomically replaced.

### Precision and formatting

- Correlations: 6 decimal digits everywhere  
- **VAR**: contiguous bitstring (no brackets)  
- **TOP**: topic labels pipe-delimited (`label1|label2|…`), no brackets

---

## 🗂️ Outputs

Per-run container folder (derived from dataset, correlation, topics, systems, iterations, population, repetitions/executions, and target) containing:

```
.../<run-container>/CSV/
.../<run-container>/Parquet/
```

### CSV
- `...-Fun.csv`: `K corr` (space-separated; `K` int; `corr` with 6 digits)
- `...-Var.csv`: contiguous bitstring for variables
- `...-Top-10-Solutions.csv`: `Cardinality,Correlation,Topics` (topics pipe-delimited)
- `...-Final.csv`, `...-Info.csv`: final summary/metadata

### Parquet
- `...-Fun.parquet`: schema `{ K:int, Correlation:double }`
- `...-Var.parquet`: schema `{ K:int, Bits:string }` (`Bits` is the contiguous bitstring)
- `...-Top-10-Solutions.parquet`: schema `{ K:int, Correlation:double, Topics:string }` (topics pipe-delimited)
- `...-Aggregated.parquet`, `...-Info.parquet`: final tables

---

## 🧠 Targets & objectives (external view)

- **BEST**: maximize correlation; reports true `K` and true correlation  
- **WORST**: minimize correlation; reports true `K` and true correlation  
- **AVERAGE**: single pass per K with repetitions; streamed directly  
- **ALL**: runs BEST, WORST, and AVERAGE

Internal signs may be flipped to simplify search; outputs are always unflipped (human-readable).

---

## 📦 Input dataset format

CSV with header row:

- Header: `,<topic_1>,<topic_2>,...,<topic_n>`
- One row per system: `<system_id>,<AP_t1>,<AP_t2>,...,<AP_tn>`

Example:
```csv
, t1, t2, t3
BM25, 0.31, 0.45, 0.22
QL,   0.28, 0.48, 0.19
RM3,  0.40, 0.51, 0.26
```

---

## 🧷 Deterministic execution

- `--seed <long>` sets the master seed explicitly  
- `--deterministic` enables deterministic mode; if `--seed` is absent, a stable seed is derived from key parameters  
- Effective seed is logged and embedded in the output folder name

---

## 🖥️ CLI options

**Required**
- `-fi, --fileIn <file>` input CSV *basename* (no extension)  
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

## 🧪 Testing

JUnit 5 with Surefire 3.x:

```bash
mvn -DskipTests=false -Dmaven.test.skip=false -Dsurefire.printSummary=true test
```

Notes:
- Import `org.uma.jmetal.solution.binarysolution.BinarySolution` (jMetal 6.x).
- Surefire picks up `**/*Test.class`; module path disabled.

---

## ⚙️ Build & logging

Key versions (see `pom.xml`):
- **Kotlin** 2.2.0  
- **Java** 21  
- **jMetal** 6.9.1  
- **Parquet** 1.15.2 / **Hadoop** 3.3.6  
- **Log4j2** 2.24.3  
- **JUnit** 5.13.4

Logging:
- Console + rolling files (`log4j2.xml`)
- Uses `baseLogFileName` system property for destinations

---

## 🩺 Troubleshooting

- **AIOOBE in crossover**  
  Fixed by a **length-safe** `BinaryPruningCrossover` that caps loops to the smallest bit-vector length and repairs empty children by forcing one selected topic.
- **`.crc` sidecars with Parquet**  
  Local writes use Hadoop `RawLocalFileSystem` to avoid `.crc`.
- **Multiple SLF4J bindings**  
  Avoid `slf4j-log4j12`; this project uses Log4j2 bindings.
- **BEST sign confusion**  
  Internal negatives are flipped back on output; reported correlations are always the true values.

---

## 🧭 Changelog (2025-08-21)

- Migrated to **jMetal 6.9.x** and Kotlin 2.2, Java 21 toolchain  
- Added **streaming NSGA-II wrapper** with per-generation progress  
- Integrated **MNDS** (`MergeNonDominatedSortRanking`) in environmental selection with local crowding distance  
- Hardened operators: length-safe pruning crossover; feasibility repair in crossover & mutation  
- Unified **VAR** format to a contiguous bitstring (CSV/Parquet)  
- Polished logging, output paths, and finalization order

---

## 📚 Citation

If you use this software in academic work, please cite:

- M. Soprano, K. Roitero, S. Mizzaro. **Best Topic Subsets for IR Evaluation**. *JDIQ*, 2018. <https://doi.org/10.1145/3239573>  
- SIGIR 2018 short version: <https://dl.acm.org/citation.cfm?doid=3209978.3210108>

---

## 📝 License

© University of Udine. See `LICENSE`.
