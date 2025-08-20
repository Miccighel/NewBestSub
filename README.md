# NewBestSub

Efficient topic-set reduction for IR evaluation using a multi-objective evolutionary algorithm (NSGA‑II). This repo reproduces and extends results on selecting small topic subsets that preserve the system ranking induced by the full set.

> JDIQ 2018: <https://doi.org/10.1145/3239573>  
> SIGIR 2018: <https://dl.acm.org/citation.cfm?doid=3209978.3210108>

---

## ✨ What’s inside

- **BEST / WORST / AVERAGE** experiments with **NSGA‑II (jMetal 5.10)**
- **Streaming I/O** for results  
  - **FUN / VAR**: append during the run; on close, globally sort by **(K asc, corr asc)** for aligned and stable files  
  - **TOP**: replace‑batch semantics, for each K always write the exact 10 lines, replacing the block when it changes
- **Dual outputs**  
  - **CSV** via `CSVView` (streaming‑first)  
  - **Parquet** via `ParquetView` (streaming‑first, no coupling to CSV)
- **Consistent formatting**  
  - Correlations serialized with 6‑digit precision in both CSV and Parquet  
  - **VAR** stores only the topic labels set to 1, pipe‑delimited (`label1|label2|…`), not the full bitstring  
  - **TOP** topics are pipe‑delimited too, no brackets
- **Clean layout**: per‑run subfolders `.../CSV/` and `.../Parquet/`
- **Robust Parquet**  
  - Snappy compression, overwrite semantics  
  - Uses Hadoop `RawLocalFileSystem` to avoid local `.crc` sidecar files
- **Deterministic mode**  
  - Reproducible runs by fixing the master seed via `--seed` or by enabling `--deterministic`, which derives a stable seed from key parameters

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

This produces:

```
target/NewBestSub-2.0-jar-with-dependencies.jar
```

### Run
```bash
java -Xmx4g -jar target/NewBestSub-2.0-jar-with-dependencies.jar --help
```

Example run:
```bash
java -Xmx4g -jar target/NewBestSub-2.0-jar-with-dependencies.jar \
  --fileIn data/TREC8/AP \
  --corr Pearson \
  --targ Best \
  --log Verbose \
  --iter 50 \
  --pop 200 \
  --deterministic --seed 1337
```

---

## 🧩 Architecture overview

- **`DatasetModel`**  
  Loads data, manages run parameters, runs NSGA‑II, emits streaming progress events:  
  `CardinalityResult` (append to FUN/VAR), `TopKReplaceBatch` (replace‑block write for TOP), `RunCompleted`.
- **`DatasetView` (composite façade)**  
  Fans out to **`CSVView`** (streaming‑first CSV) and **`ParquetView`** (streaming‑first Parquet).  
  Public API: `print(runResult, model)`, `appendCardinality(model, event)`, `replaceTopBatch(model, blocks)`, `closeStreams(model)`.  
  Helpers: `writeCsv(rows, path)` and `writeParquet(rows, path)` for final tables.
- **`ViewPaths`**  
  Canonical run folders and path builders that ensure the `CSV/` and `Parquet/` subfolders.

### Streaming details

- **FUN/VAR (CSV and Parquet)**  
  Append during the run for visibility, maintain an in‑memory buffer per `(dataset, exec, target)`, and on `closeStreams` globally sort by `(K asc, corr asc)` before the final write.
- **TOP (CSV and Parquet)**  
  Cache per‑K blocks (exact 10 lines). On replace batches rewrite the full TOP file (header plus K‑ordered blocks). Finalize again at close to guarantee completeness.

### Precision and formatting

- Correlations are serialized with 6‑digit precision in CSV and Parquet.  
- Topics use the pipe `|` delimiter, with no brackets in any output.

---

## 🗂️ Outputs

All output files are placed under a per‑run container folder (constructed from dataset name, correlation, topics, systems, iterations, population, repetitions or executions, and target), then split into:

```
.../<run-container>/CSV/
.../<run-container>/Parquet/
```

### CSV files
- **Function values** (`.../CSV/...-Fun.csv`)  
  Space‑separated: `K corr`, with `K` as integer and `corr` with 6 digits.
- **Variable values** (`.../CSV/...-Var.csv`)  
  Topic labels with bit=1, pipe‑delimited.
- **Top solutions** (`.../CSV/...-Top.csv`)  
  Header: `Cardinality,Correlation,Topics`. Topics are pipe‑delimited, 10 rows per K.
- **Aggregated / Info** (`.../CSV/...-Aggregated.csv`, `.../CSV/...-Info.csv`)  
  Final tables for analysis and plots.

### Parquet files
- **Function values** (`.../Parquet/...-Fun.parquet`)  
  Schema: `message Fun { required int32 K; required double Correlation; }`
- **Variable values** (`.../Parquet/...-Var.parquet`)  
  Schema: `message Var { required int32 K; required binary Labels (UTF8); }` where `Labels` is the pipe‑delimited set with bit=1.
- **Top solutions** (`.../Parquet/...-Top.parquet`)  
  Schema: `message Top { required int32 K; required double Correlation; required binary Topics (UTF8); }` where `Topics` is pipe‑delimited, 10 entries per K.
- **Aggregated / Info** (`.../Parquet/...-Aggregated.parquet`, `.../Parquet/...-Info.parquet`)  
  Written via a generic header‑driven table writer.

---

## 🧠 Targets and objectives

- **BEST**  
  Internal search may use a sign flip for correlation. All outputs contain the true correlation.
- **WORST**  
  Internal search negates K. Outputs contain the true K and correlation.
- **AVERAGE**  
  One pass per cardinality K, streamed directly.
- **ALL**  
  Runs BEST, WORST, and AVERAGE in one execution.

Reporting is always the external view. Correlations grow toward 1.0 as K approaches N.

---

## 📦 Input dataset format

CSV with header row:

- First row: `,<topic_1>,<topic_2>,...,<topic_n>`
- Then one row per system: `<system_id>,<AP_t1>,<AP_t2>,...,<AP_tn>`

Example with 3 topics:
```csv
, t1, t2, t3
BM25, 0.31, 0.45, 0.22
QL,   0.28, 0.48, 0.19
RM3,  0.40, 0.51, 0.26
```

---

## 🧷 Deterministic execution

Use this mode to make runs reproducible.

- `--seed <long>` sets the master seed explicitly.  
- `--deterministic` enables deterministic mode; if `--seed` is not provided, a stable seed is derived from key parameters.  
- The effective seed is logged at startup and embedded in the output folder name.

---

## 🖥️ CLI options

All flags support a short and a long form. Required flags depend on the selected target.

### Required

- `-fi, --fileIn <file>`  
  Relative path to the CSV dataset file, without extension.

- `-c, --corr <method>`  
  Correlation method. Available: `Pearson`, `Kendall`.

- `-t, --targ <target>`  
  Target to run. Available: `Best`, `Worst`, `Average`, `All`.

- `-l, --log <level>`  
  Logging level. Available: `Verbose`, `Limited`, `Off`.

### Optional, general

- `--copy`  
  Copy results of the current execution into `NewBestSub-Experiments` under the same base folder. Requires the following folder layout to exist: `baseFolder/NewBestSub/...` and `baseFolder/NewBestSub-Experiments/...`.

- `-det, --deterministic`  
  Enable deterministic execution. If used without `--seed`, a stable seed is derived from key parameters.

- `-sd, --seed <long>`  
  Explicit master seed for deterministic execution. Implies `--deterministic`.

- `-mr, --mrg <int>`  
  Number of executions to merge. Must be a positive integer.

### Optional, target‑specific

- `-i, --iter <int>`  
  Number of iterations. Used for `Best`, `Worst`, `All`.

- `-po, --pop <int>`  
  Initial population size. Must be an integer, greater than or equal to the number of topics, and greater than the value used for `--max` if that option is set. Used for `Best`, `Worst`, `All`.

- `-r, --rep <int>`  
  Number of repetitions per cardinality when running `Average`. Must be a positive integer. Used for `Average`, `All`.

- `-pe, --perc <a,b>`  
  Percentile range to compute, as two comma‑separated integers, for example `-pe 1,100`. Used for `Average`, `All`.

- `-et, --expt <int>`  
  Number of fake topics to add at each iteration. Must be a positive integer.

- `-es, --exps <int>`  
  Number of fake systems to add at each iteration. Must be a positive integer.

- `-mx, --max <int>`  
  Maximum number of fake topics or systems to reach when using expansion options.

---

## 🧪 Testing

JUnit 5 with Surefire 3.x.

```bash
mvn -DskipTests=false -Dmaven.test.skip=false -Dsurefire.printSummary=true test
```

Tips:
- Import `org.uma.jmetal.solution.binarysolution.BinarySolution` for jMetal 5.10.
- Surefire configuration matches `**/*Test.class` and disables the module path.

---

## ⚙️ Build and logging

Key build dependencies (see `pom.xml`):

- Kotlin 2.2.0  
- jMetal 5.10  
- Parquet 1.15.2  
- Hadoop 3.3.6  
- Log4j2 2.24.3  
- JUnit 5.13.4

**Logging** (`log4j2.xml`):
- Console and rolling file appenders.
- Uses the `baseLogFileName` system property for file destinations.

---

## 🩺 Troubleshooting

- **`.crc` files appear next to Parquet outputs**  
  We set `fs.file.impl=org.apache.hadoop.fs.RawLocalFileSystem` to avoid them for local writes.
- **Multiple SLF4J bindings warning**  
  Avoid mixing `slf4j-log4j12`; we use Log4j2 bindings.
- **Opposite‑sign correlations in BEST**  
  Internal search may negate correlation; outputs always store the true (positive) correlation.
- **Empty VAR for AVERAGE**  
  Fixed: we serialize pipe‑delimited labels for bits set to 1 for all targets.

---

## 🔌 Extending

- Plug your own correlation function in `DatasetModel.loadCorrelationMethod(...)`.
- Swap crossover/mutation operators; keep the `BinarySolution` API.
- Add new final tables with `DatasetView.writeCsv(...)` and `DatasetView.writeParquet(...)`.

---

## 📚 Citation

If you use this software in academic work, please cite:

- M. Soprano, K. Roitero, S. Mizzaro. **Best Topic Subsets for IR Evaluation**. *JDIQ*, 2018. <https://doi.org/10.1145/3239573>  
- SIGIR 2018 short version: <https://dl.acm.org/citation.cfm?doid=3209978.3210108>

---

## 📝 License

© University of Udine. See `LICENSE`.
