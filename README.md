# NewBestSub

Efficient topic-set reduction for IR evaluation using a multi-objective evolutionary algorithm (NSGA-II). This repo reproduces and extends results on selecting small topic subsets that preserve the system ranking induced by the full set.

> JDIQ 2018: <https://doi.org/10.1145/3239573>  
> SIGIR 2018: <https://dl.acm.org/citation.cfm?doid=3209978.3210108>

---

## ✨ What’s inside

- **BEST / WORST / AVERAGE** experiments with **NSGA-II (jMetal 5.10)**.
- **Streaming I/O** for results:
  - **FUN / VAR**: append during the run; on close we **globally sort** by **(K asc, corr asc)** to keep files aligned and stable.
  - **TOP**: **replace-batch** semantics — for each K we always write the **exact 10 lines**, replacing the block when it changes.
- **Dual outputs**:
  - **CSV** via `CSVView` (streaming-first).
  - **Parquet** via `ParquetView` (streaming-first, **no coupling** to CSV).
- **Consistent formatting**:
  - Correlations serialized with **6-digit precision** in **both CSV and Parquet**.
  - **VAR** stores only the **topic labels set to 1**, **pipe-delimited** (`label1|label2|…`), not the full bitstring.
  - **TOP** topics are pipe-delimited too (no square brackets).
- **Clean layout**: per-run subfolders `.../CSV/` and `.../Parquet/`.
- **Robust Parquet**:
  - Snappy compression, **OVERWRITE** semantics.
  - Uses Hadoop **RawLocalFileSystem** to avoid local `.crc` sidecar files.

---

## 🚀 Quick start

### Requirements
- **Java 21**
- **Maven 3.9+**
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

> CLI flags: dataset path, target (BEST/WORST/AVERAGE), correlation method (PEARSON/KENDALL), iterations, population, repetitions, etc.  
> See `it.uniud.newbestsub.program.Program` for all options.

---

## 📦 Input dataset format

CSV with header row:

- First row: `,<topic_1>,<topic_2>,...,<topic_n>`
- Then one row per system: `<system_id>,<AP_t1>,<AP_t2>,...,<AP_tn>`

Example (3 topics):
```csv
, t1, t2, t3
BM25, 0.31, 0.45, 0.22
QL,   0.28, 0.48, 0.19
RM3,  0.40, 0.51, 0.26
```

---

## 🧠 Targets & objectives

- **BEST**  
  Internal search uses a sign flip for corr; **outputs always contain the true correlation**.
- **WORST**  
  Internal search negates K; **outputs contain the true K and correlation**.
- **AVERAGE**  
  One pass per cardinality K; streams directly.

> Reporting is always the **external view**; you’ll see positive correlations growing toward 1.0 as K→N.

---

## 🗂️ Outputs

All output files are placed under a **per-run container folder** (constructed from dataset name, correlation, topics, systems, iterations, population, repetitions/exec, and target), then split into:

```
.../<run-container>/CSV/
.../<run-container>/Parquet/
```

### CSV files
- **Function values** (`.../CSV/...-Fun.csv`)  
  Space-separated: `K corr`, **K as integer**, `corr` with **6 digits**.
- **Variable values** (`.../CSV/...-Var.csv`)  
  `label1|label2|...` for topics with bit=1 (pipe-delimited).
- **Top solutions** (`.../CSV/...-Top.csv`)  
  Header: `Cardinality,Correlation,Topics`.  
  Topics are pipe-delimited, **no brackets**. Exactly **10 rows per K**.
- **Aggregated / Info** (`.../CSV/...-Aggregated.csv`, `.../CSV/...-Info.csv`)  
  Final tables for analysis/plots.

### Parquet files
- **Function values** (`.../Parquet/...-Fun.parquet`)  
  Schema: `message Fun { required int32 K; required double Correlation; }`  
  Correlation stored with **6-digit precision**.
- **Variable values** (`.../Parquet/...-Var.parquet`)  
  Schema: `message Var { required int32 K; required binary Labels (UTF8); }`  
  `Labels` is the pipe-delimited set with bit=1.
- **Top solutions** (`.../Parquet/...-Top.parquet`)  
  Schema: `message Top { required int32 K; required double Correlation; required binary Topics (UTF8); }`  
  `Topics` is pipe-delimited, **no brackets**, 10 entries per K.
- **Aggregated / Info** (`.../Parquet/...-Aggregated.parquet`, `.../Parquet/...-Info.parquet`)  
  Written via a generic **header-driven** table writer.

> Parquet writing uses Hadoop’s `RawLocalFileSystem` to suppress `.crc` files and Snappy compression for size/speed.

---

## 🧩 Architecture overview

- **`DatasetModel`**  
  Loads data, manages run parameters, runs NSGA-II, emits **streaming progress events**:
  - `CardinalityResult` (append to FUN/VAR),
  - `TopKReplaceBatch` (replace-block write for TOP),
  - `RunCompleted`.
- **`DatasetView` (composite façade)**  
  Fans out to:
  - **`CSVView`** – streaming-first CSV; owns buffering + canonical sort/rewrite.
  - **`ParquetView`** – streaming-first Parquet; independent buffers/lifecycle.
  Public API:
  - `print(runResult, model)`,
  - `appendCardinality(model, event)`,
  - `replaceTopBatch(model, blocks)`,
  - `closeStreams(model)`.
  Plus helpers: `writeCsv(rows, path)` and `writeParquet(rows, path)` for **Final tables**.
- **`ViewPaths`**  
  Canonical run folders + path builders, ensuring the `CSV/` and `Parquet/` subfolders.

### Streaming details

- **FUN/VAR (CSV & Parquet)**  
  - Append during the run for visibility.  
  - Maintain in-memory buffer per `(dataset, exec, target)`.  
  - On `closeStreams`: **globally sort** by `(K asc, corr asc)` and **rewrite** to produce aligned, stable files.
- **TOP (CSV & Parquet)**  
  - Cache per-K blocks (exact 10 lines).  
  - On replace batches, rewrite the full TOP file (header + K-ordered blocks).  
  - Finalize again at close to guarantee completeness.

### Precision & formatting

- Correlations always serialized with **6-digit precision** in CSV **and** Parquet.  
- Topics use **pipe `|`** as a delimiter; **no square brackets** in any output.

---

## 🧪 Testing

We use **JUnit 5** + **Surefire 3.x**.

```bash
mvn -DskipTests=false -Dmaven.test.skip=false -Dsurefire.printSummary=true test
```

Tips:
- Ensure you import `org.uma.jmetal.solution.binarysolution.BinarySolution` (5.10).
- Surefire config disables module path and matches `**/*Test.class`.

---

## ⚙️ Build & logging

Key build deps (see `pom.xml`):
- Kotlin **2.2.0**
- jMetal **5.10**
- Parquet **1.15.2**
- Hadoop **3.3.6**
- Log4j2 **2.24.3**
- JUnit **5.13.4**

Logging (`log4j2.xml`):
- Console + rolling file appenders.
- Uses `baseLogFileName` system property for file destinations.

---

## 🩺 Troubleshooting

- **`.crc` files appear next to Parquet outputs**  
  We set `fs.file.impl=org.apache.hadoop.fs.RawLocalFileSystem` to avoid them for local writes.
- **Multiple SLF4J bindings warning**  
  Avoid mixing slf4j-log4j12; we use **Log4j2** bindings.
- **Opposite sign correlations in BEST**  
  Internal search may negate corr; **outputs always store true (positive) corr**.
- **Empty VAR for AVERAGE**  
  Fixed — we serialize pipe-delimited labels for bits set to 1 for all targets.

---

## 🔌 Extending

- Plug your own correlation function in `DatasetModel.loadCorrelationMethod`.
- Swap crossover/mutation operators; keep `BinarySolution` API.
- Add new Final tables with `DatasetView.writeCsv(...)` and `DatasetView.writeParquet(...)`.

---

## 📚 Citation

If you use this software in academic work, please cite:

- M. Soprano, K. Roitero, S. Mizzaro. **Best Topic Subsets for IR Evaluation**. *JDIQ*, 2018. <https://doi.org/10.1145/3239573>  
- SIGIR 2018 short version: <https://dl.acm.org/citation.cfm?doid=3209978.3210108>

---

## 📝 License

© University of Udine. See `LICENSE` (or add your preferred license).
