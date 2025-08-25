# NewBestSub

Efficient topic-set reduction for IR evaluation using a multi-objective evolutionary algorithm (NSGA-II).  
Select small topic subsets that preserve the **system ranking** induced by the full set.

[![Docs](https://img.shields.io/badge/docs-website-blue)](https://miccighel.github.io/NewBestSub/)
[![CI](https://github.com/Miccighel/NewBestSub/actions/workflows/ci.yml/badge.svg)](https://github.com/Miccighel/NewBestSub/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Miccighel/NewBestSub)](https://github.com/Miccighel/NewBestSub/releases)
[![License](https://img.shields.io/github/license/Miccighel/NewBestSub)](LICENSE)

![Java 22](https://img.shields.io/badge/Java-22-007396?logo=openjdk&logoColor=white)
![Kotlin 2.2](https://img.shields.io/badge/Kotlin-2.2-7F52FF?logo=kotlin&logoColor=white)
![jMetal 6.9.1](https://img.shields.io/badge/jMetal-6.9.1-0A7BBB)
![Parquet 1.15.2](https://img.shields.io/badge/Parquet-1.15.2-50A9E5)
![Deterministic mode](https://img.shields.io/badge/Reproducible-Deterministic%20mode-brightgreen)

![Last Commit](https://img.shields.io/github/last-commit/Miccighel/NewBestSub)
![Contributors](https://img.shields.io/github/contributors/Miccighel/NewBestSub)
![Open Issues](https://img.shields.io/github/issues/Miccighel/NewBestSub)
![Pull Requests](https://img.shields.io/github/issues-pr/Miccighel/NewBestSub)
![GitHub stars](https://img.shields.io/github/stars/Miccighel/NewBestSub?style=social)
![Downloads](https://img.shields.io/github/downloads/Miccighel/NewBestSub/total)

> JDIQ 2018: <https://doi.org/10.1145/3239573> • SIGIR 2018: <https://doi.org/10.1145/3209978.3210108>

---

> ⚠️ **Important notice: Migration from 1.0 to 2.0**
>
> Starting with version 2.0, NewBestSub has undergone major refactoring:
> - Requires Java 22 and Kotlin 2.2 (previously Java 8 and Kotlin 1.x).
> - Output format is now streaming-based with FUN, VAR, and TOP separation and 6-digit correlation precision.
> - New deterministic mode and reproducible folder naming pattern.
> - Updated CLI options (-fi, -c, -t, and others) see [CLI options](#cli-options).
> - Artifacts renamed: `NewBestSub-2.0-jar-with-dependencies.jar` instead of `NewBestSub-1.0-...`.
>
> If you have scripts or pipelines based on 1.0, they will not work unmodified.
> Please adapt your datasets, CLI calls, and parsing code to the new schema.

---

## Table of contents

- [Features](#features)
- [Requirements](#requirements)
- [Build](#build)
- [Quick start](#quick-start)
- [Dataset schema](#dataset-schema)
- [Outputs](#outputs)
  - [CSV](#csv)
  - [Parquet](#parquet)
  - [VAR/TOP Base64 layout](#vartop-base64-layout)
  - [Decoding snippets](#decoding-snippets)
  - [Additional notes](#additional-notes)
- [CLI options](#cli-options)
- [Deterministic execution](#deterministic-execution)
- [Folder naming pattern](#folder-naming-pattern)
- [Architecture overview](#architecture-overview)
- [Testing](#testing)
- [Build and logging](#build-and-logging)
- [Troubleshooting](#troubleshooting)
- [Changelog](#changelog)
- [Citation](#citation)
- [License](#license)

---

## Features

- BEST, WORST, and AVERAGE experiments powered by NSGA-II (jMetal 6.9.x).
- Streaming engine:
  - FUN and VAR: append only when the per-K representative improves for Best and Worst; exactly one row per K for Average.
  - TOP: replace the entire per-K block on change (up to 10 rows), keeping output stable.
- MNDS environmental selection (Merge Non-Dominated Sort) plus local crowding distance.
- Incremental evaluation ("Step 3"): O(S) updates via cached per-system sums and fixed-K swap hints.
- Compact genotype I/O: VAR and mask-based TOP carry packed Base64 bitmasks (see layout below).
- Dual outputs: streaming-first CSV and Parquet; consistent 6-digit precision for correlations.
- Deterministic mode: explicit `--seed` or stable derived seed from run parameters.

---

## Requirements

- Java 22, Maven 3.9 or later
- Internet access to Maven Central

---

## Build

```bash
mvn -DskipTests=false clean package
```

Artifacts:
```
target/NewBestSub-2.0-jar-with-dependencies.jar
target/NewBestSub-2.0-test-jar-with-dependencies.jar
```

---

## Run from release

Instead of building from source, you can download a ready-to-use JAR:

1. Go to the [Releases page](https://github.com/Miccighel/NewBestSub/releases).
2. Download the asset named:

   ```
   NewBestSub-<version>-jar-with-dependencies.jar
   ```

   (e.g., `NewBestSub-2.0-jar-with-dependencies.jar`).

3. Run it with Java 22:

   ```bash
   java -Xmx4g -jar NewBestSub-2.0-jar-with-dependencies.jar --help
   ```

4. Example toy run:

   ```bash
   java -Xmx1g -jar NewBestSub-2.0-jar-with-dependencies.jar -fi samples/toy -c Pearson -t Best -po 50 -i 5000 -log Limited
   ```


---

## Quick start

Show help:
```bash
java -Xmx4g -jar target/NewBestSub-2.0-jar-with-dependencies.jar --help
```

Small toy run (adjust -i, -po, and -r to your machine):
```bash
# BEST (Pearson)
java -Xmx1g -jar target/NewBestSub-2.0-jar-with-dependencies.jar   -fi samples/toy -c Pearson -t Best -po 50 -i 5000 -log Limited

# WORST (Pearson)
java -Xmx1g -jar target/NewBestSub-2.0-jar-with-dependencies.jar   -fi samples/toy -c Pearson -t Worst -po 50 -i 5000 -log Limited

# AVERAGE (Kendall, 5 to 95th percentiles, 100 reps)
java -Xmx1g -jar target/NewBestSub-2.0-jar-with-dependencies.jar   -fi samples/toy -c Kendall -t Average -r 100 -pe 5,95 -log Limited

# ALL (combine Best, Worst, and Average)
java -Xmx1g -jar target/NewBestSub-2.0-jar-with-dependencies.jar   -fi samples/toy -c Pearson -t All -po 50 -i 5000 -r 100 -pe 1,100 -log Limited
```

Tip: on large runs ensure -Xmx is sized appropriately, and keep `-po >= #topics`.

---

## Dataset schema

CSV layout (wide table, systems by topics of AP values):

- Header row: first cell is empty; remaining cells are topic labels (`t1,t2,...`).
- Rows: first cell is a system label; remaining cells are Average Precision (AP) values for each topic.
- AP range: expected `0.0 .. 1.0` (no hard clamp). Missing values are not allowed.
- Order: row and column order is preserved and used in outputs.
- Constraint: when running Best, Worst, or All, population must satisfy `-po >= #topics`.

Example (`samples/toy.csv`):
```csv
, t1, t2, t3, t4, t5
BM25, 0.31, 0.45, 0.22, 0.18, 0.40
QL,   0.28, 0.48, 0.19, 0.21, 0.36
RM3,  0.40, 0.51, 0.26, 0.17, 0.42
```

---

## Outputs

Results are written into a per-run container named from parameters (see Folder naming pattern).
Each container has `CSV/` and `Parquet/` subfolders.

---

### CSV

- `...-Fun.csv` contains `K corr` (space-separated; `K` integer; `corr` with 6 digits; always natural correlation).
- `...-Var.csv` contains `K B64:<payload>` where `<payload>` is the packed Base64 mask.
- `...-Top-10-Solutions.csv` is either:
  - `K,Correlation,B64:<payload>` (mask-based), or
  - `K,Correlation,topic1;topic2;...` (label-based), depending on the view configuration.
- `...-Final.csv` and `...-Info.csv` contain summaries and metadata.

### Parquet

- `...-Fun.parquet` schema: `{ K:int, Correlation:double }`
- `...-Var.parquet` schema: `{ K:int, Mask:string }` (bare Base64, no `B64:` prefix)
- `...-Top-10-Solutions.parquet` schema: `{ K:int, Correlation:double, MaskOrTopics:string }`

### VAR/TOP Base64 layout

We pack the topic-presence mask into Base64 as follows:

- Pack bits into 64-bit words, LSB-first within each word (bit 0 becomes topic 0).
- Serialize each 64-bit word as little-endian bytes.
- Encode the concatenated bytes using Base64 without padding.
- CSV uses a `B64:` prefix; Parquet stores the bare payload.

This layout is used by:
- `-Var` rows (one per representative solution)
- mask-based `-Top-10-Solutions` rows (if the writer is configured that way)

### Decoding snippets

**Kotlin** (mirror of production code):

```kotlin
fun decodeMaskFromBase64(b64OrPrefixed: String, expectedSize: Int): BooleanArray {
    val payload = if (b64OrPrefixed.startsWith("B64:")) b64OrPrefixed.substring(4) else b64OrPrefixed
    val raw = java.util.Base64.getDecoder().decode(payload)
    val out = BooleanArray(expectedSize)

    var bitAbsolute = 0
    var offset = 0
    while (offset < raw.size && bitAbsolute < expectedSize) {
        var w = 0L
        var i = 0
        while (i < 8 && offset + i < raw.size) {
            val b = (raw[offset + i].toLong() and 0xFF)
            w = w or (b shl (8 * i))
            i++
        }
        var bitInWord = 0
        while (bitInWord < 64 && bitAbsolute < expectedSize) {
            out[bitAbsolute] = ((w ushr bitInWord) and 1L) != 0L
            bitInWord++
            bitAbsolute++
        }
        offset += 8
    }
    return out
}
```

**Python** (compatible with the same packing):

```python
import base64

def decode_mask_from_base64(b64_or_prefixed: str, n_topics: int) -> list[bool]:
    payload = b64_or_prefixed[4:] if b64_or_prefixed.startswith("B64:") else b64_or_prefixed
    raw = base64.b64decode(payload)
    out = [False] * n_topics

    bit_abs = 0
    off = 0
    while off < len(raw) and bit_abs < n_topics:
        # read little-endian 64-bit word
        w = 0
        for i in range(8):
            if off + i >= len(raw):
                break
            w |= raw[off + i] << (8 * i)

        for bit in range(64):
            if bit_abs >= n_topics:
                break
            out[bit_abs] = ((w >> bit) & 1) != 0
            bit_abs += 1

        off += 8
    return out
```

### Additional notes

- **FUN**  
  - Contains the history of *best-so-far improvements* for each K.  
  - BEST/WORST: only written when the incumbent improves → few, strictly monotone rows.  
  - AVERAGE: exactly one row per K (no incumbent concept).  

- **VAR**  
  - Genotypes aligned 1:1 with FUN rows (same number of lines).  
  - BEST/WORST: written only on improvement.  
  - AVERAGE: exactly one line per K.  

- **TOP**  
  - Maintains a per-K pool of best solutions, replaced as new ones are found.  
  - May contain multiple rows per K (≤ 10).  
  - Downstream readers should select the extremum: max for BEST, min for WORST.  

- **Negative correlations**  
  - Valid and expected for WORST (objective = minimize correlation).  
  - Can also appear in AVERAGE at small K, if many random subsets invert the ranking.  
  - Not a bug — reflects genuine inversions of system orderings.

---

## CLI options

**Required**
- `-fi, --fileIn <file>` input CSV basename (no extension)
- `-c, --corr <Pearson|Kendall>` correlation method
- `-t, --targ <Best|Worst|Average|All>` target
- `-l, --log <Verbose|Limited|Off>` logging level

**Optional, general**
- `--copy` copy results into `NewBestSub-Experiments` sibling folder
- `-det, --deterministic` enable deterministic mode
- `-sd, --seed <long>` master seed (implies deterministic mode)
- `-mr, --mrg <int>` merge N executions
- `-et, --expt <int>` add fake topics per step
- `-es, --exps <int>` add fake systems per step
- `-mx, --max <int>` cap for expansions

**Optional, target-specific**
- `-i, --iter <int>` iterations (Best, Worst, All)
- `-po, --pop <int>` population size (must be >= number of topics; Best, Worst, All)
- `-r, --rep <int>` repetitions per K (Average, All)
- `-pe, --perc <a,b>` percentile range (Average, All)

---

## Deterministic execution

- `--seed <long>` sets the master seed explicitly.
- `--deterministic` enables reproducibility. If `--seed` is absent, a stable seed is derived from core parameters.
- The effective seed is logged and embedded in the output folder name.

---

## Folder naming pattern

Example:
```
AH99-Pearson-top50-sys129-po1000-i10000-r2000-time2025-08-22-19-19-40
```

Pattern:
```
<DATASET>-<CORR>-top<Topics>-sys<Systems>-po<Population>-i<Iterations>[-r<Repetitions>]
[-exec<Executions>][-seed<Seed>][-det]-time<YYYY-MM-DD-HH-mm-ss>
```

---

## Architecture overview

- DatasetModel: loads data, wires correlation and targets, runs NSGA-II, emits streaming events:
  - `CardinalityResult` leads to FUN and VAR append
  - `TopKReplaceBatch` leads to block replace for TOP
  - `RunCompleted` leads to finalize and close writers
- Streaming NSGA-II wrapper: calls a per-generation hook and uses MNDS plus crowding in replacement.
- Incremental evaluation: maintains `cachedSumsBySystem` and `lastEvaluatedMask`; applies swap-delta or mask diff; computes correlation (Pearson or Kendall) vs full-set means.
- Operators: `BinaryPruningCrossover` (length-safe, non-empty repairs), `FixedKSwapMutation` (fixed K with hints).

---

## Testing

JUnit 5 with Surefire 3.x:
```bash
mvn -DskipTests=false -Dmaven.test.skip=false -Dsurefire.printSummary=true test
```

---

## Build and logging

Key versions (see `pom.xml`):
- Kotlin 2.2.0, Java 22, jMetal 6.9.1
- Parquet 1.15.2 and Hadoop 3.3.6, Log4j2 2.24.3, JUnit 5.13.4

Logging:
- Console plus rolling file (`log4j2.xml`)
- Uses `baseLogFileName` system property for destinations

---

## Troubleshooting

- OutOfMemoryError: increase `-Xmx`, or reduce `-po`, `-i`, or `-r`. Consider `-XX:+HeapDumpOnOutOfMemoryError`.
- Population smaller than number of topics: Best, Worst, and All require `-po >= #topics`.
- TOP seems incomplete: blocks are emitted only when enough entries are available; small runs may delay some Ks.
- Multiple SLF4J bindings: project uses Log4j2; avoid extra SLF4J bindings on the classpath.

---

## Changelog

**2025-08-22**
- jMetal 6.9.x and Kotlin 2.2 upgrade, Java 22 toolchain
- Streaming NSGA-II with per-generation progress
- MNDS environmental selection plus crowding
- Step-3 incremental evaluation (subset-mean deltas with swap hints)
- Compact VAR and TOP Base64 mask format
- Standardized 6-digit correlation precision

---

## Citation

- Kevin Roitero, Michael Soprano, Andrea Brunello, Stefano Mizzaro. *Reproduce and Improve: An Evolutionary Approach to Select a Few Good Topics for Information Retrieval Evaluation*. ACM JDIQ 10(3), Article 12, 2018. https://doi.org/10.1145/3239573
- Kevin Roitero, Michael Soprano, Stefano Mizzaro. *Effectiveness Evaluation with a Subset of Topics: A Practical Approach*. In SIGIR '18, 1145–1148. https://doi.org/10.1145/3209978.3210108

---

## License

Released under the [MIT License](LICENSE).
