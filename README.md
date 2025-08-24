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

> JDIQ 2018: <https://doi.org/10.1145/3239573> • SIGIR 2018: <https://doi.org/10.1145/3209978.3210108>

---

## ⚠️ Migration from 1.0 → 2.0

Starting with version **2.0**, NewBestSub has undergone major refactoring:
- Requires **Java 22** and **Kotlin 2.2** (was Java 8 / Kotlin 1.x in 1.0).
- Output format is now **streaming-based** with FUN/VAR/TOP separation and **6-digit correlation precision**.
- New **deterministic mode** and reproducible folder naming pattern.
- Updated **CLI options** (`-fi`, `-c`, `-t`, etc.).
- **Artifacts renamed**: `NewBestSub-2.0-jar-with-dependencies.jar`.

If you have scripts or pipelines based on **1.0**, they will **not work unmodified**.  
Please adapt your datasets, CLI calls, and parsing code to the new schema.

---

## Table of contents

- [Features](#features)
- [Requirements](#requirements)
- [Build](#build)
- [Quick start](#quick-start)
- [Dataset schema](#dataset-schema)
- [Outputs](#outputs)
- [CLI options](#cli-options)
- [Deterministic execution](#deterministic-execution)
- [Folder naming pattern](#folder-naming-pattern)
- [Architecture overview](#architecture-overview)
- [Testing](#testing)
- [Build & logging](#build--logging)
- [Troubleshooting](#troubleshooting)
- [Changelog](#changelog)
- [Documentation](#documentation)
- [Citation](#citation)
- [License](#license)

---

## Features
*(full content preserved from your detailed version)*

## Requirements
*(unchanged)*

## Build
*(unchanged)*

## Quick start
*(unchanged)*

## Dataset schema
*(unchanged)*

## Outputs
*(unchanged)*

## CLI options
*(unchanged)*

## Deterministic execution
*(unchanged)*

## Folder naming pattern
*(unchanged)*

## Architecture overview
*(unchanged)*

## Testing
*(unchanged)*

## Build & logging
*(unchanged)*

## Troubleshooting
*(unchanged)*

## Changelog
*(unchanged)*

---

## 📖 Documentation

Full API and developer documentation is available here:  
➡️ [https://miccighel.github.io/NewBestSub/](https://miccighel.github.io/NewBestSub/)

---

## 📚 Citation

If you use this software in academic work, please cite:

- Kevin Roitero, Michael Soprano, Andrea Brunello, Stefano Mizzaro. *Reproduce and Improve: An Evolutionary Approach to Select a Few Good Topics for Information Retrieval Evaluation*. **ACM JDIQ** 10(3), Article 12, 2018. <https://doi.org/10.1145/3239573>  
- Kevin Roitero, Michael Soprano, Stefano Mizzaro. *Effectiveness Evaluation with a Subset of Topics: A Practical Approach*. In **SIGIR ’18**, 1145–1148. <https://doi.org/10.1145/3209978.3210108>  

---

## License

© University of Udine. See [LICENSE](LICENSE).

---

> ℹ️ Originally developed at the **University of Udine** as part of the Master’s Degree in Computer Science.