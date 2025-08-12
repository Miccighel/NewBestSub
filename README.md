# NewBestSub

Efficient topic-set reduction for IR evaluation using a multi-objective evolutionary algorithm (NSGA-II). This repo reproduces and extends results on selecting small topic subsets that preserve the system ranking induced by the full set.

## Useful Links

- Original Article (JDIQ 2018): <https://doi.org/10.1145/3239573>  
- Short Version (SIGIR 2018): <https://dl.acm.org/citation.cfm?doid=3209978.3210108>

## Abstract

_Excerpted from the JDIQ article_

> Effectiveness evaluation of information retrieval systems by means of a test collection is a widely used methodology. However, it is rather expensive in terms of resources, time, and money; therefore, many researchers have proposed methods for a cheaper evaluation. One particular approach, on which we focus in this article, is to use fewer topics: in TREC-like initiatives, usually system effectiveness is evaluated as the average effectiveness on a set of n topics (usually, n = 50, but more than 1,000 have been also adopted); instead of using the full set, it has been proposed to find the best subsets of a few good topics that evaluate the systems in the most similar way to the full set. The computational complexity of the task has so far limited the analysis that has been performed. We develop a novel and efficient approach based on a multi-objective evolutionary algorithm. The higher efficiency of our new implementation allows us to reproduce some notable results on topic set reduction, as well as perform new experiments to generalize and improve such results. We show that our approach is able to both reproduce the main state-of-the-art results and to allow us to analyze the effect of the collection, metric, and pool depth used for the evaluation. Finally, differently from previous studies, which have been mainly theoretical, we are also able to discuss some practical topic selection strategies, integrating results of automatic evaluation approaches.

---

## What this repo does

- Implements **Best/Worst/Average** topic-subset search with **NSGA-II** (jMetal **5.10**).
- Uses correlation (Pearson or Kendall) between **reduced mean AP per system** and the **full mean AP** as the primary objective.
- Produces:
  - **Non-dominated** solutions (Pareto front),
  - Per-cardinality **dominated best** solutions,
  - Per-cardinality **top-K** solutions,
  - CSV outputs for function/variable values and aggregated info.

---

## Requirements

- **Java 21**
- **Kotlin** (built via Maven plugin)
- **Maven 3.9+**

All runtime dependencies are shaded into the fat jar via the Maven Assembly Plugin.

---

## Build

```bash
# from project root
mvn -DskipTests=false clean package
```

This produces:

```
target/NewBestSub-2.0-jar-with-dependencies.jar
```

---

## Running

There is a `mainClass` in the shaded jar:

```bash
java -Xmx4g -jar target/NewBestSub-2.0-jar-with-dependencies.jar --help
```

> Tip: CLI flags vary per setup; use `--help` or check `it.uniud.newbestsub.program.Program`.  
> At minimum you’ll specify dataset path, target (BEST/WORST/AVERAGE), correlation method, iterations, population, repetitions, etc.

### Dataset format

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

## Outputs

Paths are constructed like:

```
<outputRoot>/<dataset>/<correlation>/<#topics>/<#systems>/<iterations>/<population>[/<repetitions>][/currentExec]/<target>/
```

Key files (CSV):

- **Function values**: correlation & cardinality
- **Variable values**: selected topics bitstring
- **Top solutions**: per-cardinality best K
- **Aggregated data / info**: summary for plotting/analysis

> When `target = AVERAGE`, repetitions are included; for BEST/WORST you’ll see iterations and population size.

---

## Targets & objectives

- **BEST**  
  - Objective 0: cardinality (as positive, then negated internally for search where needed)  
  - Objective 1: **−correlation** (so minimizing finds highest correlation)

- **WORST**  
  - Objective 0: **−cardinality** (favor larger K in search)  
  - Objective 1: **correlation** (minimize correlation)

> ✦ Reporting uses the **true correlation**. As **K → N**, the reduced mean vector converges to the full mean ⇒ correlation → **1.0** even for “worst” subsets. If you prefer a “lower is worse” display for WORST, print `1 − corr` instead.

---

## Correlation methods

- **Pearson** (default)
- **Kendall**

Set via parameters/CLI. You can plug in a custom strategy: see `DatasetModel.loadCorrelationMethod` and pass a `(Array<Double>, Array<Double>) -> Double`.

---

## jMetal 5.10 notes (what we changed)

- We **avoid** abstract bases and implement the interfaces directly:
  - `BestSubsetProblem : org.uma.jmetal.problem.BinaryProblem`
  - `BestSubsetSolution : org.uma.jmetal.solution.BinarySolution`
- Implemented required methods:  
  `getListOfBitsPerVariable()`, `getBitsFromVariable(i)`, variables/objectives/constraints/attributes accessors.
- **Random indices**: jMetal **5.10**’s `JMetalRandom.nextInt(lower, upper)` is **inclusive** on both ends.  
  We always use `nextInt(0, N-1)` with guard for `N <= 1`. No `Math.floor(nextDouble()*N)`.
- **NSGA-II builder** (5.10): use 3-arg ctor and set population/iterations via setters; manual timing or `AlgorithmRunner`.

---

## Testing

We use **JUnit 5** + **Surefire 3.x**.

Run:

```bash
mvn -DskipTests=false -Dmaven.test.skip=false -Dsurefire.printSummary=true test
```

If your tests don’t fire:

- Ensure imports use `org.uma.jmetal.solution.BinarySolution` (5.10), **not** `…binarysolution…`.
- Surefire config sets:
  - `useModulePath=false`
  - `failIfNoTests=true`
  - Includes: `**/*Test.class`, etc.
- Verify compiled tests in `target/test-classes/…`.

---

## Project structure

```
src/
  main/
    kotlin/it/uniud/newbestsub/
      problem/            # Problem, Solution, operators (crossover/mutation)
      dataset/            # Model, controller, parameters, I/O
      program/            # Main entry point
      utils/              # Constants, Tools
  test/
    kotlin/it/uniud/newbestsub/problem/
      BestSubsetSolutionTest.kt
      BitFlipMutationTest.kt
```

---

## Reproducibility

- RNG: `JMetalRandom.getInstance()` (deterministic if you seed it before runs).
- Constructors/operators ensure **at least one** topic is selected.

---

## Extending

- **Custom correlation**: implement `(Array<Double>, Array<Double>) -> Double`.
- **Alternative distance**: e.g., MAE/RMSE between reduced and full mean AP vectors.
- **Operators**: swap in a different crossover/mutation; keep `BinarySolution` API.

---

## Troubleshooting

- **ArrayIndexOutOfBounds (index == numberOfTopics)**  
  Use `nextInt(0, N-1)` (inclusive upper bound in 5.10) and `for (i in 0 until N)`.
- **Compilation errors mentioning `binarysolution`**  
  Fix imports to `org.uma.jmetal.solution.BinarySolution` and `org.uma.jmetal.problem.BinaryProblem`.
- **0 tests run**  
  See the Testing section; set `useModulePath=false`, check includes and scopes.

---

## Citation

If you use this software in academic work, please cite the JDIQ 2018 paper:

- M. Soprano, K. Roitero, S. Mizzaro. _“[Best Topic Subsets for IR Evaluation](https://doi.org/10.1145/3239573)”_, **JDIQ**, 2018.

Short version:

- SIGIR 2018: <https://dl.acm.org/citation.cfm?doid=3209978.3210108>

---

## License

© University of Udine. See `LICENSE` (or fill in your preferred license).