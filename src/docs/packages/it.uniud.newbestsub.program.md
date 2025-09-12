# Package it.uniud.newbestsub.program

Command-line entry point and runtime glue for **NewBestSub**.

## Responsibilities
- Parse and validate CLI options (dataset, target, correlation, iterations, repetitions, population, percentiles, expansions, deterministic/seed).
- Initialize logging twice: a **bootstrap** log early, then a **parameterized** log once the dataset and tokens are known.
- Load data via `DatasetController.load(...)` and orchestrate `DatasetModel.solve(...)` in the selected run mode(s).
- Provide clear, actionable error messages (CLI parse errors vs. runtime validation) and friendly help output.

## Key types
- `Program` — the `main(...)` entrypoint. Owns CLI parsing, logging setup, deterministic seeding, and run dispatch.
- `Parameters` — immutable snapshot of run configuration passed to the dataset/model layer.
- `LogManager` (utils) — handles log directory creation, file path promotion, and root level changes.

## CLI flags (essentials)
- `-fi, --fileIn <name>`: dataset file (CSV, without extension) under `data/` (required).
- `-c,  --corr {Pearson|Kendall}`: correlation method (required).
- `-t,  --targ {Best|Worst|Average|All}`: target mode (required).
- `-l,  --log {Verbose|Limited|Off}`: logging verbosity (required).

### Mode-specific
- **Best/Worst/All** require:
  - `-i,  --iter <int>`: iterations (must be > 0).
  - `-po, --pop  <int>`: population (must be > 0 and **≥ #topics**).
- **Average/All** require:
  - `-r,  --rep  <int>`: repetitions per K (must be > 0).
  - `-pe, --perc <a,b>`: percentile bounds (two integers; order-agnostic, clamped to 1..100).

### Expansions & multi-run
- `-et, --expt <int>`: expand topics by this step up to `--max`.
- `-es, --exps <int>`: expand systems by this step up to `--max`.
- `-mx, --max  <int>`: maximum topics/systems when using expansions (must be < population).
- `-mr, --mrg  <int>`: run N executions and merge results.

### Determinism
- `--det, --deterministic`: enable reproducible execution.
- `--sd,  --seed <long>`: explicit master seed (implies deterministic).

### Misc
- `--copy`: copy run outputs into `NewBestSub-Experiments/` if present alongside the project.

## Determinism & seeding
- All randomness flows through jMetal’s singleton RNG.
- Deterministic mode installs a **SplittableRandom adapter** (see `RandomBridge`) with a **master seed**:
  - From `--seed` if provided, otherwise derived stably from `Parameters` (`Tools.stableSeedFrom`).
- Sub-runs may derive labeled child seeds to keep streams decorrelated yet reproducible.

## Logging
- Bootstrap log is created early under `log/Execution<timestamp>.log`.
- After dataset load, the log is **promoted** to a parameterized filename built from a token:
  - `<dataset>-<corr>-top<T>-sys<S>-po<P>-i<I>[-r<R>][-mx<M>][-peA_B]-time<timestamp>.log`
- Root log level is switched to the requested verbosity after promotion.

## Error handling
- Distinguishes **CLI parse errors** (pretty help printed) from **runtime validation** (e.g., `-po < #topics`).
- Catches `JMetalException`, `FileSystemException`, and `OutOfMemoryError` with targeted hints:
  - `-po ≥ #topics`, adjust `-i/-r`, or increase `-Xmx`; optional GC tips are printed when relevant.

## Run modes
- **Single run** (default): execute the selected target once.
- **Multi-run merge** (`--mrg N`): run N times and merge aggregated outcomes.
- **Expansion** (`--expt/--exps` + `--max`): repeatedly enlarge topics/systems, solving and cleaning between steps.

## Examples
```bash
# Best (Pearson), deterministic with derived seed
java -Xms8g -Xmx8g -jar NewBestSub-2.0-jar-with-dependencies.jar \
  -fi "mmlu" -c "Pearson" -t "Best" -po 20000 -i 100000 -l Limited --det

# Average (Kendall), percentiles 1..100 with explicit seed
java -Xms8g -Xmx8g -jar NewBestSub-2.0-jar-with-dependencies.jar \
  -fi "mmlu" -c "Kendall" -t "Average" -r 2000 -pe 1,100 -l Limited --seed 123456789

# All (Best/Worst + Average)
java -Xms8g -Xmx8g -jar NewBestSub-2.0-jar-with-dependencies.jar \
  -fi "mmlu" -c "Pearson" -t "All" -po 20000 -i 100000 -r 2000 -pe 1,100 -l Limited
```
## Notes & tips
- Ensure `-po (population) ≥ #topics` for Best/Worst/All (validated early to fail fast).
- For large runs, prefer increasing heap (`-Xmx`) over disabling GC overhead limits; only use `-XX:-UseGCOverheadLimit` as a last resort.
- Log and result paths are echoed at startup for traceability.