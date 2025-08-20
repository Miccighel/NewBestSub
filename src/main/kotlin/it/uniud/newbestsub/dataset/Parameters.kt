package it.uniud.newbestsub.dataset

/*
 * Parameters
 * ----------
 * Centralized container for all run-time settings that control a single execution
 * of NewBestSub. This version adds deterministic execution support while preserving
 * backward compatibility with existing constructor calls.
 *
 * Deterministic mode:
 *  - Set `deterministic = true` to request a reproducible run.
 *  - Provide `seed` to fix the master seed explicitly. If `seed` is null and
 *    `deterministic` is true, a stable seed can be derived externally
 *    (e.g., Tools.stableSeedFrom(Parameters)).
 *
 * Backward compatibility:
 *  - New fields were appended with default values, so positional constructor
 *    calls compiled against the previous signature continue to work unchanged.
 */

data class Parameters(
    val datasetName: String,            /* Dataset identifier (e.g., collection name or tag) */
    val correlationMethod: String,      /* Correlation function key (e.g., "kendall", "spearman", "pearson") */
    val targetToAchieve: String,        /* One of BEST | WORST | AVERAGE (see Constants) */
    val numberOfIterations: Int,        /* NSGA-II iterations (generations) per repetition */
    val numberOfRepetitions: Int,       /* Independent solver repetitions with different seeds (unless deterministic) */
    val populationSize: Int,            /* Population size for the evolutionary search */
    val currentExecution: Int,          /* Execution index/counter used for output naming and logging */
    val percentiles: List<Int>,         /* Percentiles to compute/export for summaries (e.g., [5, 50, 95]) */

    /* --- Deterministic mode (new) --- */
    val deterministic: Boolean = false, /* If true, force reproducible behavior (single master RNG, stable ordering) */
    val seed: Long? = null              /* Optional explicit master seed; null ⇒ derive from params when deterministic */
)
