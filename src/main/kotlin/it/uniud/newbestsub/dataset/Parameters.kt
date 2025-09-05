package it.uniud.newbestsub.dataset

/**
 * Parameters
 * ==========
 *
 * Centralized container for all runtime settings that control a single execution
 * of NewBestSub.
 *
 * ## Deterministic mode
 * - Set [deterministic] = `true` to request a reproducible run.
 * - Provide [seed] to fix the master seed explicitly.
 * - If [seed] is `null` and [deterministic] is `true`, a stable seed can be
 *   derived externally (e.g., via `Tools.stableSeedFrom(Parameters)`).
 *
 * ## Parallel evaluator threads
 * - [evaluatorThreads] controls the **internal** jMetal parallel evaluator thread pool
 *   used by BEST/WORST (jMetal 6.9 `MultiThreadedSolutionListEvaluator`).
 * - `0` (default) means “auto”: the model will use `Runtime.getRuntime().availableProcessors()`.
 * - A **controller** may set a specific value to **avoid over-subscription** when running
 *   multiple NSGA-II jobs concurrently (e.g., split cores between BEST and WORST).
 *
 * ## Backward compatibility
 * - New fields ([deterministic], [seed], [evaluatorThreads]) have defaults, so positional
 *   constructor calls compiled against previous signatures continue to work unchanged.
 *
 * @property datasetName Dataset identifier (e.g., collection name or tag).
 * @property correlationMethod Correlation function key (e.g., "kendall", "spearman", "pearson").
 * @property targetToAchieve Run target: one of BEST, WORST, or AVERAGE (see [Constants]).
 * @property numberOfIterations Number of NSGA-II iterations (generations) per repetition.
 * @property numberOfRepetitions Number of independent solver repetitions with different seeds (unless deterministic).
 * @property populationSize Population size for the evolutionary search.
 * @property currentExecution Execution index/counter used for output naming and logging.
 * @property percentiles Percentiles to compute/export for summaries (e.g., [5, 50, 95]).
 * @property deterministic If `true`, force reproducible behavior (single master RNG, stable ordering).
 * @property seed Optional explicit master seed; `null` ⇒ derive from params when deterministic.
 * @property evaluatorThreads Size of the jMetal evaluator thread pool; 0 ⇒ auto (#cores).
 */
data class Parameters(
    val datasetName: String,
    val correlationMethod: String,
    val targetToAchieve: String,
    val numberOfIterations: Int,
    val numberOfRepetitions: Int,
    val populationSize: Int,
    val currentExecution: Int,
    val percentiles: List<Int>,
    val deterministic: Boolean = false,
    val seed: Long? = null,
    val evaluatorThreads: Int = 0
)
