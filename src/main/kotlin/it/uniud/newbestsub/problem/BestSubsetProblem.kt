package it.uniud.newbestsub.problem

import it.uniud.newbestsub.dataset.Parameters
import it.uniud.newbestsub.dataset.model.PrecomputedData
import it.uniud.newbestsub.dataset.model.buildPrecomputedData
import it.uniud.newbestsub.dataset.model.toPrimitiveAPRows
import it.uniud.newbestsub.math.Correlations
import it.uniud.newbestsub.utils.Constants
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.problem.binaryproblem.BinaryProblem
import org.uma.jmetal.solution.binarysolution.BinarySolution

/**
 * jMetal [BinaryProblem] implementation for the ** the best subset selection** task.
 *
 * The problem receives a precomputed bundle of AP values per (system, topic) and evaluates
 * binary masks over topics by computing a correlation between:
 *
 * - **X**: per-system *means* of AP over the currently selected topics (subset means).
 * - **Y**: per-system *full-set* mean AP (precomputed once).
 *
 * Correlation backend:
 * - `Pearson` (default) via [Correlations.fastPearsonPrimitive]
 * - `Kendall τ-b` via [Correlations.kendallTauBPrimitive]
 *
 * Performance considerations:
 * - Uses incremental subset SUMS (`DoubleArray`) with an in-place *diff* update against
 *   the last evaluated mask or fixed‑K swap hints from [BestSubsetSolution].
 * - Per-thread `DoubleArray` buffers for X means are stored in a `ThreadLocal`
 *   to be parallel-safe and GC‑friendly.
 * - The problem **does not** retain clones for streaming; writers/streaming are handled
 *   outside in the dataset layer.
 *
 * Objective encoding is delegated via [targetStrategy], which sets objectives in the
 * underlying [BestSubsetSolution] according to the selected target (e.g., BEST/WORST).
 *
 * @property parameters Run parameters (target, iterations, etc.)
 * @property numberOfTopics Number of topics (bitset length).
 * @property precomputedData Precomputed AP data and views (systems × topics).
 * @property topicLabels Human-readable labels per topic, forwarded to solutions.
 * @property legacyBoxedCorrelation Kept for ctor API compatibility; not used when [precomputedData] is supplied.
 * @property targetStrategy Function that installs objective values into a solution given the natural correlation.
 */
class BestSubsetProblem(
    private val parameters: Parameters,
    private val numberOfTopics: Int,
    private val precomputedData: PrecomputedData,
    val topicLabels: Array<String>,
    @Suppress("unused") /* kept for ctor API compatibility (old boxed path) */
    private val legacyBoxedCorrelation: (DoubleArray, DoubleArray) -> Double,
    private val targetStrategy: (BinarySolution, Double) -> BinarySolution
) : BinaryProblem {

    /**
     * Secondary constructor kept for **API compatibility** with older boxed‑array call sites.
     * It converts boxed AP rows to primitives and builds [PrecomputedData] internally.
     *
     * @param parameters Run parameters.
     * @param numberOfTopics Number of topics.
     * @param averagePrecisions Map: systemId → AP vector (boxed `Double`).
     * @param meanAveragePrecisions Historical/compatibility param (unused; see warning).
     * @param topicLabels Human‑readable topic labels.
     * @param correlationStrategy Old boxed correlation `(Array<Double>, Array<Double>) -> Double`.
     * @param targetStrategy Objective installer for the selected target.
     */
    constructor(
        parameters: Parameters,
        numberOfTopics: Int,
        averagePrecisions: MutableMap<String, Array<Double>>,
        meanAveragePrecisions: Array<Double>, /* kept for API compat only */
        topicLabels: Array<String>,
        correlationStrategy: (Array<Double>, Array<Double>) -> Double,
        targetStrategy: (BinarySolution, Double) -> BinarySolution
    ) : this(
        parameters = parameters,
        numberOfTopics = numberOfTopics,
        precomputedData = buildPrecomputedData(toPrimitiveAPRows(averagePrecisions)),
        topicLabels = topicLabels,
        legacyBoxedCorrelation = { l, r ->
            val lb = Array(l.size) { i -> l[i] }
            val rb = Array(r.size) { i -> r[i] }
            correlationStrategy.invoke(lb, rb)
        },
        targetStrategy = targetStrategy
    ) {
        /* meanAveragePrecisions may differ from the precomputed bundle; rely on PrecomputedData instead. */
        val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
        if (meanAveragePrecisions.size != precomputedData.numberOfSystems) {
            logger.warn(
                "meanAveragePrecisions length (${meanAveragePrecisions.size}) differs from numberOfSystems " +
                        "(${precomputedData.numberOfSystems}). Using PrecomputedData.fullSetMeanAPBySystem instead."
            )
        }
    }

    /* ====================================================================================
     * Correlation strategy (delegated to Correlations)
     *  - We fill a reusable buffer with subset MEANS from per-system SUMS, then call
     *    Correlations.fastPearsonPrimitive or Correlations.kendallTauBPrimitive.
     *  - Buffers are kept in ThreadLocal to be parallel-eval safe without GC churn.
     * ==================================================================================== */

    /** Strategy interface: compute corr(X_means, Y_means) from subset SUMS and selected K. */
    private interface CorrelationFromSums {
        /** @return correlation value for the current subset (natural sign). */
        fun correlationFromSums(sumsBySystem: DoubleArray, kSelected: Int): Double
    }

    /* ---- Thread-safe buffer store (one DoubleArray per worker thread) ---- */
    private val xMeansBufferTL = ThreadLocal.withInitial { DoubleArray(precomputedData.numberOfSystems) }

    /**
     * Pearson correlation strategy.
     *
     * Fills the per-thread `xMeans` buffer with SUMS/K and calls
     * [Correlations.fastPearsonPrimitive] against the fixed [yMeans].
     */
    private inner class PearsonCorr : CorrelationFromSums {
        private val systemsCount = precomputedData.numberOfSystems
        private val yMeans: DoubleArray = precomputedData.fullSetMeanAPBySystem
        override fun correlationFromSums(sumsBySystem: DoubleArray, kSelected: Int): Double {
            if (kSelected <= 0) return 0.0
            val xMeans = xMeansBufferTL.get()
            val invK = 1.0 / kSelected
            var s = 0
            while (s < systemsCount) {
                xMeans[s] = sumsBySystem[s] * invK
                s++
            }
            return Correlations.fastPearsonPrimitive(xMeans, yMeans)
        }
    }

    /**
     * Kendall τ-b correlation strategy.
     *
     * Fills the per-thread `xMeans` buffer with SUMS/K and calls
     * [Correlations.kendallTauBPrimitive] against the fixed [yMeans].
     */
    private inner class KendallCorr : CorrelationFromSums {
        private val systemsCount = precomputedData.numberOfSystems
        private val yMeans: DoubleArray = precomputedData.fullSetMeanAPBySystem
        override fun correlationFromSums(sumsBySystem: DoubleArray, kSelected: Int): Double {
            if (kSelected <= 0) return 0.0
            val xMeans = xMeansBufferTL.get()
            val invK = 1.0 / kSelected
            var s = 0
            while (s < systemsCount) {
                xMeans[s] = sumsBySystem[s] * invK
                s++
            }
            return Correlations.kendallTauBPrimitive(xMeans, yMeans)
        }
    }

    /** Concrete correlation strategy chosen once per run based on [parameters.correlationMethod]. */
    private val correlationFromSums: CorrelationFromSums =
        if (parameters.correlationMethod == Constants.CORRELATION_KENDALL) KendallCorr() else PearsonCorr()

    /* --------------------- Streaming / bookkeeping & ctor-calculated values --------------------- */

    /** Dominated solutions per K (kept only for compatibility with existing writers). */
    val dominatedSolutions = linkedMapOf<Int, BinarySolution>()

    /** Top solutions per K (public lists are preserved for writer compatibility). */
    val topSolutions = linkedMapOf<Int, MutableList<BinarySolution>>()

    private var iterationCounter = 0
    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
    private var progressCounter = 0
    private var cardinalityToGenerate = 1
    private val systemsCount = precomputedData.numberOfSystems

    /** Precomputed progress logging cadence (every LOGGING_FACTOR%). */
    private val loggingEvery: Int = run {
        val step = (parameters.numberOfIterations * Constants.LOGGING_FACTOR) / 100
        if (step <= 0) Int.MAX_VALUE else step
    }

    // -----------------------------------------------------------------------------------------
    // BinaryProblem / Problem API
    // -----------------------------------------------------------------------------------------

    /** Problem name. */
    override fun name(): String = "BestSubsetProblem"

    /** Single binary variable (topic mask). */
    override fun numberOfVariables(): Int = 1

    /** Two objectives (K and correlation with target-dependent signs). */
    override fun numberOfObjectives(): Int = 2

    /** No constraints. */
    override fun numberOfConstraints(): Int = 0

    /** Total number of bits equals number of topics. */
    override fun totalNumberOfBits(): Int = numberOfTopics

    /** Bits per variable (single variable with [numberOfTopics] bits). */
    override fun numberOfBitsPerVariable(): List<Int> = listOf(numberOfTopics)

    /**
     * Creates a new solution:
     * - For the first `N-1` creations, generates masks with increasing fixed cardinalities
     *   `K = 1, 2, …, N-1` to prime the population with diverse sizes.
     * - Afterward, generates a random non‑empty mask.
     */
    override fun createSolution(): BinarySolution {
        return if (cardinalityToGenerate < numberOfTopics) {
            BestSubsetSolution(1, 2, numberOfTopics, topicLabels, forcedCardinality = cardinalityToGenerate++)
        } else {
            BestSubsetSolution(1, 2, numberOfTopics, topicLabels, forcedCardinality = null)
        }
    }

    /**
     * Evaluates a solution by computing the **natural correlation** from the current subset
     * and delegating objective encoding to [targetStrategy].
     *
     * Steps:
     * 1. **Throttled progress logging** every [loggingEvery] iterations.
     * 2. **Incremental SUMS update**:
     *    - Cold start: compute Σ_t AP[s][t] directly from the bitset.
     *    - Warm path: apply fixed‑K swap hints or generic diffs vs last mask.
     * 3. **Correlation from SUMS**:
     *    - Fill per-thread means buffer `xMeans = SUMS / K`.
     *    - Call Pearson or Kendall τ‑b backend.
     * 4. **Objective encoding** via [targetStrategy] (e.g., BEST encodes `+K, -corr`).
     * 5. **Streaming bookkeeping** is handled elsewhere (no retention here).
     *
     * @param solution The solution to evaluate (must be a [BestSubsetSolution]).
     * @return The same [solution] instance, after objectives are set.
     */
    override fun evaluate(solution: BinarySolution): BinarySolution {
        val bs = solution as BestSubsetSolution

        // ---------- Progress logging with debounce message ----------
        if ((iterationCounter % loggingEvery) == 0 && iterationCounter <= parameters.numberOfIterations) {
            logger.info(
                "Completed iterations: {}/{} ({}%) for evaluations on \"{}\" with target {}.",
                iterationCounter, parameters.numberOfIterations, progressCounter,
                Thread.currentThread().name, parameters.targetToAchieve
            )
            logger.debug(
                "/* debounce */ log messages throttled every {} iterations ({}% of total).",
                loggingEvery, Constants.LOGGING_FACTOR
            )
            progressCounter += Constants.LOGGING_FACTOR
        }

        // ----------------------------- Incremental per-system SUMS (zero-copy) -----------------------------
        val bits = bs.variables()[0] as org.uma.jmetal.util.binarySet.BinarySet
        val selectedTopicCount = bs.numberOfSelectedTopics

        var sumsBySystem = bs.cachedSumsBySystem
        val lastMask = bs.lastEvaluatedMask

        if (sumsBySystem == null || lastMask == null) {
            // Cold start: compute Σ_t AP[s][t] from scratch using current bitset.
            val fresh = DoubleArray(systemsCount)
            var t = 0
            while (t < numberOfTopics) {
                if (bits.get(t)) {
                    val col = precomputedData.topicColumnViewByTopic[t]
                    var s = 0
                    while (s < systemsCount) {
                        fresh[s] += col[s]; s++
                    }
                }
                t++
            }
            sumsBySystem = fresh
        } else {
            // Warm path: apply fixed-K swap hints when available, otherwise generic diff vs lastMask.
            val usedSwapHints =
                bs.lastMutationWasFixedKSwap &&
                        (bs.lastSwapOutIndex != null || bs.lastSwapInIndex != null)

            if (usedSwapHints) {
                bs.lastSwapOutIndex?.let { outIdx ->
                    val col = precomputedData.topicColumnViewByTopic[outIdx]
                    var s = 0
                    while (s < systemsCount) {
                        sumsBySystem[s] -= col[s]; s++
                    }
                }
                bs.lastSwapInIndex?.let { inIdx ->
                    val col = precomputedData.topicColumnViewByTopic[inIdx]
                    var s = 0
                    while (s < systemsCount) {
                        sumsBySystem[s] += col[s]; s++
                    }
                }
            } else {
                var t = 0
                while (t < numberOfTopics) {
                    val isSel = bits.get(t)
                    val was = lastMask[t]
                    if (was != isSel) {
                        val col = precomputedData.topicColumnViewByTopic[t]
                        val sign = if (isSel) +1.0 else -1.0
                        var s = 0
                        while (s < systemsCount) {
                            sumsBySystem[s] += sign * col[s]; s++
                        }
                    }
                    t++
                }
            }
        }

        // Persist incremental state for the next evaluation (no new arrays).
        bs.cachedSumsBySystem = sumsBySystem
        run {
            // Refresh lastEvaluatedMask by filling the reusable buffer directly from the bitset.
            val target = bs.ensureReusableLastMaskBuffer()
            var i = 0
            while (i < numberOfTopics) {
                target[i] = bits.get(i); i++
            }
            bs.lastEvaluatedMask = target
            bs.clearLastMutationFlags()
        }

        // ----------------------------- Correlation from SUMS (single call site) -----------------------------
        val naturalCorrelation = correlationFromSums.correlationFromSums(sumsBySystem, selectedTopicCount)

        // Internal objective encoding (BEST: +K, -corr) (WORST: -K, +corr).
        targetStrategy(bs, naturalCorrelation)

        // Streaming / Top-K maintenance is now handled in DatasetModel.onGeneration.

        iterationCounter++
        return bs
    }
}
