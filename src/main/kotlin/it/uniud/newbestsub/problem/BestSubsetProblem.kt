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

class BestSubsetProblem(
    private val parameters: Parameters,
    private val numberOfTopics: Int,
    private val precomputedData: PrecomputedData,
    val topicLabels: Array<String>,
    @Suppress("unused") /* kept for ctor API compatibility (old boxed path) */
    private val legacyBoxedCorrelation: (DoubleArray, DoubleArray) -> Double,
    private val targetStrategy: (BinarySolution, Double) -> BinarySolution
) : BinaryProblem {

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
     * ==================================================================================== */

    /** Compute corr(X_means, Y_means) from subset SUMS and K. */
    private interface CorrelationFromSums {
        fun correlationFromSums(sumsBySystem: DoubleArray, kSelected: Int): Double
    }

    /** Pearson: fill X means buffer, then Correlations.fastPearsonPrimitive(). */
    private inner class PearsonCorr : CorrelationFromSums {
        private val systemsCount = precomputedData.numberOfSystems
        private val yMeans: DoubleArray = precomputedData.fullSetMeanAPBySystem
        private val xMeansBuffer = DoubleArray(systemsCount)
        override fun correlationFromSums(sumsBySystem: DoubleArray, kSelected: Int): Double {
            if (kSelected <= 0) return 0.0
            val invK = 1.0 / kSelected
            var s = 0
            while (s < systemsCount) {
                xMeansBuffer[s] = sumsBySystem[s] * invK
                s++
            }
            return Correlations.fastPearsonPrimitive(xMeansBuffer, yMeans)
        }
    }

    /** Kendall τ-b: fill X means buffer, then Correlations.kendallTauBPrimitive(). */
    private inner class KendallCorr : CorrelationFromSums {
        private val systemsCount = precomputedData.numberOfSystems
        private val yMeans: DoubleArray = precomputedData.fullSetMeanAPBySystem
        private val xMeansBuffer = DoubleArray(systemsCount)
        override fun correlationFromSums(sumsBySystem: DoubleArray, kSelected: Int): Double {
            if (kSelected <= 0) return 0.0
            val invK = 1.0 / kSelected
            var s = 0
            while (s < systemsCount) {
                xMeansBuffer[s] = sumsBySystem[s] * invK
                s++
            }
            return Correlations.kendallTauBPrimitive(xMeansBuffer, yMeans)
        }
    }

    /** Concrete strategy chosen once per run via parameter (Pearson by default). */
    private val correlationFromSums: CorrelationFromSums =
        if (parameters.correlationMethod == Constants.CORRELATION_KENDALL) KendallCorr() else PearsonCorr()

    /* --------------------- Streaming / bookkeeping & ctor-calculated values --------------------- */

    val dominatedSolutions = linkedMapOf<Double, BinarySolution>()
    val topSolutions = linkedMapOf<Double, MutableList<BinarySolution>>()
    private var iterationCounter = 0
    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
    private var progressCounter = 0
    private var cardinalityToGenerate = 1
    private val systemsCount = precomputedData.numberOfSystems

    /* -------- BinaryProblem / Problem API -------- */
    override fun name(): String = "BestSubsetProblem"
    override fun numberOfVariables(): Int = 1
    override fun numberOfObjectives(): Int = 2
    override fun numberOfConstraints(): Int = 0
    override fun totalNumberOfBits(): Int = numberOfTopics
    override fun numberOfBitsPerVariable(): List<Int> = listOf(numberOfTopics)

    override fun createSolution(): BinarySolution {
        return if (cardinalityToGenerate < numberOfTopics) {
            BestSubsetSolution(1, 2, numberOfTopics, topicLabels, forcedCardinality = cardinalityToGenerate++)
        } else {
            BestSubsetSolution(1, 2, numberOfTopics, topicLabels, forcedCardinality = null)
        }
    }

    override fun evaluate(solution: BinarySolution): BinarySolution {
        solution as BestSubsetSolution

        /* ---------- Progress logging with debounce message ---------- */
        val loggingEvery = (parameters.numberOfIterations * Constants.LOGGING_FACTOR) / 100
        if (loggingEvery > 0 &&
            (iterationCounter % loggingEvery) == 0 &&
            parameters.numberOfIterations >= loggingEvery &&
            iterationCounter <= parameters.numberOfIterations
        ) {
            logger.info(
                "Completed iterations: $iterationCounter/${parameters.numberOfIterations} ($progressCounter%) " +
                    "for evaluations on \"${Thread.currentThread().name}\" with target ${parameters.targetToAchieve}."
            )
            logger.debug(
                "/* debounce */ log messages throttled every $loggingEvery iterations " +
                    "(${Constants.LOGGING_FACTOR}% of total)."
            )
            progressCounter += Constants.LOGGING_FACTOR
        }

        /* ----------------------------- Incremental per-system SUMS ----------------------------- */
        val currentTopicMask: BooleanArray = solution.retrieveTopicStatus()
        val selectedTopicCount = solution.numberOfSelectedTopics

        var sumsBySystem = solution.cachedSumsBySystem
        val lastMask = solution.lastEvaluatedMask

        if (sumsBySystem == null || lastMask == null) {
            /* Cold start: compute SUMS from scratch for selected topics only. */
            val fresh = DoubleArray(systemsCount)
            var t = 0
            while (t < numberOfTopics) {
                if (currentTopicMask[t]) {
                    val col = precomputedData.topicColumnViewByTopic[t]
                    var s = 0
                    while (s < systemsCount) { fresh[s] += col[s]; s++ }
                }
                t++
            }
            sumsBySystem = fresh
        } else {
            /* Warm path: apply swap hints or generic diff. */
            val usedSwapHints =
                solution.lastMutationWasFixedKSwap &&
                (solution.lastSwapOutIndex != null || solution.lastSwapInIndex != null)

            if (usedSwapHints) {
                solution.lastSwapOutIndex?.let { outIdx ->
                    val col = precomputedData.topicColumnViewByTopic[outIdx]
                    var s = 0
                    while (s < systemsCount) { sumsBySystem[s] -= col[s]; s++ }
                }
                solution.lastSwapInIndex?.let { inIdx ->
                    val col = precomputedData.topicColumnViewByTopic[inIdx]
                    var s = 0
                    while (s < systemsCount) { sumsBySystem[s] += col[s]; s++ }
                }
            } else {
                var t = 0
                while (t < numberOfTopics) {
                    val was = lastMask[t]
                    val isSel = currentTopicMask[t]
                    if (was != isSel) {
                        val col = precomputedData.topicColumnViewByTopic[t]
                        val sign = if (isSel) +1.0 else -1.0
                        var s = 0
                        while (s < systemsCount) { sumsBySystem[s] += sign * col[s]; s++ }
                    }
                    t++
                }
            }
        }

        /* Persist incremental state for the next evaluation. */
        solution.cachedSumsBySystem = sumsBySystem
        solution.lastEvaluatedMask = currentTopicMask.copyOf()
        solution.clearLastMutationFlags()

        /* ----------------------------- Correlation from SUMS (single call site) ----------------------------- */
        val naturalCorrelation = correlationFromSums.correlationFromSums(sumsBySystem, selectedTopicCount)

        /* Mirror topic status for downstream consumers. */
        solution.topicStatus = currentTopicMask.toTypedArray()

        /* objective[0] = ±K, objective[1] = ±corr (branch-dependent) */
        targetStrategy(solution, naturalCorrelation)

        /* ----------------------------- Streaming book-keeping ----------------------------- */
        val kKey = solution.numberOfSelectedTopics.toDouble()
        val candidateCopy = solution.copy()

        fun naturalCorr(sln: BinarySolution): Double =
            if (parameters.targetToAchieve == Constants.TARGET_BEST) -sln.getCorrelation() else sln.getCorrelation()

        val prev = dominatedSolutions[kKey]
        if (prev == null ||
            (if (parameters.targetToAchieve == Constants.TARGET_BEST)
                naturalCorr(candidateCopy) > naturalCorr(prev)
            else
                naturalCorr(candidateCopy) < naturalCorr(prev))
        ) {
            dominatedSolutions[kKey] = candidateCopy
        }

        val entriesForK = topSolutions.getOrPut(kKey) { mutableListOf() }
        entriesForK += solution.copy()
        val genotypeString: (BinarySolution) -> String = { (it as BestSubsetSolution).getVariableValueString(0) }

        if (parameters.targetToAchieve == Constants.TARGET_BEST) {
            entriesForK.sortWith(compareByDescending<BinarySolution> { naturalCorr(it) }.thenBy { genotypeString(it) })
        } else {
            entriesForK.sortWith(compareBy<BinarySolution> { naturalCorr(it) }.thenBy { genotypeString(it) })
        }

        val seen = LinkedHashSet<String>(entriesForK.size)
        val deduped = mutableListOf<BinarySolution>()
        for (sln in entriesForK) {
            val key = genotypeString(sln)
            if (seen.add(key)) deduped += sln
        }
        topSolutions[kKey] = deduped.take(Constants.TOP_SOLUTIONS_NUMBER).toMutableList()

        iterationCounter++
        return solution
    }
}
