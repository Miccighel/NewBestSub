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
     *  - Buffers are kept in ThreadLocal to be parallel-eval safe without GC churn.
     * ==================================================================================== */

    /** Compute corr(X_means, Y_means) from subset SUMS and K. */
    private interface CorrelationFromSums {
        fun correlationFromSums(sumsBySystem: DoubleArray, kSelected: Int): Double
    }

    /* ---- Thread-safe buffer store (one DoubleArray per worker thread) ---- */
    private val xMeansBufferTL = ThreadLocal.withInitial { DoubleArray(precomputedData.numberOfSystems) }

    /** Pearson: fill X means buffer, then Correlations.fastPearsonPrimitive(). */
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

    /** Kendall τ-b: fill X means buffer, then Correlations.kendallTauBPrimitive(). */
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

    /** Concrete strategy chosen once per run via parameter (Pearson by default). */
    private val correlationFromSums: CorrelationFromSums =
        if (parameters.correlationMethod == Constants.CORRELATION_KENDALL) KendallCorr() else PearsonCorr()

    /* --------------------- Streaming / bookkeeping & ctor-calculated values --------------------- */

    /* Use Int keys for K to simplify hashing and avoid Double quirks. */
    val dominatedSolutions = linkedMapOf<Int, BinarySolution>()

    /* Keep only the lists in the public map for compatibility with existing writers. */
    val topSolutions = linkedMapOf<Int, MutableList<BinarySolution>>()

    /* Per-K small bin (bounded Top-N and fast duplicate checks). */
    private data class TopBin(
        val bestGenotypes: HashSet<String>,        /* dedupe by genotype string */
        val topList: MutableList<BinarySolution>   /* kept sorted by naturalCorr */
    )

    /* K → bin */
    private val topBins = HashMap<Int, TopBin>()

    private var iterationCounter = 0
    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
    private var progressCounter = 0
    private var cardinalityToGenerate = 1
    private val systemsCount = precomputedData.numberOfSystems

    /* Precompute cadence once (avoid math in hot path). */
    private val loggingEvery: Int = run {
        val step = (parameters.numberOfIterations * Constants.LOGGING_FACTOR) / 100
        if (step <= 0) Int.MAX_VALUE else step
    }

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

        /* Persist incremental state for the next evaluation without allocating new arrays. */
        solution.cachedSumsBySystem = sumsBySystem
        val targetMaskBuffer = solution.ensureReusableLastMaskBuffer()
        System.arraycopy(currentTopicMask, 0, targetMaskBuffer, 0, numberOfTopics)
        solution.lastEvaluatedMask = targetMaskBuffer
        solution.clearLastMutationFlags()

        /* ----------------------------- Correlation from SUMS (single call site) ----------------------------- */
        val naturalCorrelation = correlationFromSums.correlationFromSums(sumsBySystem, selectedTopicCount)

        /* Mirror topic status for downstream consumers (kept for compatibility). */
        solution.topicStatus = Array(numberOfTopics) { idx -> currentTopicMask[idx] }

        /* objective[0] = ±K, objective[1] = ±corr (branch-dependent) */
        targetStrategy(solution, naturalCorrelation)

        /* ----------------------------- Streaming book-keeping ----------------------------- */
        val kKey: Int = solution.numberOfSelectedTopics /* Int key */

        /* Maintain “dominatedSolutions” best/worst per K (naturalCorr comparable). */
        fun naturalCorr(sln: BinarySolution): Double =
            if (parameters.targetToAchieve == Constants.TARGET_BEST) -(sln as BestSubsetSolution).getCorrelation()
            else (sln as BestSubsetSolution).getCorrelation()

        val candidateCopy = solution.copy()
        val prev = dominatedSolutions[kKey]
        if (prev == null ||
            (if (parameters.targetToAchieve == Constants.TARGET_BEST)
                naturalCorr(candidateCopy) > naturalCorr(prev)
            else
                naturalCorr(candidateCopy) < naturalCorr(prev))
        ) {
            dominatedSolutions[kKey] = candidateCopy
        }

        /* Bounded Top-N maintenance without full sort/dedupe on each eval. */
        val bin = topBins.getOrPut(kKey) { TopBin(HashSet(), mutableListOf()) }
        val genotypeString: (BinarySolution) -> String = { (it as BestSubsetSolution).getVariableValueString(0) }
        val geno = genotypeString(candidateCopy)

        if (bin.bestGenotypes.add(geno)) {
            val list = bin.topList
            val score = naturalCorr(candidateCopy)

            /* binary insert to keep list sorted (desc for BEST, asc for WORST) */
            var lo = 0
            var hi = list.size
            while (lo < hi) {
                val mid = (lo + hi).ushr(1)
                val midScore = naturalCorr(list[mid])
                val cmp = if (parameters.targetToAchieve == Constants.TARGET_BEST) {
                    -java.lang.Double.compare(score, midScore) /* descending */
                } else {
                    java.lang.Double.compare(score, midScore)  /* ascending */
                }
                if (cmp < 0) lo = mid + 1 else hi = mid
            }
            list.add(lo, candidateCopy)

            /* cap list and set */
            val cap = Constants.TOP_SOLUTIONS_NUMBER
            if (list.size > cap) {
                val removed = list.removeAt(list.lastIndex)
                bin.bestGenotypes.remove(genotypeString(removed))
            }
            /* keep public mirror map in sync (writers read from here) */
            topSolutions[kKey] = list
        }

        iterationCounter++
        return solution
    }
}
