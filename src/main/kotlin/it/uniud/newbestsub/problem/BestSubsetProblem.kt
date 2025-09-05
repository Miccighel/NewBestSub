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
import org.uma.jmetal.util.binarySet.BinarySet
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.LongAdder
import kotlin.math.min

/**
 * jMetal [BinaryProblem] for the **best subset selection** task.
 *
 * Correlates, per-system:
 * - **X**: mean AP over the currently selected topics (subset means, built via incremental SUMS)
 * - **Y**: mean AP over the full topic set (precomputed once)
 *
 * Correlation backends:
 * - Pearson (default): via SUMS+K path [correlationFromSums], internally vectorized when enabled
 * - Kendall τ-b: via [Correlations.kendallTauBPrimitive] (ordering invariant to positive scaling)
 *
 * Hot-path notes:
 * - Keeps per-solution SUMS (`DoubleArray`) and a bitset snapshot; updates via XOR diff or fixed-K swap hints
 * - First-time accumulation touches **only set bits** (O(K)), not all N topics
 * - Uses a tiny in-place **AXPY** kernel for column add/sub to reduce loop overhead
 * - Objective encoding is delegated to [targetStrategy]
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
     * Secondary constructor kept for **API compatibility** with older boxed-array call sites.
     */
    constructor(
        parameters: Parameters,
        numberOfTopics: Int,
        averagePrecisions: MutableMap<String, Array<Double>>,
        meanAveragePrecisions: Array<Double>,
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
     * ==================================================================================== */

    private val yMeans: DoubleArray = precomputedData.fullSetMeanAPBySystem
    private val yStats: Correlations.CorrelationYStats = Correlations.precomputeYStats(yMeans)
    private val correlationFromSums: (DoubleArray, Int) -> Double =
        Correlations.makeCorrelationFromSums(parameters.correlationMethod, yStats)

    /* --------------------- Streaming / bookkeeping --------------------- */

    val dominatedSolutions = linkedMapOf<Int, BinarySolution>()
    val topSolutions = linkedMapOf<Int, MutableList<BinarySolution>>()

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
    private val systemsCount = precomputedData.numberOfSystems
    private val columnByTopic: Array<DoubleArray> = precomputedData.topicColumnViewByTopic

    private val totalEvaluations: Int = parameters.numberOfIterations
    private val logStrideEvaluations: Int = run {
        val step = (totalEvaluations * Constants.LOGGING_FACTOR) / 100
        if (step <= 0) Int.MAX_VALUE else step
    }
    private val evalCounter = LongAdder()
    private val nextLogAt = AtomicInteger(logStrideEvaluations)

    private companion object {
        const val SNAPSHOT_ATTR = "nbsub:lastBitsetSnapshot"
    }
    private val diffBitsetTL = ThreadLocal.withInitial { BinarySet(numberOfTopics) }

    override fun name(): String = "BestSubsetProblem"
    override fun numberOfVariables(): Int = 1
    override fun numberOfObjectives(): Int = 2
    override fun numberOfConstraints(): Int = 0
    override fun totalNumberOfBits(): Int = numberOfTopics
    override fun numberOfBitsPerVariable(): List<Int> = listOf(numberOfTopics)

    private var cardinalityToGenerate = 1
    override fun createSolution(): BinarySolution {
        return if (cardinalityToGenerate < numberOfTopics) {
            BestSubsetSolution(1, 2, numberOfTopics, topicLabels, forcedCardinality = cardinalityToGenerate++)
        } else {
            BestSubsetSolution(1, 2, numberOfTopics, topicLabels, forcedCardinality = null)
        }
    }

    override fun evaluate(solution: BinarySolution): BinarySolution {
        val bestSubsetSolution = solution as BestSubsetSolution

        // ---------- progress logging (debounced) ----------
        evalCounter.increment()
        val completedEvaluations = evalCounter.sum().toInt()
        if (completedEvaluations >= nextLogAt.get()) {
            var loggedThisStride = false
            while (!loggedThisStride) {
                val threshold = nextLogAt.get()
                if (completedEvaluations < threshold) break
                if (nextLogAt.compareAndSet(threshold, threshold + logStrideEvaluations)) {
                    val shownCompleted = min(completedEvaluations, totalEvaluations)
                    val shownPercent =
                        if (totalEvaluations > 0) ((completedEvaluations * 100L) / totalEvaluations).toInt().coerceIn(0, 100)
                        else 0
                    logger.info(
                        "Completed evaluations: {}/{} ({}%) on \"{}\" with target {}.",
                        shownCompleted, totalEvaluations, shownPercent,
                        Thread.currentThread().name, parameters.targetToAchieve
                    )
                    logger.debug(
                        "/* Debounce */ progress logs every {} evaluations (~{}%).",
                        logStrideEvaluations, Constants.LOGGING_FACTOR
                    )
                    loggedThisStride = true
                }
            }
        }

        val currentBitset = bestSubsetSolution.variables()[0] as BinarySet
        val selectedTopicCount = bestSubsetSolution.numberOfSelectedTopics

        var sumsBySystem = bestSubsetSolution.cachedSumsBySystem
        var lastSnapshot = bestSubsetSolution.attributes()[SNAPSHOT_ATTR] as? BinarySet
        var lastEvaluatedMask = bestSubsetSolution.lastEvaluatedMask

        if (sumsBySystem == null || lastSnapshot == null || lastEvaluatedMask == null) {
            // ---------------- First-time build for this solution ----------------
            val freshSums = DoubleArray(systemsCount)
            val snapshotBitset = BinarySet(numberOfTopics)
            val reusableMask = bestSubsetSolution.ensureReusableLastMaskBuffer()

            // Iterate only set bits: O(K)
            var idx = currentBitset.nextSetBit(0)
            while (idx >= 0) {
                snapshotBitset.set(idx, true)
                reusableMask[idx] = true
                axpyInPlace(freshSums, columnByTopic[idx], +1.0)
                idx = currentBitset.nextSetBit(idx + 1)
            }

            sumsBySystem = freshSums
            bestSubsetSolution.cachedSumsBySystem = freshSums
            bestSubsetSolution.lastEvaluatedMask = reusableMask
            bestSubsetSolution.attributes()[SNAPSHOT_ATTR] = snapshotBitset

        } else {
            // ---------------- Incremental update (swap hints or generic diff) ----------------
            val hasSwapHints =
                bestSubsetSolution.lastMutationWasFixedKSwap &&
                        (bestSubsetSolution.lastSwapOutIndex != null || bestSubsetSolution.lastSwapInIndex != null)

            if (hasSwapHints) {
                // Targeted updates with swap hints
                bestSubsetSolution.lastSwapOutIndex?.let { outIndex ->
                    axpyInPlace(sumsBySystem!!, columnByTopic[outIndex], -1.0)
                    lastEvaluatedMask!![outIndex] = false
                    lastSnapshot!!.set(outIndex, false)
                }
                bestSubsetSolution.lastSwapInIndex?.let { inIndex ->
                    axpyInPlace(sumsBySystem!!, columnByTopic[inIndex], +1.0)
                    lastEvaluatedMask!![inIndex] = true
                    lastSnapshot!!.set(inIndex, true)
                }
            } else {
                // XOR diff against previous snapshot: touch only changed bits
                val diff = diffBitsetTL.get()
                diff.clear()
                diff.or(lastSnapshot)
                diff.xor(currentBitset)

                var i = diff.nextSetBit(0)
                while (i >= 0) {
                    val nowSelected = currentBitset.get(i)
                    val alpha = if (nowSelected) +1.0 else -1.0
                    axpyInPlace(sumsBySystem!!, columnByTopic[i], alpha)
                    lastEvaluatedMask!![i] = nowSelected
                    lastSnapshot!!.set(i, nowSelected)
                    i = diff.nextSetBit(i + 1)
                }
            }

            bestSubsetSolution.cachedSumsBySystem = sumsBySystem
            bestSubsetSolution.clearLastMutationFlags()
        }

        val naturalCorrelationValue = correlationFromSums(sumsBySystem!!, selectedTopicCount)
        targetStrategy(bestSubsetSolution, naturalCorrelationValue)
        return bestSubsetSolution
    }

    /**
     * In-place AXPY micro-kernel: dst[i] += alpha * src[i]
     * - Unrolled by 4 to reduce loop overhead
     * - Works for +/-1 alpha and general doubles
     */
    private fun axpyInPlace(dst: DoubleArray, src: DoubleArray, alpha: Double) {
        var i = 0
        val n = dst.size
        while (i + 3 < n) {
            dst[i    ] += alpha * src[i    ]
            dst[i + 1] += alpha * src[i + 1]
            dst[i + 2] += alpha * src[i + 2]
            dst[i + 3] += alpha * src[i + 3]
            i += 4
        }
        while (i < n) {
            dst[i] += alpha * src[i]
            i++
        }
    }
}
