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

/**
 * BestSubsetProblem
 *
 * Binary jMetal problem that evaluates topic subsets by correlating:
 *  • X: mean AP over the selected topics, maintained incrementally as system-level SUMS.
 *  • Y: full-set mean AP per system, precomputed once and reused.
 *
 * Correlation backends
 *  • Pearson via a SUMS+K path with optional vectorization.
 *  • Kendall τ-b via a primitive O(n log n) implementation with ties.
 *
 * Hot-path invariants
 *  • Primitive arrays end-to-end.
 *  • Incremental updates via XOR diffs or fixed-K swap hints.
 *  • AXPY micro-kernel with ±1 fast paths.
 *  • Objective encoding delegated to the provided targetStrategy.
 */
class BestSubsetProblem(
    private val parameters: Parameters,
    private val numberOfTopics: Int,
    private val precomputedData: PrecomputedData,
    val topicLabels: Array<String>,
    @Suppress("unused")
    private val legacyBoxedCorrelation: (DoubleArray, DoubleArray) -> Double,
    private val targetStrategy: (BinarySolution, Double) -> BinarySolution
) : BinaryProblem {

    /**
     * Compatibility constructor for callers that still pass boxed arrays.
     * The boxed correlation function is bridged to the primitive path.
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
        legacyBoxedCorrelation = { left, right ->
            val leftBoxed = Array(left.size) { i -> left[i] }
            val rightBoxed = Array(right.size) { i -> right[i] }
            correlationStrategy.invoke(leftBoxed, rightBoxed)
        },
        targetStrategy = targetStrategy
    ) {
        val rootLogger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
        if (meanAveragePrecisions.size != precomputedData.numberOfSystems) {
            rootLogger.warn(
                "meanAveragePrecisions length (${meanAveragePrecisions.size}) differs from numberOfSystems " +
                        "(${precomputedData.numberOfSystems}). Using PrecomputedData.fullSetMeanAPBySystem instead."
            )
        }
    }

    /* Correlation strategy compiled once, reused in evaluate() */
    private val fullSetMeanAPBySystem: DoubleArray = precomputedData.fullSetMeanAPBySystem
    private val ySideStats: Correlations.CorrelationYStats = Correlations.precomputeYStats(fullSetMeanAPBySystem)
    private val correlationFromSums: (DoubleArray, Int) -> Double =
        Correlations.makeCorrelationFromSums(parameters.correlationMethod, ySideStats)

    /* Streaming / bookkeeping for optional consumers */
    val dominatedSolutionsByK = linkedMapOf<Int, BinarySolution>()
    val topSolutionsByK = linkedMapOf<Int, MutableList<BinarySolution>>()

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
    private val numberOfSystems = precomputedData.numberOfSystems
    private val topicColumnByTopicIndex: Array<DoubleArray> = precomputedData.topicColumnViewByTopic
    private val maxEvaluations: Int = parameters.numberOfIterations

    /* Low-contention progress counter and percent-gated logger */
    private val evaluationCounter = LongAdder()
    private val lastLoggedPct = AtomicInteger(-1)

    /**
     * Attribute key to cache the last evaluated snapshot as a BinarySet.
     * Using an object key avoids string hashing and reduces GC pressure in the hot path.
     */
    private object SnapshotKey

    /**
     * Thread-local bitset to compute XOR diffs without allocation.
     * Each evaluation thread gets its own instance; capacity matches numberOfTopics.
     */
    private val diffBitsetThreadLocal = ThreadLocal.withInitial { BinarySet(numberOfTopics) }

    override fun name(): String = "BestSubsetProblem"
    override fun numberOfVariables(): Int = 1
    override fun numberOfObjectives(): Int = 2
    override fun numberOfConstraints(): Int = 0
    override fun totalNumberOfBits(): Int = numberOfTopics
    override fun numberOfBitsPerVariable(): List<Int> = listOf(numberOfTopics)

    /**
     * Seed initial individuals by sweeping increasing forced cardinalities
     * to ensure early generations cover small and medium K values.
     */
    private var nextForcedCardinality = 1

    override fun createSolution(): BinarySolution {
        return if (nextForcedCardinality < numberOfTopics) {
            BestSubsetSolution(
                1,
                2,
                numberOfTopics,
                topicLabels,
                forcedCardinality = nextForcedCardinality++
            )
        } else {
            BestSubsetSolution(
                1,
                2,
                numberOfTopics,
                topicLabels,
                forcedCardinality = null
            )
        }
    }

    /**
     * Evaluate a candidate by producing or updating the system-level SUMS vector,
     * then computing correlation in the requested backend.
     *
     * Two mutually exclusive paths
     *  • First-time build for this candidate: iterate only set bits (O(K)) and accumulate SUMS.
     *    Also materializes a BinarySet snapshot and a reusable boolean mask for future diffs.
     *  • Incremental update for a previously seen candidate:
     *      – If a fixed-K swap was recorded, apply a pair of AXPY updates for out/in indices.
     *      – Otherwise, XOR the previous snapshot with the current mask and AXPY only changed topics.
     *
     * The correlation is computed from SUMS and K without constructing means explicitly in the Pearson case.
     * The targetStrategy encodes objectives in-place using the natural correlation value.
     */
    override fun evaluate(solution: BinarySolution): BinarySolution {
        val subsetSolution = solution as BestSubsetSolution

        /* Percent-gated progress with minimal contention */
        evaluationCounter.add(1L)
        logProgressByPercent()

        val currentBitset: BinarySet = subsetSolution.variables()[0] as BinarySet
        val selectedTopicCount: Int = subsetSolution.numberOfSelectedTopics

        var cachedSumsBySystem: DoubleArray? = subsetSolution.cachedSumsBySystem
        var lastSnapshotBitset: BinarySet? = subsetSolution.attributes()[SnapshotKey] as? BinarySet
        var lastEvaluatedMask: BooleanArray? = subsetSolution.lastEvaluatedMask

        if (cachedSumsBySystem == null || lastSnapshotBitset == null || lastEvaluatedMask == null) {
            /**
             * First-time path for this candidate
             *  Accumulates SUMS by scanning set bits only and builds:
             *   • a BinarySet snapshot for XOR diffs,
             *   • a boolean[] mask for quick reads and external consumers.
             *  Complexity: O(K + S) where S is systems count hidden in AXPY.
             */
            val freshSumsBySystem = DoubleArray(numberOfSystems)
            val newSnapshotBitset = BinarySet(numberOfTopics)
            val reusableLastMask = subsetSolution.ensureReusableLastMaskBuffer()

            var topicIndex = currentBitset.nextSetBit(0)
            while (topicIndex >= 0) {
                newSnapshotBitset.set(topicIndex, true)
                reusableLastMask[topicIndex] = true
                axpyInPlace(freshSumsBySystem, topicColumnByTopicIndex[topicIndex], +1.0)
                topicIndex = currentBitset.nextSetBit(topicIndex + 1)
            }

            cachedSumsBySystem = freshSumsBySystem
            subsetSolution.cachedSumsBySystem = freshSumsBySystem
            subsetSolution.lastEvaluatedMask = reusableLastMask
            subsetSolution.attributes()[SnapshotKey] = newSnapshotBitset

        } else {
            /**
             * Incremental path
             *  Prefer a constant-time update when the mutation recorded a fixed-K swap,
             *  otherwise compute the XOR difference and touch only changed topics.
             *  After applying updates, keep snapshot and mask in sync with the candidate bitset.
             */
            val hasSwapHints =
                subsetSolution.lastMutationWasFixedKSwap &&
                        (subsetSolution.lastSwapOutIndex != null || subsetSolution.lastSwapInIndex != null)

            if (hasSwapHints) {
                subsetSolution.lastSwapOutIndex?.let { outTopicIndex ->
                    axpyInPlace(cachedSumsBySystem!!, topicColumnByTopicIndex[outTopicIndex], -1.0)
                    lastEvaluatedMask!![outTopicIndex] = false
                    lastSnapshotBitset!!.set(outTopicIndex, false)
                }
                subsetSolution.lastSwapInIndex?.let { inTopicIndex ->
                    axpyInPlace(cachedSumsBySystem!!, topicColumnByTopicIndex[inTopicIndex], +1.0)
                    lastEvaluatedMask!![inTopicIndex] = true
                    lastSnapshotBitset!!.set(inTopicIndex, true)
                }
            } else {
                /* XOR-diff path: visit only indices that changed since the last evaluation */
                val diffBitset = diffBitsetThreadLocal.get()
                diffBitset.clear()
                diffBitset.or(lastSnapshotBitset)
                diffBitset.xor(currentBitset)

                var changedTopic = diffBitset.nextSetBit(0)
                while (changedTopic >= 0) {
                    val nowSelected = currentBitset.get(changedTopic)
                    val alpha = if (nowSelected) +1.0 else -1.0
                    axpyInPlace(cachedSumsBySystem!!, topicColumnByTopicIndex[changedTopic], alpha)
                    lastEvaluatedMask!![changedTopic] = nowSelected
                    lastSnapshotBitset!!.set(changedTopic, nowSelected)
                    changedTopic = diffBitset.nextSetBit(changedTopic + 1)
                }
            }

            subsetSolution.cachedSumsBySystem = cachedSumsBySystem
            subsetSolution.clearLastMutationFlags()
        }

        /* Natural correlation from SUMS and K; targetStrategy encodes objectives in-place */
        val naturalCorrelation = correlationFromSums(cachedSumsBySystem!!, selectedTopicCount)
        targetStrategy(subsetSolution, naturalCorrelation)
        return subsetSolution
    }

    /* Progress logging */

    /**
     * Log “Completed evaluations …” every Constants.LOGGING_FACTOR percentage points.
     * Uses an AtomicInteger bucket to avoid duplicate logs under concurrency and to
     * ensure a monotone, at-most-once emission per bucket across threads.
     */
    private fun logProgressByPercent() {
        val total = maxEvaluations.coerceAtLeast(0)
        if (total == 0) return

        val completed = evaluationCounter.sum().coerceAtMost(total.toLong())
        val currentPct = ((completed * 100L) / total).toInt().coerceIn(0, 100)
        val step = Constants.LOGGING_FACTOR.coerceAtLeast(1)

        while (true) {
            val prev = lastLoggedPct.get()
            val nextThreshold = prev + step
            if (currentPct < nextThreshold && !(prev < 0 && currentPct >= 0)) return

            val newBucket = (currentPct / step) * step
            if (newBucket <= prev) return

            if (lastLoggedPct.compareAndSet(prev, newBucket)) {
                logger.info(
                    "Completed evaluations: {}/{} ({}%) on \"{}\" with target {}.",
                    completed,
                    total,
                    newBucket,
                    Thread.currentThread().name,
                    parameters.targetToAchieve
                )
                return
            }
        }
    }

    /* AXPY micro-kernel */

    /**
     * destination[i] += alpha * source[i]
     *
     * Micro-optimizations
     *  • Fast paths for alpha = ±1 to skip multiplications.
     *  • Unrolled by 4 to reduce loop overhead and enable simple pipelining.
     *  • Branchless tails for the remaining elements.
     */
    private fun axpyInPlace(destination: DoubleArray, source: DoubleArray, alpha: Double) {
        val n = destination.size

        if (alpha == 1.0) {
            var i = 0
            while (i + 3 < n) {
                destination[i] += source[i]
                destination[i + 1] += source[i + 1]
                destination[i + 2] += source[i + 2]
                destination[i + 3] += source[i + 3]
                i += 4
            }
            while (i < n) {
                destination[i] += source[i]; i++
            }
            return
        }

        if (alpha == -1.0) {
            var i = 0
            while (i + 3 < n) {
                destination[i] -= source[i]
                destination[i + 1] -= source[i + 1]
                destination[i + 2] -= source[i + 2]
                destination[i + 3] -= source[i + 3]
                i += 4
            }
            while (i < n) {
                destination[i] -= source[i]; i++
            }
            return
        }

        var i = 0
        while (i + 3 < n) {
            destination[i] += alpha * source[i]
            destination[i + 1] += alpha * source[i + 1]
            destination[i + 2] += alpha * source[i + 2]
            destination[i + 3] += alpha * source[i + 3]
            i += 4
        }
        while (i < n) {
            destination[i] += alpha * source[i]; i++
        }
    }
}
