package it.uniud.newbestsub.problem

import it.uniud.newbestsub.dataset.Parameters
import it.uniud.newbestsub.dataset.PrecomputedData
import it.uniud.newbestsub.dataset.buildPrecomputedData
import it.uniud.newbestsub.dataset.toPrimitiveAPRows
import it.uniud.newbestsub.utils.Constants
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.problem.binaryproblem.BinaryProblem
import org.uma.jmetal.solution.binarysolution.BinarySolution

/**
 * BestSubsetProblem
 * -----------------
 * Binary subset selection for IR-style evaluation:
 *  - Objective[0] encodes cardinality (or -cardinality for WORST)
 *  - Objective[1] encodes correlation (possibly negated for BEST)
 *
 * This version adopts a shared, read-only PrecomputedData to eliminate boxing
 * and map lookups on the hot path. It preserves the previous semantics and
 * public API usage via a secondary constructor that bridges legacy types.
 *
 * Step 1 goals (this file):
 *  - Move to primitive DoubleArray matrices (no more Array<Double> in the loop)
 *  - Keep the same create/evaluate behavior and targetStrategy wiring
 *  - Prepare for Step 2 (fixed-K swap mutation) and Step 3 (delta evaluation)
 */
class BestSubsetProblem(

    /** Run-time parameters (seeds, logging cadence, target branch, etc.). */
    private val parameters: Parameters,

    /** Number of topics = total number of decision bits. */
    private val numberOfTopics: Int,

    /**
     * Immutable, shared numeric bundle:
     *  - averagePrecisionBySystem[s][t]  : AP of system s on topic t
     *  - topicColumnViewByTopic[t][s]    : column view for fast ± updates
     *  - fullSetMeanAPBySystem[s]        : mean AP on the full topic set
     */
    private val precomputedData: PrecomputedData,

    /** Human-readable topic labels (kept as-is). */
    val topicLabels: Array<String>,

    /**
     * Correlation function on primitive vectors (DoubleArray).
     * Typical choice: Pearson between per-system means.
     * NOTE: a legacy Array<Double>-based function can be plugged via the
     *       secondary constructor below; this keeps callers unchanged.
     */
    private val correlationFunction: (DoubleArray, DoubleArray) -> Double,

    /**
     * Caller-provided strategy that writes objectives according to branch:
     *  - BEST  : [0]= +K, [1]= -corr
     *  - WORST : [0]= -K, [1]= +corr
     *  - AVERAGE handled elsewhere as needed
     */
    private val targetStrategy: (BinarySolution, Double) -> BinarySolution

) : BinaryProblem {

    /* --- Compatibility constructor (keeps your current call sites unchanged) --- */
    constructor(
        parameters: Parameters,
        numberOfTopics: Int,
        averagePrecisions: MutableMap<String, Array<Double>>,
        meanAveragePrecisions: Array<Double>,  // retained for compatibility; not used directly
        topicLabels: Array<String>,
        correlationStrategy: (Array<Double>, Array<Double>) -> Double,
        targetStrategy: (BinarySolution, Double) -> BinarySolution
    ) : this(
        parameters = parameters,
        numberOfTopics = numberOfTopics,
        precomputedData = buildPrecomputedData(toPrimitiveAPRows(averagePrecisions)),
        topicLabels = topicLabels,
        correlationFunction = { left: DoubleArray, right: DoubleArray ->
            // Bridge to legacy signature with minimal boxing (vectors are short = #systems)
            val leftBoxed = Array(left.size) { i -> left[i] }
            val rightBoxed = Array(right.size) { i -> right[i] }
            correlationStrategy.invoke(leftBoxed, rightBoxed)
        },
        targetStrategy = targetStrategy
    ) {
        // Optional sanity check: warn if the provided meanAveragePrecisions
        // don't match the precomputed full-set means (ordering or values differ).
        val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
        if (meanAveragePrecisions.size != precomputedData.numberOfSystems) {
            logger.warn(
                "meanAveragePrecisions length (${meanAveragePrecisions.size}) " +
                    "differs from numberOfSystems (${precomputedData.numberOfSystems}). " +
                    "Using PrecomputedData.fullSetMeanAPBySystem instead."
            )
        }
    }

    /* --- State used for streaming and progress reporting --- */
    val dominatedSolutions = linkedMapOf<Double, BinarySolution>()                 // best/worst representative per K
    val topSolutions = linkedMapOf<Double, MutableList<BinarySolution>>()          // top-N per K (genotype-deduped)
    private var iterationCounter = 0
    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
    private var progressCounter = 0
    private var cardinalityToGenerate = 1

    /* -------- BinaryProblem / Problem API (jMetal 6.x+) -------- */
    override fun name(): String = "BestSubsetProblem"
    override fun numberOfVariables(): Int = 1
    override fun numberOfObjectives(): Int = 2
    override fun numberOfConstraints(): Int = 0
    override fun totalNumberOfBits(): Int = numberOfTopics
    override fun numberOfBitsPerVariable(): List<Int> = listOf(numberOfTopics)

    /**
     * createSolution()
     * ----------------
     * Two-phase generation:
     *  1) Deterministic sweep of exact cardinalities K = 1..(n-1)
     *  2) Random subsets (ensuring at least one bit set)
     *
     * This is unchanged; Step 2 will introduce a fixed-K swap mutation operator,
     * not changes here.
     */
    override fun createSolution(): BinarySolution {
        return if (cardinalityToGenerate < numberOfTopics) {
            val solution = BestSubsetSolution(
                numberOfVariables = 1,
                numberOfObjectives = 2,
                numberOfTopics = numberOfTopics,
                topicLabels = topicLabels,
                forcedCardinality = cardinalityToGenerate   // exact K
            )
            cardinalityToGenerate++
            solution
        } else {
            BestSubsetSolution(
                numberOfVariables = 1,
                numberOfObjectives = 2,
                numberOfTopics = numberOfTopics,
                topicLabels = topicLabels,
                forcedCardinality = null                    // random, but ≥1 bit set
            )
        }
    }

    /**
     * evaluate()
     * ----------
     * Computes correlation between:
     *  - Per-system mean AP **restricted to selected topics** (subset means)
     *  - Precomputed per-system mean AP **over the full set** (full means)
     *
     * Step 1 implementation uses primitive arrays and a cache-friendly
     * column-sum pass. Step 3 will replace the O(S * K) mean computation
     * with O(S) delta updates (using topicColumnViewByTopic) after swaps.
     */
    override fun evaluate(solution: BinarySolution): BinarySolution {
        solution as BestSubsetSolution

        /* Progress logging (unchanged) */
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
            progressCounter += Constants.LOGGING_FACTOR
        }

        /* --- Build subset means using primitive arrays --- */
        val selectedMask: BooleanArray = solution.retrieveTopicStatus()
        val selectedTopicCount = solution.numberOfSelectedTopics

        // Sums over selected topics for each system (size = #systems)
        val sumsOfAPOverSelectedTopicsBySystem = DoubleArray(precomputedData.numberOfSystems)

        // Cache-friendly pass: iterate selected topics, add their AP column to the running sums
        var topicIndex = 0
        while (topicIndex < numberOfTopics) {
            if (selectedMask[topicIndex]) {
                val columnView = precomputedData.topicColumnViewByTopic[topicIndex] // [system]
                var systemIndex = 0
                while (systemIndex < precomputedData.numberOfSystems) {
                    sumsOfAPOverSelectedTopicsBySystem[systemIndex] += columnView[systemIndex]
                    systemIndex++
                }
            }
            topicIndex++
        }

        // Convert sums to means for the left-hand vector of the correlation
        val subsetMeanAPBySystem = DoubleArray(precomputedData.numberOfSystems)
        if (selectedTopicCount > 0) {
            var systemIndex = 0
            while (systemIndex < precomputedData.numberOfSystems) {
                subsetMeanAPBySystem[systemIndex] =
                    sumsOfAPOverSelectedTopicsBySystem[systemIndex] / selectedTopicCount
                systemIndex++
            }
        } // else remains zeros

        // Right-hand vector: full-set means from PrecomputedData
        val fullSetMeanAPBySystem = precomputedData.fullSetMeanAPBySystem

        // Compute correlation using the provided primitive correlation function
        val correlation = correlationFunction.invoke(subsetMeanAPBySystem, fullSetMeanAPBySystem)

        // Keep topic status mirrored for downstream consumers (unchanged external behavior)
        solution.topicStatus = selectedMask.toTypedArray()

        /* objective[0] = cardinality (or -cardinality), objective[1] = correlation (possibly negated) */
        targetStrategy(solution, correlation)  // side-effect: writes objectives()

        /* Maintain per-K dominated representative (best or worst) */
        val existingRepresentative = dominatedSolutions[solution.numberOfSelectedTopics.toDouble()]
        val candidateCopy = solution.copy()

        if (existingRepresentative != null) {
            // Re-evaluate the existing representative's correlation using the same primitive path
            val oldMask = (existingRepresentative as BestSubsetSolution).retrieveTopicStatus()
            val oldSums = DoubleArray(precomputedData.numberOfSystems)
            var t = 0
            while (t < numberOfTopics) {
                if (oldMask[t]) {
                    val col = precomputedData.topicColumnViewByTopic[t]
                    var s = 0
                    while (s < precomputedData.numberOfSystems) {
                        oldSums[s] += col[s]; s++
                    }
                }
                t++
            }
            val oldMeans = DoubleArray(precomputedData.numberOfSystems)
            if (existingRepresentative.numberOfSelectedTopics > 0) {
                var s = 0
                while (s < precomputedData.numberOfSystems) {
                    oldMeans[s] = oldSums[s] / existingRepresentative.numberOfSelectedTopics
                    s++
                }
            }
            val oldCorrelation = correlationFunction.invoke(oldMeans, fullSetMeanAPBySystem)

            when (parameters.targetToAchieve) {
                Constants.TARGET_BEST ->
                    if (correlation > oldCorrelation) {
                        dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = candidateCopy
                    }
                Constants.TARGET_WORST ->
                    if (correlation < oldCorrelation) {
                        dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = candidateCopy
                    }
            }
        } else {
            dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = candidateCopy
        }

        /* Keep the top list per K (deduped by genotype), ordered by true correlation scale */
        val kKey = solution.numberOfSelectedTopics.toDouble()
        val entriesForK = topSolutions.getOrPut(kKey) { mutableListOf() }
        entriesForK += solution.copy()

        fun realCorrelationForRanking(s: BinarySolution): Double =
            if (parameters.targetToAchieve == Constants.TARGET_BEST) -s.getCorrelation() else s.getCorrelation()

        val genotypeString: (BinarySolution) -> String = { (it as BestSubsetSolution).getVariableValueString(0) }

        if (parameters.targetToAchieve == Constants.TARGET_BEST) {
            entriesForK.sortWith(
                compareByDescending<BinarySolution> { realCorrelationForRanking(it) }
                    .thenBy { genotypeString(it) }
            )
        } else {
            entriesForK.sortWith(
                compareBy<BinarySolution> { realCorrelationForRanking(it) }
                    .thenBy { genotypeString(it) }
            )
        }

        // Dedupe by genotype while preserving order, then cap list length
        run {
            val seenGenotypes = LinkedHashSet<String>(entriesForK.size)
            val deduped = mutableListOf<BinarySolution>()
            for (s in entriesForK) {
                val key = genotypeString(s)
                if (seenGenotypes.add(key)) deduped += s
            }
            topSolutions[kKey] = deduped.take(Constants.TOP_SOLUTIONS_NUMBER).toMutableList()
        }

        iterationCounter++
        return solution
    }
}
