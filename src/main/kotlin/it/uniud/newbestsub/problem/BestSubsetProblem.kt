package it.uniud.newbestsub.problem

import it.uniud.newbestsub.dataset.Parameters
import it.uniud.newbestsub.dataset.PrecomputedData
import it.uniud.newbestsub.dataset.buildPrecomputedData
import it.uniud.newbestsub.dataset.toPrimitiveAPRows
import it.uniud.newbestsub.utils.Constants
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.problem.binaryproblem.BinaryProblem
import org.uma.jmetal.solution.binarysolution.BinarySolution

class BestSubsetProblem(
    private val parameters: Parameters,
    private val numberOfTopics: Int,
    private val precomputedData: PrecomputedData,
    val topicLabels: Array<String>,
    @Suppress("unused")
    private val correlationFunction: (DoubleArray, DoubleArray) -> Double,
    private val targetStrategy: (BinarySolution, Double) -> BinarySolution
) : BinaryProblem {

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
        correlationFunction = { left: DoubleArray, right: DoubleArray ->
            val leftBoxed = Array(left.size) { i -> left[i] }
            val rightBoxed = Array(right.size) { i -> right[i] }
            correlationStrategy.invoke(leftBoxed, rightBoxed)
        },
        targetStrategy = targetStrategy
    )

    /* --- Pearson precomputation (Y vector = full mean APs) --- */
    private lateinit var y: DoubleArray
    private var yMean = 0.0
    private var yVarTerm = 0.0
    private var systemsCount = 0

    /* --- Streaming / bookkeeping --- */
    val dominatedSolutions = linkedMapOf<Double, BinarySolution>()
    val topSolutions = linkedMapOf<Double, MutableList<BinarySolution>>()
    private var iterationCounter = 0
    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
    private var progressCounter = 0
    private var cardinalityToGenerate = 1

    init {
        systemsCount = precomputedData.numberOfSystems
        y = precomputedData.fullSetMeanAPBySystem
        var sumY = 0.0
        var sumY2 = 0.0
        for (v in y) {
            sumY += v
            sumY2 += v * v
        }
        yMean = sumY / systemsCount
        yVarTerm = sumY2 - systemsCount * yMean * yMean
    }

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

        /* ---------- Progress logging with debounce msg ---------- */
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
                "[debounce] Log messages are throttled to every $loggingEvery iterations " +
                "(${Constants.LOGGING_FACTOR}% of total)."
            )
            progressCounter += Constants.LOGGING_FACTOR
        }

        /* ----------------------------- Incremental sums ----------------------------- */
        val currentMask: BooleanArray = solution.retrieveTopicStatus()
        val selectedTopicCount = solution.numberOfSelectedTopics

        var sumsBySystem = solution.cachedSumsBySystem
        val lastMask = solution.lastEvaluatedMask

        if (sumsBySystem == null || lastMask == null) {
            val fresh = DoubleArray(systemsCount)
            for (t in 0 until numberOfTopics) {
                if (currentMask[t]) {
                    val col = precomputedData.topicColumnViewByTopic[t]
                    for (s in 0 until systemsCount) fresh[s] += col[s]
                }
            }
            sumsBySystem = fresh
        } else {
            val usedSwapHints =
                solution.lastMutationWasFixedKSwap &&
                (solution.lastSwapOutIndex != null || solution.lastSwapInIndex != null)

            if (usedSwapHints) {
                solution.lastSwapOutIndex?.let { outIdx ->
                    val col = precomputedData.topicColumnViewByTopic[outIdx]
                    for (s in 0 until systemsCount) sumsBySystem[s] -= col[s]
                }
                solution.lastSwapInIndex?.let { inIdx ->
                    val col = precomputedData.topicColumnViewByTopic[inIdx]
                    for (s in 0 until systemsCount) sumsBySystem[s] += col[s]
                }
            } else {
                for (t in 0 until numberOfTopics) {
                    val was = lastMask[t]
                    val isSel = currentMask[t]
                    if (was != isSel) {
                        val col = precomputedData.topicColumnViewByTopic[t]
                        val sign = if (isSel) +1.0 else -1.0
                        for (s in 0 until systemsCount) sumsBySystem[s] += sign * col[s]
                    }
                }
            }
        }

        solution.cachedSumsBySystem = sumsBySystem
        solution.lastEvaluatedMask = currentMask.copyOf()
        solution.clearLastMutationFlags()

        /* ----------------------------- Inline Pearson ----------------------------- */
        val k = selectedTopicCount
        val corr = if (k <= 0 || yVarTerm == 0.0) {
            0.0
        } else {
            var sumX = 0.0
            var sumX2 = 0.0
            var sumXY = 0.0
            val invK = 1.0 / k
            for (s in 0 until systemsCount) {
                val x = sumsBySystem[s] * invK
                sumX += x
                sumX2 += x * x
                sumXY += x * y[s]
            }
            val meanX = sumX / systemsCount
            val num = sumXY - systemsCount * meanX * yMean
            val denLeft = sumX2 - systemsCount * meanX * meanX
            val den = kotlin.math.sqrt(denLeft * yVarTerm)
            if (den == 0.0) 0.0 else num / den
        }

        solution.topicStatus = currentMask.toTypedArray()
        targetStrategy(solution, corr)

        /* ----------------------------- Streaming book-keeping ----------------------------- */
        val kKeyRep = solution.numberOfSelectedTopics.toDouble()
        val candidateCopy = solution.copy()

        fun naturalCorr(sln: BinarySolution): Double =
            if (parameters.targetToAchieve == Constants.TARGET_BEST) -sln.getCorrelation() else sln.getCorrelation()

        val prev = dominatedSolutions[kKeyRep]
        if (prev == null ||
            (if (parameters.targetToAchieve == Constants.TARGET_BEST)
                naturalCorr(candidateCopy) > naturalCorr(prev)
            else
                naturalCorr(candidateCopy) < naturalCorr(prev))
        ) {
            dominatedSolutions[kKeyRep] = candidateCopy
        }

        val entriesForK = topSolutions.getOrPut(kKeyRep) { mutableListOf() }
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
        topSolutions[kKeyRep] = deduped.take(Constants.TOP_SOLUTIONS_NUMBER).toMutableList()

        iterationCounter++
        return solution
    }
}
