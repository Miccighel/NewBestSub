package it.uniud.newbestsub.problem

import it.uniud.newbestsub.dataset.Parameters
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.problem.binaryproblem.BinaryProblem
import org.uma.jmetal.solution.binarysolution.BinarySolution

class BestSubsetProblem(

    private val parameters: Parameters,
    private val numberOfTopics: Int,
    private val averagePrecisions: MutableMap<String, Array<Double>>,
    private val meanAveragePrecisions: Array<Double>,
    val topicLabels: Array<String>,
    private val correlationStrategy: (Array<Double>, Array<Double>) -> Double,
    private val targetStrategy: (BinarySolution, Double) -> BinarySolution

) : BinaryProblem {

    val dominatedSolutions = linkedMapOf<Double, BinarySolution>()
    val topSolutions = linkedMapOf<Double, MutableList<BinarySolution>>()
    private var iterationCounter = 0
    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
    private var progressCounter = 0
    private var cardinalityToGenerate = 1

    /* -------- BinaryProblem / Problem API (jMetal 6.x+) --------
     * jMetal 6 uses property-style method names: name(), numberOfVariables(), etc. */
    override fun name(): String = "BestSubsetProblem"
    override fun numberOfVariables(): Int = 1
    override fun numberOfObjectives(): Int = 2
    override fun numberOfConstraints(): Int = 0
    override fun totalNumberOfBits(): Int = numberOfTopics

    /* Generate solutions in two phases:
     *  1) Deterministic sweep of exact cardinalities K = 1..(n-1).
     *  2) Random subsets (ensuring at least one bit set). */
    override fun createSolution(): BinarySolution {
        return if (cardinalityToGenerate < numberOfTopics) {
            val s = BestSubsetSolution(
                /* Core sizes required by Solution API */
                numberOfVariables = 1,
                numberOfObjectives = 2,
                /* Domain-specific */
                numberOfTopics = numberOfTopics,
                topicLabels = topicLabels,
                /* Force exact cardinality K */
                forcedCardinality = cardinalityToGenerate
            )
            cardinalityToGenerate++
            s
        } else {
            /* Random subset (ensures at least one bit set) */
            BestSubsetSolution(
                numberOfVariables = 1,
                numberOfObjectives = 2,
                numberOfTopics = numberOfTopics,
                topicLabels = topicLabels,
                forcedCardinality = null
            )
        }
    }

    /* jMetal 6.x+: evaluate(...) returns the same instance after side-effects on objectives(). */
    override fun evaluate(solution: BinarySolution): BinarySolution {
        solution as BestSubsetSolution

        /* FIRST PART: compute correlation against MAP values and write objectives through the target strategy. */
        val loggingFactor = (parameters.numberOfIterations * Constants.LOGGING_FACTOR) / 100
        if (loggingFactor > 0 &&
            (iterationCounter % loggingFactor) == 0 &&
            parameters.numberOfIterations >= loggingFactor &&
            iterationCounter <= parameters.numberOfIterations
        ) {
            logger.info(
                "Completed iterations: $iterationCounter/${parameters.numberOfIterations} ($progressCounter%) " +
                    "for evaluations on \"${Thread.currentThread().name}\" with target ${parameters.targetToAchieve}."
            )
            progressCounter += Constants.LOGGING_FACTOR
        }

        var iterator = averagePrecisions.entries.iterator()
        val mask = solution.retrieveTopicStatus()
        val meanAveragePrecisionsReduced = Array(averagePrecisions.size) {
            Tools.getMean(iterator.next().value.toDoubleArray(), mask)
        }
        val correlation = correlationStrategy.invoke(meanAveragePrecisionsReduced, meanAveragePrecisions)
        solution.topicStatus = mask.toTypedArray()

        val oldSolution = dominatedSolutions[solution.numberOfSelectedTopics.toDouble()]
        logger.debug(
            "<Correlation: $correlation, Num. Sel. Topics: ${solution.numberOfSelectedTopics}, " +
                "Sel. Topics: ${solution.getTopicLabelsFromTopicStatus()}, Ev. Gene: ${solution.getVariableValueString(0)}>"
        )

        /* objective[0] = cardinality (or -cardinality), objective[1] = correlation (possibly negated) */
        targetStrategy(solution, correlation)  /* side-effect: writes objectives() */

        /* SECOND PART: evaluate a copy to maintain per-K dominated representative if it improves. */
        val candidateCopy = solution.copy()
        if (oldSolution != null) {
            oldSolution as BestSubsetSolution
            iterator = averagePrecisions.entries.iterator()
            val oldMask = oldSolution.retrieveTopicStatus()
            val oldMeanAPReduced = Array(averagePrecisions.size) {
                Tools.getMean(iterator.next().value.toDoubleArray(), oldMask)
            }
            val oldCorrelation = correlationStrategy.invoke(oldMeanAPReduced, meanAveragePrecisions)
            when (parameters.targetToAchieve) {
                Constants.TARGET_BEST ->
                    if (correlation > oldCorrelation)
                        dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = candidateCopy
                Constants.TARGET_WORST ->
                    if (correlation < oldCorrelation)
                        dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = candidateCopy
            }
        } else {
            dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = candidateCopy
        }

        /* THIRD PART: push a copy into the top list for this K, keep only the first TOP_SOLUTIONS_NUMBER entries.
         * Sorting uses the real correlation scale (undoing the sign if BEST negates correlation). */
        val kKey = solution.numberOfSelectedTopics.toDouble()
        val entryList = topSolutions.getOrPut(kKey) { mutableListOf() }
        entryList += solution.copy()

        fun scoreForRanking(s: BinarySolution): Double =
            if (parameters.targetToAchieve == Constants.TARGET_BEST) -s.getCorrelation() else s.getCorrelation()

        val tieBreak: (BinarySolution) -> String = { (it as BestSubsetSolution).getVariableValueString(0) }

        if (parameters.targetToAchieve == Constants.TARGET_BEST) {
            entryList.sortWith(
                compareByDescending<BinarySolution> { scoreForRanking(it) }
                    .thenBy { tieBreak(it) }
            )
        } else {
            entryList.sortWith(
                compareBy<BinarySolution> { scoreForRanking(it) }
                    .thenBy { tieBreak(it) }
            )
        }

        /* Remove duplicates by genotype, preserving order. */
        run {
            val seen = LinkedHashSet<String>(entryList.size)
            val dedup = mutableListOf<BinarySolution>()
            for (s in entryList) {
                val key = tieBreak(s)
                if (seen.add(key)) dedup += s
            }
            topSolutions[kKey] = dedup.take(Constants.TOP_SOLUTIONS_NUMBER).toMutableList()
        }

        iterationCounter++
        return solution
    }

    /* -------- BinaryProblem bit layout (jMetal 6.1) --------
     * Report per-variable bit lengths using the new API. */
    override fun numberOfBitsPerVariable(): List<Int> = listOf(numberOfTopics)
}
