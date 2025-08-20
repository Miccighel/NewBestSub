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

    override fun getName(): String = "BestSubsetProblem"
    override fun getNumberOfVariables(): Int = 1
    override fun getNumberOfObjectives(): Int = 2
    override fun getNumberOfConstraints(): Int = 0
    override fun getTotalNumberOfBits(): Int = numberOfTopics

    override fun createSolution(): BinarySolution {
        /* Generate increasing K = 1..(n-1), then random */
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

    override fun evaluate(solution: BinarySolution) {

        solution as BestSubsetSolution

        /* FIRST EVALUATION PART: The selected solution is evaluated to compute correlation with MAP values. */
        val loggingFactor = (parameters.numberOfIterations * Constants.LOGGING_FACTOR) / 100
        if (loggingFactor > 0 &&
            (iterationCounter % loggingFactor) == 0 &&
            parameters.numberOfIterations >= loggingFactor &&
            iterationCounter <= parameters.numberOfIterations
        ) {
            logger.info("Completed iterations: $iterationCounter/${parameters.numberOfIterations} ($progressCounter%) for evaluations being computed on \"${Thread.currentThread().name}\" with target ${parameters.targetToAchieve}.")
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
            "<Correlation: $correlation, Num. Sel. Topics: ${solution.numberOfSelectedTopics}, Sel. Topics: ${solution.getTopicLabelsFromTopicStatus()}, Ev. Gene: ${solution.getVariableValueString(0)}>"
        )

        /* objective[0] = cardinality (or -cardinality), objective[1] = correlation (possibly negated) */
        targetStrategy(solution, correlation)

        /* SECOND EVALUATION PART: A copy of the selected solution is evaluated to verify if it's a better dominated solution */
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
                Constants.TARGET_BEST -> if (correlation > oldCorrelation) dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = candidateCopy
                Constants.TARGET_WORST -> if (correlation < oldCorrelation) dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = candidateCopy
            }
        } else {
            dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = candidateCopy
        }

        /* THIRD EVALUATION PART: Push a copy into the top solutions list. Keep only the first CONSTANTS.TOP_SOLUTIONS_NUMBER. */
        val kKey = solution.numberOfSelectedTopics.toDouble()
        val entryList = topSolutions.getOrPut(kKey) { mutableListOf() }
        entryList += solution.copy()

        /* Sort consistently on the REAL correlation scale (undo sign if BEST uses -corr). */
        fun scoreForRanking(s: BinarySolution): Double =
            if (parameters.targetToAchieve == Constants.TARGET_BEST) -s.getCorrelation() else s.getCorrelation()

        /* Determinism: add a stable tie-breaker on the genotype string so equal scores are reproducible. */
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

        /* Determinism: remove duplicates by genotype (not by reference), preserving order. */
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
    }

    /* Add these overrides to satisfy BinaryProblem in jMetal 5.10 */
    override fun getListOfBitsPerVariable(): List<Int> = listOf(numberOfTopics)

    override fun getBitsFromVariable(index: Int): Int {
        /* We have a single binary variable whose length equals numberOfTopics */
        require(index == 0) { "BestSubsetProblem has exactly 1 variable; requested index=$index" }
        return numberOfTopics
    }
}
