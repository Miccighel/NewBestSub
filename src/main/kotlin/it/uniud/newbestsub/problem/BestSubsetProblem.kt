package it.uniud.newbestsub.problem

import it.uniud.newbestsub.dataset.Parameters
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.problem.impl.AbstractBinaryProblem
import org.uma.jmetal.solution.BinarySolution

class BestSubsetProblem(

        private var parameters: Parameters,
        private var numberOfTopics: Int,
        private var averagePrecisions: MutableMap<String, Array<Double>>,
        private var meanAveragePrecisions: Array<Double>,
        var topicLabels: Array<String>,
        private var correlationStrategy: (Array<Double>, Array<Double>) -> Double,
        private var targetStrategy: (BinarySolution, Double) -> BinarySolution

) : AbstractBinaryProblem() {

    val dominatedSolutions = linkedMapOf<Double, BinarySolution>()
    val topSolutions = linkedMapOf<Double, MutableList<BinarySolution>>()
    private var iterationCounter = 0
    private lateinit var solution: BestSubsetSolution
    private val logger = LogManager.getLogger()
    private var progressCounter = 0
    private var cardinalityToGenerate = 1

    init {
        numberOfVariables = 1
        numberOfObjectives = 2
        name = "BestSubsetProblem"
    }

    public override fun getBitsPerVariable(index: Int): Int {
        return solution.getNumberOfBits(0)
    }

    override fun getNumberOfBits(index: Int): Int {
        return solution.getNumberOfBits(0)
    }

    override fun createSolution(): BestSubsetSolution {
        if (cardinalityToGenerate < numberOfTopics) {
            solution = BestSubsetSolution(this, numberOfTopics, cardinalityToGenerate)
            cardinalityToGenerate++
        } else solution = BestSubsetSolution(this, numberOfTopics)
        return solution
    }

    override fun evaluate(solution: BinarySolution) {

        solution as BestSubsetSolution

        // FIRST EVALUATION PART: The selected solution is evaluated to compute correlation with MAP values.

        val loggingFactor = (parameters.numberOfIterations * Constants.LOGGING_FACTOR) / 100
        if ((iterationCounter % loggingFactor) == 0 && parameters.numberOfIterations > loggingFactor && iterationCounter <= parameters.numberOfIterations) {
            logger.info("Completed iterations: $iterationCounter/${parameters.numberOfIterations} ($progressCounter%) for evaluations being computed on \"${Thread.currentThread().name}\" with target ${parameters.targetToAchieve}.")
            progressCounter += Constants.LOGGING_FACTOR
        }

        var iterator = averagePrecisions.entries.iterator()
        val meanAveragePrecisionsReduced = Array(averagePrecisions.entries.size, { Tools.getMean(iterator.next().value.toDoubleArray(), solution.retrieveTopicStatus()) })
        val correlation = correlationStrategy.invoke(meanAveragePrecisionsReduced, meanAveragePrecisions)
        solution.topicStatus = solution.retrieveTopicStatus().toTypedArray()
        val oldSolution = dominatedSolutions[solution.numberOfSelectedTopics.toDouble()]
        val oldCorrelation: Double

        logger.debug("<Correlation: $correlation, Num. Sel. Topics: ${solution.numberOfSelectedTopics}, Sel. Topics: ${solution.getTopicLabelsFromTopicStatus()}, Ev. Gene: ${solution.getVariableValueString(0)}>")

        targetStrategy(solution, correlation)

        // SECOND EVALUATION PART: A copy of the selected solution is evaluated to verify if it's a better dominated solution

        val firstSolutionCopy = solution.copy()
        if (oldSolution != null) {
            oldSolution as BestSubsetSolution
            iterator = averagePrecisions.entries.iterator()
            val oldMeanAveragePrecisionsReduced = Array(averagePrecisions.entries.size, { Tools.getMean(iterator.next().value.toDoubleArray(), oldSolution.retrieveTopicStatus()) })
            oldCorrelation = correlationStrategy.invoke(oldMeanAveragePrecisionsReduced, meanAveragePrecisions)
            when (parameters.targetToAchieve) {
                Constants.TARGET_BEST -> if (correlation > oldCorrelation) dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = firstSolutionCopy
                Constants.TARGET_WORST -> if (correlation < oldCorrelation) dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = firstSolutionCopy
            }
        } else dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = firstSolutionCopy

        // THIRD EVALUATION PART: A copy of the selected solution is pushed into the top solutions list. At every evaluation, only the first CONSTANTS.TOP_SOLUTIONS_NUMBER are retained.

        val secondSolutionCopy = solution.copy()
        var topSolutionsList = topSolutions[solution.numberOfSelectedTopics.toDouble()]
        if (topSolutionsList == null) topSolutionsList = mutableListOf()
        topSolutionsList.plusAssign(secondSolutionCopy)
        if (topSolutionsList.size > Constants.TOP_SOLUTIONS_NUMBER) {
            topSolutionsList.sortWith(kotlin.Comparator { firstSolution: BinarySolution, secondSolution: BinarySolution ->
                if (firstSolution.getCorrelation() < secondSolution.getCorrelation()) 1 else if (firstSolution.getCorrelation() == secondSolution.getCorrelation()) 0 else -1
            })
            topSolutionsList = topSolutionsList.distinct().toMutableList()
            topSolutionsList = topSolutionsList.asReversed().take(Constants.TOP_SOLUTIONS_NUMBER).toMutableList()
        }
        topSolutions[solution.numberOfSelectedTopics.toDouble()] = topSolutionsList

        iterationCounter++

    }
}
