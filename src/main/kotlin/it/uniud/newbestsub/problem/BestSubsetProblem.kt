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
        var topicLabels : Array<String>,
        private var correlationStrategy: (Array<Double>, Array<Double>) -> Double,
        private var targetStrategy: (BinarySolution, Double) -> BinarySolution

) : AbstractBinaryProblem() {

    val dominatedSolutions = linkedMapOf<Double, BinarySolution>()
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

    override fun createSolution(): BestSubsetSolution {
        if (cardinalityToGenerate < numberOfTopics) {
            solution = BestSubsetSolution(this, numberOfTopics, cardinalityToGenerate)
            cardinalityToGenerate++
        } else solution = BestSubsetSolution(this, numberOfTopics)
        return solution
    }

    override fun evaluate(solution: BinarySolution) {

        solution as BestSubsetSolution

        val loggingFactor = (parameters.numberOfIterations * Constants.LOGGING_FACTOR) / 100

        if ((iterationCounter % loggingFactor) == 0 && parameters.numberOfIterations > loggingFactor) {
            logger.info("Completed iterations: $iterationCounter/${parameters.numberOfIterations} ($progressCounter%) for evaluations being computed on \"${Thread.currentThread().name}\" with target ${parameters.targetToAchieve}.")
            progressCounter += Constants.LOGGING_FACTOR
        }

        var iterator = averagePrecisions.entries.iterator()
        val meanAveragePrecisionsReduced = Array(averagePrecisions.entries.size, { Tools.getMean(iterator.next().value.toDoubleArray(), solution.retrieveTopicStatus()) })
        val correlation = correlationStrategy.invoke(meanAveragePrecisionsReduced, meanAveragePrecisions)

        val solutionOld = dominatedSolutions[solution.numberOfSelectedTopics.toDouble()]
        val correlationOld: Double

        logger.debug("<Correlation: $correlation, Num. Sel. Topics: ${solution.numberOfSelectedTopics}, Sel. Topics: ${solution.getTopicLabelsFromTopicStatus()}, Ev. Gene: ${solution.getVariableValueString(0)}>")

        targetStrategy(solution, correlation)

        val solutionCopy = solution.copy()
        if (solutionOld != null) {
            solutionOld as BestSubsetSolution
            iterator = averagePrecisions.entries.iterator()
            val meanAveragePrecisionReducedOld = Array(averagePrecisions.entries.size, { Tools.getMean(iterator.next().value.toDoubleArray(), solutionOld.retrieveTopicStatus()) })
            correlationOld = correlationStrategy.invoke(meanAveragePrecisionReducedOld, meanAveragePrecisions)
            when (parameters.targetToAchieve) {
                Constants.TARGET_BEST -> if (correlation > correlationOld) dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = solutionCopy
                Constants.TARGET_WORST -> if (correlation < correlationOld) dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = solutionCopy
            }
        } else dominatedSolutions[solution.numberOfSelectedTopics.toDouble()] = solutionCopy

        iterationCounter++

    }
}
