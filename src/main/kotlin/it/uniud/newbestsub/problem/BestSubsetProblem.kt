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
        private var correlationStrategy: (Array<Double>, Array<Double>) -> Double,
        private var targetStrategy: (BinarySolution, Double) -> BinarySolution

) : AbstractBinaryProblem() {

    val dominatedSolutions = linkedMapOf<Double, BinarySolution>()
    private val logger = LogManager.getLogger()
    private var iterationCounter = 0
    private lateinit var solution: BestSubsetSolution

    init {
        numberOfVariables = 1
        numberOfObjectives = 2
        name = "BestSubsetProblem"
    }

    public override fun getBitsPerVariable(index: Int): Int {
        return solution.getNumberOfBits(0)
    }

    override fun createSolution(): BestSubsetSolution {
        solution = BestSubsetSolution(this, numberOfTopics); return solution
    }

    override fun evaluate(solution: BinarySolution) {

        solution as BestSubsetSolution

        if ((iterationCounter % Constants.ITERATION_LOGGING_FACTOR) == 0)
            logger.info("Current iteration: $iterationCounter/${parameters.numberOfIterations} for evaluations on \"${Thread.currentThread().name}\" with target ${parameters.targetToAchieve}")

        var iterator = averagePrecisions.entries.iterator()
        val meanAveragePrecisionsReduced = Array(averagePrecisions.entries.size, { Tools.getMean(iterator.next().value.toDoubleArray(), solution.retrieveTopicStatus()) })
        val correlation = correlationStrategy.invoke(meanAveragePrecisionsReduced, meanAveragePrecisions)

        logger.debug("<Correlation: $correlation, " + "Num. Sel. Topics: " + "${solution.numberOfSelectedTopics}, " + "Ev. Gene: ${solution.getVariableValueString(0)}>")

        val oldSolution = dominatedSolutions[solution.getObjective(1)]
        val oldCorrelation: Double

        if (oldSolution != null) {
            iterator = averagePrecisions.entries.iterator()
            val oldMeanAveragePrecisionReduced = Array(averagePrecisions.entries.size, { Tools.getMean(iterator.next().value.toDoubleArray(), solution.copy().retrieveTopicStatus()) })
            oldCorrelation = correlationStrategy.invoke(oldMeanAveragePrecisionReduced, meanAveragePrecisions)
            when (parameters.targetToAchieve) {
                Constants.TARGET_BEST -> if (correlation > oldCorrelation) dominatedSolutions[solution.getObjective(1)] = solution
                Constants.TARGET_WORST -> if (correlation < oldCorrelation) dominatedSolutions[solution.getObjective(1)] = solution
            }
        } else dominatedSolutions[solution.getObjective(1)] = solution

        targetStrategy(solution, correlation)
        iterationCounter++

    }

}
