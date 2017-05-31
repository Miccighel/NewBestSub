package it.uniud.newbestsub.problem

import it.uniud.newbestsub.utils.Tools
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.problem.impl.AbstractBinaryProblem
import org.uma.jmetal.solution.BinarySolution

class BestSubsetProblem(

        private var numberOfTopics: Int,
        private var averagePrecisions: MutableMap<String, Array<Double>>,
        private var meanAveragePrecisions: Array<Double>,
        private var correlationStrategy: (Array<Double>, Array<Double>) -> Double,
        private var targetToAchieve: (BinarySolution, Double) -> BinarySolution

) : AbstractBinaryProblem() {

    private val logger = LogManager.getLogger()
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

        val iterator = averagePrecisions.entries.iterator()
        val meanAveragePrecisionsReduced = Array(averagePrecisions.entries.size, {Tools.getMean(iterator.next().value.toDoubleArray(), solution.retrieveTopicStatus())})
        val correlation = correlationStrategy.invoke(meanAveragePrecisionsReduced, meanAveragePrecisions)

        logger.debug("<Correlation: $correlation, " + "Num. Sel. Topics: " + "${solution.numberOfSelectedTopics}, " + "Ev. Gene: ${solution.getVariableValueString(0)}>")

        targetToAchieve(solution, correlation)

    }
}
