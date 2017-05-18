package it.uniud.newbestsub.problem

import it.uniud.newbestsub.utils.Formula
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.problem.impl.AbstractBinaryProblem
import org.uma.jmetal.solution.BinarySolution

class BestSubsetProblem(

        private var numberOfTopics: Int,
        private var averagePrecisions: MutableMap<String, DoubleArray>,
        private var meanAveragePrecisions: DoubleArray,
        private var correlationStrategy: (DoubleArray, DoubleArray) -> Double,
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

        val meanAveragePrecisionsReduced = DoubleArray(averagePrecisions.entries.size)

        averagePrecisions.entries.forEachIndexed {
            index, singleSystem ->
            meanAveragePrecisionsReduced[index] = Formula.getMean(singleSystem.value, solution.retrieveTopicStatus())
        }

        val correlation = correlationStrategy.invoke(meanAveragePrecisionsReduced, meanAveragePrecisions)

        logger.debug("Correlation: $correlation - " +
                "Number of selected topics: " + "${solution.numberOfSelectedTopics} - " +
                "Evaluating gene: ${solution.getVariableValueString(0)}")

        targetToAchieve(solution, correlation)

    }

}
