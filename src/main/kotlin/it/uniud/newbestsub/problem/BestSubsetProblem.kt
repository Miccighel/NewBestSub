package it.uniud.newbestsub.problem

import it.uniud.newbestsub.utils.BestSubsetLogger
import it.uniud.newbestsub.utils.Formula

import org.uma.jmetal.problem.impl.AbstractBinaryProblem

import org.uma.jmetal.solution.BinarySolution

class BestSubsetProblem(

        private var numberOfTopics: Int,
        private var averagePrecisions: Map<String, DoubleArray>,
        private var meanAveragePrecisions: DoubleArray,
        private var correlationStrategy: (DoubleArray, DoubleArray) -> Double,
        private var targetToAchieve: (BinarySolution, Double) -> BinarySolution

) : AbstractBinaryProblem() {

    private lateinit var solution: BestSubsetSolution

    init {
        numberOfVariables = 1
        numberOfObjectives = 2
        name = "BestSubsetProblem"
    }

    public override fun getBitsPerVariable(index: Int): Int {
        return solution.getNumberOfBits(0)
    }

    override fun createSolution(): BestSubsetSolution { solution = BestSubsetSolution(this, numberOfTopics); return solution }

    override fun evaluate(solution: BinarySolution) {

        solution as BestSubsetSolution

        BestSubsetLogger.log("PROBLEM - Evaluating gene: ${solution.getVariableValueString(0)}")
        BestSubsetLogger.log("PROBLEM - Number of selected topics: ${solution.numberOfSelectedTopics}")

        val meanAveragePrecisionsReduced = DoubleArray(averagePrecisions.entries.size)
        var counter = 0

        for(singleSystem in averagePrecisions) {
            meanAveragePrecisionsReduced[counter] = Formula.getMean(singleSystem.value, solution.retrieveTopicStatus())
            counter++
        }

        // logger.log("PROBLEM - Mean Average Precisions: " + Arrays.toString(meanAveragePrecisions));
        // logger.log("PROBLEM - Mean Average Precisions reduced: " + Arrays.toString(meanAveragePrecisionsReduced));

        val correlation = correlationStrategy.invoke(meanAveragePrecisionsReduced, meanAveragePrecisions)
        BestSubsetLogger.log("PROBLEM - Correlation: $correlation")
        targetToAchieve.invoke(solution, correlation)

    }

}
