package it.uniud.newbestsub.problem

import java.util.Arrays

import it.uniud.newbestsub.utils.BestSubsetLogger
import it.uniud.newbestsub.utils.Formula

import org.uma.jmetal.problem.impl.AbstractBinaryProblem

import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.solution.Solution

import org.uma.jmetal.util.binarySet.BinarySet

class BestSubsetProblem(

        protected var numberOfTopics: Int,
        protected var averagePrecisions: Map<String, DoubleArray>,
        protected var meanAveragePrecisions: DoubleArray,
        protected var correlationStrategy: (DoubleArray, DoubleArray) -> Double,
        protected var targetToAchieve: (Solution<BinarySolution>, Double) -> Solution<BinarySolution>)

    : AbstractBinaryProblem() {

    protected var systemSize: Int = 0
    protected lateinit var solution: BestSubsetSolution

    init {

        numberOfVariables = 1
        numberOfObjectives = 2
        name = "BestSubsetProblem"
    }

    public override fun getBitsPerVariable(index: Int): Int {
        return solution.getNumberOfBits(0)
    }

    override fun createSolution(): BestSubsetSolution {
        solution = BestSubsetSolution(this, numberOfTopics)
        return solution
    }

    override fun evaluate(solution: BinarySolution) {

        val topicsStatus = solution.getVariableValue(0)
        val topicStatusValues = BooleanArray(topicsStatus.binarySetLength)
        for (i in 0..topicsStatus.binarySetLength - 1) {
            topicStatusValues[i] = topicsStatus.get(i)
        }

        BestSubsetLogger.log("PROBLEM - Evaluating gene: " + solution.getVariableValueString(0))
        BestSubsetLogger.log("PROBLEM - Number of selected topics: " + (solution as BestSubsetSolution).numberOfSelectedTopics)

        val meanAveragePrecisionsReduced = DoubleArray(averagePrecisions.entries.size)

        val iterator = averagePrecisions.entries.iterator()
        var counter = 0
        while (iterator.hasNext()) {
            val singleSystem = iterator.next()
            meanAveragePrecisionsReduced[counter] = Formula.getMean(singleSystem.value, topicStatusValues)
            counter++
        }

        // logger.log("PROBLEM - Mean Average Precisions: " + Arrays.toString(meanAveragePrecisions));
        // logger.log("PROBLEM - Mean Average Precisions reduced: " + Arrays.toString(meanAveragePrecisionsReduced));

        val correlation = correlationStrategy.invoke(meanAveragePrecisionsReduced, meanAveragePrecisions)

        BestSubsetLogger.log("PROBLEM - Correlation: " + correlation)

        targetToAchieve.invoke(solution as Solution<BinarySolution>, correlation)

    }

}
