package it.uniud.newbestsub.problem

import it.uniud.newbestsub.utils.BestSubsetLogger
import org.uma.jmetal.problem.BinaryProblem
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.solution.impl.AbstractGenericSolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom

class BestSubsetSolution : AbstractGenericSolution<BinarySet, BinaryProblem>, BinarySolution, Comparable<BestSubsetSolution> {

    var topicStatus: BooleanArray
    var numberOfSelectedTopics: Int = 0

    constructor(problem: BinaryProblem, numberOfTopics: Int) : super(problem) {

        initializeObjectiveValues()

        topicStatus = BooleanArray(numberOfTopics)
        numberOfSelectedTopics = 0

        val columnKeepProbability = JMetalRandom.getInstance().nextDouble()

        for (i in 0..numberOfTopics - 1) {
            val pointProbability = JMetalRandom.getInstance().nextDouble()
            if (pointProbability > columnKeepProbability) {
                topicStatus[i] = true
                numberOfSelectedTopics++
            } else topicStatus[i] = false
        }

        if (numberOfSelectedTopics == 0) {
            var flipIndex = Math.floor(JMetalRandom.getInstance().nextDouble() * topicStatus.size).toInt()
            if (flipIndex == topicStatus.size) flipIndex -= 1
            topicStatus[flipIndex] = true
            numberOfSelectedTopics++
        }

        setVariableValue(0, createNewBitSet(topicStatus.size, topicStatus))

        BestSubsetLogger.log("SOLUTION - (New) Gene: ${getVariableValueString(0)}")
        BestSubsetLogger.log("SOLUTION - (New) Number of selected topics: $numberOfSelectedTopics")
    }

    constructor(solution: BestSubsetSolution) : super(solution.problem) {

        topicStatus = solution.topicStatus
        numberOfSelectedTopics = solution.numberOfSelectedTopics

        for (i in 0..problem.numberOfVariables - 1) setVariableValue(i, solution.getVariableValue(i).clone() as BinarySet)
        for (i in 0..problem.numberOfObjectives - 1) setObjective(i, solution.getObjective(i))

    }

    override fun copy(): BestSubsetSolution {
        return BestSubsetSolution(this)
    }

    fun createNewBitSet(numberOfBits: Int, values: BooleanArray): BinarySet {
        val bitSet = BinarySet(numberOfBits)
        for (i in 0..numberOfBits - 1) if (values[i]) bitSet.set(i) else bitSet.clear(i)
        return bitSet
    }

    fun retrieveTopicStatus(): BooleanArray {
        val topicStatusValues = BooleanArray(getVariableValue(0).binarySetLength)
        for (i in topicStatusValues.indices) topicStatusValues[i] = getVariableValue(0).get(i)
        return topicStatusValues
    }

    fun setBitValue(index: Int, value: Boolean) {
        val topicStatusValues = getVariableValue(0)
        if (topicStatusValues.get(index) != value) {
            topicStatusValues.set(index, value)
            if (value) numberOfSelectedTopics++ else numberOfSelectedTopics--
        }
        setVariableValue(0, topicStatusValues)
    }

    override fun getNumberOfBits(index: Int): Int { return getVariableValue(index).binarySetLength }

    override fun getTotalNumberOfBits(): Int {
        var sum = 0
        (0..numberOfVariables - 1).forEach { index -> sum += getVariableValue(index).binarySetLength }
        return sum
    }

    override fun getVariableValueString(index: Int): String {
        var toReturn = ""
        for (i in 0..getVariableValue(index).binarySetLength - 1) if (getVariableValue(index).get(i)) toReturn += "1" else toReturn += "0"
        return toReturn
    }

    override fun compareTo(other: BestSubsetSolution): Int {
        if (this.getObjective(1) > other.getObjective(1)) return 1 else return if (this.getObjective(1) == other.getObjective(1)) 0 else -1
    }

}
