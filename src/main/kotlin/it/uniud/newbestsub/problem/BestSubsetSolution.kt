package it.uniud.newbestsub.problem

import org.apache.commons.lang3.builder.EqualsBuilder
import org.apache.commons.lang3.builder.HashCodeBuilder
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.problem.BinaryProblem
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.solution.impl.AbstractGenericSolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom

class BestSubsetSolution : AbstractGenericSolution<BinarySet, BinaryProblem>, BinarySolution, Comparable<BestSubsetSolution> {

    var topicStatus: Array<Boolean>
    var numberOfSelectedTopics = 0
    private val logger = LogManager.getLogger()

    constructor(problem: BinaryProblem, numberOfTopics: Int) : super(problem) {

        initializeObjectiveValues()

        numberOfSelectedTopics = 0

        val columnKeepProbability = JMetalRandom.getInstance().nextDouble()

        topicStatus = Array(numberOfTopics, {
            val result: Boolean
            val pointProbability = JMetalRandom.getInstance().nextDouble()
            if (pointProbability > columnKeepProbability) {
                result = true
                numberOfSelectedTopics++
            } else result = false
            result
        })

        if (numberOfSelectedTopics == 0) {
            var flipIndex = Math.floor(JMetalRandom.getInstance().nextDouble() * topicStatus.size).toInt()
            if (flipIndex == topicStatus.size) flipIndex -= 1
            topicStatus[flipIndex] = true
            numberOfSelectedTopics++
        }

        setVariableValue(0, createNewBitSet(topicStatus.size, topicStatus))

        logger.debug("<Num. Sel. Topics: $numberOfSelectedTopics, Gene: ${getVariableValueString(0)}>")
    }

    constructor(solution: BestSubsetSolution) : super(solution.problem) {

        topicStatus = solution.topicStatus
        numberOfSelectedTopics = solution.numberOfSelectedTopics

        (0..problem.numberOfVariables - 1).forEach { i -> setVariableValue(i, solution.getVariableValue(i).clone() as BinarySet) }
        (0..problem.numberOfObjectives - 1).forEach { i -> setObjective(i, solution.getObjective(i)) }

    }

    override fun copy(): BestSubsetSolution {
        return BestSubsetSolution(this)
    }

    fun createNewBitSet(numberOfBits: Int, values: Array<Boolean>): BinarySet {
        val bitSet = BinarySet(numberOfBits)
        (0..numberOfBits - 1).forEach { i -> if (values[i]) bitSet.set(i) else bitSet.clear(i) }
        return bitSet
    }

    fun setBitValue(index: Int, value: Boolean) {
        val topicStatusValues = getVariableValue(0)
        if (topicStatusValues.get(index) != value) {
            topicStatusValues.set(index, value)
            if (value) numberOfSelectedTopics++ else numberOfSelectedTopics--
        }
        setVariableValue(0, topicStatusValues)
    }

    fun retrieveTopicStatus(): BooleanArray {
        val topicStatusValues = BooleanArray(getVariableValue(0).binarySetLength)
        for (i in topicStatusValues.indices) topicStatusValues[i] = getVariableValue(0).get(i)
        return topicStatusValues
    }

    override fun getNumberOfBits(index: Int): Int {
        return getVariableValue(index).binarySetLength
    }

    override fun getTotalNumberOfBits(): Int {
        var sum = 0
        (0..numberOfVariables - 1).forEach { index -> sum += getVariableValue(index).binarySetLength }
        return sum
    }

    override fun getVariableValueString(index: Int): String {
        var toReturn = ""
        (0..getVariableValue(index).binarySetLength - 1).forEach { i -> if (getVariableValue(index).get(i)) toReturn += "1" else toReturn += "0" }
        return toReturn
    }

    override fun compareTo(other: BestSubsetSolution): Int {
        if (this.getObjective(1) > other.getObjective(1)) return 1 else return if (this.getObjective(1) == other.getObjective(1)) 0 else -1
    }

    override fun equals(other: Any?): Boolean {
        val aSolution = other as BestSubsetSolution
        return EqualsBuilder()
                .append(this.getObjective(0), aSolution.getObjective(0))
                .append(this.getObjective(1), aSolution.getObjective(1))
                .isEquals
    }

    override fun hashCode(): Int {
        return HashCodeBuilder(17, 37).append(this.getObjective(0)).append(this.getObjective(1)).toHashCode()
    }

}

fun BinarySolution.getCardinality(): Double {
    return getObjective(1)
}

fun BinarySolution.getCorrelation(): Double {
    return getObjective(0)
}