package it.uniud.newbestsub.problem

import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.CrossoverOperator
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.util.pseudorandom.JMetalRandom
import java.util.*

class BinaryPruningCrossover(var probability: Double) : CrossoverOperator<BinarySolution> {

    private val logger = LogManager.getLogger()

    override fun getNumberOfParents(): Int {
        return 2
    }

    override fun execute(solutionList: List<BinarySolution>): List<BinarySolution> {

        val firstSolution = solutionList[0] as BestSubsetSolution
        val secondSolution = solutionList[1] as BestSubsetSolution
        val firstTopicStatus = firstSolution.retrieveTopicStatus()
        val secondTopicStatus = secondSolution.retrieveTopicStatus()

        val childrenSolution = LinkedList<BinarySolution>()

        childrenSolution.add(BestSubsetSolution(firstSolution))
        childrenSolution.add(BestSubsetSolution(secondSolution))

        val firstChildren = childrenSolution[0] as BestSubsetSolution
        val secondChildren = childrenSolution[1] as BestSubsetSolution

        if (JMetalRandom.getInstance().nextDouble() < probability) {

            for (i in firstTopicStatus.indices) {
                firstChildren.setBitValue(i, firstTopicStatus[i] && secondTopicStatus[i])
                secondChildren.setBitValue(i, firstTopicStatus[i] || secondTopicStatus[i])
            }

            if (firstChildren.numberOfSelectedTopics == 0) {
                var flipIndex = Math.floor(JMetalRandom.getInstance().nextDouble() * firstChildren.getNumberOfBits(0)).toInt()
                if (flipIndex == firstChildren.getNumberOfBits(0)) flipIndex -= 1
                firstChildren.setBitValue(flipIndex, true)
            }

        }

        childrenSolution[0] = firstChildren
        childrenSolution[1] = secondChildren

        logger.debug("<Num. Sel. Topics: ${firstSolution.numberOfSelectedTopics}, Parent 1: ${firstSolution.getVariableValueString(0)}>")
        logger.debug("<Num. Sel. Topics: ${secondSolution.numberOfSelectedTopics}, Parent 2: ${secondSolution.getVariableValueString(0)}>")
        logger.debug("<Num. Sel. Topics: ${firstChildren.numberOfSelectedTopics}, Children 1: ${firstChildren.getVariableValueString(0)}>")
        logger.debug("<Num. Sel. Topics: ${secondChildren.numberOfSelectedTopics}, Children 2: ${secondChildren.getVariableValueString(0)}>")

        return childrenSolution
    }
}
