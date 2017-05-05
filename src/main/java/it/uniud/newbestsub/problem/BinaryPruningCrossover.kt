package it.uniud.newbestsub.problem

import it.uniud.newbestsub.utils.BestSubsetLogger
import org.uma.jmetal.operator.CrossoverOperator
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.util.pseudorandom.JMetalRandom

import java.util.LinkedList

class BinaryPruningCrossover(var probability: Double) : CrossoverOperator<BinarySolution> {
    
    override fun getNumberOfParents(): Int {
        return 2
    }

    override fun execute(solutionList: List<BinarySolution>): List<BinarySolution> {

        val firstSolution = solutionList[0] as BestSubsetSolution
        val secondSolution = solutionList[1] as BestSubsetSolution

        val firstTopicStatus = firstSolution.getTopicStatus()
        val secondTopicStatus = secondSolution.getTopicStatus()

        val childrenSolution = LinkedList<BinarySolution>()

        childrenSolution.add(BestSubsetSolution(firstSolution))
        childrenSolution.add(BestSubsetSolution(secondSolution))

        val firstChildren = childrenSolution[0] as BestSubsetSolution
        val secondChildren = childrenSolution[1] as BestSubsetSolution

        if (JMetalRandom.getInstance().nextDouble() < probability) {

            BestSubsetLogger.Companion.log("CROSSOVER: Starting to apply it")

            for (i in firstTopicStatus.indices) {
                firstChildren.setBitValue(i, firstTopicStatus[i] && secondTopicStatus[i])
                secondChildren.setBitValue(i, firstTopicStatus[i] || secondTopicStatus[i])
            }

            if (firstChildren.getNumberOfSelectedTopics() == 0) {
                var flipIndex = Math.floor(JMetalRandom.getInstance().nextDouble() * firstChildren.getNumberOfBits(0)).toInt()
                if (flipIndex == firstChildren.getNumberOfBits(0)) {
                    flipIndex = flipIndex - 1
                }
                firstChildren.setBitValue(flipIndex, true)
            }

        }

        childrenSolution[0] = firstChildren
        childrenSolution[1] = secondChildren

        BestSubsetLogger.Companion.log("CROSSOVER - Parent 1: " + firstSolution.getVariableValueString(0))
        BestSubsetLogger.Companion.log("CROSSOVER - Number of selected topics: " + firstSolution.getNumberOfSelectedTopics())
        BestSubsetLogger.Companion.log("CROSSOVER - Parent 2: " + secondSolution.getVariableValueString(0))
        BestSubsetLogger.Companion.log("CROSSOVER - Number of selected topics: " + secondSolution.getNumberOfSelectedTopics())
        BestSubsetLogger.Companion.log("CROSSOVER - Child 1: " + firstChildren.getVariableValueString(0))
        BestSubsetLogger.Companion.log("CROSSOVER - Number of selected topics: " + firstChildren.getNumberOfSelectedTopics())
        BestSubsetLogger.Companion.log("CROSSOVER - Child 2: " + secondChildren.getVariableValueString(0))
        BestSubsetLogger.Companion.log("CROSSOVER - Number of selected topics: " + secondChildren.getNumberOfSelectedTopics())

        return childrenSolution
    }
}
