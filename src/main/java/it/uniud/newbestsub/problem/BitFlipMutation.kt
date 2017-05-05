package it.uniud.newbestsub.problem

import it.uniud.newbestsub.utils.BestSubsetLogger
import it.uniud.newbestsub.utils.Formula

import org.uma.jmetal.operator.MutationOperator

import org.uma.jmetal.solution.BinarySolution

import org.uma.jmetal.util.pseudorandom.JMetalRandom
import org.uma.jmetal.util.binarySet.BinarySet

class BitFlipMutation(var probability: Double) : MutationOperator<BinarySolution> {

    override fun execute(solution: BinarySolution): BinarySolution {

        val topicStatus = solution.getVariableValue(0)
        val totalNumberOfTopics = solution.getNumberOfBits(0)
        val oldGene = solution.getVariableValueString(0)

        BestSubsetLogger.Companion.log("MUTATION - (Pre) Gene: " + solution.getVariableValueString(0))
        BestSubsetLogger.Companion.log("MUTATION - (Pre) Number of selected topics: " + (solution as BestSubsetSolution).getNumberOfSelectedTopics())

        if (JMetalRandom.getInstance().nextDouble() < probability) {

            var flipIndex = Math.floor(JMetalRandom.getInstance().nextDouble() * totalNumberOfTopics).toInt()
            if (flipIndex == totalNumberOfTopics) {
                flipIndex = flipIndex - 1
            }

            solution.setBitValue(flipIndex, !topicStatus.get(flipIndex))

            if (solution.getNumberOfSelectedTopics() == 0) {
                flipIndex = Math.floor(JMetalRandom.getInstance().nextDouble() * totalNumberOfTopics).toInt()
                if (flipIndex == totalNumberOfTopics) {
                    flipIndex = flipIndex - 1
                }
                solution.setBitValue(flipIndex, !topicStatus.get(flipIndex))
            }

        }

        val newGene = solution.getVariableValueString(0)

        BestSubsetLogger.Companion.log("MUTATION - (Post) Gene: " + solution.getVariableValueString(0))
        BestSubsetLogger.Companion.log("MUTATION - (Post) Number of selected topics: " + solution.getNumberOfSelectedTopics())
        BestSubsetLogger.Companion.log("MUTATION - Hamming distance: " + Formula.stringComparison(oldGene, newGene))

        return solution

    }

}
