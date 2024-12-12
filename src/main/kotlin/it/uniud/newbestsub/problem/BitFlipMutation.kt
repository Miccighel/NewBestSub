package it.uniud.newbestsub.problem

import it.uniud.newbestsub.utils.Tools
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.MutationOperator
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.util.pseudorandom.JMetalRandom

class BitFlipMutation(var probability: Double) : MutationOperator<BinarySolution> {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    override fun execute(solution: BinarySolution): BinarySolution {

        val topicStatus = solution.getVariableValue(0)
        val totalNumberOfTopics = solution.getNumberOfBits(0)
        val oldGene = solution.getVariableValueString(0)

        logger.debug("<(Pre) Num. Sel. Topics: ${(solution as BestSubsetSolution).numberOfSelectedTopics}, " +
                "<(Pre) Gene: ${solution.getVariableValueString(0)}>")

        if (JMetalRandom.getInstance().nextDouble() < probability) {

            var flipIndex = Math.floor(JMetalRandom.getInstance().nextDouble() * totalNumberOfTopics).toInt()
            if (flipIndex == totalNumberOfTopics) flipIndex -= 1
            solution.setBitValue(flipIndex, !topicStatus.get(flipIndex))

            if (solution.numberOfSelectedTopics == 0) {
                flipIndex = Math.floor(JMetalRandom.getInstance().nextDouble() * totalNumberOfTopics).toInt()
                if (flipIndex == totalNumberOfTopics) flipIndex -= 1
                solution.setBitValue(flipIndex, !topicStatus[flipIndex])
            }

        }

        val newGene = solution.getVariableValueString(0)

        logger.debug("<Hamming Distance: ${Tools.stringComparison(oldGene, newGene)}, (Post) Num. Sel. Topics: ${solution.numberOfSelectedTopics}, " + "(Post) Gene: $newGene>")

        return solution

    }
}
