package it.uniud.newbestsub.problem

import it.uniud.newbestsub.utils.Tools
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.pseudorandom.JMetalRandom

class BitFlipMutation(var probability: Double) : MutationOperator<BinarySolution> {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    override fun execute(solution: BinarySolution): BinarySolution {
        val s = solution as BestSubsetSolution

        /* (Pre) state */
        val oldGene = s.getVariableValueString(0)
        logger.debug("<(Pre) Num. Sel. Topics: ${s.numberOfSelectedTopics}, (Pre) Gene: $oldGene>")

        val bitset = s.getVariableValue(0)
        val totalNumberOfTopics = s.numberOfBitsPerVariable()[0]

        /* Determinism: use jMetal's singleton RNG (seeded via your RngBridge when enabled). */
        if (JMetalRandom.getInstance().nextDouble() < probability) {

            var flipIndex = if (totalNumberOfTopics <= 1) 0
            else JMetalRandom.getInstance().nextInt(0, totalNumberOfTopics - 1)  /* inclusive upper */

            /* Toggle without '!' to avoid platform-type quirks */
            val currentVal = bitset.get(flipIndex)
            val toggledVal = (currentVal == false)
            s.setBitValue(flipIndex, toggledVal)

            /* Ensure at least one topic remains selected */
            if (s.numberOfSelectedTopics == 0) {
                flipIndex = if (totalNumberOfTopics <= 1) 0
                else JMetalRandom.getInstance().nextInt(0, totalNumberOfTopics - 1)  /* inclusive upper */
                s.setBitValue(flipIndex, true)
            }
        }

        val newGene = s.getVariableValueString(0)
        logger.debug("<Hamming Distance: ${Tools.stringComparison(oldGene, newGene)}, (Post) Num. Sel. Topics: ${s.numberOfSelectedTopics}, (Post) Gene: $newGene>")

        return s
    }

    /* jMetal 6.x: property-style accessor (replaces getMutationProbability()) */
    override fun mutationProbability(): Double = probability
}
