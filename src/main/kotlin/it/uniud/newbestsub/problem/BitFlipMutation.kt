package it.uniud.newbestsub.problem

import it.uniud.newbestsub.utils.Tools
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/**
 * Single-bit flip mutation:
 *  - With probability p, flip exactly one randomly chosen bit.
 *  - Guarantee feasibility: if the flip would produce an all-zero mask, turn one random bit on.
 *  - All randomness goes through jMetal's singleton RNG for determinism.
 */
class BitFlipMutation(private var probability: Double) : MutationOperator<BinarySolution> {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    override fun mutationProbability(): Double = probability // keep this one for your current build

    override fun execute(solution: BinarySolution): BinarySolution {
        val bestSubset = solution as BestSubsetSolution

        val before = bestSubset.getVariableValueString(0)
        logger.debug("<(Pre) NumSel={}, Gene={}>", bestSubset.numberOfSelectedTopics, before)

        val bitset = bestSubset.getVariableValue(0)
        val totalBits = bestSubset.numberOfBitsPerVariable()[0]
        if (totalBits <= 0) return bestSubset // nothing to do (defensive)

        val rng = JMetalRandom.getInstance()

        if (rng.nextDouble() < probability) {
            // Flip one random bit
            val flipIndex = if (totalBits == 1) 0 else rng.nextInt(0, totalBits - 1)
            val current = bitset.get(flipIndex)
            bestSubset.setBitValue(flipIndex, !current)

            // Keep at least one topic selected
            if (bestSubset.numberOfSelectedTopics == 0) {
                val rescueIndex = if (totalBits == 1) 0 else rng.nextInt(0, totalBits - 1)
                bestSubset.setBitValue(rescueIndex, true)
            }
        }

        val after = bestSubset.getVariableValueString(0)
        logger.debug(
            "<Hamming={}, (Post) NumSel={}, Gene={}>",
            Tools.stringComparison(before, after),
            bestSubset.numberOfSelectedTopics,
            after
        )
        return bestSubset
    }
}
