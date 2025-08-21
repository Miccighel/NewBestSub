package it.uniud.newbestsub.problem

import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/* --------------------------------------------------------------------------------------------------------------------
 * BitFlipMutation (jMetal 6.x)
 * --------------------------------------------------------------------------------------------------------------------
 * Simple per-bit flip with probability p. Uses jMetal 6.x variables()/BinarySet API.
 * ------------------------------------------------------------------------------------------------------------------ */
class BitFlipMutation(private var probability: Double) : MutationOperator<BinarySolution> {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    override fun mutationProbability(): Double = probability

    override fun execute(candidate: BinarySolution): BinarySolution {
        val sol = candidate as BestSubsetSolution
        val n = sol.numberOfBitsPerVariable()[0]
        val rng = JMetalRandom.getInstance()

        if (n == 0 || rng.nextDouble() >= probability) return sol

        val idx = rng.nextInt(0, n - 1)
        sol.setBitValue(idx, !sol.retrieveTopicStatus()[idx])

        // Not a fixed-K swap → just nuke caches/flags
        sol.resetIncrementalEvaluationState()
        sol.clearLastMutationFlags()

        return sol
    }

}
