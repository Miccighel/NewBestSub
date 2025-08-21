package it.uniud.newbestsub.problem

import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/**
 * FixedKSwapMutation
 * ------------------
 * Preserves cardinality K by swapping one 1 -> 0 and one 0 -> 1.
 * Does a single swap with probability p; no-ops if K in {0, N}.
 */
class FixedKSwapMutation(private val mutationProbability: Double) : MutationOperator<BinarySolution> {
    private val rng = JMetalRandom.getInstance()

    override fun execute(solution: BinarySolution): BinarySolution {
        if (rng.nextDouble() >= mutationProbability) return solution

        val s = solution as BestSubsetSolution
        val mask = s.retrieveTopicStatus()
        val n = mask.size
        val k = s.numberOfSelectedTopics

        // Nothing to swap if all-zeros or all-ones (shouldn't happen often)
        if (k !in 1..<n) return solution

        val ones = mutableListOf<Int>()
        val zeros = mutableListOf<Int>()
        for (i in 0 until n) if (mask[i]) ones += i else zeros += i
        if (ones.isEmpty() || zeros.isEmpty()) return solution

        val i1 = ones[rng.nextInt(0, ones.size - 1)]
        val i0 = zeros[rng.nextInt(0, zeros.size - 1)]

        mask[i1] = false
        mask[i0] = true

        // Push back via BestSubsetSolution helpers (keeps internal counters consistent)
        s.setVariableValue(0, s.createNewBitSet(n, mask.toTypedArray()))
        return s
    }

    override fun mutationProbability(): Double {
        return mutationProbability
    }

    override fun toString(): String = "FixedKSwapMutation(p=$mutationProbability)"
}
