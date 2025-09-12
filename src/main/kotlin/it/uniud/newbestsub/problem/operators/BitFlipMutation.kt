package it.uniud.newbestsub.problem.operators

import it.uniud.newbestsub.problem.BestSubsetSolution
import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/**
 * BitFlipMutation — single-bit variable-K mutation with repair.
 *
 * Behavior:
 * • With probability [probability], flip one random bit.
 * • If this makes K == 0, flip a second random bit to ensure K ≥ 1.
 * • Cardinality is not preserved (variable-K mutation).
 *
 * Determinism:
 * All randomness uses jMetal's [JMetalRandom] singleton; set the seed upstream.
 *
 * @param probability Per-candidate probability in (0.0, 1.0].
 */
class BitFlipMutation(private var probability: Double) : MutationOperator<BinarySolution> {

    /** Returns the configured mutation probability. */
    override fun mutationProbability(): Double = probability

    /**
     * Execute a bit-flip mutation with repair to avoid empty masks.
     *
     * @param candidate Input solution (expected: [BestSubsetSolution]).
     * @return The same instance, possibly mutated.
     * @throws ClassCastException if [candidate] is not a [BestSubsetSolution].
     */
    override fun execute(candidate: BinarySolution): BinarySolution {
        val sol = candidate as BestSubsetSolution

        /* Total number of bits in the mask (variable 0). */
        val n = sol.numberOfBitsPerVariable()[0]
        if (n == 0) return sol

        val rng = JMetalRandom.getInstance()

        /* Probability gate: if mutation does not trigger, clear flags and return. */
        if (rng.nextDouble() >= probability) {
            sol.clearLastMutationFlags()
            return sol
        }

        /* First flip (variable-K). */
        var idx = rng.nextInt(0, n - 1)
        sol.setBitValue(idx, !sol.retrieveTopicStatus()[idx])

        /* Repair: ensure K ≥ 1 by flipping a second random bit if the mask became empty. */
        if (sol.numberOfSelectedTopics == 0) {
            idx = rng.nextInt(0, n - 1)
            sol.setBitValue(idx, !sol.retrieveTopicStatus()[idx])
        }

        /* Not a fixed-K swap → reset incremental caches/flags so the evaluator recomputes safely. */
        sol.resetIncrementalEvaluationState()
        sol.clearLastMutationFlags()

        return sol
    }
}
