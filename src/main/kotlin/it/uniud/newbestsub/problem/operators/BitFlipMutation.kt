package it.uniud.newbestsub.problem.operators

import it.uniud.newbestsub.problem.BestSubsetSolution
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/**
 * **BitFlipMutation (jMetal 6.x)** — flips a single random bit of the solution with a
 * per-candidate probability `p`.
 *
 * Behavior:
 * - Draw once per candidate; if the draw `< p`, select one random bit index and flip it.
 * - This operator **does not preserve cardinality K** (unlike [FixedKSwapMutation]).
 * - After mutation, incremental caches and swap flags in [BestSubsetSolution] are cleared.
 *
 * Determinism:
 * - All randomness goes through jMetal’s [JMetalRandom] singleton, so external seeding applies.
 *
 * @param probability Per-candidate mutation probability in `(0.0..1.0]`. If the random draw
 *   is `>= probability`, no mutation is applied.
 */
class BitFlipMutation(private var probability: Double) : MutationOperator<BinarySolution> {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /** jMetal 6.x mutation API: returns the configured per‑candidate mutation probability. */
    override fun mutationProbability(): Double = probability

    /**
     * Executes a single-bit flip on the candidate solution with probability [probability].
     *
     * Implementation notes:
     * - Assumes [candidate] is a [BestSubsetSolution] (cast is unchecked and will throw if not).
     * - Chooses a random index in `[0, n)` and toggles the bit.
     * - Resets incremental evaluation state and mutation flags, since this is **not** a fixed‑K swap.
     *
     * @param candidate input solution (expected: [BestSubsetSolution]).
     * @return the same instance after potential mutation.
     * @throws ClassCastException if [candidate] is not a [BestSubsetSolution].
     */
    override fun execute(candidate: BinarySolution): BinarySolution {
        val sol = candidate as BestSubsetSolution
        val n = sol.numberOfBitsPerVariable()[0]
        val rng = JMetalRandom.getInstance()

        if (n == 0 || rng.nextDouble() >= probability) return sol

        val idx = rng.nextInt(0, n - 1)
        sol.setBitValue(idx, !sol.retrieveTopicStatus()[idx])

        // Not a fixed-K swap → clear delta-eval caches and flags.
        sol.resetIncrementalEvaluationState()
        sol.clearLastMutationFlags()

        return sol
    }
}
