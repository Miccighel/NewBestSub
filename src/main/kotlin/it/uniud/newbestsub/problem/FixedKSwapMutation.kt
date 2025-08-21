package it.uniud.newbestsub.problem

import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/* --------------------------------------------------------------------------------------------------------------------
 * FixedKSwapMutation
 * --------------------------------------------------------------------------------------------------------------------
 * Intent
 * ------
 * Preserve cardinality K during mutation by swapping:
 *   • pick one index with bit = 1  → set it to 0   (swap OUT)
 *   • pick one index with bit = 0  → set it to 1   (swap IN)
 *
 * Guarantees
 * ----------
 *   • If both 1-pool and 0-pool are non-empty → K is preserved exactly.
 *   • Degenerate cases (all-zeros / all-ones) are handled safely:
 *       - all-zeros  → flip one random bit to true (K = 1)
 *       - all-ones   → flip one random bit to false (K = N - 1)
 *   • Operates in-place on the given solution; no private fields are accessed.
 *
 * Step 3 hook (delta evaluation)
 * ------------------------------
 *   • On a true swap, we set:
 *       solution.lastSwapOutIndex = index turned 1→0
 *       solution.lastSwapInIndex  = index turned 0→1
 *       solution.lastMutationWasFixedKSwap = true
 *     BestSubsetProblem.evaluate(...) can then update cached per-system sums in O(S).
 *
 * Determinism
 * -----------
 * All randomness goes through jMetal’s singleton RNG, so external seeding works.
 *
 * Complexity
 * ----------
 * O(N) to collect indices + O(1) updates.
 * ------------------------------------------------------------------------------------------------------------------ */
class FixedKSwapMutation(private var probability: Double) : MutationOperator<BinarySolution> {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /* jMetal 6.x mutation API */
    override fun mutationProbability(): Double = probability

    override fun execute(candidate: BinarySolution): BinarySolution {
        val solution = candidate as BestSubsetSolution
        val numBits = solution.numberOfBitsPerVariable()[0]
        val rng = JMetalRandom.getInstance()

        /* No-op branch if mutation does not trigger or N == 0. */
        if (rng.nextDouble() >= probability || numBits == 0) {
            return solution
        }

        /* Snapshot current mask (exactly N bits by construction). */
        val topicMask: BooleanArray = solution.retrieveTopicStatus()

        /* Collect indices for 1-bits and 0-bits. */
        val oneBitIndices = ArrayList<Int>()
        val zeroBitIndices = ArrayList<Int>()
        var bitIndex = 0
        while (bitIndex < numBits) {
            if (topicMask[bitIndex]) oneBitIndices.add(bitIndex) else zeroBitIndices.add(bitIndex)
            bitIndex++
        }

        when {
            /* Typical path: both pools available → true swap, K preserved. */
            oneBitIndices.isNotEmpty() && zeroBitIndices.isNotEmpty() -> {
                val indexToTurnOff = oneBitIndices[rng.nextInt(0, oneBitIndices.size - 1)]
                val indexToTurnOn  = zeroBitIndices[rng.nextInt(0, zeroBitIndices.size - 1)]

                /* Apply swap in-place. */
                solution.setBitValue(indexToTurnOff, false)  /* 1 → 0 */
                solution.setBitValue(indexToTurnOn,  true)   /* 0 → 1 */

                /* Record swap for delta evaluation. */
                solution.lastSwapOutIndex = indexToTurnOff
                solution.lastSwapInIndex  = indexToTurnOn
                solution.lastMutationWasFixedKSwap = true

                logger.debug(
                    "FixedKSwapMutation: swap(1→0 at $indexToTurnOff, 0→1 at $indexToTurnOn), " +
                    "K=${solution.numberOfSelectedTopics}"
                )
            }

            /* Degenerate: all-zeros → ensure at least one true. (K grows to 1) */
            oneBitIndices.isEmpty() && zeroBitIndices.isNotEmpty() -> {
                val turnedOnIndex = zeroBitIndices[rng.nextInt(0, zeroBitIndices.size - 1)]
                solution.setBitValue(turnedOnIndex, true)

                /* Mark only IN-side; OUT is null to signal a non-swap repair. */
                solution.lastSwapOutIndex = null
                solution.lastSwapInIndex  = turnedOnIndex
                solution.lastMutationWasFixedKSwap = true

                logger.debug("FixedKSwapMutation: repaired all-zeros by setting index $turnedOnIndex = true")
            }

            /* Degenerate: all-ones → ensure at least one false. (K drops to N-1) */
            zeroBitIndices.isEmpty() && oneBitIndices.isNotEmpty() -> {
                val turnedOffIndex = oneBitIndices[rng.nextInt(0, oneBitIndices.size - 1)]
                solution.setBitValue(turnedOffIndex, false)

                /* Mark only OUT-side; IN is null to signal a non-swap repair. */
                solution.lastSwapOutIndex = turnedOffIndex
                solution.lastSwapInIndex  = null
                solution.lastMutationWasFixedKSwap = true

                logger.debug("FixedKSwapMutation: repaired all-ones by setting index $turnedOffIndex = false")
            }

            /* numBits == 0 was handled by the early return. */
        }

        return solution
    }
}
