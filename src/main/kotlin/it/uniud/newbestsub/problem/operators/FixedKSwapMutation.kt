package it.uniud.newbestsub.problem.operators

import it.uniud.newbestsub.problem.BestSubsetSolution
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.solution.binarysolution.impl.DefaultBinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/**
 * Mutation operator that **preserves subset cardinality K** by swapping one `1` → `0` and one `0` → `1`.
 *
 * Intent:
 * - Pick one index from the **1‑pool** (selected topics) and set it to `false` (swap OUT).
 * - Pick one index from the **0‑pool** (unselected topics) and set it to `true` (swap IN).
 *
 * Robustness:
 * - jMetal may occasionally pass [DefaultBinarySolution] instances. This operator **lifts** any
 *   [BinarySolution] into a [BestSubsetSolution] view before mutating (cloning bitset/objectives and
 *   synthesizing labels as needed).
 *
 * Guarantees:
 * - If both pools are non‑empty, **K is preserved** exactly.
 * - Degenerate masks are repaired safely:
 *   - all‑zeros  → flip one random bit to `true` (K becomes 1)
 *   - all‑ones   → flip one random bit to `false` (K becomes N‑1)
 * - Operates in‑place on the (adapted) [BestSubsetSolution].
 *
 * Delta‑evaluation hook:
 * - On a true swap or repair, sets:
 *   - [BestSubsetSolution.lastSwapOutIndex]
 *   - [BestSubsetSolution.lastSwapInIndex]
 *   - [BestSubsetSolution.lastMutationWasFixedKSwap] = `true`
 *   so that `BestSubsetProblem.evaluate(...)` can update cached per‑system sums in **O(S)**.
 *
 * Determinism:
 * - All randomness goes through jMetal’s singleton RNG, so external seeding works.
 *
 * Complexity:
 * - O(N) to collect indices + O(1) updates.
 *
 * @param probability Per‑candidate mutation probability in `(0.0..1.0]`. When the random draw is
 *   ≥ `probability`, the operator is a no‑op.
 */
class FixedKSwapMutation(private var probability: Double) : MutationOperator<BinarySolution> {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /** jMetal 6.x mutation API: returns the configured per‑candidate mutation probability. */
    override fun mutationProbability(): Double = probability

    /**
     * Executes the fixed‑K swap mutation on [candidate]. If the candidate is not a
     * [BestSubsetSolution], it is **lifted** to one (bitset and objectives are copied).
     *
     * Steps:
     * 1. Early exit if mutation does not trigger or there are no bits.
     * 2. Snapshot current mask into a primitive array.
     * 3. Collect indices with `1` and with `0`.
     * 4. Perform:
     *    - **Swap** if both pools have elements (K preserved).
     *    - **Repair** if the mask is all‑zeros or all‑ones (K set to 1 or N‑1).
     * 5. Record swap/repair metadata for delta‑evaluation.
     *
     * @param candidate input solution (possibly a [DefaultBinarySolution]).
     * @return the mutated [BestSubsetSolution] (same instance if already compatible; otherwise lifted).
     */
    override fun execute(candidate: BinarySolution): BinarySolution {
        /* ------------------------------------------------------------------------------------
         * Ensure we operate on BestSubsetSolution (adapter if jMetal hands us DefaultBinarySolution).
         * RATIONALE:
         *  - BestSubsetProblem.evaluate(...) relies on BestSubsetSolution (delta-eval caches etc.).
         *  - Some components may introduce DefaultBinarySolution; we “lift” them here.
         * ---------------------------------------------------------------------------------- */
        val solution = ensureBestSubsetView(candidate)

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

    // -------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------

    /**
     * Ensures we operate on a [BestSubsetSolution]. If [src] already is one, returns it.
     * Otherwise, **lifts** the solution by copying the bitset and objectives into a fresh
     * [BestSubsetSolution] with synthesized labels.
     *
     * Side effects on lifted instance:
     * - Delta‑eval state is cleared ([BestSubsetSolution.resetIncrementalEvaluationState]).
     * - Mutation flags are cleared ([BestSubsetSolution.clearLastMutationFlags]).
     *
     * @param src a possibly generic [BinarySolution].
     * @return a [BestSubsetSolution] view suitable for downstream delta‑evaluation.
     */
    private fun ensureBestSubsetView(src: BinarySolution): BestSubsetSolution {
        if (src is BestSubsetSolution) return src

        // Synthesize labels when none are available; they’re used only for pretty-printing.
        val nBits = src.totalNumberOfBits()
        val labels = Array(nBits) { idx -> "t$idx" }

        // Build a fresh BestSubsetSolution and copy bitset + objectives.
        val lifted = BestSubsetSolution(
            numberOfVariables = 1,
            numberOfObjectives = src.objectives().size,
            numberOfTopics = nBits,
            topicLabels = labels,
            forcedCardinality = null
        )

        // Copy bits
        val srcBits = src.variables()[0] as BinarySet
        val dstBits = BinarySet(nBits).apply { this.or(srcBits) }
        lifted.variables()[0] = dstBits

        // Copy objectives
        val m = src.objectives().size
        var i = 0
        while (i < m) {
            lifted.objectives()[i] = src.objectives()[i]
            i++
        }

        // Clear delta-eval state; this is a fresh lifted view.
        lifted.resetIncrementalEvaluationState()
        lifted.clearLastMutationFlags()

        return lifted
    }
}
