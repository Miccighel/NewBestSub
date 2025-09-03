package it.uniud.newbestsub.problem.operators

import it.uniud.newbestsub.problem.BestSubsetSolution
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.crossover.CrossoverOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.solution.binarysolution.impl.DefaultBinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/**
 * # BinaryPruningCrossover
 *
 * Logical **pruning crossover** operator for binary masks.
 *
 * Given two parent topic masks (`true` = topic selected), produces two children:
 *
 * - **Selective child**: `bit = parentA AND parentB` (intersection).
 * - **Inclusive child**: `bit = parentA OR  parentB` (union).
 *
 * ## Behavior
 * - Each child has exactly `N` bits (`N = totalNumberOfBits()`).
 * - If a child mask becomes all-false, we flip one random bit to `true` (feasibility repair).
 * - Children are returned as copies of the parents with their bitsets replaced.
 *
 * ## Robustness
 * - jMetal may pass [DefaultBinarySolution] instances. This operator **lifts** any
 *   [BinarySolution] into a [BestSubsetSolution] before operating (bits/objectives copied,
 *   labels synthesized).
 *
 * ## Determinism
 * - All RNG goes through jMetal’s singleton [JMetalRandom], so external seeding works.
 *
 * ## Complexity
 * - O(N) per crossover.
 *
 * @param probability Per-pair crossover probability in `(0.0..1.0]`. If the random draw is `>= probability`,
 * no crossover is performed (children are normalized clones of the parents).
 */
class BinaryPruningCrossover(private var probability: Double) : CrossoverOperator<BinarySolution> {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /** jMetal 6.x operator API: returns the configured crossover probability. */
    override fun crossoverProbability(): Double = probability

    /** Always requires 2 parents. */
    override fun numberOfRequiredParents(): Int = 2

    /** Always generates 2 children. */
    override fun numberOfGeneratedChildren(): Int = 2

    /**
     * Executes the pruning crossover on [parents].
     *
     * Workflow:
     * 1) Ensure both parents are [BestSubsetSolution] (lift if necessary) and share the same bit length N.
     * 2) Normalize parent masks to exactly N bits.
     * 3) With probability [probability], build:
     *    - **Selective child** = bitwise `AND`.
     *    - **Inclusive child** = bitwise `OR`.
     *    Otherwise, clone normalized parent masks.
     * 4) Ensure each child has at least one `true`.
     * 5) Materialize children as copies of parents with new BinarySets installed.
     * 6) Reset delta-eval caches/flags on the children.
     *
     * @throws IllegalArgumentException if fewer than 2 parents are supplied or bit lengths differ.
     */
    override fun execute(parents: MutableList<BinarySolution>): MutableList<BinarySolution> {
        require(parents.size >= 2) {
            "BinaryPruningCrossover requires 2 parents (got ${parents.size})"
        }

        // Normalize inputs: operate on BestSubsetSolution (adapter handles DefaultBinarySolution).
        val parentA: BestSubsetSolution = ensureBestSubsetView(parents[0])
        val parentB: BestSubsetSolution = ensureBestSubsetView(parents[1])

        // Invariant: both parents must have the same number of bits (topics).
        val nBits = parentA.totalNumberOfBits()
        require(parentB.totalNumberOfBits() == nBits) {
            "Parents bit-length mismatch: ${parentA.totalNumberOfBits()} vs ${parentB.totalNumberOfBits()}"
        }

        // Normalize masks to exactly N bits.
        val parentAMask: BooleanArray = normalizeToLength(parentA.retrieveTopicStatus(), nBits)
        val parentBMask: BooleanArray = normalizeToLength(parentB.retrieveTopicStatus(), nBits)

        val rng = JMetalRandom.getInstance()
        val doCrossover = rng.nextDouble() < probability

        // Build children masks (always length = N).
        val childSelectiveMask = BooleanArray(nBits)
        val childInclusiveMask = BooleanArray(nBits)

        if (doCrossover) {
            var i = 0
            while (i < nBits) {
                val a = parentAMask[i]
                val b = parentBMask[i]
                childSelectiveMask[i] = a && b  // AND (intersection)
                childInclusiveMask[i] = a || b  // OR  (union)
                i++
            }
        } else {
            // No crossover: normalized clones.
            if (nBits > 0) {
                System.arraycopy(parentAMask, 0, childSelectiveMask, 0, nBits)
                System.arraycopy(parentBMask, 0, childInclusiveMask, 0, nBits)
            }
        }

        // Ensure feasibility: at least one 'true' bit in each child.
        ensureAtLeastOneTrue(childSelectiveMask, nBits, rng)
        ensureAtLeastOneTrue(childInclusiveMask, nBits, rng)

        // Materialize children as copies of parents; install explicit BinarySets from masks.
        val childSelective: BestSubsetSolution = parentA.copy().apply {
            variables()[0] = binarySetFrom(childSelectiveMask)
            resetIncrementalEvaluationState()
            clearLastMutationFlags()
        }
        val childInclusive: BestSubsetSolution = parentB.copy().apply {
            variables()[0] = binarySetFrom(childInclusiveMask)
            resetIncrementalEvaluationState()
            clearLastMutationFlags()
        }

        // Debug traces (safe at DEBUG level).
        logger.debug("<P1 sel=${parentA.numberOfSelectedTopics}> ${parentA.getVariableValueString(0)}")
        logger.debug("<P2 sel=${parentB.numberOfSelectedTopics}> ${parentB.getVariableValueString(0)}")
        logger.debug("<C1 sel=${childSelective.numberOfSelectedTopics}> ${childSelective.getVariableValueString(0)}")
        logger.debug("<C2 sel=${childInclusive.numberOfSelectedTopics}> ${childInclusive.getVariableValueString(0)}")

        return mutableListOf(childSelective, childInclusive)
    }

    // ------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------

    /** Copy/resize a [BooleanArray] to exactly [n] elements (truncate or pad with `false`). */
    private fun normalizeToLength(mask: BooleanArray, n: Int): BooleanArray {
        if (mask.size == n) return mask
        val resized = BooleanArray(n)
        val copyLen = if (mask.size < n) mask.size else n
        if (copyLen > 0) System.arraycopy(mask, 0, resized, 0, copyLen)
        return resized
    }

    /**
     * Ensures at least one `true` in [mask]. If none, flips a random index to `true`.
     *
     * @param mask boolean array to validate/repair (mutated in place).
     * @param n length of mask.
     * @param rng jMetal RNG for deterministic reproducibility.
     */
    private fun ensureAtLeastOneTrue(mask: BooleanArray, n: Int, rng: JMetalRandom) {
        var anyTrue = false
        var i = 0
        while (i < n) {
            if (mask[i]) { anyTrue = true; break }
            i++
        }
        if (!anyTrue) {
            val idx = if (n <= 1) 0 else rng.nextInt(0, n - 1)
            mask[idx] = true
        }
    }

    /**
     * Builds a [BinarySet] from a boolean mask (`true` → set bit *i*).
     */
    private fun binarySetFrom(mask: BooleanArray): BinarySet {
        val n = mask.size
        val bs = BinarySet(n)
        var i = 0
        while (i < n) {
            if (mask[i]) bs.set(i)
            i++
        }
        return bs
    }

    /**
     * Ensures we operate on a [BestSubsetSolution]. If [src] already is one, returns it.
     * Otherwise, **lifts** the solution by copying the bitset and objectives into a fresh
     * [BestSubsetSolution] with synthesized labels.
     *
     * Side effects on lifted instance:
     * - Delta-eval state is cleared ([BestSubsetSolution.resetIncrementalEvaluationState]).
     * - Mutation flags are cleared ([BestSubsetSolution.clearLastMutationFlags]).
     */
    private fun ensureBestSubsetView(src: BinarySolution): BestSubsetSolution {
        if (src is BestSubsetSolution) return src

        // Synthesize labels when none are available; used only for pretty-printing.
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

        // Reset delta-eval state for safety.
        lifted.resetIncrementalEvaluationState()
        lifted.clearLastMutationFlags()

        return lifted
    }
}