package it.uniud.newbestsub.problem.operators

import it.uniud.newbestsub.problem.BestSubsetSolution
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.crossover.CrossoverOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.solution.binarysolution.impl.DefaultBinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom
import kotlin.math.min

/* --------------------------------------------------------------------------------------------------------------------
 * BinaryPruningCrossover
 * --------------------------------------------------------------------------------------------------------------------
 * Behavior
 * --------
 * Given two parent masks (true = topic selected):
 *   • ChildSelective:  bit = (ParentA AND ParentB)
 *   • ChildInclusive:  bit = (ParentA OR  ParentB)
 *
 * Robustness
 * ----------
 * jMetal may occasionally provide DefaultBinarySolution instances. We *lift* any BinarySolution
 * to BestSubsetSolution before operating, cloning the bitset/objectives and synthesizing labels.
 *
 * Guarantees
 * ----------
 *   • Both children have EXACTLY N bits (N = totalNumberOfBits()).
 *   • Each child has at least one bit set; if not, one random bit is flipped to true.
 *   • No access to private fields (e.g., topicLabels) — children are copies with replaced bitsets.
 *
 * Determinism
 * -----------
 * All randomness goes through jMetal’s singleton RNG, so external seeding works.
 *
 * Complexity
 * ----------
 * O(N) bit ops per crossover call.
 * ------------------------------------------------------------------------------------------------------------------ */
class BinaryPruningCrossover(private var probability: Double) : CrossoverOperator<BinarySolution> {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /* jMetal 6.x operator API */
    override fun crossoverProbability(): Double = probability
    override fun numberOfRequiredParents(): Int = 2
    override fun numberOfGeneratedChildren(): Int = 2

    override fun execute(parents: MutableList<BinarySolution>): MutableList<BinarySolution> {
        /* Defensive: NSGA-II should always pass exactly 2 parents. */
        require(parents.size >= 2) {
            "BinaryPruningCrossover requires 2 parents (got ${parents.size})"
        }

        /* ------------------------------------------------------------------------------------
         * Normalize inputs: operate on BestSubsetSolution (adapter handles DefaultBinarySolution).
         * ---------------------------------------------------------------------------------- */
        val parentA: BestSubsetSolution = ensureBestSubsetView(parents[0])
        val parentB: BestSubsetSolution = ensureBestSubsetView(parents[1])

        /* Invariant: both parents must share the same N (number of topics). */
        val nBits = parentA.totalNumberOfBits()
        require(parentB.totalNumberOfBits() == nBits) {
            "Parents bit-length mismatch: ${parentA.totalNumberOfBits()} vs ${parentB.totalNumberOfBits()}"
        }

        /* Normalize masks to exactly N bits to avoid length drift. */
        val maskA: BooleanArray = normalizeToLength(parentA.retrieveTopicStatus(), nBits)
        val maskB: BooleanArray = normalizeToLength(parentB.retrieveTopicStatus(), nBits)

        val rng = JMetalRandom.getInstance()
        val performCrossover = rng.nextDouble() < probability

        /* Build children masks (always length = N). */
        val childSelectiveMask = BooleanArray(nBits)
        val childInclusiveMask = BooleanArray(nBits)

        if (performCrossover) {
            var i = 0
            while (i < nBits) {
                val a = maskA[i]
                val b = maskB[i]
                childSelectiveMask[i] = a && b  /* selective (AND)   */
                childInclusiveMask[i] = a || b  /* inclusive (OR)    */
                i++
            }
        } else {
            /* No crossover → normalized clones. */
            System.arraycopy(maskA, 0, childSelectiveMask, 0, nBits)
            System.arraycopy(maskB, 0, childInclusiveMask, 0, nBits)
        }

        /* Ensure feasibility: at least one topic selected per child. */
        ensureAtLeastOneTrue(childSelectiveMask, nBits, rng)
        ensureAtLeastOneTrue(childInclusiveMask, nBits, rng)

        /* Materialize children as COPIES of the parents, then overwrite their bitset with N-bit masks. */
        val childSelective: BestSubsetSolution = parentA.copy().apply {
            variables()[0] = createNewBitSet(nBits, childSelectiveMask.toTypedArray())
            resetIncrementalEvaluationState()
            clearLastMutationFlags()
        }
        val childInclusive: BestSubsetSolution = parentB.copy().apply {
            variables()[0] = createNewBitSet(nBits, childInclusiveMask.toTypedArray())
            resetIncrementalEvaluationState()
            clearLastMutationFlags()
        }

        /* Debug traces (safe at DEBUG level). */
        logger.debug("<P1 sel=${parentA.numberOfSelectedTopics}> ${parentA.getVariableValueString(0)}")
        logger.debug("<P2 sel=${parentB.numberOfSelectedTopics}> ${parentB.getVariableValueString(0)}")
        logger.debug("<C1 sel=${childSelective.numberOfSelectedTopics}> ${childSelective.getVariableValueString(0)}")
        logger.debug("<C2 sel=${childInclusive.numberOfSelectedTopics}> ${childInclusive.getVariableValueString(0)}")

        return mutableListOf(childSelective, childInclusive)
    }

    /* ------------------------------------- Helpers ------------------------------------- */

    /* Copy/resize a BooleanArray to exactly n elements (truncate or pad with false). */
    private fun normalizeToLength(mask: BooleanArray, n: Int): BooleanArray {
        if (mask.size == n) return mask
        val resized = BooleanArray(n)
        val copyLen = min(mask.size, n)
        if (copyLen > 0) System.arraycopy(mask, 0, resized, 0, copyLen)
        return resized
    }

    /* Ensure at least one 'true' in mask; if none, flip a random index to true. */
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

    /* If input is BestSubsetSolution, return it; otherwise lift DefaultBinarySolution into BestSubsetSolution. */
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

        // Reset delta-eval state for safety.
        lifted.resetIncrementalEvaluationState()
        lifted.clearLastMutationFlags()

        return lifted
    }
}
