package it.uniud.newbestsub.problem

import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.binarysolution.impl.DefaultBinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/* --------------------------------------------------------------------------------------------------------------------
 * BestSubsetSolution
 * --------------------------------------------------------------------------------------------------------------------
 * Concrete binary solution backed by jMetal's BinarySet with a few conveniences:
 *
 *  • Construction helpers for either a fixed cardinality K or a random non-empty mask.
 *  • Readable accessors used across the project (retrieveTopicStatus, numberOfSelectedTopics, …).
 *  • Mirror field `topicStatus` kept for downstream writers (-Top CSV).
 *  • Step 3 (delta evaluation) scaffolding:
 *      - `lastEvaluatedMask`, `cachedSumsBySystem` (subset-sum cache)
 *      - `lastSwapOutIndex`, `lastSwapInIndex`, `lastMutationWasFixedKSwap` (operator hook)
 *
 * Notes:
 *  - Updated to jMetal 6.x API: use variables() / objectives() lists (no get/setVariableValue()).
 *  - All randomness uses JMetalRandom's singleton to preserve determinism when seeded.
 * ------------------------------------------------------------------------------------------------------------------ */
class BestSubsetSolution(
    /* Kept for call-site compatibility; only numberOfObjectives matters for parent */
    numberOfVariables: Int,
    numberOfObjectives: Int,

    /* Total number of decision bits (topics). */
    private val numberOfTopics: Int,

    /* Human-readable labels for each topic (private; use getters to read safely). */
    private val topicLabels: Array<String>,

    /* If non-null, build an initial mask with exactly this many 1s; otherwise random non-empty. */
    private val forcedCardinality: Int?
) : DefaultBinarySolution(listOf(numberOfTopics), numberOfObjectives) {

    /* External mirror for writers (-Top CSV, etc.). */
    var topicStatus: Array<Boolean> = Array(numberOfTopics) { false }

    /* Step 3 (delta evaluation) caches. */
    var lastEvaluatedMask: BooleanArray? = null
    var cachedSumsBySystem: DoubleArray? = null

    /* Step 3 (delta evaluation) operator flags. */
    var lastSwapOutIndex: Int? = null
    var lastSwapInIndex: Int? = null
    var lastMutationWasFixedKSwap: Boolean = false

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /* -------------------------------- Initialization ------------------------------- */
    init {
        val initialMask: BinarySet = if (forcedCardinality != null) {
            buildMaskWithExactCardinality(forcedCardinality)
        } else {
            buildRandomNonEmptyMask()
        }

        /* jMetal 6.x: variables() returns a mutable list. Put our mask in slot 0. */
        variables()[0] = initialMask

        /* Keep the mirror in sync for downstream writers. */
        topicStatus = retrieveTopicStatus().toTypedArray()
    }

    /* ------------------------------ Mask Builders ---------------------------------- */

    /** Build a BinarySet with exactly K bits set (K clamped to [1, numberOfTopics]). */
    private fun buildMaskWithExactCardinality(desiredK: Int): BinarySet {
        val clampedK = desiredK.coerceIn(1, numberOfTopics)
        val mask = BinarySet(numberOfTopics)

        val rng = JMetalRandom.getInstance()
        val pool = IntArray(numberOfTopics) { it }  /* 0..n-1 */
        var i = 0
        while (i < clampedK) {
            val swapWith = i + rng.nextInt(0, numberOfTopics - i - 1)
            val tmp = pool[i]; pool[i] = pool[swapWith]; pool[swapWith] = tmp
            mask.set(pool[i], true)
            i++
        }
        return mask
    }

    /** Build a BinarySet with random bits and ensure it's not empty (flip 1 if needed). */
    private fun buildRandomNonEmptyMask(): BinarySet {
        val mask = BinarySet(numberOfTopics)
        val rng = JMetalRandom.getInstance()

        var anySelected = false
        var bitIndex = 0
        while (bitIndex < numberOfTopics) {
            val pick = rng.nextDouble() < 0.5
            mask.set(bitIndex, pick)
            anySelected = anySelected or pick
            bitIndex++
        }

        if (!anySelected) {
            val flipIndex = if (numberOfTopics == 1) 0 else rng.nextInt(0, numberOfTopics - 1)
            mask.set(flipIndex, true)
        }
        return mask
    }

    /* ------------------------------ Public Helpers --------------------------------- */

    /** Return the current topic mask as a primitive BooleanArray (fast for numeric loops). */
    fun retrieveTopicStatus(): BooleanArray {
        val bits: BinarySet = variables()[0]
        val status = BooleanArray(numberOfTopics)
        var i = 0
        while (i < numberOfTopics) {
            status[i] = bits.get(i)
            i++
        }
        return status
    }

    /** Count of selected topics (cardinality of the bitset). */
    val numberOfSelectedTopics: Int
        get() = variables()[0].cardinality()

    /** Set a single bit in the underlying BinarySet. */
    fun setBitValue(bitIndex: Int, value: Boolean) {
        variables()[0].set(bitIndex, value)
    }

    /** String form of variable bits without separators (used by logging/ranking). */
    fun getVariableValueString(variableIndex: Int): String {
        val bits: BinarySet = variables()[variableIndex]
        val sb = StringBuilder(numberOfTopics)
        var i = 0
        while (i < numberOfTopics) {
            sb.append(if (bits.get(i)) '1' else '0')
            i++
        }
        return sb.toString()
    }

    /** Build a BinarySet from a genes array (used by streaming/Average paths). */
    fun createNewBitSet(numBits: Int, genes: Array<Boolean>? = null): BinarySet {
        val bs = BinarySet(numBits)
        if (genes != null) {
            val safeLen = minOf(numBits, genes.size)
            var i = 0
            while (i < safeLen) {
                bs.set(i, genes[i])
                i++
            }
        }
        return bs
    }

    /** Join labels of selected topics for -Top CSV (use ';' to avoid CSV commas). */
    fun getTopicLabelsFromTopicStatus(): String {
        val bits: BinarySet = variables()[0]
        val names = ArrayList<String>()
        var i = 0
        while (i < numberOfTopics) {
            if (bits.get(i)) names.add(topicLabels[i])
            i++
        }
        return names.joinToString(separator = ";")
    }

    /** Expose a single topic label safely (for operator logs). */
    fun topicLabelAt(index: Int): String = topicLabels[index]

    /** Clear Step-3 caches (subset sums). */
    fun resetIncrementalEvaluationState() {
        lastEvaluatedMask = null
        cachedSumsBySystem = null
    }

    /** Clear Step-3 operator flags (swap info). */
    fun clearLastMutationFlags() {
        lastMutationWasFixedKSwap = false
        lastSwapOutIndex = null
        lastSwapInIndex = null
    }

    /* ---------------------------------- Copy --------------------------------------- */

    /**
     * Deep copy preserving:
     *  - Variable bitset
     *  - Objectives
     *  - topicStatus mirror
     *  - Step-3 caches and operator flags
     */
    override fun copy(): BestSubsetSolution {
        val clone = BestSubsetSolution(
            numberOfVariables = 1,
            numberOfObjectives = objectives().size,  // jMetal 6.x: objectives() is a DoubleArray
            numberOfTopics = numberOfTopics,
            topicLabels = topicLabels.copyOf(),
            forcedCardinality = null /* not used on clones; we copy bits instead */
        )

        /* Copy bitset */
        val srcBits: BinarySet = variables()[0]
        val dstBits = BinarySet(numberOfTopics).apply { this.or(srcBits) }
        clone.variables()[0] = dstBits

        /* Copy objectives (jMetal 6.x: use the objectives() array) */
        val m = objectives().size
        var i = 0
        while (i < m) {
            clone.objectives()[i] = this.objectives()[i]
            i++
        }

        /* Copy mirrors & caches */
        clone.topicStatus = this.topicStatus.copyOf()
        clone.lastEvaluatedMask = this.lastEvaluatedMask?.copyOf()
        clone.cachedSumsBySystem = this.cachedSumsBySystem?.copyOf()

        /* Copy operator flags */
        clone.lastSwapOutIndex = this.lastSwapOutIndex
        clone.lastSwapInIndex = this.lastSwapInIndex
        clone.lastMutationWasFixedKSwap = this.lastMutationWasFixedKSwap

        return clone
    }

    /** External-friendly: cardinality as Double (used by writers/streams). */
    fun getCardinality(): Double = numberOfSelectedTopics.toDouble()

    /** Returns the correlation as stored in objectives()[1]:
     *  - BEST  : negative of the natural correlation (internal encoding)
     *  - WORST : natural correlation
     * External printing logic must convert to the natural view where required.
     */
    fun getCorrelation(): Double = objectives()[1]

}