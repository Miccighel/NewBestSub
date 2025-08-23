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
 *  • Delta-eval scaffolding:
 *      - `lastEvaluatedMask`, `cachedSumsBySystem` (subset-sum cache)
 *      - `lastSwapOutIndex`, `lastSwapInIndex`, `lastMutationWasFixedKSwap` (operator hook)
 *  • Performance:
 *      - Cached genotype key (bitstring) with dirty tracking to avoid rebuilding strings.
 *      - Cached Base64 mask derived directly from BinarySet (avoids BooleanArray round-trips).
 *      - Reusable boolean buffer for `lastEvaluatedMask` to avoid per-eval allocations.
 *
 * Notes:
 *  - jMetal 6.x API: use variables() / objectives() lists.
 *  - All randomness uses JMetalRandom's singleton to preserve determinism when seeded.
 * ------------------------------------------------------------------------------------------------------------------ */
class BestSubsetSolution(
    numberOfVariables: Int,
    numberOfObjectives: Int,
    private val numberOfTopics: Int,
    private val topicLabels: Array<String>,
    private val forcedCardinality: Int?
) : DefaultBinarySolution(listOf(numberOfTopics), numberOfObjectives) {

    /* External mirror for writers (-Top CSV, etc.). */
    var topicStatus: Array<Boolean> = Array(numberOfTopics) { false }

    /* Delta-eval caches. */
    var lastEvaluatedMask: BooleanArray? = null
    var cachedSumsBySystem: DoubleArray? = null

    /* Operator flags (for Fixed-K swap hints). */
    var lastSwapOutIndex: Int? = null
    var lastSwapInIndex: Int? = null
    var lastMutationWasFixedKSwap: Boolean = false

    /* Genotype / mask caches (avoid repeated string/materialization). */
    private var cachedGenotypeKey: String? = null
    private var cachedMaskB64: String? = null
    private var genotypeDirty: Boolean = true

    /* -------------------------------- Perf: reusable buffers -------------------------------- */
    /* Reusable mask buffer to avoid allocating a new BooleanArray at every evaluation. */
    private val reusableMaskBuffer: BooleanArray = BooleanArray(numberOfTopics)

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /* -------------------------------- Initialization ------------------------------- */
    init {
        val initialMask: BinarySet =
            if (forcedCardinality != null) buildMaskWithExactCardinality(forcedCardinality)
            else buildRandomNonEmptyMask()

        variables()[0] = initialMask
        topicStatus = retrieveTopicStatus().toTypedArray()
        genotypeDirty = true
        cachedMaskB64 = null
    }

    /* ------------------------------ Mask Builders ---------------------------------- */

    /**
     * Build a BinarySet with exactly K bits set.
     *
     * - K is clamped to [0, numberOfTopics].
     * - K == 0  → all false.
     * - K == N  → all true (fast path; avoids sampling edge-cases).
     * - 0 < K < N → partial Fisher–Yates with a correct [i, N-1] pick.
     */
    private fun buildMaskWithExactCardinality(kRequested: Int): BinarySet {
        val n = numberOfTopics
        val k = kRequested.coerceIn(0, n)
        val mask = BinarySet(n)

        if (k == 0) return mask
        if (k == n) {
            var i = 0
            while (i < n) { mask.set(i, true); i++ }   // set all bits via API
            return mask
        }

        // Use the same RNG source to preserve determinism.
        val rng = JMetalRandom.getInstance().randomGenerator

        // Index pool: 0..n-1; fix the first k slots via partial Fisher–Yates.
        val pool = IntArray(n) { it }

        var i = 0
        while (i < k) {
            // Pick j in [i, n-1]. IMPORTANT: bound is (n - i), not n.
            val j = i + (rng.nextDouble() * (n - i)).toInt()
            val tmp = pool[i]; pool[i] = pool[j]; pool[j] = tmp
            mask.set(pool[i], true)
            i++
        }
        return mask
    }

    /** Build a BinarySet with random bits and ensure it's not empty (flip 1 if needed). */
    private fun buildRandomNonEmptyMask(): BinarySet {
        val mask = BinarySet(numberOfTopics)
        val rng = JMetalRandom.getInstance().randomGenerator

        var anySelected = false
        var bitIndex = 0
        while (bitIndex < numberOfTopics) {
            val pick = rng.nextDouble() < 0.5
            mask.set(bitIndex, pick)
            anySelected = anySelected or pick
            bitIndex++
        }

        if (!anySelected) {
            // Choose one random bit in [0, n) and set it.
            val flipIndex = if (numberOfTopics == 1) 0 else (rng.nextDouble() * numberOfTopics).toInt()
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

    /** Ensure and return the reusable last-mask buffer (no allocation when persisting state). */
    fun ensureReusableLastMaskBuffer(): BooleanArray = reusableMaskBuffer

    /** Count of selected topics (cardinality of the bitset). */
    val numberOfSelectedTopics: Int
        get() = variables()[0].cardinality()

    /** Set a single bit in the underlying BinarySet. */
    fun setBitValue(bitIndex: Int, value: Boolean) {
        variables()[0].set(bitIndex, value)
        genotypeDirty = true
        cachedMaskB64 = null
    }

    /** String form of variable bits without separators (used by logging/ranking). */
    fun getVariableValueString(variableIndex: Int): String {
        var key = cachedGenotypeKey
        if (genotypeDirty || key == null) {
            val bits: BinarySet = variables()[variableIndex]
            val sb = StringBuilder(numberOfTopics)
            var i = 0
            while (i < numberOfTopics) {
                sb.append(if (bits.get(i)) '1' else '0')
                i++
            }
            key = sb.toString()
            cachedGenotypeKey = key
            // Do not clear dirty here: we also rebuild Base64 lazily.
        }
        return key
    }

    /** Base64 for current mask, computed directly from BinarySet and cached. */
    fun maskB64Cached(): String {
        var b64 = cachedMaskB64
        if (genotypeDirty || b64 == null) {
            b64 = binarySetToBase64(variables()[0], numberOfTopics)
            cachedMaskB64 = b64
            genotypeDirty = false
        }
        return b64
    }

    /** Build a BinarySet from a genes array (used by streaming/Average paths). */
    fun createNewBitSet(numBits: Int, genes: Array<Boolean>? = null): BinarySet {
        val bs = BinarySet(numBits)
        if (genes != null) {
            val safeLen = minOf(numBits, genes.size)
            var i = 0
            while (i < safeLen) { bs.set(i, genes[i]); i++ }
        }
        // When external code installs this into variables()[0], that constitutes a mutation.
        genotypeDirty = true
        cachedMaskB64 = null
        return bs
    }

    /** Clear delta-eval caches (subset sums). */
    fun resetIncrementalEvaluationState() {
        lastEvaluatedMask = null
        cachedSumsBySystem = null
    }

    /** Clear operator flags (swap info). */
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
     *  - Delta-eval caches and operator flags
     *  - Genotype/cache state (safe to reuse)
     */
    override fun copy(): BestSubsetSolution {
        val clone = BestSubsetSolution(
            numberOfVariables = 1,
            numberOfObjectives = objectives().size,
            numberOfTopics = numberOfTopics,
            topicLabels = topicLabels.copyOf(),
            forcedCardinality = null
        )

        /* Copy bitset */
        val srcBits: BinarySet = variables()[0]
        val dstBits = BinarySet(numberOfTopics).apply { this.or(srcBits) }
        clone.variables()[0] = dstBits

        /* Copy objectives */
        val m = objectives().size
        var i = 0
        while (i < m) {
            clone.objectives()[i] = this.objectives()[i]
            i++
        }

        /* Mirrors & caches */
        clone.topicStatus = this.topicStatus.copyOf()

        /* If we held an evaluated mask, copy its content into clone's reusable buffer. */
        this.lastEvaluatedMask?.let { src ->
            val dst = clone.reusableMaskBuffer
            System.arraycopy(src, 0, dst, 0, src.size)
            clone.lastEvaluatedMask = dst
        }

        clone.cachedSumsBySystem = this.cachedSumsBySystem?.copyOf()

        /* Operator flags */
        clone.lastSwapOutIndex = this.lastSwapOutIndex
        clone.lastSwapInIndex = this.lastSwapInIndex
        clone.lastMutationWasFixedKSwap = this.lastMutationWasFixedKSwap

        /* Genotype/mask caches */
        clone.cachedGenotypeKey = this.cachedGenotypeKey
        clone.cachedMaskB64 = this.cachedMaskB64
        clone.genotypeDirty = this.genotypeDirty

        return clone
    }

    /** Friendly cardinality accessor (Double) used by writers/streams. */
    fun getCardinality(): Double = numberOfSelectedTopics.toDouble()

    /**
     * Returns the correlation as stored in objectives()[1]:
     *  - BEST  : negative of the natural correlation (internal encoding)
     *  - WORST : natural correlation
     * External printing logic must convert to the natural view where required.
     */
    fun getCorrelation(): Double = objectives()[1]

    /* --------------------------- Bitset → Base64 (no arrays) ------------------------ */

    /** BinarySet → Base64 (unpadded) directly, little-endian by word, LSB-first within word. */
    private fun binarySetToBase64(bits: BinarySet, totalBits: Int): String {
        val numberOfWords = (totalBits + 63) ushr 6
        val byteBuffer = ByteArray(numberOfWords * java.lang.Long.BYTES)

        var bitIndexWithinWord = 0
        var accumulator = 0L
        var byteWriteOffset = 0

        var i = 0
        while (i < totalBits) {
            if (bits.get(i)) accumulator = accumulator or (1L shl bitIndexWithinWord)
            bitIndexWithinWord++
            if (bitIndexWithinWord == 64) {
                // Write 8 bytes, least-significant first (matches booleanMaskToBase64 convention).
                var w = accumulator
                var b = 0
                while (b < 8) {
                    byteBuffer[byteWriteOffset + b] = (w and 0xFF).toByte()
                    w = w ushr 8
                    b++
                }
                byteWriteOffset += 8
                bitIndexWithinWord = 0
                accumulator = 0L
            }
            i++
        }

        if (bitIndexWithinWord != 0) {
            var w = accumulator
            var b = 0
            while (b < 8) {
                byteBuffer[byteWriteOffset + b] = (w and 0xFF).toByte()
                w = w ushr 8
                b++
            }
        }

        return java.util.Base64.getEncoder().withoutPadding().encodeToString(byteBuffer)
    }
}
