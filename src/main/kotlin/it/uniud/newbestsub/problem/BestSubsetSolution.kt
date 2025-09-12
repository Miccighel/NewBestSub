package it.uniud.newbestsub.problem

import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.binarysolution.impl.DefaultBinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom
import java.util.Base64

/**
 * BestSubsetSolution
 *
 * Concrete binary solution for the “best subset” problem, backed by jMetal's BinarySet.
 *
 * Highlights
 *  • Construction helpers for fixed cardinality K or a random non-empty mask.
 *  • Readable accessors used across the project (retrieveTopicStatus, numberOfSelectedTopics, etc.).
 *  • Delta evaluation scaffolding:
 *      – lastEvaluatedMask, cachedSumsBySystem (subset-sum cache)
 *      – lastSwapOutIndex, lastSwapInIndex, lastMutationWasFixedKSwap (operator hooks)
 *  • Performance
 *      – Cached genotype key (bitstring) with dirty tracking to avoid repeated builds.
 *      – Cached Base64 of the mask computed directly from BinarySet (no BooleanArray round trip).
 *      – Reusable boolean buffer for lastEvaluatedMask to avoid per-evaluation allocations.
 *      – Sparse-aware packing and status extraction using nextSetBit.
 *
 * Notes
 *  • jMetal 6.x API uses variables() and objectives() lists.
 *  • All randomness comes from the JMetalRandom singleton to preserve determinism when seeded.
 */
class BestSubsetSolution(
    numberOfVariables: Int,
    numberOfObjectives: Int,
    private val numberOfTopics: Int,
    val topicLabels: Array<String>,
    private val forcedCardinality: Int?
) : DefaultBinarySolution(listOf(numberOfTopics), numberOfObjectives) {

    /** External mirror for writers (for example, -Top CSV). */
    var topicStatus: Array<Boolean> = Array(numberOfTopics) { false }

    /** Last evaluated mask snapshot (reused to avoid allocations). */
    var lastEvaluatedMask: BooleanArray? = null

    /** Cached per-system subset sums for incremental or delta evaluation. */
    var cachedSumsBySystem: DoubleArray? = null

    /** Index swapped out by the last fixed-K mutation (if any). */
    var lastSwapOutIndex: Int? = null

    /** Index swapped in by the last fixed-K mutation (if any). */
    var lastSwapInIndex: Int? = null

    /** Whether the last mutation was a fixed-K swap (used by delta-eval fast path). */
    var lastMutationWasFixedKSwap: Boolean = false

    /* Genotype and mask caches (avoid repeated string or materialization). */
    private var cachedGenotypeKey: String? = null
    private var cachedMaskB64: String? = null
    private var genotypeDirty: Boolean = true

    /* Reusable buffer to persist last evaluated mask without reallocations. */
    private val reusableMaskBuffer: BooleanArray = BooleanArray(numberOfTopics)

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /* Initialization
       Choose either a fixed-K mask or a random non-empty mask, then install it.
       Keep the external topicStatus mirror in sync and mark caches dirty. */
    init {
        val initialMask: BinarySet =
            if (forcedCardinality != null) buildMaskWithExactCardinality(forcedCardinality)
            else buildRandomNonEmptyMask()

        variables()[0] = initialMask
        topicStatus = retrieveTopicStatus().toTypedArray()
        genotypeDirty = true
        cachedMaskB64 = null
    }

    /* Mask builders */

    /**
     * Build a BinarySet with exactly K bits set.
     *
     * Rules
     *  • K is clamped to [0, numberOfTopics].
     *  • K == 0  → all false.
     *  • K == N  → all true (fast path).
     *  • 0 < K < N → partial Fisher–Yates over indices with a correct [i, N-1] pick.
     */
    private fun buildMaskWithExactCardinality(requestedCardinality: Int): BinarySet {
        val totalTopicCount = numberOfTopics
        val clampedCardinality = requestedCardinality.coerceIn(0, totalTopicCount)
        val topicMask = BinarySet(totalTopicCount)

        if (clampedCardinality == 0) return topicMask
        if (clampedCardinality == totalTopicCount) {
            var topicIndex = 0
            while (topicIndex < totalTopicCount) {
                topicMask.set(topicIndex, true)
                topicIndex++
            }
            return topicMask
        }

        val randomGenerator = JMetalRandom.getInstance().randomGenerator

        /* Index pool 0..N-1; fix the first K slots via partial Fisher–Yates. */
        val topicIndexPool = IntArray(totalTopicCount) { it }
        var poolIndex = 0
        while (poolIndex < clampedCardinality) {
            val swapIndex = poolIndex + (randomGenerator.nextDouble() * (totalTopicCount - poolIndex)).toInt()
            val tmp = topicIndexPool[poolIndex]
            topicIndexPool[poolIndex] = topicIndexPool[swapIndex]
            topicIndexPool[swapIndex] = tmp
            topicMask.set(topicIndexPool[poolIndex], true)
            poolIndex++
        }
        return topicMask
    }

    /**
     * Build a BinarySet with random bits and guarantee it is not empty
     * by flipping one bit to true if necessary.
     */
    private fun buildRandomNonEmptyMask(): BinarySet {
        val topicMask = BinarySet(numberOfTopics)
        val randomGenerator = JMetalRandom.getInstance().randomGenerator

        var anySelected = false
        var topicIndex = 0
        while (topicIndex < numberOfTopics) {
            val chooseBit = randomGenerator.nextDouble() < 0.5
            topicMask.set(topicIndex, chooseBit)
            anySelected = anySelected or chooseBit
            topicIndex++
        }

        if (!anySelected) {
            val flipIndex = if (numberOfTopics == 1) 0 else (randomGenerator.nextDouble() * numberOfTopics).toInt()
            topicMask.set(flipIndex, true)
        }
        return topicMask
    }

    /* Public helpers */

    /**
     * Current topic mask as a primitive BooleanArray.
     * Uses nextSetBit so complexity is proportional to K.
     */
    fun retrieveTopicStatus(): BooleanArray {
        val bits: BinarySet = variables()[0]
        val status = BooleanArray(numberOfTopics) /* all false by default */
        var i = bits.nextSetBit(0)
        while (i in 0..<numberOfTopics) {
            status[i] = true
            i = bits.nextSetBit(i + 1)
        }
        return status
    }

    /** Reusable buffer to persist the last evaluated mask without new allocations. */
    fun ensureReusableLastMaskBuffer(): BooleanArray = reusableMaskBuffer

    /** Count of selected topics (bitset cardinality). */
    val numberOfSelectedTopics: Int
        get() = variables()[0].cardinality()

    /**
     * Set a single bit and mark caches dirty, keeping topicStatus in sync.
     */
    fun setBitValue(bitIndex: Int, newValue: Boolean) {
        variables()[0].set(bitIndex, newValue)
        if (bitIndex in 0 until topicStatus.size) topicStatus[bitIndex] = newValue
        genotypeDirty = true
        cachedMaskB64 = null
    }

    /**
     * Create a fresh BinarySet of numBits and optionally set bits from genes.
     * Marks caches dirty so callers can install the returned bitset into variables()[0].
     */
    fun createNewBitSet(numBits: Int, genes: Array<Boolean>? = null): BinarySet {
        val newBitSet = BinarySet(numBits)
        if (genes != null) {
            val safeLen = minOf(numBits, genes.size)
            var i = 0
            while (i < safeLen) {
                newBitSet.set(i, genes[i])
                i++
            }
        }
        genotypeDirty = true
        cachedMaskB64 = null
        return newBitSet
    }

    /** Clear delta-evaluation caches (subset sums and last evaluated mask). */
    fun resetIncrementalEvaluationState() {
        lastEvaluatedMask = null
        cachedSumsBySystem = null
    }

    /** Clear operator flags (swap info used by fixed-K operators). */
    fun clearLastMutationFlags() {
        lastMutationWasFixedKSwap = false
        lastSwapOutIndex = null
        lastSwapInIndex = null
    }

    /** Friendly cardinality accessor (as Double) used by writers and streams. */
    fun getCardinality(): Double = numberOfSelectedTopics.toDouble()

    /**
     * Correlation objective stored in objectives()[1].
     * BEST encodes -corr internally, WORST encodes +corr.
     */
    fun getCorrelation(): Double = objectives()[1]

    /* Compact mask serializers */

    /**
     * Serialize the current BinarySet to unpadded Base64.
     *
     * Layout
     *  • Bits are packed LSB-first into 64-bit words.
     *  • Words are emitted in little-endian byte order.
     *
     * Properties
     *  • Sparse-aware: iterates only set bits via nextSetBit.
     *  • Cached: recomputed only when the genotype changes.
     */
    fun packMaskToBase64(): String {
        var cached = cachedMaskB64
        if (genotypeDirty || cached == null) {
            val bits = variables()[0] as BinarySet
            val totalBits = numberOfTopics
            val wordCount = (totalBits + 63) ushr 6
            val words = LongArray(wordCount) /* zero-initialized */

            /* Fill only set bits into the word array */
            var idx = bits.nextSetBit(0)
            while (idx >= 0 && idx < totalBits) {
                val wi = idx ushr 6            /* word index */
                val bp = idx and 63            /* bit position within word */
                words[wi] = words[wi] or (1L shl bp)
                idx = bits.nextSetBit(idx + 1)
            }

            /* Write words to a byte buffer in little-endian order */
            val outBytes = ByteArray(wordCount * java.lang.Long.BYTES)
            var off = 0
            var wIdx = 0
            while (wIdx < wordCount) {
                var w = words[wIdx]
                var b = 0
                while (b < 8) {
                    outBytes[off++] = (w and 0xFF).toByte()
                    w = w ushr 8
                    b++
                }
                wIdx++
            }

            cached = Base64.getEncoder().withoutPadding().encodeToString(outBytes)
            cachedMaskB64 = cached
            genotypeDirty = false
        }
        return cached!!
    }

    /** VAR line serializer using the cached Base64 from packMaskToBase64. */
    fun toVarLinePackedBase64(): String = "B64:" + packMaskToBase64()

    /* Deep copy */

    override fun copy(): BestSubsetSolution {
        val clone = BestSubsetSolution(
            1,
            objectives().size,
            numberOfTopics,
            topicLabels.copyOf(),
            null
        )

        /* Bitset */
        val sourceBits: BinarySet = variables()[0]
        val destinationBits = BinarySet(numberOfTopics).apply { this.or(sourceBits) }
        clone.variables()[0] = destinationBits

        /* Objectives */
        val objectiveCount = objectives().size
        var objectiveIndex = 0
        while (objectiveIndex < objectiveCount) {
            clone.objectives()[objectiveIndex] = this.objectives()[objectiveIndex]
            objectiveIndex++
        }

        /* Mirrors and caches */
        clone.topicStatus = this.topicStatus.copyOf()
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

        /* Genotype & mask caches */
        clone.cachedGenotypeKey = this.cachedGenotypeKey
        clone.cachedMaskB64 = this.cachedMaskB64
        clone.genotypeDirty = this.genotypeDirty

        return clone
    }
}
