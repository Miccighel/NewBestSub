package it.uniud.newbestsub.problem

import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.binarysolution.impl.DefaultBinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/**
 * Concrete binary solution for the “best subset” problem, backed by jMetal's [BinarySet].
 *
 * Highlights:
 * - Construction helpers for fixed cardinality `K` or a random non-empty mask.
 * - Readable accessors used across the project (retrieveTopicStatus, numberOfSelectedTopics, and similar).
 * - Mirror field [topicStatus] kept for downstream writers (for example, -Top CSV).
 * - Delta evaluation scaffolding:
 *   - [lastEvaluatedMask], [cachedSumsBySystem] (subset-sum cache)
 *   - [lastSwapOutIndex], [lastSwapInIndex], [lastMutationWasFixedKSwap] (operator hooks)
 * - Performance:
 *   - Cached genotype key (bitstring) with dirty tracking to avoid repeated builds.
 *   - Cached Base64 of the mask computed directly from [BinarySet] (no BooleanArray round trips).
 *   - Reusable boolean buffer for [lastEvaluatedMask] to avoid per-evaluation allocations.
 *
 * Notes:
 * - jMetal 6.x API uses `variables()` and `objectives()` lists.
 * - All randomness comes from the [JMetalRandom] singleton to preserve determinism when seeded.
 *
 * @param numberOfVariables the number of variables (always 1 for this problem)
 * @param numberOfObjectives the number of objectives
 * @param numberOfTopics number of topics (size of the binary mask)
 * @param topicLabels human-readable labels for topics; mirrored to copies
 * @param forcedCardinality if non-null, initialize with exactly this many bits set; otherwise random non-empty
 */
class BestSubsetSolution(
    numberOfVariables: Int,
    numberOfObjectives: Int,
    private val numberOfTopics: Int,
    private val topicLabels: Array<String>,
    private val forcedCardinality: Int?
) : DefaultBinarySolution(listOf(numberOfTopics), numberOfObjectives) {

    /** External mirror for writers (-Top CSV and similar). */
    var topicStatus: Array<Boolean> = Array(numberOfTopics) { false }

    /** Last evaluated mask snapshot (reused to avoid allocations). */
    var lastEvaluatedMask: BooleanArray? = null

    /** Cached per-system subset sums for incremental or delta evaluation. */
    var cachedSumsBySystem: DoubleArray? = null

    /** Index swapped out by the last fixed-K mutation (if any). */
    var lastSwapOutIndex: Int? = null

    /** Index swapped in by the last fixed-K mutation (if any). */
    var lastSwapInIndex: Int? = null

    /** Whether the last mutation was a fixed-K swap. */
    var lastMutationWasFixedKSwap: Boolean = false

    /* Genotype and mask caches (avoid repeated string or materialization). */
    private var cachedGenotypeKey: String? = null
    private var cachedMaskB64: String? = null
    private var genotypeDirty: Boolean = true

    /* -------------------------------- Performance: reusable buffers -------------------------------- */

    /** Reusable mask buffer to avoid allocating a new BooleanArray at every evaluation. */
    private val reusableMaskBuffer: BooleanArray = BooleanArray(numberOfTopics)

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /* -------------------------------- Initialization -------------------------------- */

    init {
        val initialMask: BinarySet =
            if (forcedCardinality != null) buildMaskWithExactCardinality(forcedCardinality)
            else buildRandomNonEmptyMask()

        variables()[0] = initialMask
        topicStatus = retrieveTopicStatus().toTypedArray()
        genotypeDirty = true
        cachedMaskB64 = null
    }

    // -----------------------------------------------------------------------------------------
    // Mask builders
    // -----------------------------------------------------------------------------------------

    /**
     * Builds a [BinarySet] with exactly K bits set.
     *
     * Rules:
     * - K is clamped to [0, numberOfTopics].
     * - K == 0  -> all false.
     * - K == N  -> all true (fast path).
     * - 0 < K < N -> partial Fisher–Yates over indices with a correct [i, N-1] pick.
     *
     * Determinism: uses [JMetalRandom]'s singleton.
     */
    private fun buildMaskWithExactCardinality(requestedCardinality: Int): BinarySet {
        val totalTopics = numberOfTopics
        val cardinality = requestedCardinality.coerceIn(0, totalTopics)
        val topicMask = BinarySet(totalTopics)

        if (cardinality == 0) return topicMask
        if (cardinality == totalTopics) {
            var topicIndex = 0
            while (topicIndex < totalTopics) {
                topicMask.set(topicIndex, true)
                topicIndex++
            }
            return topicMask
        }

        val randomGenerator = JMetalRandom.getInstance().randomGenerator

        // Index pool: 0..N-1; fix the first K slots via partial Fisher–Yates.
        val topicIndexPool = IntArray(totalTopics) { it }

        var currentIndex = 0
        while (currentIndex < cardinality) {
            // Pick swapIndex in [currentIndex, totalTopics - 1]. Bound is (totalTopics - currentIndex), not totalTopics.
            val swapIndex = currentIndex + (randomGenerator.nextDouble() * (totalTopics - currentIndex)).toInt()
            val temp = topicIndexPool[currentIndex]
            topicIndexPool[currentIndex] = topicIndexPool[swapIndex]
            topicIndexPool[swapIndex] = temp
            topicMask.set(topicIndexPool[currentIndex], true)
            currentIndex++
        }
        return topicMask
    }

    /**
     * Builds a [BinarySet] with random bits and guarantees it is not empty
     * (flips one bit to true if necessary).
     *
     * Determinism: uses [JMetalRandom]'s singleton.
     */
    private fun buildRandomNonEmptyMask(): BinarySet {
        val topicMask = BinarySet(numberOfTopics)
        val randomGenerator = JMetalRandom.getInstance().randomGenerator

        var anySelected = false
        var topicIndex = 0
        while (topicIndex < numberOfTopics) {
            val selectBit = randomGenerator.nextDouble() < 0.5
            topicMask.set(topicIndex, selectBit)
            anySelected = anySelected or selectBit
            topicIndex++
        }

        if (!anySelected) {
            // Choose one random bit in [0, numberOfTopics) and set it.
            val flipIndex =
                if (numberOfTopics == 1) 0 else (randomGenerator.nextDouble() * numberOfTopics).toInt()
            topicMask.set(flipIndex, true)
        }
        return topicMask
    }

    // -----------------------------------------------------------------------------------------
    // Public helpers
    // -----------------------------------------------------------------------------------------

    /**
     * Returns the current topic mask as a primitive [BooleanArray].
     * This is faster for numeric loops than boxing or unboxing via [Array].
     */
    fun retrieveTopicStatus(): BooleanArray {
        val bits: BinarySet = variables()[0]
        val status = BooleanArray(numberOfTopics)
        var topicIndex = 0
        while (topicIndex < numberOfTopics) {
            status[topicIndex] = bits.get(topicIndex)
            topicIndex++
        }
        return status
    }

    /** Returns a reusable buffer to persist the last evaluated mask without reallocations. */
    fun ensureReusableLastMaskBuffer(): BooleanArray = reusableMaskBuffer

    /** Count of selected topics (bitset cardinality). */
    val numberOfSelectedTopics: Int
        get() = variables()[0].cardinality()

    /**
     * Sets a single bit in the underlying [BinarySet] and marks caches dirty.
     *
     * @param bitIndex the bit to set
     * @param value new boolean value
     */
    fun setBitValue(bitIndex: Int, value: Boolean) {
        variables()[0].set(bitIndex, value)
        genotypeDirty = true
        cachedMaskB64 = null
    }

    /**
     * Returns the string form of the variable bits without separators (for example, "101001...").
     * Cached and rebuilt only when mutated.
     *
     * @param variableIndex index of the variable (always 0 for this problem)
     */
    fun getVariableValueString(variableIndex: Int): String {
        var key = cachedGenotypeKey
        if (genotypeDirty || key == null) {
            val bits: BinarySet = variables()[variableIndex]
            val bitStringBuilder = StringBuilder(numberOfTopics)
            var topicIndex = 0
            while (topicIndex < numberOfTopics) {
                bitStringBuilder.append(if (bits.get(topicIndex)) '1' else '0')
                topicIndex++
            }
            key = bitStringBuilder.toString()
            cachedGenotypeKey = key
            // Keep dirty flag; Base64 is rebuilt lazily and clears the dirty flag there.
        }
        return key
    }

    /**
     * Creates a new [BinarySet] with [numBits] bits and, if provided, sets bits from [genes].
     * Marks caches dirty so callers can install the returned bitset into `variables()[0]`.
     *
     * @param numBits number of bits
     * @param genes optional boolean array to initialize bits (truncated to [numBits])
     */
    fun createNewBitSet(numBits: Int, genes: Array<Boolean>? = null): BinarySet {
        val newBitSet = BinarySet(numBits)
        if (genes != null) {
            val safeLength = minOf(numBits, genes.size)
            var bitIndex = 0
            while (bitIndex < safeLength) {
                newBitSet.set(bitIndex, genes[bitIndex])
                bitIndex++
            }
        }
        genotypeDirty = true
        cachedMaskB64 = null
        return newBitSet
    }

    /** Clears delta evaluation caches (subset sums and last evaluated mask). */
    fun resetIncrementalEvaluationState() {
        lastEvaluatedMask = null
        cachedSumsBySystem = null
    }

    /** Clears operator flags (swap info used by fixed-K operators). */
    fun clearLastMutationFlags() {
        lastMutationWasFixedKSwap = false
        lastSwapOutIndex = null
        lastSwapInIndex = null
    }

    /** Friendly cardinality accessor (as Double) used by writers or streams. */
    fun getCardinality(): Double = numberOfSelectedTopics.toDouble()

    /**
     * Returns the correlation objective stored in `objectives()[1]`.
     *
     * Encoding:
     * - BEST: negative of the natural correlation (internal encoding).
     * - WORST: natural correlation.
     *
     * External printing or aggregation may convert it back to the natural view.
     */
    fun getCorrelation(): Double = objectives()[1]

    // -----------------------------------------------------------------------------------------
    // Copy
    // -----------------------------------------------------------------------------------------

    /**
     * Deep copy preserving:
     * - Variable bitset
     * - Objectives
     * - [topicStatus] mirror
     * - Delta evaluation caches and operator flags
     * - Genotype or cache state
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
        val sourceBits: BinarySet = variables()[0]
        val destinationBits = BinarySet(numberOfTopics).apply { this.or(sourceBits) }
        clone.variables()[0] = destinationBits

        /* Copy objectives */
        val objectiveCount = objectives().size
        var objectiveIndex = 0
        while (objectiveIndex < objectiveCount) {
            clone.objectives()[objectiveIndex] = this.objectives()[objectiveIndex]
            objectiveIndex++
        }

        /* Mirrors and caches */
        clone.topicStatus = this.topicStatus.copyOf()

        /* If we held an evaluated mask, copy its content into clone's reusable buffer. */
        this.lastEvaluatedMask?.let { sourceMask ->
            val destinationMask = clone.reusableMaskBuffer
            System.arraycopy(sourceMask, 0, destinationMask, 0, sourceMask.size)
            clone.lastEvaluatedMask = destinationMask
        }

        clone.cachedSumsBySystem = this.cachedSumsBySystem?.copyOf()

        /* Operator flags */
        clone.lastSwapOutIndex = this.lastSwapOutIndex
        clone.lastSwapInIndex = this.lastSwapInIndex
        clone.lastMutationWasFixedKSwap = this.lastMutationWasFixedKSwap

        /* Genotype and mask caches */
        clone.cachedGenotypeKey = this.cachedGenotypeKey
        clone.cachedMaskB64 = this.cachedMaskB64
        clone.genotypeDirty = this.genotypeDirty

        return clone
    }
}
