package it.uniud.newbestsub.problem

import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom
import java.util.Objects

class BestSubsetSolution(

    /* Core sizes required by Solution API (kept as constructor params for your callers) */
    private val numberOfVariables: Int,
    private val numberOfObjectives: Int,

    /* Domain-specific sizes & labels */
    private val numberOfTopics: Int,
    private val topicLabels: Array<String>,

    /* If not null, generate a solution with exactly this cardinality; otherwise sample randomly */
    forcedCardinality: Int? = null

) : BinarySolution, Comparable<BestSubsetSolution> {

    /* -------- jMetal 6.x storage backing fields --------
     * jMetal 6 uses property-style accessors:
     *   variables(), objectives(), constraints(), attributes()
     * We keep your internal fields and expose them via those methods. */
    private val variables: MutableList<BinarySet> = MutableList(numberOfVariables) { BinarySet(numberOfTopics) }
    private val objectives: DoubleArray = DoubleArray(numberOfObjectives) { 0.0 }
    private val constraints: DoubleArray = DoubleArray(0) /* no constraints in this problem */
    private val attributes: MutableMap<Any, Any> = LinkedHashMap()

    /* Cached convenience info */
    var topicStatus: Array<Boolean> = Array(numberOfTopics) { false }
    var numberOfSelectedTopics: Int = 0
        private set

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    init {
        /* Determinism: all randomness goes through jMetal's singleton RNG, which
         * is bridged by RandomBridge when the user enables the deterministic option. */
        val rng = JMetalRandom.getInstance()
        require(numberOfTopics > 0) { "numberOfTopics must be > 0" }
        require(forcedCardinality == null || (forcedCardinality in 1..numberOfTopics)) {
            "forcedCardinality must be in 1..$numberOfTopics (was $forcedCardinality)"
        }

        /* Initialize topicStatus according to forced cardinality or random sampling (ensuring at least one bit set) */
        if (forcedCardinality != null) {
            while (numberOfSelectedTopics < forcedCardinality) {
                /* rng.nextInt(a,b) in jMetal is inclusive on 'b', hence (0..N-1) for indices */
                val idx = if (numberOfTopics <= 1) 0 else rng.nextInt(0, numberOfTopics - 1)
                if (!topicStatus[idx]) {
                    topicStatus[idx] = true
                    numberOfSelectedTopics++
                }
            }
        } else {
            val columnKeepProbability = rng.nextDouble()
            for (i in 0 until numberOfTopics) {
                val keep = rng.nextDouble() > columnKeepProbability
                if (keep) {
                    topicStatus[i] = true
                    numberOfSelectedTopics++
                }
            }
            /* ensure at least one bit set */
            if (numberOfSelectedTopics == 0) {
                val flipIndex = if (numberOfTopics <= 1) 0 else rng.nextInt(0, numberOfTopics - 1)
                topicStatus[flipIndex] = true
                numberOfSelectedTopics = 1
            }
        }

        /* Encode topicStatus into BinarySet variable 0 */
        variables[0] = createNewBitSet(numberOfTopics, topicStatus)

        logger.debug("<Num. Sel. Topics: $numberOfSelectedTopics, Sel. Topics: ${getTopicLabelsFromTopicStatus()}, Gene: ${getVariableValueString(0)}>")
    }

    // -------- BinarySolution / Solution API (jMetal 6.x) --------
    // NOTE: The old get*/set* methods disappeared from the interface.
    // We now expose property-style accessors as required by 6.x.

    override fun variables(): MutableList<BinarySet> = variables
    override fun objectives(): DoubleArray = objectives
    override fun constraints(): DoubleArray = constraints
    override fun attributes(): MutableMap<Any, Any> = attributes

    /* jMetal 6.1 bit-size reporting:
     * - numberOfBitsPerVariable(): per-variable lengths
     * - totalNumberOfBits(): sum of lengths (we only have 1 BinarySet) */
    override fun numberOfBitsPerVariable(): List<Int> = listOf(variables[0].length())
    override fun totalNumberOfBits(): Int = variables[0].length()

    /* jMetal requires a copy; we keep your deep copy semantics and return the same type. */
    override fun copy(): BestSubsetSolution {
        val clone = BestSubsetSolution(
            numberOfVariables = numberOfVariables,
            numberOfObjectives = numberOfObjectives,
            numberOfTopics = numberOfTopics,
            topicLabels = topicLabels.copyOf(),
            forcedCardinality = null /* will be overwritten below */
        )
        /* Variables */
        for (i in 0 until numberOfVariables) {
            // BinarySet supports clone(); if that ever changes, fall back to manual bit copy.
            clone.variables()[i] = (this.variables()[i].clone() as BinarySet)
        }
        /* Objectives */
        for (i in 0 until numberOfObjectives) {
            clone.objectives()[i] = this.objectives()[i]
        }
        /* Attributes */
        clone.attributes().putAll(this.attributes())

        /* Cached fields */
        clone.topicStatus = this.topicStatus.copyOf()
        clone.numberOfSelectedTopics = this.numberOfSelectedTopics

        return clone
    }

    /* -------- Convenience & domain helpers -------- */

    fun createNewBitSet(numberOfBits: Int, values: Array<Boolean>): BinarySet {
        val bitSet = BinarySet(numberOfBits)
        for (i in 0 until numberOfBits) if (values[i]) bitSet.set(i) else bitSet.clear(i)
        return bitSet
    }

    fun setBitValue(index: Int, value: Boolean) {
        val bs = variables[0]
        if (bs.get(index) != value) {
            if (value) bs.set(index) else bs.clear(index)
            if (value) numberOfSelectedTopics++ else numberOfSelectedTopics--
            topicStatus[index] = value
        }
        variables[0] = bs
    }

    fun retrieveTopicStatus(): BooleanArray {
        val bs = variables[0]
        val out = BooleanArray(bs.length())  /* jMetal 6.x: use length() instead of binarySetLength */
        for (i in out.indices) out[i] = bs.get(i)
        return out
    }

    fun getTopicLabelsFromTopicStatus(): String {
        var selectedTopicLabels = "["
        topicStatus.forEachIndexed { index, keep ->
            if (keep) selectedTopicLabels += "${topicLabels[index]} "
        }
        if (selectedTopicLabels.length > 1) selectedTopicLabels = selectedTopicLabels.dropLast(1)
        selectedTopicLabels += "]"
        return selectedTopicLabels
    }

    fun getVariableValueString(index: Int): String {
        val bs = variables[index]
        val sb = StringBuilder(bs.length())  /* jMetal 6.x: use length() */
        for (i in 0 until bs.length()) sb.append(if (bs.get(i)) '1' else '0')
        return sb.toString()
    }

    /* Convenience wrappers for legacy code (keep callers stable where possible) */
    fun getVariableValue(index: Int) = variables()[index]
    fun setVariableValue(index: Int, value: BinarySet) {
        variables()[index] = value
    }

    override fun compareTo(other: BestSubsetSolution): Int =
        this.getCardinality().compareTo(other.getCardinality())

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is BestSubsetSolution) return false
        return this.getCorrelation() == other.getCorrelation() && this.getCardinality() == other.getCardinality()
    }

    override fun hashCode(): Int = Objects.hash(getCorrelation(), getCardinality())
}

/* -------- Convenience accessors for objective semantics (jMetal 6.x) --------
 * Old calls like solution.getObjective(i) must migrate to solution.objectives()[i].
 * These helpers keep your meaning explicit across the codebase. */
fun BinarySolution.getCardinality(): Double = this.objectives()[0]
fun BinarySolution.getCorrelation(): Double = this.objectives()[1]
