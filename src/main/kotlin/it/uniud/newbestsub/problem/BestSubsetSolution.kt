package it.uniud.newbestsub.problem

import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom
import java.util.Objects

class BestSubsetSolution(

    /* Core sizes required by Solution API */
    private val numberOfVariables: Int,
    private val numberOfObjectives: Int,

    /* Domain-specific sizes & labels */
    private val numberOfTopics: Int,
    private val topicLabels: Array<String>,

    /* If not null, generate a solution with exactly this cardinality; otherwise sample randomly */
    forcedCardinality: Int? = null

) : BinarySolution, Comparable<BestSubsetSolution> {

    /* Variables, objectives, constraints, attributes storage */
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
        val rng = JMetalRandom.getInstance()
        require(numberOfTopics > 0) { "numberOfTopics must be > 0" }

        /* Initialize topicStatus according to forced cardinality or random sampling (ensuring at least one bit set) */
        if (forcedCardinality != null) {
            while (numberOfSelectedTopics < forcedCardinality) {
                val idx = if (numberOfTopics <= 1) 0 else rng.nextInt(0, numberOfTopics - 1)  /* inclusive upper bound */
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
                val flipIndex = if (numberOfTopics <= 1) 0 else rng.nextInt(0, numberOfTopics - 1)  /* inclusive upper bound */
                topicStatus[flipIndex] = true
                numberOfSelectedTopics = 1
            }
        }

        /* Encode topicStatus into BinarySet variable 0 */
        variables[0] = createNewBitSet(numberOfTopics, topicStatus)

        logger.debug("<Num. Sel. Topics: $numberOfSelectedTopics, Sel. Topics: ${getTopicLabelsFromTopicStatus()}, Gene: ${getVariableValueString(0)}>")
    }


    /* -------- BinarySolution / Solution API implementation (jMetal 5.10) -------- */

    override fun getNumberOfVariables(): Int = numberOfVariables
    override fun getNumberOfObjectives(): Int = numberOfObjectives
    override fun getNumberOfConstraints(): Int = constraints.size

    override fun getVariables(): MutableList<BinarySet> = variables
    override fun getVariable(index: Int): BinarySet = variables[index]
    override fun setVariable(index: Int, value: BinarySet) {
        variables[index] = value
    }

    override fun getObjectives(): DoubleArray = objectives
    override fun getObjective(index: Int): Double = objectives[index]
    override fun setObjective(index: Int, value: Double) {
        objectives[index] = value
    }

    override fun getConstraints(): DoubleArray = constraints
    override fun getConstraint(index: Int): Double = constraints[index]
    override fun setConstraint(index: Int, value: Double) {
        /* No constraints: keep for API completeness; guard if ever called. */
        throw IndexOutOfBoundsException("No constraints defined (size=${constraints.size}), requested index=$index")
    }

    override fun getNumberOfBits(index: Int): Int {
        require(index == 0) { "BestSubsetSolution exposes 1 BinarySet variable; requested index=$index" }
        return variables[0].binarySetLength
    }

    override fun getTotalNumberOfBits(): Int = variables[0].binarySetLength

    override fun getAttributes(): MutableMap<Any, Any> = attributes
    override fun setAttribute(id: Any, value: Any) {
        attributes[id] = value
    }

    override fun getAttribute(id: Any): Any? = attributes[id]
    override fun hasAttribute(id: Any): Boolean = attributes.containsKey(id)

    /* -------- Convenience & domain helpers -------- */

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
            clone.setVariable(i, (this.getVariable(i).clone() as BinarySet))
        }
        /* Objectives */
        for (i in 0 until numberOfObjectives) {
            clone.setObjective(i, this.getObjective(i))
        }
        /* Attributes */
        clone.getAttributes().putAll(this.getAttributes())

        /* Cached fields */
        clone.topicStatus = this.topicStatus.copyOf()
        clone.numberOfSelectedTopics = this.numberOfSelectedTopics

        return clone
    }

    fun createNewBitSet(numberOfBits: Int, values: Array<Boolean>): BinarySet {
        val bitSet = BinarySet(numberOfBits)
        for (i in 0 until numberOfBits) if (values[i]) bitSet.set(i) else bitSet.clear(i)
        return bitSet
    }

    fun setBitValue(index: Int, value: Boolean) {
        val bs = variables[0]
        if (bs.get(index) != value) {
            bs.set(index, value)
            if (value) numberOfSelectedTopics++ else numberOfSelectedTopics--
            topicStatus[index] = value
        }
        variables[0] = bs
    }

    fun retrieveTopicStatus(): BooleanArray {
        val bs = variables[0]
        val out = BooleanArray(bs.binarySetLength)
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
        val sb = StringBuilder(bs.binarySetLength)
        for (i in 0 until bs.binarySetLength) sb.append(if (bs.get(i)) '1' else '0')
        return sb.toString()
    }

    /* Convenience wrappers for legacy code */
    fun getVariableValue(index: Int) = getVariable(index)
    fun setVariableValue(index: Int, value: BinarySet) = setVariable(index, value)

    override fun compareTo(other: BestSubsetSolution): Int {
        return this.getCardinality().compareTo(other.getCardinality())
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is BestSubsetSolution) return false
        return this.getCorrelation() == other.getCorrelation() && this.getCardinality() == other.getCardinality()
    }

    override fun hashCode(): Int = Objects.hash(getCorrelation(), getCardinality())
}

/* Convenience accessors for objective semantics. */
fun BinarySolution.getCardinality(): Double = getObjective(0)
fun BinarySolution.getCorrelation(): Double = getObjective(1)
