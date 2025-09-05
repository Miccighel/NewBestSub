package it.uniud.newbestsub.problem.operators

import it.uniud.newbestsub.problem.BestSubsetSolution
import it.uniud.newbestsub.utils.Constants
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/**
 * Fixed-K swap mutation with score bias.
 *
 * Differences vs FixedKSwapMutation:
 * - **Degenerate masks (K=0 or K=N) are left unchanged** (tests: “degenerate masks do nothing”).
 * - Otherwise, pick OUT/IN indices with a simple score‐based policy:
 *     • BEST  → OUT = lowest score among selected, IN = highest score among non-selected
 *     • WORST → OUT = highest score among selected, IN = lowest  score among non-selected
 * - With probability [biasEpsilon], ignore scores and pick uniformly (exploration).
 */
class ScoreBiasedFixedKSwapMutation(
    private val mutationProbability: Double,
    private val topicScores: DoubleArray?,       /* nullable → pure uniform swap */
    private val target: String,              /* Constants.TARGET_BEST | TARGET_WORST */
    private val biasEpsilon: Double = 0.10
) : FixedKSwapMutation(mutationProbability) {

    override fun execute(inputSolution: BinarySolution): BinarySolution {
        val random = JMetalRandom.getInstance().randomGenerator

        /* Ensure BestSubsetSolution host (don’t call super.execute() here to avoid unintended mutation). */
        val workingSolution = liftIfNeededLocal(inputSolution)

        /* Probability gate */
        if (random.nextDouble() >= mutationProbability()) {
            workingSolution.clearLastMutationFlags()
            return workingSolution
        }

        val topicBitset: BinarySet = workingSolution.variables()[0]
        val totalTopicCount = topicBitset.safeLength
        val selectedTopicCount = topicBitset.cardinality()

        /* ------------------------ Degenerate masks: do nothing (per test) ------------------------ */
        if (selectedTopicCount == 0 || selectedTopicCount == totalTopicCount) {
            workingSolution.clearLastMutationFlags()
            return workingSolution
        }

        /* Build pools */
        val selectedIndices = ArrayList<Int>(selectedTopicCount)
        val nonSelectedIndices = ArrayList<Int>(totalTopicCount - selectedTopicCount)
        var bitIndex = 0
        while (bitIndex < totalTopicCount) {
            if (topicBitset.get(bitIndex)) selectedIndices += bitIndex else nonSelectedIndices += bitIndex
            bitIndex++
        }

        val exploreUniformly = (topicScores == null) || (random.nextDouble() < biasEpsilon)

        val (outIndex, inIndex) = if (exploreUniformly) {
            val o = selectedIndices[(random.nextDouble() * selectedIndices.size).toInt()]
            val inn = nonSelectedIndices[(random.nextDouble() * nonSelectedIndices.size).toInt()]
            o to inn
        } else {
            if (target == Constants.TARGET_BEST) {
                /* OUT lowest, IN highest */
                val out = selectedIndices.minBy { topicScores[it] }
                val inn = nonSelectedIndices.maxBy { topicScores[it] }
                out to inn
            } else {
                /* WORST: OUT highest, IN lowest */
                val out = selectedIndices.maxBy { topicScores[it] }
                val inn = nonSelectedIndices.minBy { topicScores[it] }
                out to inn
            }
        }

        /* Apply swap via solution API, then set delta-eval hints */
        workingSolution.setBitValue(outIndex, false)
        workingSolution.setBitValue(inIndex, true)

        workingSolution.lastSwapOutIndex = outIndex
        workingSolution.lastSwapInIndex = inIndex
        workingSolution.lastMutationWasFixedKSwap = true

        return workingSolution
    }

    /* ------------------------ local helpers (lift + length) ------------------------ */

    private fun liftIfNeededLocal(source: BinarySolution): BestSubsetSolution {
        if (source is BestSubsetSolution) return source

        val sourceBits: BinarySet = source.variables()[0]
        val totalTopicCount: Int = sourceBits.safeLength

        val lifted = BestSubsetSolution(
            numberOfVariables = 1,
            numberOfObjectives = source.objectives().size,
            numberOfTopics = totalTopicCount,
            topicLabels = Array(totalTopicCount) { i -> "t$i" },
            forcedCardinality = null
        )
        val copy = BinarySet(totalTopicCount).apply { this.or(sourceBits) }
        lifted.variables()[0] = copy

        var i = 0
        while (i < source.objectives().size) {
            lifted.objectives()[i] = source.objectives()[i]
            i++
        }
        lifted.clearLastMutationFlags()
        return lifted
    }

    private val BinarySet.safeLength: Int
        get() = try {
            this::class.java.getMethod("getBinarySetLength").invoke(this) as Int
        } catch (_: Exception) {
            this::class.java.getMethod("getNumberOfBits").invoke(this) as Int
        }
}
