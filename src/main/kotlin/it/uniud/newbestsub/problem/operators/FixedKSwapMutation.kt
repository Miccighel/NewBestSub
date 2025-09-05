package it.uniud.newbestsub.problem.operators

import it.uniud.newbestsub.problem.BestSubsetSolution
import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/**
 * Fixed-K swap mutation.
 *
 * Semantics:
 * - Preserves K by swapping one selected bit OUT and one non-selected bit IN.
 * - Repairs degenerates prior to swapping:
 *     • all zeros → set exactly one bit to 1 (K := 1)
 *     • all ones  → set exactly one bit to 0 (K := N-1)
 * - On **repair** this operator sets `lastMutationWasFixedKSwap = true` as your tests expect.
 * - Always lifts a plain DefaultBinarySolution into BestSubsetSolution.
 */
open class FixedKSwapMutation(
    private val configuredMutationProbability: Double
) : MutationOperator<BinarySolution> {

    /* Expose the configured probability for tests */
    override fun mutationProbability(): Double = configuredMutationProbability

    override fun execute(inputSolution: BinarySolution): BinarySolution {
        val random = JMetalRandom.getInstance().randomGenerator

        /* Always operate on a BestSubsetSolution (tests expect lifting). */
        val workingSolution: BestSubsetSolution = liftIfNeeded(inputSolution)

        /* Probability gate: no-op but keep "lifting" effect. */
        if (random.nextDouble() >= configuredMutationProbability) {
            workingSolution.clearLastMutationFlags()
            return workingSolution
        }

        val topicBitset: BinarySet = workingSolution.variables()[0]
        val totalTopicCount: Int = topicBitset.safeLength
        val selectedTopicCount: Int = topicBitset.cardinality()

        /* ------------------------------- Degenerate repairs ------------------------------- */
        if (selectedTopicCount == 0) {
            /* all zeros → flip one random bit to 1 (K=1) */
            val flipIndex = (random.nextDouble() * totalTopicCount).toInt()
            workingSolution.setBitValue(flipIndex, true)

            /* Tests require the swap flag also for repairs */
            workingSolution.lastSwapOutIndex = null
            workingSolution.lastSwapInIndex = flipIndex
            workingSolution.lastMutationWasFixedKSwap = true
            return workingSolution
        }

        if (selectedTopicCount == totalTopicCount) {
            /* all ones → flip one random bit to 0 (K=N-1) */
            val flipIndex = (random.nextDouble() * totalTopicCount).toInt()
            workingSolution.setBitValue(flipIndex, false)

            workingSolution.lastSwapOutIndex = flipIndex
            workingSolution.lastSwapInIndex = null
            workingSolution.lastMutationWasFixedKSwap = true
            return workingSolution
        }

        /* ------------------------------- Proper fixed-K swap ------------------------------- */
        /* Build index pools once (selected vs non-selected). */
        val selectedIndices = IntArray(selectedTopicCount)
        val nonSelectedIndices = IntArray(totalTopicCount - selectedTopicCount)
        var writeSel = 0
        var writeNonSel = 0
        var bitIndex = 0
        while (bitIndex < totalTopicCount) {
            if (topicBitset.get(bitIndex)) selectedIndices[writeSel++] = bitIndex
            else nonSelectedIndices[writeNonSel++] = bitIndex
            bitIndex++
        }

        /* Uniform picks (two flips → preserves K). */
        val outIndex = selectedIndices[(random.nextDouble() * selectedIndices.size).toInt()]
        val inIndex = nonSelectedIndices[(random.nextDouble() * nonSelectedIndices.size).toInt()]

        /* Apply swap via solution API to keep caches coherent. */
        workingSolution.setBitValue(outIndex, false)
        workingSolution.setBitValue(inIndex, true)

        /* Delta-eval hints */
        workingSolution.lastSwapOutIndex = outIndex
        workingSolution.lastSwapInIndex = inIndex
        workingSolution.lastMutationWasFixedKSwap = true

        return workingSolution
    }

    /* ------------------------------- helpers ------------------------------- */

    /** Lift a generic BinarySolution into BestSubsetSolution (deep copy of bits & objectives). */
    private fun liftIfNeeded(source: BinarySolution): BestSubsetSolution {
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

        /* Copy bitset */
        val copy = BinarySet(totalTopicCount).apply { this.or(sourceBits) }
        lifted.variables()[0] = copy

        /* Copy objectives */
        var i = 0
        while (i < source.objectives().size) {
            lifted.objectives()[i] = source.objectives()[i]
            i++
        }

        lifted.clearLastMutationFlags()
        return lifted
    }

    /** Version-agnostic BinarySet length (jMetal 6.x differences). */
    private val BinarySet.safeLength: Int
        get() = try {
            this::class.java.getMethod("getBinarySetLength").invoke(this) as Int
        } catch (_: Exception) {
            this::class.java.getMethod("getNumberOfBits").invoke(this) as Int
        }
}
