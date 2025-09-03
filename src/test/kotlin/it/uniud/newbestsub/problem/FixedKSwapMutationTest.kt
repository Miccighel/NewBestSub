package it.uniud.newbestsub.problem.operators

import it.uniud.newbestsub.problem.BestSubsetSolution
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.solution.binarysolution.impl.DefaultBinarySolution
import org.uma.jmetal.util.binarySet.BinarySet

@DisplayName("FixedKSwapMutation – preserves K, repairs degenerates, and lifts DefaultBinarySolution")
class FixedKSwapMutationTest {

    /* ------------------------------ helpers ------------------------------ */

    private fun makeBestSubset(mask: BooleanArray, objectives: Int = 2): BestSubsetSolution {
        val bss = BestSubsetSolution(
            numberOfVariables = 1,
            numberOfObjectives = objectives,
            numberOfTopics = mask.size,
            topicLabels = Array(mask.size) { i -> "t$i" },
            forcedCardinality = null
        )
        // setBitValue updates internal counters
        for (i in mask.indices) bss.setBitValue(i, mask[i])
        return bss
    }

    private fun hamming(a: BooleanArray, b: BooleanArray): Int {
        require(a.size == b.size)
        var d = 0
        for (i in a.indices) if (a[i] != b[i]) d++
        return d
    }

    private fun toMask(s: BestSubsetSolution): BooleanArray = s.retrieveTopicStatus()

    /* ------------------------------ tests ------------------------------ */

    @Test
    @DisplayName("mutationProbability exposes configured probability")
    fun testMutationProbabilityAccessor() {
        val op = FixedKSwapMutation(0.75)
        assertEquals(0.75, op.mutationProbability(), 1e-12)
    }

    @Test
    @DisplayName("Typical swap: preserves K, flips two bits, flags set (IN and OUT)")
    fun testSwapPreservesKAndSetsFlags() {
        println("[FixedKSwapMutation swap] - Test begins.")

        val initial = booleanArrayOf(true, false, true, false, false, true, false, false) // K=3
        val bss = makeBestSubset(initial)
        val beforeK = bss.numberOfSelectedTopics
        val beforeMask = toMask(bss).clone()

        val op = FixedKSwapMutation(1.0) // force mutation
        val out: BinarySolution = op.execute(bss)
        val mutated = out as BestSubsetSolution
        val afterMask = toMask(mutated)

        val afterK = mutated.numberOfSelectedTopics
        val dist = hamming(beforeMask, afterMask)

        println("[FixedKSwapMutation swap] - Hamming=$dist, beforeK=$beforeK, afterK=$afterK")
        println("[FixedKSwapMutation swap] - lastSwapOutIndex=${mutated.lastSwapOutIndex} lastSwapInIndex=${mutated.lastSwapInIndex} fixedKFlag=${mutated.lastMutationWasFixedKSwap}")

        // K preserved, exactly two flips
        assertEquals(beforeK, afterK, "K must be preserved by a true swap.")
        assertEquals(2, dist, "A true swap must flip exactly two bits.")

        // Flags
        assertTrue(mutated.lastMutationWasFixedKSwap, "Operator must mark lastMutationWasFixedKSwap=true on swap.")
        assertNotNull(mutated.lastSwapOutIndex, "Swap OUT index must be set.")
        assertNotNull(mutated.lastSwapInIndex, "Swap IN index must be set.")

        // Validate flips correspond to flags
        val outIdx = mutated.lastSwapOutIndex!!
        val inIdx = mutated.lastSwapInIndex!!
        assertTrue(beforeMask[outIdx] && !afterMask[outIdx], "OUT index must flip 1 -> 0.")
        assertTrue(!beforeMask[inIdx] && afterMask[inIdx], "IN index must flip 0 -> 1.")

        println("[FixedKSwapMutation swap] - Test ends.")
    }

    @Test
    @DisplayName("Repair all-zeros: K becomes 1, one flip, flags IN only")
    fun testRepairAllZeros() {
        println("[FixedKSwapMutation zeros] - Test begins.")

        val initial = BooleanArray(7) { false } // K=0
        val bss = makeBestSubset(initial)
        val beforeMask = toMask(bss).clone()

        val op = FixedKSwapMutation(1.0)
        val mutated = op.execute(bss) as BestSubsetSolution
        val afterMask = toMask(mutated)

        val dist = hamming(beforeMask, afterMask)
        val afterK = mutated.numberOfSelectedTopics

        println("[FixedKSwapMutation zeros] - Hamming=$dist, afterK=$afterK")
        println("[FixedKSwapMutation zeros] - lastSwapOutIndex=${mutated.lastSwapOutIndex} lastSwapInIndex=${mutated.lastSwapInIndex} fixedKFlag=${mutated.lastMutationWasFixedKSwap}")

        assertEquals(1, afterK, "All-zeros repair must yield K=1.")
        assertEquals(1, dist, "Repair must flip exactly one bit.")

        assertTrue(mutated.lastMutationWasFixedKSwap, "Operator must mark lastMutationWasFixedKSwap=true on repair.")
        assertNull(mutated.lastSwapOutIndex, "All-zeros repair: OUT index should be null.")
        assertNotNull(mutated.lastSwapInIndex, "All-zeros repair: IN index must be set.")

        val inIdx = mutated.lastSwapInIndex!!
        assertTrue(!beforeMask[inIdx] && afterMask[inIdx], "IN index must flip 0 -> 1.")

        println("[FixedKSwapMutation zeros] - Test ends.")
    }

    @Test
    @DisplayName("Repair all-ones: K becomes N-1, one flip, flags OUT only")
    fun testRepairAllOnes() {
        println("[FixedKSwapMutation ones] - Test begins.")

        val initial = BooleanArray(6) { true } // K=N
        val bss = makeBestSubset(initial)
        val beforeMask = toMask(bss).clone()

        val op = FixedKSwapMutation(1.0)
        val mutated = op.execute(bss) as BestSubsetSolution
        val afterMask = toMask(mutated)

        val dist = hamming(beforeMask, afterMask)
        val afterK = mutated.numberOfSelectedTopics

        println("[FixedKSwapMutation ones] - Hamming=$dist, afterK=$afterK (N-1 expected)")
        println("[FixedKSwapMutation ones] - lastSwapOutIndex=${mutated.lastSwapOutIndex} lastSwapInIndex=${mutated.lastSwapInIndex} fixedKFlag=${mutated.lastMutationWasFixedKSwap}")

        assertEquals(initial.size - 1, afterK, "All-ones repair must yield K=N-1.")
        assertEquals(1, dist, "Repair must flip exactly one bit.")

        assertTrue(mutated.lastMutationWasFixedKSwap, "Operator must mark lastMutationWasFixedKSwap=true on repair.")
        assertNotNull(mutated.lastSwapOutIndex, "All-ones repair: OUT index must be set.")
        assertNull(mutated.lastSwapInIndex, "All-ones repair: IN index should be null.")

        val outIdx = mutated.lastSwapOutIndex!!
        assertTrue(beforeMask[outIdx] && !afterMask[outIdx], "OUT index must flip 1 -> 0.")

        println("[FixedKSwapMutation ones] - Test ends.")
    }

    @Test
    @DisplayName("Lifts DefaultBinarySolution to BestSubsetSolution and performs a swap")
    fun testLiftsDefaultBinarySolution() {
        println("[FixedKSwapMutation lifting] - Test begins.")

        val n = 6
        // Build a DefaultBinarySolution with one variable of length n and two objectives
        val bitsPerVar = listOf(n)
        val def = DefaultBinarySolution(bitsPerVar, 2)
        val bs = BinarySet(n).apply {
            // mask = 110000 (K=2)
            set(0, true); set(1, true)
        }
        def.variables()[0] = bs
        def.objectives()[0] = 10.0
        def.objectives()[1] = -5.0

        val op = FixedKSwapMutation(1.0)
        val out = op.execute(def) // should be a lifted BestSubsetSolution
        assertTrue(out is BestSubsetSolution, "Operator must lift DefaultBinarySolution to BestSubsetSolution.")

        val bss = out as BestSubsetSolution
        val mask = toMask(bss)
        val k = bss.numberOfSelectedTopics

        println("[FixedKSwapMutation lifting] - after type=${bss.javaClass.simpleName}, K=$k")
        println("[FixedKSwapMutation lifting] - lastSwapOutIndex=${bss.lastSwapOutIndex} lastSwapInIndex=${bss.lastSwapInIndex} fixedKFlag=${bss.lastMutationWasFixedKSwap}")

        // Since pools exist on both sides, this must be a real swap (K preserved)
        assertEquals(2, k, "Swap must preserve K when both pools are non-empty.")
        assertTrue(bss.lastMutationWasFixedKSwap, "Must mark swap/repair flag.")
        assertNotNull(bss.lastSwapOutIndex)
        assertNotNull(bss.lastSwapInIndex)

        println("[FixedKSwapMutation lifting] - Test ends.")
    }
}
