package it.uniud.newbestsub.problem.operators

import it.uniud.newbestsub.problem.BestSubsetSolution
import it.uniud.newbestsub.utils.Constants
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom

class ScoreBiasedFixedKSwapMutationTest {

    @BeforeEach
    fun setUp() {
        /* Make RNG deterministic for repeatable tests */
        JMetalRandom.getInstance().setSeed(42L)
    }

    /* Helper: build a solution with given selected indices */
    private fun makeSolution(topicCount: Int, selectedIdx: IntArray): BestSubsetSolution {
        val labels = Array(topicCount) { i -> "T$i" }
        val sol = BestSubsetSolution(1, 2, topicCount, labels, forcedCardinality = null)
        val bits = sol.variables()[0] as BinarySet
        /* Clear all, then set selected */
        var i = 0
        while (i < topicCount) { bits.set(i, false); i++ }
        selectedIdx.forEach { idx -> bits.set(idx, true) }
        /* Clear any previous hints just in case */
        sol.lastMutationWasFixedKSwap = false
        sol.lastSwapOutIndex = null
        sol.lastSwapInIndex = null
        return sol
    }

    @Test
    fun `preserves cardinality and sets hints`() {
        val n = 10
        val initialSelected = intArrayOf(1,3,5,7) /* K=4 */
        val s = makeSolution(n, initialSelected)

        val mut = ScoreBiasedFixedKSwapMutation(
            mutationProbability = 1.0,
            topicScores = null,                  /* unbiased random swap */
            target = Constants.TARGET_BEST,
            biasEpsilon = 1.0                    /* force random */
        )

        val out = mut.execute(s) as BestSubsetSolution
        val bits = out.variables()[0] as BinarySet
        val newK = (0 until n).count { bits.get(it) }

        assertEquals(initialSelected.size, newK, "Mutation must preserve K")
        assertTrue(out.lastMutationWasFixedKSwap, "Swap hint flag must be set")
        assertNotNull(out.lastSwapOutIndex, "Swap OUT index must be set")
        assertNotNull(out.lastSwapInIndex, "Swap IN index must be set")

        /* The swapped positions must reflect the bit changes */
        val outIdx = out.lastSwapOutIndex!!
        val inIdx = out.lastSwapInIndex!!
        assertFalse(bits.get(outIdx), "OUT index must be turned OFF")
        assertTrue(bits.get(inIdx), "IN index must be turned ON")
    }

    @Test
    fun `BEST bias drops lowest score from selected and adds highest score from not selected`() {
        val n = 8
        /* Selected = {1,3,6}, NotSelected = {0,2,4,5,7} */
        val s = makeSolution(n, intArrayOf(1,3,6))

        /* Unique scores so argmin/argmax are deterministic:
           - Among selected: lowest at 3
           - Among not-selected: highest at 7
        */
        val scores = doubleArrayOf(
            0.10, /* 0 */
            0.80, /* 1 (selected) */
            0.20, /* 2 */
            0.05, /* 3 (selected, lowest among selected -> should go OUT) */
            0.30, /* 4 */
            0.25, /* 5 */
            0.60, /* 6 (selected) */
            0.95  /* 7 (not selected, highest -> should come IN) */
        )

        val mut = ScoreBiasedFixedKSwapMutation(
            mutationProbability = 1.0,
            topicScores = scores,
            target = Constants.TARGET_BEST,
            biasEpsilon = 0.0 /* fully greedy/bias */
        )

        val out = mut.execute(s) as BestSubsetSolution
        val bits = out.variables()[0] as BinarySet

        assertEquals(3, (0 until n).count { bits.get(it) }, "K must be preserved")

        /* Check chosen indices by hints */
        assertTrue(out.lastMutationWasFixedKSwap)
        assertEquals(3, out.lastSwapOutIndex, "BEST should drop the lowest-scored selected")
        assertEquals(7, out.lastSwapInIndex,  "BEST should add the highest-scored not-selected")

        /* Sanity on the bitset state */
        assertFalse(bits.get(3))
        assertTrue(bits.get(7))
    }

    @Test
    fun `no mutation when probability is zero`() {
        val n = 6
        val initialSel = intArrayOf(0,2,4)
        val s = makeSolution(n, initialSel)
        val before = (s.variables()[0] as BinarySet).clone() as BinarySet

        val mut = ScoreBiasedFixedKSwapMutation(
            mutationProbability = 0.0,
            topicScores = null,
            target = Constants.TARGET_WORST,
            biasEpsilon = 1.0
        )

        val out = mut.execute(s) as BestSubsetSolution
        val after = (out.variables()[0] as BinarySet)

        /* No changes and no hints */
        var identical = true
        for (i in 0 until n) identical = identical && (before.get(i) == after.get(i))
        assertTrue(identical, "Bitset must be unchanged if p=0")
        assertFalse(out.lastMutationWasFixedKSwap, "No hints should be set when no mutation happens")
        assertNull(out.lastSwapOutIndex)
        assertNull(out.lastSwapInIndex)
    }

    @Test
    fun `degenerate masks do nothing`() {
        /* All zeros: nothing to swap in the selected set */
        val n = 5
        val allZero = makeSolution(n, intArrayOf())
        val mut1 = ScoreBiasedFixedKSwapMutation(1.0, null, Constants.TARGET_BEST, 1.0)
        val out1 = mut1.execute(allZero) as BestSubsetSolution
        assertFalse(out1.lastMutationWasFixedKSwap)
        assertEquals(0, (0 until n).count { (out1.variables()[0] as BinarySet).get(it) })

        /* All ones: nothing to swap in the not-selected set */
        val allOne = makeSolution(n, intArrayOf(0,1,2,3,4))
        val mut2 = ScoreBiasedFixedKSwapMutation(1.0, null, Constants.TARGET_WORST, 1.0)
        val out2 = mut2.execute(allOne) as BestSubsetSolution
        assertFalse(out2.lastMutationWasFixedKSwap)
        assertEquals(n, (0 until n).count { (out2.variables()[0] as BinarySet).get(it) })
    }
}
