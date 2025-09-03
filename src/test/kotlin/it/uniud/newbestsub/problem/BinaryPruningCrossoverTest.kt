package it.uniud.newbestsub.problem.operators

import it.uniud.newbestsub.problem.BestSubsetSolution
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom

@DisplayName("BinaryPruningCrossover – AND/OR semantics and lifting")
class BinaryPruningCrossoverTest {

    /** Build a BinarySet from a BooleanArray using the LEFT→RIGHT convention (i = 0..N-1). */
    private fun binarySetFrom(mask: BooleanArray): BinarySet {
        val bs = BinarySet(mask.size)
        var i = 0
        while (i < mask.size) {
            if (mask[i]) bs.set(i); i++
        }
        return bs
    }

    /** Pretty string from BooleanArray using the same LEFT→RIGHT convention. */
    private fun maskToString(mask: BooleanArray): String =
        buildString(mask.size) { mask.forEach { append(if (it) '1' else '0') } }

    private fun newSolution(mask: BooleanArray): BestSubsetSolution =
        BestSubsetSolution(
            numberOfVariables = 1,
            numberOfObjectives = 2,
            numberOfTopics = mask.size,
            topicLabels = Array(mask.size) { i -> "t$i" },
            forcedCardinality = null
        ).apply {
            variables()[0] = binarySetFrom(mask)
            // objective slot 1 is correlation in this project, values don’t matter for crossover test
            objectives()[0] = 0.0
            objectives()[1] = 0.0
        }

    @Test
    @DisplayName("prob=1.0 → children are AND and OR (no repair needed)")
    fun testAndOrChildren() {
        // Fix RNG so any random repairs would be deterministic (not needed here, but harmless)
        JMetalRandom.getInstance().setSeed(12345L)

        // Parents (LEFT→RIGHT). A: 01010100 (1,3,5), B: 00011000 (3,4)
        val a = booleanArrayOf(false, true, false, true, false, true, false, false)
        val b = booleanArrayOf(false, false, false, true, true, false, false, false)

        val p1 = newSolution(a)
        val p2 = newSolution(b)

        val op = BinaryPruningCrossover(probability = 1.0)
        val children = op.execute(mutableListOf(p1, p2))

        val cAnd = (children[0] as BestSubsetSolution).retrieveTopicStatus()
        val cOr = (children[1] as BestSubsetSolution).retrieveTopicStatus()

        // Expected (elementwise):
        // AND = 00010000 ; OR = 01011100
        val expAnd = BooleanArray(a.size) { i -> a[i] && b[i] }
        val expOr = BooleanArray(a.size) { i -> a[i] || b[i] }

        println("[BinaryPruningCrossoverTest] - ParentA: ${maskToString(a)}")
        println("[BinaryPruningCrossoverTest] - ParentB: ${maskToString(b)}")
        println("[BinaryPruningCrossoverTest] - ChildAND: ${maskToString(cAnd)} (exp=${maskToString(expAnd)})")
        println("[BinaryPruningCrossoverTest] - ChildOR : ${maskToString(cOr)}  (exp=${maskToString(expOr)})")

        assertArrayEquals(expAnd, cAnd, "AND child mask mismatch")
        assertArrayEquals(expOr, cOr, "OR child mask mismatch")
    }

    @Test
    @DisplayName("prob=0.0 → no crossover: children are normalized clones (lifting OK)")
    fun testNoCrossoverAndLifting() {
        JMetalRandom.getInstance().setSeed(7L)

        // Build parents via DefaultBinarySolution path if you want to exercise lifting,
        // but constructing BestSubsetSolution directly is fine for this test too.
        val a = booleanArrayOf(true, false, true, false, false, false)
        val b = booleanArrayOf(false, true, false, true, false, false)

        val p1 = newSolution(a)
        val p2 = newSolution(b)

        val op = BinaryPruningCrossover(probability = 0.0)
        val children = op.execute(mutableListOf(p1, p2))

        val c1 = (children[0] as BestSubsetSolution).retrieveTopicStatus()
        val c2 = (children[1] as BestSubsetSolution).retrieveTopicStatus()

        println("[BinaryPruningCrossoverTest] - ParentA: ${maskToString(a)}")
        println("[BinaryPruningCrossoverTest] - ParentB: ${maskToString(b)}")
        println("[BinaryPruningCrossoverTest] - Child1 : ${maskToString(c1)} (clone of A)")
        println("[BinaryPruningCrossoverTest] - Child2 : ${maskToString(c2)} (clone of B)")

        assertArrayEquals(a, c1, "Child1 should be a clone of ParentA when prob=0.0")
        assertArrayEquals(b, c2, "Child2 should be a clone of ParentB when prob=0.0")
    }
}
