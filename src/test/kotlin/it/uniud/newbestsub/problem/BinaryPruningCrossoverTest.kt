package it.uniud.newbestsub.problem.operators

import it.uniud.newbestsub.problem.BestSubsetSolution
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/**
 * BinaryPruningCrossover – uniform mixing + repair
 *
 * What we assert:
 * - With `prob = 1.0`, per-bit uniform mixing holds: for each index `i`, the multiset
 *   `{child1[i], child2[i]}` equals `{parentA[i], parentB[i]}` (order-agnostic).
 * - Children are repaired to be non-empty (at least one `true`).
 * - With `prob = 0.0`, there is no crossover: children are clones of the parents.
 *
 * Conventions:
 * - Masks are depicted and constructed LEFT→RIGHT (`i = 0 .. N-1`).
 * - Randomness is pinned through `JMetalRandom` to keep tests deterministic.
 */
@DisplayName("BinaryPruningCrossover – uniform mixing + repair")
class BinaryPruningCrossoverTest {

    /**
     * Build a [BinarySet] from a [BooleanArray] using the LEFT→RIGHT convention (i = 0..N-1).
     */
    private fun binarySetFrom(mask: BooleanArray): BinarySet {
        val bitset = BinarySet(mask.size)
        var i = 0
        while (i < mask.size) {
            if (mask[i]) bitset.set(i)
            i++
        }
        return bitset
    }

    /**
     * Pretty string from [BooleanArray] using the same LEFT→RIGHT convention.
     * Useful for debugging printouts.
     */
    private fun maskToString(mask: BooleanArray): String =
        buildString(mask.size) { mask.forEach { append(if (it) '1' else '0') } }

    /**
     * Construct a fresh [BestSubsetSolution] with the given mask and neutral objective values.
     * The correlation objective slot (index 1 in this project) is set to 0.0; values do not
     * affect crossover behavior and are present only for structural completeness.
     */
    private fun newSolution(mask: BooleanArray): BestSubsetSolution =
        BestSubsetSolution(
            numberOfVariables = 1,
            numberOfObjectives = 2,
            numberOfTopics = mask.size,
            topicLabels = Array(mask.size) { i -> "t$i" },
            forcedCardinality = null
        ).apply {
            variables()[0] = binarySetFrom(mask)
            /* Objective slot 0 and 1 are irrelevant for crossover tests. */
            objectives()[0] = 0.0
            objectives()[1] = 0.0
        }

    @Test
    @DisplayName("prob=1.0 → per-bit uniform mixing; each child bit equals one of the parents; non-empty repair holds")
    fun testUniformMixingPerBitAndRepair() {
        /* Fix RNG so any random branches are deterministic. */
        JMetalRandom.getInstance().setSeed(12345L)

        /* Parents (LEFT→RIGHT). */
        val parentMaskA = booleanArrayOf(false, true, false, true, false, true, false, false)
        val parentMaskB = booleanArrayOf(false, false, false, true, true, false, false, false)

        val parentA = newSolution(parentMaskA)
        val parentB = newSolution(parentMaskB)

        val op = BinaryPruningCrossover(crossoverProbability = 1.0)
        val children = op.execute(mutableListOf(parentA, parentB))

        val childMask1 = (children[0] as BestSubsetSolution).retrieveTopicStatus()
        val childMask2 = (children[1] as BestSubsetSolution).retrieveTopicStatus()

        /*
         * Property (uniform mixing): for each bit i, the multiset {c1[i], c2[i]} == {A[i], B[i]}.
         * Equivalently: (c1==A && c2==B) || (c1==B && c2==A).
         */
        var i = 0
        while (i < parentMaskA.size) {
            val a = parentMaskA[i]
            val b = parentMaskB[i]
            val c1 = childMask1[i]
            val c2 = childMask2[i]
            val matches = (c1 == a && c2 == b) || (c1 == b && c2 == a)
            assertTrue(matches, "Bit $i mismatch: A=$a, B=$b, got c1=$c1, c2=$c2")
            i++
        }

        /* Repair invariant: resulting masks must be non-empty. */
        assertTrue(childMask1.any { it }, "Child1 should be non-empty")
        assertTrue(childMask2.any { it }, "Child2 should be non-empty")

        println("[BinaryPruningCrossoverTest] A : ${maskToString(parentMaskA)}")
        println("[BinaryPruningCrossoverTest] B : ${maskToString(parentMaskB)}")
        println("[BinaryPruningCrossoverTest] C1: ${maskToString(childMask1)}")
        println("[BinaryPruningCrossoverTest] C2: ${maskToString(childMask2)}")
    }

    @Test
    @DisplayName("prob=0.0 → no crossover: children are clones")
    fun testNoCrossoverReturnsClones() {
        /* Still set a seed to keep any potential randomness deterministic. */
        JMetalRandom.getInstance().setSeed(7L)

        val parentMaskA = booleanArrayOf(true, false, true, false, false, false)
        val parentMaskB = booleanArrayOf(false, true, false, true, false, false)

        val parentA = newSolution(parentMaskA)
        val parentB = newSolution(parentMaskB)

        val op = BinaryPruningCrossover(crossoverProbability = 0.0)
        val children = op.execute(mutableListOf(parentA, parentB))

        val childMask1 = (children[0] as BestSubsetSolution).retrieveTopicStatus()
        val childMask2 = (children[1] as BestSubsetSolution).retrieveTopicStatus()

        println("[BinaryPruningCrossoverTest] A : ${maskToString(parentMaskA)}")
        println("[BinaryPruningCrossoverTest] B : ${maskToString(parentMaskB)}")
        println("[BinaryPruningCrossoverTest] C1: ${maskToString(childMask1)} (clone of A)")
        println("[BinaryPruningCrossoverTest] C2: ${maskToString(childMask2)} (clone of B)")

        /* With prob=0.0, the operator should return exact clones. */
        assertArrayEquals(parentMaskA, childMask1, "Child1 should be a clone of ParentA when prob=0.0")
        assertArrayEquals(parentMaskB, childMask2, "Child2 should be a clone of ParentB when prob=0.0")
    }
}
