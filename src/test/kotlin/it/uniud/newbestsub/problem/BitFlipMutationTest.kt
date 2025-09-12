package it.uniud.newbestsub.problem

import it.uniud.newbestsub.problem.operators.BitFlipMutation
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Tests for the [BitFlipMutation] operator.
 *
 * Covered behavior:
 * - Mutation flips exactly one bit in the solution (Hamming distance = 1).
 * - The number of selected topics remains ≥ 1 after mutation (safety constraint).
 * - Cardinality changes by exactly ±1 relative to the pre-mutation state.
 * - Underlying bitset capacity is preserved and remains equal to `numberOfTopics`.
 *
 * The operator is tested with probability = 1.0 to guarantee that one mutation occurs.
 */
@DisplayName("BitFlipMutation – single-bit flip semantics")
class BitFlipMutationTest {

    /**
     * Ensures that executing [BitFlipMutation]:
     * 1. flips exactly one bit in the binary mask,
     * 2. preserves the invariant that at least one topic remains selected,
     * 3. changes the number of selected topics by exactly ±1,
     * 4. leaves the bitset capacity unchanged.
     *
     * Setup:
     * - Construct a [BestSubsetSolution] with 10 topics.
     * - Initialize with a known mask where two topics are selected.
     * - Apply mutation with probability = 1.0 (guaranteed single flip).
     */
    @Test
    @DisplayName("BitFlipMutation flips exactly one bit and keeps ≥1 topic selected")
    fun testExecute() {
        println("[BitFlipMutationTest execute] - Test begins.")

        /* Arrange */
        val numberOfTopics = 10
        val topicLabels = Array(numberOfTopics) { i -> "t$i" }

        val solution = BestSubsetSolution(
            numberOfVariables = 1,
            numberOfObjectives = 2,
            numberOfTopics = numberOfTopics,
            topicLabels = topicLabels,
            forcedCardinality = null
        )

        /* Ensure the internal bitset has the right length (avoid length=1 defaults). */
        val emptyGenes = Array(numberOfTopics) { false }
        solution.variables()[0] = solution.createNewBitSet(numberOfTopics, emptyGenes)

        /* Start from a known mask with at least two 1s so we cannot drop to zero selections. */
        solution.setBitValue(0, true)
        solution.setBitValue(1, true)

        val mutation = BitFlipMutation(probability = 1.0)  /* force exactly one flip */

        val beforeMask = solution.retrieveTopicStatus()
        val beforeSelectedCount = solution.numberOfSelectedTopics

        /* Act */
        mutation.execute(solution)

        val afterMask = solution.retrieveTopicStatus()
        val afterSelectedCount = solution.numberOfSelectedTopics

        /* Assert */
        val hammingDistance = beforeMask.indices.count { idx -> beforeMask[idx] != afterMask[idx] }

        println(
            "[BitFlipMutationTest execute] - Hamming: $hammingDistance, " +
                "beforeSelected=$beforeSelectedCount, afterSelected=$afterSelectedCount"
        )

        assertEquals(1, hammingDistance, "Mutation should flip exactly one bit (Hamming=1)")
        assertTrue(afterSelectedCount >= 1, "At least one topic must remain selected")
        assertTrue(
            afterSelectedCount == beforeSelectedCount + 1 || afterSelectedCount == beforeSelectedCount - 1,
            "Cardinality should change by exactly ±1"
        )

        val capacitySum = solution.numberOfBitsPerVariable().sum()
        assertEquals(numberOfTopics, capacitySum, "Bitset capacity must remain equal to numberOfTopics")

        println("[BitFlipMutationTest execute] - Test ends.")
    }
}
