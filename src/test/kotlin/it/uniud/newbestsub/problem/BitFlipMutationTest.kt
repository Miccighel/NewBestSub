package it.uniud.newbestsub.problem

import it.uniud.newbestsub.problem.operators.BitFlipMutation
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@DisplayName("BitFlipMutation – single-bit flip semantics")
class BitFlipMutationTest {

    @Test
    @DisplayName("BitFlipMutation flips exactly one bit and keeps ≥1 topic selected")
    fun testExecute() {
        println("[BitFlipMutationTest execute] - Test begins.")

        /* ----------------------------------------------------------------------------------------------------------------
         * Arrange
         * ---------------------------------------------------------------------------------------------------------------- */
        val numberOfTopics = 10
        val topicLabels = Array(numberOfTopics) { i -> "t$i" }

        val solution = BestSubsetSolution(
            numberOfVariables = 1,
            numberOfObjectives = 2,
            numberOfTopics = numberOfTopics,
            topicLabels = topicLabels,
            forcedCardinality = null
        )

        /* Ensure the internal bitset has the right length (avoid length=1 defaults) */
        val emptyGenes = Array(numberOfTopics) { false }
        solution.variables()[0] = solution.createNewBitSet(numberOfTopics, emptyGenes)

        /* Start from a known mask with at least two 1s so we can't drop to zero selections */
        solution.setBitValue(0, true)
        solution.setBitValue(1, true)

        val mutation = BitFlipMutation(probability = 1.0)  // force exactly one flip

        val beforeMask = solution.retrieveTopicStatus()
        val beforeSelectedCount = solution.numberOfSelectedTopics

        /* ----------------------------------------------------------------------------------------------------------------
         * Act
         * ---------------------------------------------------------------------------------------------------------------- */
        mutation.execute(solution)

        val afterMask = solution.retrieveTopicStatus()
        val afterSelectedCount = solution.numberOfSelectedTopics

        /* ----------------------------------------------------------------------------------------------------------------
         * Assert
         * ---------------------------------------------------------------------------------------------------------------- */

        // Hamming distance between before/after should be exactly 1
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

        // Capacity must remain unchanged
        val capacitySum = solution.numberOfBitsPerVariable().sum()
        assertEquals(numberOfTopics, capacitySum, "Bitset capacity must remain equal to numberOfTopics")

        println("[BitFlipMutationTest execute] - Test ends.")
    }
}
