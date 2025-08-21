package it.uniud.newbestsub.problem

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BitFlipMutationTest {

    @Test
    @DisplayName("BitFlipMutation flips exactly one bit and keeps ≥1 topic selected")
    fun testExecute() {
        println("[BitFlipMutationTest execute] - Test begins.")

        val numTopics = 10
        val labels = Array(numTopics) { "t$it" }

        val sol = BestSubsetSolution(
            numberOfVariables = 1,
            numberOfObjectives = 2,
            numberOfTopics = numTopics,
            topicLabels = labels,
            forcedCardinality = null
        )

        // Ensure the internal bitset has the right length (avoid length=1 default)
        val emptyGenes = Array(numTopics) { false }
        sol.variables()[0] = sol.createNewBitSet(numTopics, emptyGenes)

        // Start from a known mask with at least two 1s
        sol.setBitValue(0, true)
        sol.setBitValue(1, true)

        val mutation = BitFlipMutation(probability = 1.0)

        val before = sol.retrieveTopicStatus()
        val beforeSelected = sol.numberOfSelectedTopics

        mutation.execute(sol)

        val after = sol.retrieveTopicStatus()
        val afterSelected = sol.numberOfSelectedTopics

        // Hamming distance between before/after should be exactly 1
        val hamming = before.indices.count { before[it] != after[it] }

        println(
            "[BitFlipMutationTest execute] - Hamming: $hamming, " +
                "beforeSelected=$beforeSelected, afterSelected=$afterSelected"
        )

        assertEquals(1, hamming, "Mutation should flip exactly one bit (Hamming=1)")
        assertTrue(afterSelected >= 1, "At least one topic must remain selected")
        assertTrue(
            afterSelected == beforeSelected + 1 || afterSelected == beforeSelected - 1,
            "Cardinality should change by exactly ±1"
        )

        println("[BitFlipMutationTest execute] - Test ends.")
    }
}
