package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.problem.getCardinality
import it.uniud.newbestsub.utils.Constants
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class DatasetModelTest {

    @Test
    @DisplayName("Solve")
    fun testSolve() {
        println("[DatasetModelTest solve] - Test begins.")

        // Run the AVERAGE path: it deterministically emits exactly one solution per K = 1..N.
        val controller = DatasetController(Constants.TARGET_AVERAGE)
        controller.load("src/test/resources/AP96.csv")

        val params = Parameters(
            datasetName = "AH99",
            correlationMethod = Constants.CORRELATION_KENDALL,
            targetToAchieve = Constants.TARGET_AVERAGE,
            numberOfIterations = 100000,
            numberOfRepetitions = 1000,
            populationSize = 1000,
            currentExecution = 0,
            percentiles = listOf(1, 5, 25, 99)
        )

        val solutions = controller.models[0].solve(params).first
        val computedKs = IntArray(solutions.size)
        solutions.forEachIndexed { idx, s -> computedKs[idx] = s.getCardinality().toInt() }

        val nTopics = controller.models[0].numberOfTopics
        val expectedKs = IntArray(nTopics) { it + 1 }

        for (j in 0 until nTopics) {
            println("[DatasetModelTest solve] - Testing: <Expected Card. Val.: ${expectedKs[j]}, Computed Card. Val.: ${computedKs[j]}>")
            assertEquals(expectedKs[j], computedKs[j])
        }

        println("[DatasetModelTest solve] - Test ends.")
    }
}
