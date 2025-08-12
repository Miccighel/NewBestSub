package it.uniud.newbestsub.problem

import it.uniud.newbestsub.dataset.Parameters
import it.uniud.newbestsub.utils.Constants
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.uma.jmetal.solution.binarysolution.BinarySolution
import java.util.*
import kotlin.test.assertEquals

class BitFlipMutationTest {

    @Test
    @DisplayName("Execute")
    fun testExecute() {

        println("[BitFlipMutationTest execute] - Test begins.")

        val testAvgPrec: MutableMap<String, Array<Double>> = LinkedHashMap()
        val testCorr = { _: Array<Double>, _: Array<Double> -> 0.0 }
        val testTarg = { sol: BinarySolution, _: Double -> sol }
        val testMut = BitFlipMutation(1.0)

        /* Consistent sizes */
        val numTopics = 10
        val numSystems = numTopics + 1

        for (index in 0 until numSystems) {
            val random = Random()
            val fakeAvgPrec = Array(numTopics) { (1 + (100 - 1) * random.nextDouble()) / 100 }
            testAvgPrec["Test $index"] = fakeAvgPrec
        }

        val parameters = Parameters(
            "AH99",
            Constants.CORRELATION_PEARSON,
            Constants.TARGET_BEST,
            10000, 1000, 1000, 0,
            listOf(50)
        )

        val meanAP = Array(numSystems) { 0.0 }
        val topicLabels = Array(numTopics) { "Test" }

        val testProb = BestSubsetProblem(
            parameters,
            numTopics,
            testAvgPrec,
            meanAP,
            topicLabels,
            testCorr,
            testTarg
        )

        /* New constructor (no problem passed) */
        var testSol = BestSubsetSolution(
            numberOfVariables = 1,
            numberOfObjectives = 2,
            numberOfTopics = numTopics,
            topicLabels = topicLabels,
            forcedCardinality = null
        )

        val oldStatus = testSol.getVariableValueString(0)
        testSol = testMut.execute(testSol) as BestSubsetSolution
        val newStatus = testSol.getVariableValueString(0)
        println("[BitFlipMutationTest execute] - Testing: <Old. Topic Stat. Val.: $oldStatus, New. Topic Stat. Val.: $newStatus>.")
        assertEquals(false, oldStatus == newStatus, "<Old. Topic Stat. Val.: $oldStatus, New. Topic Stat. Val.: $newStatus>")

        println("[BitFlipMutationTest execute] - Test ends.")
    }
}
