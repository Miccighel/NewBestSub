package it.uniud.newbestsub.problem

import it.uniud.newbestsub.dataset.Parameters
import it.uniud.newbestsub.utils.Constants
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.uma.jmetal.solution.BinarySolution
import java.util.*
import kotlin.test.assertEquals

class BitFlipMutationTest {

    @Test
    @DisplayName("Execute")

    fun testExecute() {

        println("[BitFlipMutationTest execute] - Test begins.")

        val testAvgPrec: MutableMap<String, Array<Double>> = LinkedHashMap()
        var testSol: BinarySolution
        val testCorr = { _: Array<Double>, _: Array<Double> -> 0.0 }
        val testTarg = { sol: BinarySolution, _: Double -> sol }
        val testMut = BitFlipMutation(1.0)
        val length = 10

        for (index in 0..length) {
            val random = Random()
            val fakeAvgPrec = Array(length, { (1 + (100 - 1) * random.nextDouble()) / 100 })
            testAvgPrec["Test $index"] = fakeAvgPrec
        }

        val parameters = Parameters("AH99", Constants.CORRELATION_PEARSON, Constants.TARGET_BEST, 100000, 1000, 1000, 0, listOf(50))
        val testProb = BestSubsetProblem(parameters, testAvgPrec.size, testAvgPrec, Array(0, { 0.0 }), Array(50, { "Test" }), testCorr, testTarg)
        testSol = BestSubsetSolution(testProb, testAvgPrec.size)
        val oldStatus = testSol.getVariableValueString(0)
        testSol = testMut.execute(testSol) as BestSubsetSolution
        val newStatus = testSol.getVariableValueString(0)
        println("[BitFlipMutationTest execute] - Testing: <Old. Topic Stat. Val.: $oldStatus, New. Topic Stat. Val.: $newStatus>.")
        assertEquals(false, oldStatus == newStatus, "<Old. Topic Stat. Val.: $oldStatus, New. Topic Stat. Val.: $newStatus>")

        println("[BitFlipMutationTest execute] - Test ends.")
    }

}