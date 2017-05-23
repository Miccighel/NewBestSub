package it.uniud.newbestsub.problem

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

        val testAvgPrec: MutableMap<String, DoubleArray> = LinkedHashMap()
        var testSol: BinarySolution
        val testCorr = { _: DoubleArray, _: DoubleArray -> 0.0 }
        val testTarg = { sol: BinarySolution, _: Double -> sol }
        val testMut = BitFlipMutation(1.0)
        val length = 10

        for (index in 0..length) {
            val fakeAvgPrec = DoubleArray(length)
            val random = Random()
            for (anotherIndex in 0..fakeAvgPrec.size - 1) {
                fakeAvgPrec[anotherIndex] = (1 + (100 - 1) * random.nextDouble()) / 100
            }
            testAvgPrec["Test $index"] = fakeAvgPrec
        }

        val testProb = BestSubsetProblem(testAvgPrec.size, testAvgPrec, DoubleArray(0), testCorr, testTarg)
        testSol = BestSubsetSolution(testProb, testAvgPrec.size)
        val oldStatus = testSol.getVariableValueString(0)
        testSol = testMut.execute(testSol) as BestSubsetSolution
        val newStatus = testSol.getVariableValueString(0)
        println("[BitFlipMutationTest execute] - Testing: <Old. Topic Stat. Val.: $oldStatus, New. Topic Stat. Val.: $newStatus>.")
        assertEquals(false, oldStatus == newStatus, "<Old. Topic Stat. Val.: $oldStatus, New. Topic Stat. Val.: $newStatus>")

        println("[BitFlipMutationTest execute] - Test ends.")
    }

}