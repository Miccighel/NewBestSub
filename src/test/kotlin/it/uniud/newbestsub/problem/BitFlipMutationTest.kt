package it.uniud.newbestsub.problem

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.uma.jmetal.solution.BinarySolution
import java.util.*
import kotlin.test.assertEquals

class BitFlipMutationTest {

    private lateinit var testProb: BestSubsetProblem
    private lateinit var testSol: BinarySolution
    private lateinit var testCorr: (DoubleArray, DoubleArray) -> Double
    private lateinit var testTarg: (BinarySolution, Double) -> BinarySolution
    private var testAvgPrec: MutableMap<String, DoubleArray> = LinkedHashMap()
    private var testMut = BitFlipMutation(1.0)

    @BeforeEach
    @DisplayName("BitFlipMutation - Initialize Tests")

    fun initTest() {
        val length = 10
        for (index in 0..length) {
            val fakeAvgPrec = DoubleArray(length)
            val random = Random()
            for (anotherIndex in 0..fakeAvgPrec.size - 1) {
                fakeAvgPrec[anotherIndex] = (1 + (100 - 1) * random.nextDouble()) / 100
            }
            testAvgPrec["Test $index"] = fakeAvgPrec
        }
        testCorr = { _, _ -> 0.0 }
        testTarg = { sol, _ -> sol }
    }

    @Test
    @DisplayName("Execute")

    fun testExecute() {
        testProb = BestSubsetProblem(testAvgPrec.size, testAvgPrec, DoubleArray(0), testCorr, testTarg)
        testSol = BestSubsetSolution(testProb, testAvgPrec.size)
        val oldStatus = testSol.getVariableValueString(0)
        testSol = testMut.execute(testSol)
        val newStatus = testSol.getVariableValueString(0)
        assertEquals(false, oldStatus == newStatus, "Old topic status values: $oldStatus - New topic status values: $newStatus")
    }

}