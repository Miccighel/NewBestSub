package it.uniud.newbestsub.problem

import it.uniud.newbestsub.dataset.Parameters
import it.uniud.newbestsub.utils.Constants
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.uma.jmetal.solution.BinarySolution
import java.util.*
import kotlin.test.assertEquals

class BestSubsetSolutionTest {

    private lateinit var testProb: BestSubsetProblem
    private lateinit var testSol: BestSubsetSolution
    private lateinit var testCorr: (Array<Double>, Array<Double>) -> Double
    private lateinit var testTarg: (BinarySolution, Double) -> BinarySolution
    private var testAvgPrec: MutableMap<String, Array<Double>> = LinkedHashMap()

    @BeforeEach
    @DisplayName("BestSubsetSolution - Initialize Tests")

    fun initTest() {
        val length = 10
        for (index in 0..length) {
            val fakeAvgPrec = Array(length, { 0.0 })
            val random = Random()
            for (aIndex in 0..fakeAvgPrec.size - 1) {
                fakeAvgPrec[aIndex] = (1 + (100 - 1) * random.nextDouble()) / 100
            }
            testAvgPrec["Test $index"] = fakeAvgPrec
        }
        testCorr = { _, _ -> 0.0 }
        testTarg = { sol, _ -> sol }
        val parameters = Parameters(Constants.CORRELATION_PEARSON, Constants.TARGET_BEST, 100000, 1000, listOf(50))
        testProb = BestSubsetProblem(parameters, testAvgPrec.size, testAvgPrec, Array(0, { 0.0 }), testCorr, testTarg)
        testSol = BestSubsetSolution(testProb, testAvgPrec.size)
    }

    @Test
    @DisplayName("SetBitValue")

    fun setBitValueTest() {

        println("[BestSubsetSolutionTest setBitValue] - Test begins.")

        val oldSelTop = testSol.numberOfSelectedTopics
        val oldTopStat = testSol.retrieveTopicStatus()
        var oldSum = 0; oldTopStat.forEach { value -> oldSum += if (value) 1 else 0 }
        val valueToSet = oldTopStat[0] xor true
        testSol.setBitValue(0, valueToSet)
        val newSelTop = testSol.numberOfSelectedTopics
        val newTopStat = testSol.retrieveTopicStatus()
        var newSum = 0; newTopStat.forEach { value -> newSum += if (value) 1 else 0 }
        if (!valueToSet) {
            println("[BestSubsetSolutionTest setBitValue] - Testing: <Old Sum Topic Stat. Val.: $oldSum, New Sum Topic Stat. Val: $newSum>.")
            assertEquals(true, oldSum != newSum, "<Old Sum Topic Stat. Val.: $oldSum, New Sum Topic Stat. Val: $newSum>")
        } else {
            println("[BestSubsetSolutionTest setBitValue] - Testing: <Old Sum Topic Stat. Val.: $oldSum, New Sum Topic Stat. Val: $newSum>.")
            assertEquals(oldSelTop + 1, newSelTop, "<Old Num. Sel. Topics: $oldSelTop, New Num. Sel. Topics: $newSelTop>.")
        }

        println("[BestSubsetSolutionTest setBitValue] - Test ends.")
    }

    @Test
    @DisplayName("GetTotalNumberOfBits")

    fun getTotalNumberOfBitsTest() {

        println("[BestSubsetSolutionTest getTotalNumberOfBits] - Test begins.")

        var sum = 0; (0..testSol.numberOfVariables - 1).forEachIndexed { index, _ -> sum += testSol.getVariableValue(index).binarySetLength }
        println("[BestSubsetSolutionTest setBitValue] - Testing: <Computed Num Diff. Val.: $sum, Expected Num Diff. Val.: ${testSol.totalNumberOfBits}>.")
        assertEquals(true, sum == testSol.totalNumberOfBits, "<Computed Num Diff. Val.: $sum, Expected Num Diff. Val.: ${testSol.totalNumberOfBits}>.")

        println("[BestSubsetSolutionTest getTotalNumberOfBits] - Test ends.")

    }

    @Test
    @DisplayName("Copy")

    fun copyTest() {

        println("[BestSubsetSolutionTest copy] - Test begins.")
        assertEquals(true, testSol.copy() == testSol)
        println("[BestSubsetSolutionTest copy] - Test ends.")

    }
}