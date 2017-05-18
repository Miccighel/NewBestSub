package it.uniud.newbestsub.problem

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.uma.jmetal.solution.BinarySolution
import java.util.Random
import kotlin.collections.LinkedHashMap
import kotlin.collections.set
import kotlin.test.assertEquals

class BestSubsetSolutionTest {

    private lateinit var testProb: BestSubsetProblem
    private lateinit var testSol: BestSubsetSolution
    private lateinit var testCorr: (DoubleArray, DoubleArray) -> Double
    private lateinit var testTarg: (BinarySolution, Double) -> BinarySolution
    private var testAvgPrec: MutableMap<String, DoubleArray> = LinkedHashMap()

    @BeforeEach
    @DisplayName("BestSubsetSolution - Initialize Tests")

    fun initTest() {
        val length = 10
        for (index in 0..length) {
            val fakeAvgPrec = DoubleArray(length)
            val random = Random()
            for (aIndex in 0..fakeAvgPrec.size - 1) {
                fakeAvgPrec[aIndex] = (1 + (100 - 1) * random.nextDouble()) / 100
            }
            testAvgPrec["Test $index"] = fakeAvgPrec
        }
        testCorr = { _, _ -> 0.0 }
        testTarg = { sol, _ -> sol }
        testProb = BestSubsetProblem(testAvgPrec.size, testAvgPrec, DoubleArray(0), testCorr, testTarg)
        testSol = BestSubsetSolution(testProb, testAvgPrec.size)
    }

    @Test
    @DisplayName("SetBitValue")

    fun setBitValueTest() {
        val oldSelTop = testSol.numberOfSelectedTopics
        val oldTopStat = testSol.retrieveTopicStatus()
        var oldSum = 0; oldTopStat.forEach { value -> oldSum += if (value) 1 else 0 }
        val valueToSet = oldTopStat[0] xor true
        testSol.setBitValue(0, valueToSet)
        val newSelTop = testSol.numberOfSelectedTopics
        val newTopStat = testSol.retrieveTopicStatus()
        var newSum = 0; newTopStat.forEach { value -> newSum += if (value) 1 else 0 }
        if (!valueToSet) {
            assertEquals(true, oldSum != newSum, "Old sum of topic status values: $oldSum - New sum of topic status values: $newSum")
        } else {
            assertEquals(true, oldSum != newSum, "Old sum of topic status values: $oldSum - New sum of topic status values: $newSum")
            assertEquals(oldSelTop + 1, newSelTop, "Old number of selected topics: $oldSelTop - New number of selected topics: $newSelTop")
        }
    }

    @Test
    @DisplayName("GetTotalNumberOfBits")

    fun getTotalNumberOfBitsTest() {
        var sum = 0; (0..testSol.numberOfVariables - 1).forEachIndexed { index, _ -> sum += testSol.getVariableValue(index).binarySetLength }
        assertEquals(true, sum == testSol.totalNumberOfBits, "Computed number of different values: $sum - Retrieved number of different values: ${testSol.totalNumberOfBits}")
    }

    @Test
    @DisplayName("Copy")

    fun copyTest() {
        assertEquals(true, testSol.copy() == testSol)
    }
}