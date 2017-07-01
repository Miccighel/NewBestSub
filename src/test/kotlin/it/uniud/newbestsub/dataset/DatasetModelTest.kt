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

        val testDatContr = DatasetController(Constants.TARGET_BEST)
        testDatContr.load("src/test/resources/AP96.csv")
        val testParams = Parameters(Constants.CORRELATION_KENDALL, Constants.TARGET_BEST, 100000, 1000, 1000, listOf(1, 5, 25, 99))
        val testRes = testDatContr.models[0].solve(testParams).first
        val computedCards = IntArray(testRes.size)
        testRes.forEachIndexed { index, aSol -> computedCards[index] = aSol.getCardinality().toInt() }

        var i = 1
        val expectedCards = IntArray(testDatContr.models[0].numberOfTopics, { i++ })

        for (j in 0..testDatContr.models[0].numberOfTopics - 1) {
            println("[DatasetModelTest solve] - Testing: <Expected Card. Val.: ${expectedCards[j]}, Computed Card. Val.: ${computedCards[j]}>")
            assertEquals(expectedCards[j], computedCards[j])
        }

        println("[DatasetModelTest solve] - Test ends.")

    }

}