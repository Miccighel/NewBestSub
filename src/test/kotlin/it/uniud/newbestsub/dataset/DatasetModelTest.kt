package it.uniud.newbestsub.dataset

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals


class DatasetModelTest {

    @Test
    @DisplayName("Solve")

    fun testSolve() {

        println("[DatasetModelTest solve] - Test begins.")

        val testDatContr = DatasetController()
        testDatContr.loadData("src/test/resources/AP96.csv")
        val testParams = Parameters("Pearson", "Best", 100000, 1000, listOf(1,5,25,99))
        val testRes = testDatContr.model.solve(testParams).first
        val computedCards = IntArray(testRes.size)
        testRes.forEachIndexed { index, aSol -> computedCards[index] = aSol.getObjective(1).toInt() }

        var i = 1
        val expectedCards = IntArray(testDatContr.model.numberOfTopics, { i++ })

        for (j in 0..testDatContr.model.numberOfTopics - 1) {
            println("[DatasetModelTest solve] - Testing: <Expected Card. Val.: ${expectedCards[j]}, Computed Card. Val.: ${computedCards[j]}>")
            assertEquals(expectedCards[j], computedCards[j])
        }

        println("[DatasetModelTest solve] - Test ends.")

    }

}