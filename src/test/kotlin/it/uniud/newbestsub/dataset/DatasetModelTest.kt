package it.uniud.newbestsub.dataset

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.io.FileReader

class DatasetModelTest {

    @Test
    @DisplayName("Solve")

    fun testSolve() {

        val testDatContr = DatasetController()

        testDatContr.loadData("AP96.csv")
        val reader = FileReader("AP96_Expected.csv")
        val correlationValues = reader.readLines()[0].split(";")

    }

}