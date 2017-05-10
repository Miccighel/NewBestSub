package it.uniud.newbestsub.utils

object Formula {

    fun stringComparison(firstString: String, secondString: String): Int {
        var distance = 0
        var i = 0
        while (i < firstString.length) { if (secondString[i] != firstString[i]) distance++; i++ }
        return distance
    }

    fun getMean(run: DoubleArray, useColumns: BooleanArray): Double {

        var mean = 0.0
        var numberOfUsedCols = 0

        useColumns.forEachIndexed { _, value -> if (value) { numberOfUsedCols++ } }
        useColumns.forEachIndexed { index, value -> if (value) {  mean += run[index] } }

        mean /= numberOfUsedCols.toDouble()

        return mean
    }

}
