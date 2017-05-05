package it.uniud.newbestsub.utils

object Formula {

    fun stringComparison(firstString: String, secondString: String): Int {
        var distance = 0
        var i = 0
        while (i < firstString.length) {
            if (secondString.toCharArray()[i] != firstString.toCharArray()[i]) {
                distance++
            }
            i++
        }
        return distance
    }

    fun getMean(run: DoubleArray, useColumns: BooleanArray): Double {

        var mean = 0.0

        var numberOfUsedCols = 0
        for (i in useColumns.indices) {
            if (useColumns[i]) {
                numberOfUsedCols++
            }
        }

        for (i in run.indices) {
            if (useColumns[i]) {
                mean = mean + run[i]
            }
        }
        mean = mean / numberOfUsedCols.toDouble()

        return mean
    }

    fun getMean(run: DoubleArray, useColumns: IntArray): Double {

        var mean = 0.0

        var numberOfUsedCols = 0
        for (i in useColumns.indices) {
            if (useColumns[i] == 1) {
                numberOfUsedCols++
            }
        }

        for (i in run.indices) {
            if (useColumns[i] == 1) {
                mean = mean + run[i]
            }
        }
        mean = mean / numberOfUsedCols.toDouble()

        return mean
    }

}
