package it.uniud.newbestsub.math

object Statistics {

    /*
     * getMean
     * -------
     * Mean over a run vector restricted to columns flagged as true in useColumns.
     * Throws if no columns are selected to avoid silent NaN/Infinity.
     */
    fun getMean(run: DoubleArray, useColumns: BooleanArray): Double {
        var sumOfSelectedValues = 0.0
        var numberOfSelectedColumns = 0
        val effectiveLength = minOf(run.size, useColumns.size)
        var columnIndex = 0
        while (columnIndex < effectiveLength) {
            if (useColumns[columnIndex]) {
                sumOfSelectedValues += run[columnIndex]
                numberOfSelectedColumns++
            }
            columnIndex++
        }
        require(numberOfSelectedColumns > 0) { "getMean: no columns selected (count == 0)" }
        return sumOfSelectedValues / numberOfSelectedColumns.toDouble()
    }
}
