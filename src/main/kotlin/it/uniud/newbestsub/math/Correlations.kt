package it.uniud.newbestsub.math

import it.uniud.newbestsub.utils.Constants
import kotlin.math.sqrt

/**
 * Correlations
 * ============
 * Single source of truth for Pearson and Kendall τ-b.
 *
 * - Primitive DoubleArray versions (no boxing) for hot paths.
 * - Boxed Array<Double> versions for legacy/AVERAGE branch.
 * - Factory helpers to select by method token.
 */
object Correlations {

    /* ---------------- Pearson (primitive) ---------------- */

    /** Allocation-free Pearson on primitives. */
    fun fastPearsonPrimitive(left: DoubleArray, right: DoubleArray): Double {
        val n = left.size
        var sumX = 0.0
        var sumY = 0.0
        var sumXX = 0.0
        var sumYY = 0.0
        var sumXY = 0.0
        var i = 0
        while (i < n) {
            val x = left[i]
            val y = right[i]
            sumX += x
            sumY += y
            sumXX += x * x
            sumYY += y * y
            sumXY += x * y
            i++
        }
        val num = n * sumXY - sumX * sumY
        val denX = n * sumXX - sumX * sumX
        val denY = n * sumYY - sumY * sumY
        val den = sqrt(denX) * sqrt(denY)
        return if (den == 0.0) 0.0 else num / den
    }

    /* ---------------- Pearson (boxed) ---------------- */

    /** Boxed-array Pearson (kept for AVERAGE branch compatibility). */
    fun fastPearsonBoxed(left: Array<Double>, right: Array<Double>): Double {
        val n = left.size
        var sumX = 0.0
        var sumY = 0.0
        var sumXX = 0.0
        var sumYY = 0.0
        var sumXY = 0.0
        var i = 0
        while (i < n) {
            val x = left[i]
            val y = right[i]
            sumX += x
            sumY += y
            sumXX += x * x
            sumYY += y * y
            sumXY += x * y
            i++
        }
        val num = n * sumXY - sumX * sumY
        val denX = n * sumXX - sumX * sumX
        val denY = n * sumYY - sumY * sumY
        val den = sqrt(denX) * sqrt(denY)
        return if (den == 0.0) 0.0 else num / den
    }

    /* ---------------- Kendall τ-b (primitive) ---------------- */

    /**
     * τ-b with tie correction; O(n^2), allocation-free (no temp pairs).
     * Works on primitive vectors.
     */
    fun kendallTauBPrimitive(x: DoubleArray, y: DoubleArray): Double {
        val n = x.size
        if (n <= 1) return 0.0
        var concordant = 0L
        var discordant = 0L
        var tiesX = 0L
        var tiesY = 0L
        var i = 0
        while (i < n - 1) {
            var j = i + 1
            while (j < n) {
                val dx = x[i] - x[j]
                val dy = y[i] - y[j]
                val sx = if (dx > 0) 1 else if (dx < 0) -1 else 0
                val sy = if (dy > 0) 1 else if (dy < 0) -1 else 0
                when {
                    sx == 0 && sy == 0 -> { /* both tied: accounted via ties increments below */ }
                    sx == 0 -> tiesX++
                    sy == 0 -> tiesY++
                    else -> if (sx == sy) concordant++ else discordant++
                }
                j++
            }
            i++
        }
        val cPlusD = concordant + discordant
        val denom = kotlin.math.sqrt((cPlusD + tiesX).toDouble() * (cPlusD + tiesY).toDouble())
        return if (denom == 0.0) 0.0 else (concordant - discordant).toDouble() / denom
    }

    /* ---------------- Factories ---------------- */

    /** Primitive (DoubleArray) factory. */
    fun makePrimitiveCorrelation(method: String): (DoubleArray, DoubleArray) -> Double =
        when (method) {
            Constants.CORRELATION_KENDALL -> { a, b -> kendallTauBPrimitive(a, b) }
            else -> { a, b -> fastPearsonPrimitive(a, b) } // default Pearson
        }

    /** Boxed (Array<Double>) factory. */
    fun makeBoxedCorrelation(method: String): (Array<Double>, Array<Double>) -> Double =
        when (method) {
            Constants.CORRELATION_KENDALL -> { a, b ->
                // Reuse primitive τ-b by copying once (AVERAGE branch is not ultra-hot)
                val da = DoubleArray(a.size) { i -> a[i] }
                val db = DoubleArray(b.size) { i -> b[i] }
                kendallTauBPrimitive(da, db)
            }
            else -> { a, b -> fastPearsonBoxed(a, b) } // default Pearson
        }
}
