package it.uniud.newbestsub.math

import it.uniud.newbestsub.utils.Constants
import kotlin.math.sqrt

/**
 * **Correlations**
 *
 * Centralized implementation of correlation measures used in NewBestSub.
 *
 * Features:
 * - **Single source of truth** for both Pearson and Kendall τ-b.
 * - **Primitive DoubleArray implementations** (no boxing) for hot paths.
 * - **Boxed Array<Double> implementations** for legacy branches (e.g., Average mode).
 * - **Factory helpers** to select by method token ([Constants.CORRELATION_PEARSON], [Constants.CORRELATION_KENDALL]).
 *
 * Design notes:
 * - Pearson correlation uses allocation-free streaming accumulators.
 * - Kendall τ-b is implemented in O(n²) with tie correction and no temporary pairs.
 * - Boxed correlation paths exist for compatibility; internally, they may copy into primitive arrays.
 */
object Correlations {

    // ------------------------------------------------------------------------------------
    // Pearson (primitive)
    // ------------------------------------------------------------------------------------

    /**
     * Computes **Pearson correlation** on two primitive arrays.
     *
     * - Allocation-free: only accumulators are used.
     * - Suitable for hot paths (e.g., iterative evaluations).
     *
     * @param left left-hand vector
     * @param right right-hand vector
     * @return Pearson’s r in `[-1.0, +1.0]`, or `0.0` if denominator is `0`
     */
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

    // ------------------------------------------------------------------------------------
    // Kendall τ-b (primitive)
    // ------------------------------------------------------------------------------------

    /**
     * Computes **Kendall’s τ-b correlation** on primitive arrays, with tie correction.
     *
     * - Complexity: O(n²).
     * - Allocation-free: does not build explicit pairs.
     * - Corrects for ties in X and Y dimensions.
     *
     * @param x left-hand vector
     * @param y right-hand vector
     * @return Kendall’s τ-b in `[-1.0, +1.0]`, or `0.0` if denominator is `0`
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
                    sx == 0 && sy == 0 -> { /* both tied: handled via tie counts */ }
                    sx == 0 -> tiesX++
                    sy == 0 -> tiesY++
                    else -> if (sx == sy) concordant++ else discordant++
                }
                j++
            }
            i++
        }
        val cPlusD = concordant + discordant
        val denom = sqrt((cPlusD + tiesX).toDouble() * (cPlusD + tiesY).toDouble())
        return if (denom == 0.0) 0.0 else (concordant - discordant).toDouble() / denom
    }

    // ------------------------------------------------------------------------------------
    // Factories
    // ------------------------------------------------------------------------------------

    /**
     * Returns a primitive correlation function for the given [method] token.
     *
     * @param method correlation method ([Constants.CORRELATION_PEARSON], [Constants.CORRELATION_KENDALL])
     * @return function taking two [DoubleArray]s and returning the correlation
     */
    fun makePrimitiveCorrelation(method: String): (DoubleArray, DoubleArray) -> Double =
        when (method) {
            Constants.CORRELATION_KENDALL -> { a, b -> kendallTauBPrimitive(a, b) }
            else -> { a, b -> fastPearsonPrimitive(a, b) } // default Pearson
        }

}
