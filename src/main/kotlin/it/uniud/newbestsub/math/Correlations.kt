package it.uniud.newbestsub.math

import it.uniud.newbestsub.utils.Constants
import kotlin.math.sqrt
import java.util.Arrays
import jdk.incubator.vector.DoubleVector
import jdk.incubator.vector.VectorOperators
import java.lang.Double.compare as dcmp

/**
 * Correlations
 *
 * Centralized implementations of Pearson and Kendall τ-b used in NewBestSub.
 *
 * The code favors primitive DoubleArray paths for performance. It also exposes
 * helpers that compute Pearson directly from per-system SUMS and a selected K,
 * which avoids recomputing means during Average mode.
 *
 * For Pearson, an O(n) scalar path uses fused-multiply-add and loop unrolling.
 * A JDK Vector API path is available when the module is present at runtime.
 *
 * For Kendall τ-b, the implementation runs in O(n log n). It uses a fast path
 * without ties (τ-a via inversion counting on ranks) and a full Knight (1966)
 * correction when ties are present. Per-thread scratch buffers avoid allocations.
 */
object Correlations {

    /**
     * Lazy probe for the Vector API so that the class can load even when the
     * incubator module is not available. The result is cached.
     */
    @Volatile
    private var _vectorApiAvailable: Boolean? = null

    /** Returns true if jdk.incubator.vector.DoubleVector can be loaded. */
    private fun vectorApiAvailable(): Boolean {
        _vectorApiAvailable?.let { return it }
        val ok = try {
            Class.forName("jdk.incubator.vector.DoubleVector")
            true
        } catch (_: Throwable) {
            false
        }
        _vectorApiAvailable = ok
        return ok
    }

    /**
     * Heuristic size where SIMD usually outperforms the tuned scalar loop.
     * Kept as a constant to avoid touching DoubleVector during the check.
     */
    private const val VECTOR_AUTO_MIN_N: Int = 2048

    /**
     * Thread-local scratch for Kendall paths to avoid per-call allocations.
     * Capacity grows geometrically when more space is needed.
     */
    private val tlsScratch: ThreadLocal<DoubleArray> =
        ThreadLocal.withInitial { DoubleArray(1024) }

    /** Ensures a scratch buffer with at least n elements and returns it. */
    private fun ensureScratch(n: Int): DoubleArray {
        var buf = tlsScratch.get()
        if (buf.size < n) {
            var cap = buf.size
            while (cap < n) cap = cap shl 1
            buf = DoubleArray(cap)
            tlsScratch.set(buf)
        }
        return buf
    }

    /**
     * Pearson correlation on primitive arrays.
     *
     * O(n) time, O(1) extra space. Uses 4x loop unrolling to increase ILP and
     * Math.fma to improve accuracy and throughput on supported hardware.
     */
    fun fastPearsonPrimitive(left: DoubleArray, right: DoubleArray): Double {
        val n = left.size
        var sumX = 0.0
        var sumY = 0.0
        var sumXX = 0.0
        var sumYY = 0.0
        var sumXY = 0.0

        var i = 0
        val limit = n - (n and 3)

        while (i < limit) {
            val x0 = left[i];     val y0 = right[i]
            val x1 = left[i + 1]; val y1 = right[i + 1]
            val x2 = left[i + 2]; val y2 = right[i + 2]
            val x3 = left[i + 3]; val y3 = right[i + 3]

            sumX += (x0 + x1) + (x2 + x3)
            sumY += (y0 + y1) + (y2 + y3)

            sumXX = Math.fma(x0, x0, sumXX); sumYY = Math.fma(y0, y0, sumYY); sumXY = Math.fma(x0, y0, sumXY)
            sumXX = Math.fma(x1, x1, sumXX); sumYY = Math.fma(y1, y1, sumYY); sumXY = Math.fma(x1, y1, sumXY)
            sumXX = Math.fma(x2, x2, sumXX); sumYY = Math.fma(y2, y2, sumYY); sumXY = Math.fma(x2, y2, sumXY)
            sumXX = Math.fma(x3, x3, sumXX); sumYY = Math.fma(y3, y3, sumYY); sumXY = Math.fma(x3, y3, sumXY)

            i += 4
        }

        while (i < n) {
            val x = left[i]; val y = right[i]
            sumX += x; sumY += y
            sumXX = Math.fma(x, x, sumXX)
            sumYY = Math.fma(y, y, sumYY)
            sumXY = Math.fma(x, y, sumXY)
            i++
        }

        val nD = n.toDouble()
        val numerator = nD * sumXY - sumX * sumY
        val denomX = nD * sumXX - sumX * sumX
        val denomY = nD * sumYY - sumY * sumY
        if (denomX <= 0.0 || denomY <= 0.0) return 0.0

        val denominator = sqrt(denomX * denomY)
        return if (denominator == 0.0) 0.0 else numerator / denominator
    }

    /**
     * Pearson correlation using the JDK Vector API.
     * Still O(n); processes a vector of lanes per step and reduces at the end.
     * Call only when the incubator module is available.
     */
    fun fastPearsonPrimitiveVector(left: DoubleArray, right: DoubleArray): Double {
        val n = left.size
        val sp = DoubleVector.SPECIES_PREFERRED
        val lanes = sp.length()
        var i = 0
        val upper = sp.loopBound(n)

        var vSumX  = DoubleVector.zero(sp)
        var vSumY  = DoubleVector.zero(sp)
        var vSumXX = DoubleVector.zero(sp)
        var vSumYY = DoubleVector.zero(sp)
        var vSumXY = DoubleVector.zero(sp)

        while (i < upper) {
            val vx = DoubleVector.fromArray(sp, left,  i)
            val vy = DoubleVector.fromArray(sp, right, i)

            vSumX  = vSumX.add(vx)
            vSumY  = vSumY.add(vy)
            vSumXX = vSumXX.add(vx.mul(vx))
            vSumYY = vSumYY.add(vy.mul(vy))
            vSumXY = vSumXY.add(vx.mul(vy))

            i += lanes
        }

        var sumX  = vSumX.reduceLanes(VectorOperators.ADD)
        var sumY  = vSumY.reduceLanes(VectorOperators.ADD)
        var sumXX = vSumXX.reduceLanes(VectorOperators.ADD)
        var sumYY = vSumYY.reduceLanes(VectorOperators.ADD)
        var sumXY = vSumXY.reduceLanes(VectorOperators.ADD)

        while (i < n) {
            val x = left[i]; val y = right[i]
            sumX  += x
            sumY  += y
            sumXX = Math.fma(x, x, sumXX)
            sumYY = Math.fma(y, y, sumYY)
            sumXY = Math.fma(x, y, sumXY)
            i++
        }

        val nD = n.toDouble()
        val numerator = nD * sumXY - sumX * sumY
        val denomX = nD * sumXX - sumX * sumX
        val denomY = nD * sumYY - sumY * sumY
        if (denomX <= 0.0 || denomY <= 0.0) return 0.0

        val denominator = sqrt(denomX * denomY)
        return if (denominator == 0.0) 0.0 else numerator / denominator
    }

    /**
     * Chooses the Vector API for large inputs when the module is present, otherwise
     * falls back to the tuned scalar path.
     */
    fun fastPearsonPrimitiveAuto(left: DoubleArray, right: DoubleArray): Double {
        return if (vectorApiAvailable() && left.size >= VECTOR_AUTO_MIN_N)
            fastPearsonPrimitiveVector(left, right)
        else
            fastPearsonPrimitive(left, right)
    }

    /**
     * Kendall τ-b on primitive arrays.
     *
     * If neither input contains ties, it falls back to τ-a via inversion counting
     * on ranks. Otherwise it uses Knight's O(n log n) method with full tie terms.
     */
    fun kendallTauBPrimitive(x: DoubleArray, y: DoubleArray): Double {
        val n = x.size
        if (n <= 1) return 0.0
        if (!hasTies(x) && !hasTies(y)) return kendallTauNoTies(x, y)
        return kendallTauBTiesFast(x, y)
    }

    /**
     * Exact tie detection.
     *
     * For small n uses an O(n^2) scan that avoids allocations.
     * For larger n copies into thread-local scratch, sorts, and scans.
     */
    private fun hasTies(v: DoubleArray): Boolean {
        val n = v.size
        if (n < 1024) {
            var i = 0
            while (i < n) {
                val vi = v[i]
                var j = i + 1
                while (j < n) {
                    if (vi == v[j]) return true
                    j++
                }
                i++
            }
            return false
        } else {
            val scratch = ensureScratch(n)
            System.arraycopy(v, 0, scratch, 0, n)
            Arrays.sort(scratch, 0, n)
            var i = 1
            while (i < n) {
                if (scratch[i] == scratch[i - 1]) return true
                i++
            }
            return false
        }
    }

    /**
     * No-ties Kendall (τ-a).
     *
     * Steps:
     * 1) Sort indices by x ascending.
     * 2) Build y in that order and compute strict ranks in 1..n.
     * 3) Count inversions on the ranks with a Fenwick tree.
     */
    private fun kendallTauNoTies(x: DoubleArray, y: DoubleArray): Double {
        val n = x.size
        val indexByX = IntArray(n) { it }
        sortIndicesBy(x, indexByX)

        val yInXOrder = DoubleArray(n) { y[indexByX[it]] }
        val strictRanks = rankStrict(yInXOrder)

        val discordantPairs = countInversionsFenwick(strictRanks)
        val totalPairs = n.toLong() * (n - 1L) / 2L
        val concordantPairs = totalPairs - discordantPairs
        return (concordantPairs - discordantPairs).toDouble() / totalPairs.toDouble()
    }

    /**
     * Kendall τ-b with ties (Knight 1966).
     *
     * Steps:
     * 1) Sort indices by (x asc, y asc).
     * 2) Build yInXOrder and count discordant pairs as inversions.
     * 3) Compute tie terms:
     *    n0 = n*(n-1)/2
     *    n1 = sum over x tie-runs of t_x*(t_x-1)/2
     *    n2 = sum over y tie-runs of t_y*(t_y-1)/2
     *    n3 = sum over joint (x,y) tie-runs of t_xy*(t_xy-1)/2
     * 4) C + D = n0 - n1 - n2 + n3 and τ-b = (C - D) / sqrt((n0 - n1)*(n0 - n2)).
     */
    private fun kendallTauBTiesFast(x: DoubleArray, y: DoubleArray): Double {
        val n = x.size
        val indexByLex = IntArray(n) { it }
        sortIndicesByLex(x, y, indexByLex)

        val yInXOrder = DoubleArray(n) { y[indexByLex[it]] }
        val n0 = n.toLong() * (n - 1L) / 2L

        var n1 = 0L
        var n3 = 0L
        run {
            var runLenX = 1L
            var runLenXY = 1L
            var i = 1
            while (i < n) {
                val sameX = dcmp(x[indexByLex[i]], x[indexByLex[i - 1]]) == 0
                val sameY = dcmp(y[indexByLex[i]], y[indexByLex[i - 1]]) == 0
                if (sameX) {
                    runLenX++
                    if (sameY) {
                        runLenXY++
                    } else {
                        n3 += runLenXY * (runLenXY - 1L) / 2L
                        runLenXY = 1L
                    }
                } else {
                    n1 += runLenX * (runLenX - 1L) / 2L
                    runLenX = 1L
                    n3 += runLenXY * (runLenXY - 1L) / 2L
                    runLenXY = 1L
                }
                i++
            }
            n1 += runLenX * (runLenX - 1L) / 2L
            n3 += runLenXY * (runLenXY - 1L) / 2L
        }

        val ySorted = ensureScratch(n)
        System.arraycopy(y, 0, ySorted, 0, n)
        Arrays.sort(ySorted, 0, n)
        var n2 = 0L
        run {
            var runLenY = 1L
            var i = 1
            while (i < n) {
                if (dcmp(ySorted[i], ySorted[i - 1]) == 0) {
                    runLenY++
                } else {
                    n2 += runLenY * (runLenY - 1L) / 2L
                    runLenY = 1L
                }
                i++
            }
            n2 += runLenY * (runLenY - 1L) / 2L
        }

        val discordantPairs = countInversionsByMerge(yInXOrder)
        val comparablePairs = n0 - n1 - n2 + n3
        val concordantPairs = comparablePairs - discordantPairs

        val denomLeft = n0 - n1
        val denomRight = n0 - n2
        val denominator = sqrt(denomLeft.toDouble() * denomRight.toDouble())
        if (denominator == 0.0) return 0.0

        return (concordantPairs - discordantPairs).toDouble() / denominator
    }

    /**
     * Sorts idx so that values[idx[i]] is ascending.
     * In-place quicksort over indices to avoid moving the data array.
     */
    private fun sortIndicesBy(values: DoubleArray, idx: IntArray) {
        fun swap(i: Int, j: Int) { val t = idx[i]; idx[i] = idx[j]; idx[j] = t }
        fun quicksort(lo: Int, hi: Int) {
            var l = lo
            var h = hi
            while (l < h) {
                val mid = (l + h) ushr 1
                val pivot = values[idx[mid]]
                var i = l
                var j = h
                while (i <= j) {
                    while (dcmp(values[idx[i]], pivot) < 0) i++
                    while (dcmp(values[idx[j]], pivot) > 0) j--
                    if (i <= j) { swap(i, j); i++; j-- }
                }
                if (j - l < h - i) {
                    if (l < j) quicksort(l, j)
                    l = i
                } else {
                    if (i < h) quicksort(i, h)
                    h = j
                }
            }
        }
        quicksort(0, idx.lastIndex)
    }

    /**
     * Sorts idx by (x asc, y asc).
     * In-place quicksort over indices to avoid moving the data arrays.
     */
    private fun sortIndicesByLex(x: DoubleArray, y: DoubleArray, idx: IntArray) {
        fun swap(i: Int, j: Int) { val t = idx[i]; idx[i] = idx[j]; idx[j] = t }
        fun less(iIndex: Int, pivotX: Double, pivotY: Double): Boolean {
            val cx = dcmp(x[idx[iIndex]], pivotX)
            return if (cx != 0) cx < 0 else dcmp(y[idx[iIndex]], pivotY) < 0
        }
        fun greater(iIndex: Int, pivotX: Double, pivotY: Double): Boolean {
            val cx = dcmp(x[idx[iIndex]], pivotX)
            return if (cx != 0) cx > 0 else dcmp(y[idx[iIndex]], pivotY) > 0
        }
        fun quicksort(lo: Int, hi: Int) {
            var l = lo
            var h = hi
            while (l < h) {
                val mid = (l + h) ushr 1
                val pivotX = x[idx[mid]]
                val pivotY = y[idx[mid]]
                var i = l
                var j = h
                while (i <= j) {
                    while (less(i, pivotX, pivotY)) i++
                    while (greater(j, pivotX, pivotY)) j--
                    if (i <= j) { swap(i, j); i++; j-- }
                }
                if (j - l < h - i) {
                    if (l < j) quicksort(l, j)
                    l = i
                } else {
                    if (i < h) quicksort(i, h)
                    h = j
                }
            }
        }
        quicksort(0, idx.lastIndex)
    }

    /**
     * Ranks for strictly increasing values (no ties).
     * Returns an IntArray with ranks in the closed interval [1, n].
     */
    private fun rankStrict(values: DoubleArray): IntArray {
        val n = values.size
        val idx = IntArray(n) { it }
        sortIndicesBy(values, idx)
        val ranks = IntArray(n)
        var r = 1
        var i = 0
        while (i < n) {
            ranks[idx[i]] = r
            r++
            i++
        }
        return ranks
    }

    /**
     * Inversion count on ranks in [1, n] using a Fenwick tree.
     * Counts strictly greater elements to the right.
     */
    private fun countInversionsFenwick(ranks: IntArray): Long {
        val n = ranks.size
        val bit = LongArray(n + 1)

        fun prefixSum(i0: Int): Long {
            var i = i0
            var acc = 0L
            while (i > 0) { acc += bit[i]; i -= i and -i }
            return acc
        }
        fun add(i0: Int, delta: Long) {
            var i = i0
            while (i <= n) { bit[i] += delta; i += i and -i }
        }

        var inversions = 0L
        var i = n - 1
        while (i >= 0) {
            val r = ranks[i]
            inversions += prefixSum(r - 1)
            add(r, 1)
            i--
        }
        return inversions
    }

    /**
     * Merge-sort inversion count on a DoubleArray.
     * Destructive on the input: the array is sorted on return.
     * Uses a single thread-local scratch buffer.
     */
    private fun countInversionsByMerge(values: DoubleArray): Long {
        val n = values.size
        if (n <= 1) return 0L
        val scratch = ensureScratch(n)

        fun sort(lo: Int, hi: Int): Long {
            if (lo >= hi) return 0L
            val mid = (lo + hi) ushr 1
            var inv = sort(lo, mid) + sort(mid + 1, hi)

            var i = lo
            var j = mid + 1
            var k = lo
            while (i <= mid && j <= hi) {
                if (dcmp(values[i], values[j]) <= 0) {
                    scratch[k++] = values[i++]
                } else {
                    scratch[k++] = values[j++]
                    inv += (mid - i + 1).toLong()
                }
            }
            while (i <= mid) scratch[k++] = values[i++]
            while (j <= hi)  scratch[k++] = values[j++]
            var t = lo
            while (t <= hi) { values[t] = scratch[t]; t++ }
            return inv
        }

        return sort(0, n - 1)
    }

    /**
     * Aggregates used to reuse Y-side terms across many Pearson evaluations from SUMS and K.
     */
    data class CorrelationYStats(
        val y: DoubleArray,
        val n: Int,
        val ySum: Double,
        val ySumYY: Double,
        val yDenTerm: Double
    )

    /**
     * Precompute Y-side aggregates for Pearson so that repeated calls can reuse them.
     */
    fun precomputeYStats(y: DoubleArray): CorrelationYStats {
        var sum = 0.0
        var sumYY = 0.0
        var i = 0
        while (i < y.size) {
            val v = y[i]
            sum += v
            sumYY = Math.fma(v, v, sumYY)
            i++
        }
        val n = y.size
        val nD = n.toDouble()
        val yDen = nD * sumYY - sum * sum
        return CorrelationYStats(y = y, n = n, ySum = sum, ySumYY = sumYY, yDenTerm = yDen)
    }

    /**
     * Pearson from system-level SUMS and selected K.
     * Computes correlation between per-system means of the chosen K topics and Y.
     */
    fun fastPearsonFromSumsWithK(
        sumsBySystem: DoubleArray,
        kSelected: Int,
        yStats: CorrelationYStats
    ): Double {
        if (kSelected <= 0) return 0.0
        val invK = 1.0 / kSelected.toDouble()

        var sumX = 0.0
        var sumXX = 0.0
        var sumXY = 0.0

        val y = yStats.y
        val n = yStats.n

        var i = 0
        val limit = n - (n and 3)
        while (i < limit) {
            val s0 = sumsBySystem[i];     val y0 = y[i]
            val s1 = sumsBySystem[i + 1]; val y1 = y[i + 1]
            val s2 = sumsBySystem[i + 2]; val y2 = y[i + 2]
            val s3 = sumsBySystem[i + 3]; val y3 = y[i + 3]

            sumX += (s0 + s1) + (s2 + s3)
            sumXX = Math.fma(s0, s0, sumXX); sumXY = Math.fma(s0, y0, sumXY)
            sumXX = Math.fma(s1, s1, sumXX); sumXY = Math.fma(s1, y1, sumXY)
            sumXX = Math.fma(s2, s2, sumXX); sumXY = Math.fma(s2, y2, sumXY)
            sumXX = Math.fma(s3, s3, sumXX); sumXY = Math.fma(s3, y3, sumXY)

            i += 4
        }
        while (i < n) {
            val s = sumsBySystem[i]
            sumX += s
            sumXX = Math.fma(s, s, sumXX)
            sumXY = Math.fma(s, y[i], sumXY)
            i++
        }

        val sumXScaled = sumX * invK
        val sumXXScaled = sumXX * (invK * invK)
        val sumXYScaled = sumXY * invK

        val nD = n.toDouble()
        val numerator = nD * sumXYScaled - sumXScaled * yStats.ySum
        val denomX = nD * sumXXScaled - sumXScaled * sumXScaled
        val denomY = yStats.yDenTerm
        if (denomX <= 0.0 || denomY <= 0.0) return 0.0

        val denominator = sqrt(denomX * denomY)
        return if (denominator == 0.0) 0.0 else numerator / denominator
    }

    /**
     * Vectorized Pearson from SUMS and K.
     * Requires the Vector API at runtime.
     */
    fun fastPearsonFromSumsWithKVector(
        sumsBySystem: DoubleArray,
        kSelected: Int,
        yStats: CorrelationYStats
    ): Double {
        if (kSelected <= 0) return 0.0
        val invK = 1.0 / kSelected.toDouble()

        val sp = DoubleVector.SPECIES_PREFERRED
        val lanes = sp.length()
        val y = yStats.y
        val n = yStats.n

        var i = 0
        val upper = sp.loopBound(n)

        var vSumX  = DoubleVector.zero(sp)
        var vSumXX = DoubleVector.zero(sp)
        var vSumXY = DoubleVector.zero(sp)

        while (i < upper) {
            val vs = DoubleVector.fromArray(sp, sumsBySystem, i)
            val vy = DoubleVector.fromArray(sp, y,            i)
            vSumX  = vSumX.add(vs)
            vSumXX = vSumXX.add(vs.mul(vs))
            vSumXY = vSumXY.add(vs.mul(vy))
            i += lanes
        }

        var sumX  = vSumX.reduceLanes(VectorOperators.ADD)
        var sumXX = vSumXX.reduceLanes(VectorOperators.ADD)
        var sumXY = vSumXY.reduceLanes(VectorOperators.ADD)

        while (i < n) {
            val s = sumsBySystem[i]
            sumX  += s
            sumXX = Math.fma(s, s, sumXX)
            sumXY = Math.fma(s, y[i], sumXY)
            i++
        }

        val sumXScaled = sumX * invK
        val sumXXScaled = sumXX * (invK * invK)
        val sumXYScaled = sumXY * invK

        val nD = n.toDouble()
        val numerator = nD * sumXYScaled - sumXScaled * yStats.ySum
        val denomX = nD * sumXXScaled - sumXScaled * sumXScaled
        val denomY = yStats.yDenTerm
        if (denomX <= 0.0 || denomY <= 0.0) return 0.0

        val denominator = sqrt(denomX * denomY)
        return if (denominator == 0.0) 0.0 else numerator / denominator
    }

    /**
     * Auto-dispatcher for the SUMS+K variant. Chooses the vector path when the
     * module is present and the input is large enough.
     */
    fun fastPearsonFromSumsWithKAuto(
        sumsBySystem: DoubleArray,
        kSelected: Int,
        yStats: CorrelationYStats
    ): Double {
        return if (vectorApiAvailable() && yStats.n >= VECTOR_AUTO_MIN_N)
            fastPearsonFromSumsWithKVector(sumsBySystem, kSelected, yStats)
        else
            fastPearsonFromSumsWithK(sumsBySystem, kSelected, yStats)
    }

    /**
     * Factory that returns a primitive correlation function by method token.
     * Defaults to Pearson when the token is not Kendall.
     */
    fun makePrimitiveCorrelation(method: String): (DoubleArray, DoubleArray) -> Double =
        when (method) {
            Constants.CORRELATION_KENDALL -> { a, b -> kendallTauBPrimitive(a, b) }
            else -> { a, b -> fastPearsonPrimitiveAuto(a, b) }
        }

    /**
     * Factory that returns a correlation from SUMS and K given precomputed Y stats.
     * Defaults to the Pearson path when the token is not Kendall.
     */
    fun makeCorrelationFromSums(
        method: String,
        yStats: CorrelationYStats
    ): (DoubleArray, Int) -> Double =
        when (method) {
            Constants.CORRELATION_KENDALL -> { sums, _ -> kendallTauBPrimitive(sums, yStats.y) }
            else -> { sums, k -> fastPearsonFromSumsWithKAuto(sums, k, yStats) }
        }
}