package it.uniud.newbestsub.dataset.view

import java.math.BigDecimal
import java.math.RoundingMode
import java.util.Locale
import kotlin.math.min

/**
 * Formatting helpers
 * ==================
 *
 * CSV requires locale-stable text (dot decimal) and fixed precision.
 * Parquet uses DOUBLEs but we quantize to 6 decimals for consistency across sinks.
 */

/** Locale-stable double format (dot decimal) for CSV. */
internal fun fmt(x: Double): String = String.format(Locale.ROOT, "%.6f", x)

/** Quantize to 6 decimals for Parquet (stable across readers). */
internal fun round6(x: Double): Double =
    BigDecimal.valueOf(x).setScale(6, RoundingMode.HALF_UP).toDouble()

/** Remove surrounding square brackets from topic labels (e.g., "[420 423]" -> "420 423"). */
internal fun stripBrackets(s: String): String = s.trim().removeSurrounding("[", "]")

/**
 * Convert either:
 *  - space-separated bits: "1 0 1 0 ..."
 *  - compact bits:         "1010..."
 * into a space-separated list of labels set to 1 (no brackets in output).
 *
 * Example:
 *  labels = ["401","402","403","404"], "1 0 1 0" -> "401 403"
 *  labels = ["401","402","403","404"], "1010"    -> "401 403"
 */
internal fun bitsToLabelList(bitsLine: String, labels: Array<String>): String {
    val s = bitsLine.trim()
    if (s.isEmpty()) return ""
    val labelListBuilder = StringBuilder()
    var isFirst = true

    if (s.indexOf(' ') >= 0 || s.indexOf('\t') >= 0) {
        val tokens = s.split(Regex("\\s+"))
        val n = min(tokens.size, labels.size)
        for (i in 0 until n) {
            if (tokens[i] == "1") {
                if (!isFirst) labelListBuilder.append(' ')
                labelListBuilder.append(stripBrackets(labels[i]))
                isFirst = false
            }
        }
    } else {
        val n = min(s.length, labels.size)
        for (i in 0 until n) {
            if (s[i] == '1') {
                if (!isFirst) labelListBuilder.append(' ')
                labelListBuilder.append(stripBrackets(labels[i]))
                isFirst = false
            }
        }
    }
    return labelListBuilder.toString()
}
