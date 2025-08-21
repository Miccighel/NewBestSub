package it.uniud.newbestsub.dataset.view

import com.opencsv.CSVWriter
import it.uniud.newbestsub.dataset.*
import it.uniud.newbestsub.problem.BestSubsetSolution
import it.uniud.newbestsub.problem.getCardinality
import it.uniud.newbestsub.problem.getCorrelation
import it.uniud.newbestsub.utils.Constants
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.binarysolution.BinarySolution
import java.io.File
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.Locale

/**
 * CSVView
 * =======
 *
 * Streaming-first CSV writer:
 *  - -Fun/-Var: append live, keep an in-memory buffer; on close, sort globally by (K asc, corr asc)
 *    and rewrite both files aligned. Correlations printed with **6 digits**.
 *  - -Var: **Base64-packed bitmask**, one payload per line. Filename ends with **.b64.csv**.
 *  - -Top: replace-batch semantics; CSV is compact:
 *      Header: **Cardinality,Correlation,TopicsB64**
 *      TopicsB64 is the Base64-packed topic mask for that row (pipe labels are kept in Parquet only).
 *
 * Files are written under the per-run container folder, in the **CSV** subdirectory.
 */
class CSVView {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
    private val UTF8: Charset = Charsets.UTF_8

    /* -------- Path helpers (CSV subfolder) -------- */

    fun getFunctionValuesFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) + ViewPaths.csvNameNoTs(
            ViewPaths.fileBaseParts(model, model.targetToAchieve),
            Constants.FUNCTION_VALUES_FILE_SUFFIX
        )

    /** VAR path ends with .b64.csv to signal compact payload. */
    fun getVariableValuesFilePath(model: DatasetModel): String =
        withB64CsvSuffix(
            ViewPaths.ensureCsvDir(model) + ViewPaths.csvNameNoTs(
                ViewPaths.fileBaseParts(model, model.targetToAchieve),
                Constants.VARIABLE_VALUES_FILE_SUFFIX
            )
        )

    fun getTopSolutionsFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) + ViewPaths.csvNameNoTs(
            ViewPaths.fileBaseParts(model, model.targetToAchieve),
            Constants.TOP_SOLUTIONS_FILE_SUFFIX
        )

    fun getInfoFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val token = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureCsvDir(model) + ViewPaths.csvNameNoTs(
            ViewPaths.fileBaseParts(model, token),
            Constants.INFO_FILE_SUFFIX
        )
    }

    fun getAggregatedDataFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val token = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureCsvDir(model) + ViewPaths.csvNameNoTs(
            ViewPaths.fileBaseParts(model, token),
            Constants.AGGREGATED_DATA_FILE_SUFFIX
        )
    }

    /* -------- CSV writer for tables used by controller (aggregate/info) -------- */

    fun writeCsv(data: List<Array<String>>, resultPath: String) {
        Files.newBufferedWriter(Paths.get(resultPath), UTF8).use { bw ->
            CSVWriter(bw).use { writer -> writer.writeAll(data) }
        }
    }

    /* ---------------- STREAMING SUPPORT ---------------- */

    data class StreamHandles(
        val funWriter: java.io.BufferedWriter,
        val varWriter: java.io.BufferedWriter
    )

    private val openStreams = mutableMapOf<Pair<String, String>, StreamHandles>()

    private data class TopKey(val dataset: String, val execution: Int, val target: String)

    /** In-memory cache of K→10-lines (from streaming) for Top; we will rewrite compact CSV. */
    private val topBlocks: MutableMap<TopKey, MutableMap<Int, List<String>>> = mutableMapOf()

    /** Buffer for global sort+rewrite of FUN/VAR. */
    private data class FunVarRow(val k: Int, val corr: Double, val funLine: String, val varLineB64: String)
    private val funVarBuffers: MutableMap<TopKey, MutableList<FunVarRow>> = mutableMapOf()

    /** Locale-stable double format (dot decimal). */
    private fun fmt(x: Double): String = String.format(Locale.ROOT, "%.6f", x)

    /** Throttle streaming flushes to reduce I/O overhead. */
    private companion object {
        private const val FLUSH_EVERY = 128
    }

    private val flushCounters: MutableMap<TopKey, Int> = mutableMapOf()

    /** Ensure .b64.csv suffix for VAR files. */
    private fun withB64CsvSuffix(path: String): String =
        if (path.endsWith(".csv")) path.removeSuffix(".csv") + ".b64.csv" else path + ".b64.csv"

    /* ---------------- Base64 packing (same layout as the model) ---------------- */

    /** Pack BooleanArray → Base64 payload ("B64:..." ready). */
    private fun booleanMaskToVarLineB64(mask: BooleanArray): String {
        val words = ((mask.size + 63) ushr 6)
        val packed = LongArray(words)
        var w = 0
        var bit = 0
        var acc = 0L
        for (i in 0 until mask.size) {
            if (mask[i]) acc = acc or (1L shl bit)
            bit++
            if (bit == 64) {
                packed[w++] = acc
                acc = 0L
                bit = 0
            }
        }
        if (bit != 0) packed[w] = acc
        val bytes = ByteArray(words * java.lang.Long.BYTES)
        var off = 0
        for (word in packed) {
            var x = word
            repeat(java.lang.Long.BYTES) {
                bytes[off + it] = (x and 0xFF).toByte()
                x = x ushr 8
            }
            off += java.lang.Long.BYTES
        }
        val b64 = java.util.Base64.getEncoder().withoutPadding().encodeToString(bytes)
        return "B64:$b64"
    }

    /** Convert pipe-delimited topic labels into a Base64 VAR line (TopicsB64). */
    private fun labelsPipeToVarLineB64(labelsPipe: String, topicLabels: Array<String>): String {
        val idxByLabel = hashMapOf<String, Int>().also { map ->
            for (i in topicLabels.indices) map[topicLabels[i]] = i
        }
        val mask = BooleanArray(topicLabels.size)
        if (labelsPipe.isNotBlank()) {
            labelsPipe.split('|').forEach { lab ->
                val i = idxByLabel[lab]
                if (i != null && i in mask.indices) mask[i] = true
            }
        }
        return booleanMaskToVarLineB64(mask)
    }

    /** Robust TOP field → B64:…  Accepts B64, bitstrings, label lists, or index lists. */
    private fun topicsFieldToB64(raw: String, topicLabels: Array<String>): String {
        val t = raw.trim()
        if (t.startsWith("B64:")) return t

        // raw 0/1 bitstring (spaces allowed)
        val bitsOnly = t.replace(" ", "")
        if (bitsOnly.isNotEmpty() && bitsOnly.all { it == '0' || it == '1' }) {
            val n = topicLabels.size
            val mask = BooleanArray(n)
            val m = kotlin.math.min(bitsOnly.length, n)
            for (i in 0 until m) if (bitsOnly[i] == '1') mask[i] = true
            return booleanMaskToVarLineB64(mask)
        }

        // Bracketed or delimited labels/indices: prefer comma split; fallback to '|'
        val inner = t.removePrefix("[").removeSuffix("]")
        val parts = when {
            inner.contains(',') -> inner.split(',').map { it.trim() }
            inner.contains('|') -> inner.split('|').map { it.trim() }
            else -> listOf(inner.trim()).filter { it.isNotEmpty() }
        }

        val mask = BooleanArray(topicLabels.size)
        if (parts.isNotEmpty()) {
            // Try numeric indices first
            var matchedAny = false
            for (p in parts) {
                val idx = p.toIntOrNull()
                if (idx != null && idx in mask.indices) {
                    mask[idx] = true
                    matchedAny = true
                }
            }
            if (!matchedAny) {
                // Map by exact label
                val idxByLabel = hashMapOf<String, Int>().also { map ->
                    for (i in topicLabels.indices) map[topicLabels[i]] = i
                }
                for (p in parts) {
                    val i = idxByLabel[p]
                    if (i != null && i in mask.indices) {
                        mask[i] = true
                        matchedAny = true
                    }
                }
            }
            if (!matchedAny) {
                logger.warn("TOP topics field did not match labels or indices; emitting empty mask. raw='{}'", raw)
            }
        }
        return booleanMaskToVarLineB64(mask)
    }

    /* ---------------- Open/append/close ---------------- */

    fun openStreams(model: DatasetModel): StreamHandles {
        val key = model.targetToAchieve to model.datasetName
        return openStreams.getOrPut(key) {
            /* Fresh files, then append mode. */
            fun openFresh(path: String) = Files.newBufferedWriter(
                Paths.get(path), UTF8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
            )
            fun openAppend(path: String) = Files.newBufferedWriter(
                Paths.get(path), UTF8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND
            )

            val funPath = getFunctionValuesFilePath(model)
            val varPath = getVariableValuesFilePath(model)

            openFresh(funPath).use { }
            openFresh(varPath).use { }

            StreamHandles(funWriter = openAppend(funPath), varWriter = openAppend(varPath))
        }
    }

    /** Parse a "K corr" text line into (K, corr). */
    private fun parseFunLine(line: String): Pair<Int, Double>? {
        val parts = line.trim().split(Regex("\\s+"))
        if (parts.size < 2) return null
        val k = parts[0].toDoubleOrNull()?.toInt() ?: return null
        val corr = parts[1].toDoubleOrNull() ?: return null
        return k to corr
    }

    /**
     * Append a single improved row to -Fun/-Var and buffer it for global sort.
     * We rebuild the FUN line to enforce **6-digit** precision.
     * VAR: we **do not** expand to labels; we keep the compact **Base64 payload** as-is.
     */
    fun onAppendCardinality(model: DatasetModel, ev: CardinalityResult) {
        val h = openStreams(model)
        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)

        val (k, corr) = parseFunLine(ev.functionValuesCsvLine) ?: return
        val funLine = "$k ${fmt(corr)}"

        // VAR line is already in B64:... format from the model
        val varLineB64 = ev.variableValuesCsvLine.trim()

        // Stream live (throttled)
        h.funWriter.write(funLine); h.funWriter.newLine()
        h.varWriter.write(varLineB64); h.varWriter.newLine()
        val cnt = (flushCounters[key] ?: 0) + 1
        if (cnt % FLUSH_EVERY == 0) {
            runCatching { h.funWriter.flush() }
            runCatching { h.varWriter.flush() }
        }
        flushCounters[key] = cnt

        // Buffer for final global sort
        val buf = funVarBuffers.getOrPut(key) { mutableListOf() }
        buf += FunVarRow(k = k, corr = corr, funLine = funLine, varLineB64 = varLineB64)
    }

    /**
     * Replace the K blocks and rewrite the entire -Top CSV ordered by K asc.
     * Incoming lines may be labels, indices, bitstrings, or B64 — we convert to **TopicsB64** safely.
     */
    fun onReplaceTopBatch(model: DatasetModel, blocks: Map<Int, List<String>>) {
        if (blocks.isEmpty()) return

        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val cache = topBlocks.getOrPut(key) { mutableMapOf() }
        cache.putAll(blocks)

        val outPath = getTopSolutionsFilePath(model)
        val outFile = File(outPath)
        outFile.parentFile?.mkdirs()
        outFile.bufferedWriter(UTF8).use { w ->
            w.appendLine("Cardinality,Correlation,TopicsB64")
            for ((k, lines) in cache.toSortedMap()) {
                for (line in lines) {
                    // original line: "K,Correlation,Topics" (arbitrary format)
                    val p = line.split(',', limit = 3)
                    if (p.size < 3) continue
                    val corr = p[1].trim().toDoubleOrNull() ?: continue
                    val topicsRaw = p[2]
                    val topicsB64 = topicsFieldToB64(topicsRaw, model.topicLabels)
                    w.append(k.toString()).append(',').append(fmt(corr)).append(',')
                        .appendLine(topicsB64)
                }
            }
        }
    }

    /**
     * Close writers, then globally sort & rewrite -Fun/-Var to keep them aligned.
     * VAR stays compact (B64 payloads).
     */
    fun closeStreams(model: DatasetModel) {
        val keyOfOpen = model.targetToAchieve to model.datasetName
        openStreams.remove(keyOfOpen)?.let { h ->
            runCatching { h.funWriter.flush(); h.funWriter.close() }
            runCatching { h.varWriter.flush(); h.varWriter.close() }
        }
        flushCounters.remove(TopKey(model.datasetName, model.currentExecution, model.targetToAchieve))

        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        funVarBuffers.remove(key)?.let { rows ->
            if (rows.isNotEmpty()) {
                val funPath = getFunctionValuesFilePath(model)
                val varPath = getVariableValuesFilePath(model)

                val sorted =
                    rows.sortedWith(compareBy<FunVarRow>({ it.k }, { it.corr }))

                File(funPath).bufferedWriter(UTF8).use { fw ->
                    sorted.forEach { r -> fw.append(r.k.toString()).append(' ').append(fmt(r.corr)).append('\n') }
                }
                File(varPath).bufferedWriter(UTF8).use { vw ->
                    sorted.forEach { r -> vw.appendLine(r.varLineB64) }
                }
            }
        }
    }

    /* ---------------- Final snapshot (non-streamed) ---------------- */

    fun printSnapshot(
        model: DatasetModel,
        allSolutions: List<BinarySolution>,
        topSolutions: List<BinarySolution>,
        actualTarget: String
    ) {
        // FUN
        runCatching {
            val path = getFunctionValuesFilePath(model)
            File(path).bufferedWriter(UTF8).use { fw ->
                for (s in allSolutions) {
                    val k = s.getCardinality().toInt()
                    val corr = fmt(s.getCorrelation())
                    fw.append(k.toString()).append(' ').append(corr).append('\n')
                }
            }
        }.onFailure { logger.warn("FUN CSV write failed", it) }

        // VAR (compact B64)
        runCatching {
            val path = getVariableValuesFilePath(model)
            File(path).bufferedWriter(UTF8).use { vw ->
                for (s in allSolutions) {
                    val flags = (s as BestSubsetSolution).retrieveTopicStatus()
                    val b64 = booleanMaskToVarLineB64(flags)
                    vw.appendLine(b64)
                }
            }
        }.onFailure { logger.warn("VAR CSV write failed", it) }

        // TOP (Best/Worst only): compact header + TopicsB64
        if (actualTarget != Constants.TARGET_AVERAGE) {
            runCatching {
                val path = getTopSolutionsFilePath(model)
                File(path).bufferedWriter(UTF8).use { w ->
                    w.appendLine("Cardinality,Correlation,TopicsB64")
                    for (s in topSolutions) {
                        val bss = s as BestSubsetSolution
                        val k = bss.getCardinality().toInt()
                        val corr = fmt(bss.getCorrelation())
                        val flags = bss.retrieveTopicStatus()
                        val topicsB64 = booleanMaskToVarLineB64(flags)
                        w.append(k.toString()).append(',').append(corr).append(',')
                            .appendLine(topicsB64)
                    }
                }
            }.onFailure { logger.warn("TOP CSV write failed", it) }
        }
    }
}
