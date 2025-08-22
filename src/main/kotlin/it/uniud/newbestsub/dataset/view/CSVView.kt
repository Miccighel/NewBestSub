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
import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.Base64
import java.util.Locale

/**
 * CSVView
 * =======
 *
 * Streaming-first CSV writer:
 *  - -Fun/-Var: append live into buffered writers; keep an in-memory buffer for final rewrite.
 *  - Final rewrite: (optional) globally sort by K and correlation and rewrite both files aligned.
 *      • BEST  : (K asc, corr asc)
 *      • WORST : (K asc, corr desc)
 *      • AVERAGE rows are already one-per-K, order is K asc.
 *  - -Var: Base64-packed bitmasks as "B64:<base64>" (compact, no padding).
 *  - -Top: replace-batch semantics; header announces TopicsB64.
 *      • Set -Dnbs.csv.top.live=false to buffer all batches and write once on close.
 *
 * Files live under the per-run **CSV** subdirectory.
 */
class CSVView {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /* -------- Path helpers (CSV subfolder) -------- */

    fun getFunctionValuesFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) +
            ViewPaths.csvNameNoTs(ViewPaths.fileBaseParts(model, model.targetToAchieve), Constants.FUNCTION_VALUES_FILE_SUFFIX)

    fun getVariableValuesFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) +
            ViewPaths.csvNameNoTs(ViewPaths.fileBaseParts(model, model.targetToAchieve), Constants.VARIABLE_VALUES_FILE_SUFFIX)

    fun getTopSolutionsFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) +
            ViewPaths.csvNameNoTs(ViewPaths.fileBaseParts(model, model.targetToAchieve), Constants.TOP_SOLUTIONS_FILE_SUFFIX)

    fun getInfoFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val token = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureCsvDir(model) +
            ViewPaths.csvNameNoTs(ViewPaths.fileBaseParts(model, token), Constants.INFO_FILE_SUFFIX)
    }

    fun getAggregatedDataFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val token = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureCsvDir(model) +
            ViewPaths.csvNameNoTs(ViewPaths.fileBaseParts(model, token), Constants.AGGREGATED_DATA_FILE_SUFFIX)
    }

    /* -------- CSV writer for controller tables (aggregate/info) -------- */

    fun writeCsv(data: List<Array<String>>, resultPath: String) {
        Files.newBufferedWriter(Paths.get(resultPath), Charsets.UTF_8).use { bw ->
            CSVWriter(bw).use { writer -> writer.writeAll(data) }
        }
    }

    /* ---------------- STREAMING SUPPORT ---------------- */

    /** Single no-padding encoder reused everywhere. */
    private val b64Encoder = Base64.getEncoder().withoutPadding()

    /** Buffered writer bundle for FUN/VAR. */
    data class StreamHandles(
        val funWriter: BufferedWriter,
        val varWriter: BufferedWriter
    )

    /**
     * Open stream key:
     *   (datasetName, currentExecution, target)
     * Avoids collisions when the same dataset/target is executed multiple times.
     */
    private data class StreamKey(val dataset: String, val execution: Int, val target: String)

    private val openStreams = mutableMapOf<StreamKey, StreamHandles>()

    /** Top cache key mirrors stream key. */
    private data class TopKey(val dataset: String, val execution: Int, val target: String)
    private val topBlocks: MutableMap<TopKey, MutableMap<Int, List<String>>> = mutableMapOf()

    /** Buffered Fun/Var rows for final rewrite. */
    private data class FunVarRow(val k: Int, val corr: Double, val funLine: String, val varLine: String)
    private val funVarBuffers: MutableMap<TopKey, MutableList<FunVarRow>> = mutableMapOf()

    /** Locale-stable double format (dot decimal, 6 digits). */
    private fun fmt(x: Double): String = String.format(Locale.ROOT, "%.6f", x)

    /** Throttle streaming flushes to reduce I/O overhead. (configurable) */
    private val flushEvery: Int = System.getProperty("nbs.csv.flushEvery", "256").toIntOrNull()?.coerceAtLeast(1) ?: 256
    private val flushCounters: MutableMap<TopKey, Int> = mutableMapOf()

    /** Optional: write TOP only once at close (instead of live on every batch). */
    private val topLive: Boolean = !System.getProperty("nbs.csv.top.live", "true").equals("false", ignoreCase = true)

    /** Optional: skip final rewrite (keep live-append order). */
    private val doFinalRewrite: Boolean = !System.getProperty("nbs.csv.finalRewrite", "true").equals("false", ignoreCase = true)

    /** Writer buffer size (bytes). */
    private val writerBufferSize: Int = System.getProperty("nbs.csv.buffer", "262144").toIntOrNull()?.coerceAtLeast(8192) ?: 262_144

    /** Normalize topics text already in "B64:..." form or legacy label/bits. Always return "B64:..." */
    private fun fieldToB64(raw: String, labels: Array<String>): String {
        val t = raw.trim()
        if (t.startsWith("B64:")) return t
        return try {
            if (t.any { it == '|' || it == ';' || it == ',' || it == ' ' || it == '[' }) {
                val tokens = t.removePrefix("[").removeSuffix("]").split(Regex("[,;\\s|]+")).filter { it.isNotBlank() }
                val indexByLabel = labels.withIndex().associate { it.value to it.index }
                val mask = BooleanArray(labels.size)
                var matched = 0
                for (tk in tokens) {
                    val idx = indexByLabel[tk] ?: tk.toIntOrNull()?.let { v ->
                        indexByLabel.keys.indexOfFirst { it == v.toString() }.takeIf { it >= 0 }
                    }
                    if (idx is Int && idx >= 0 && idx < mask.size) { mask[idx] = true; matched++ }
                }
                if (matched == 0) {
                    logger.warn("CSVView fieldToB64] TOP topics field did not match labels or indices; emitting empty mask. raw='{}'", raw)
                }
                "B64:" + b64Encoder.encodeToString(packMaskToLEBytes(mask))
            } else {
                val mask = BooleanArray(labels.size)
                val n = minOf(labels.size, t.length)
                for (i in 0 until n) mask[i] = (t[i] == '1')
                "B64:" + b64Encoder.encodeToString(packMaskToLEBytes(mask))
            }
        } catch (_: Exception) {
            logger.warn("CSVView fieldToB64] parse failed; emitting empty mask. raw='{}'", raw)
            "B64:"
        }
    }

    /** BooleanArray -> little-endian bytes via 64-bit words (matches model packing) */
    private fun packMaskToLEBytes(mask: BooleanArray): ByteArray {
        val words = (mask.size + 63) ushr 6
        val packed = LongArray(words)
        var bitInWord = 0
        var wIdx = 0
        var acc = 0L
        for (i in mask.indices) {
            if (mask[i]) acc = acc or (1L shl bitInWord)
            bitInWord++
            if (bitInWord == 64) {
                packed[wIdx++] = acc; acc = 0L; bitInWord = 0
            }
        }
        if (bitInWord != 0) packed[wIdx] = acc
        val out = ByteArray(words * java.lang.Long.BYTES)
        var off = 0
        for (word in packed) {
            var x = word
            for (i in 0 until java.lang.Long.BYTES) { out[off + i] = (x and 0xFF).toByte(); x = x ushr 8 }
            off += java.lang.Long.BYTES
        }
        return out
    }

    fun openStreams(model: DatasetModel): StreamHandles {
        val key = StreamKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        return openStreams.getOrPut(key) {
            fun openFresh(path: String) = Files.newBufferedWriter(
                Paths.get(path), Charsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
            )
            fun openAppend(path: String) = Files.newBufferedWriter(
                Paths.get(path), Charsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND
            )

            val funPath = getFunctionValuesFilePath(model)
            val varPath = getVariableValuesFilePath(model)

            // Ensure clean files, then open large-buffer appenders
            openFresh(funPath).use { }
            openFresh(varPath).use { }
            val funWriter = openAppend(funPath).buffered(writerBufferSize)
            val varWriter = openAppend(varPath).buffered(writerBufferSize)

            StreamHandles(funWriter = funWriter, varWriter = varWriter)
        }
    }

    private fun parseFunLine(line: String): Pair<Int, Double>? {
        val parts = line.trim().split(Regex("\\s+"))
        if (parts.size < 2) return null
        val k = parts[0].toDoubleOrNull()?.toInt() ?: return null
        val corr = parts[1].toDoubleOrNull() ?: return null
        return k to corr
    }

    fun onAppendCardinality(model: DatasetModel, ev: CardinalityResult) {
        val handles = openStreams(model)
        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)

        val parsed = parseFunLine(ev.functionValuesCsvLine) ?: return
        val k = parsed.first
        val corr = parsed.second
        val funLine = "$k ${fmt(corr)}"
        val varLine = fieldToB64(ev.variableValuesCsvLine, model.topicLabels)

        handles.funWriter.write(funLine); handles.funWriter.newLine()
        handles.varWriter.write(varLine); handles.varWriter.newLine()

        val cnt = (flushCounters[key] ?: 0) + 1
        if (cnt % flushEvery == 0) {
            runCatching { handles.funWriter.flush() }
            runCatching { handles.varWriter.flush() }
        }
        flushCounters[key] = cnt

        val buf = funVarBuffers.getOrPut(key) { mutableListOf() }
        buf += FunVarRow(k = k, corr = corr, funLine = funLine, varLine = varLine)
    }

    fun onReplaceTopBatch(model: DatasetModel, blocks: Map<Int, List<String>>) {
        if (blocks.isEmpty()) return
        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val cache = topBlocks.getOrPut(key) { mutableMapOf() }
        cache.putAll(blocks)

        if (!topLive) return  // defer write to closeStreams()

        // Live write: rewrite entire file with all cached blocks (sorted by K)
        val outPath = getTopSolutionsFilePath(model)
        val outFile = File(outPath)
        outFile.parentFile?.mkdirs()
        outFile.bufferedWriter(Charsets.UTF_8, writerBufferSize).use { w ->
            w.appendLine("Cardinality,Correlation,TopicsB64")
            for ((k, lines) in cache.toSortedMap()) {
                for (line in lines) {
                    val p = line.split(',', limit = 3)
                    if (p.size < 3) continue
                    val corr = p[1].trim().toDoubleOrNull() ?: continue
                    val topicsB64 = fieldToB64(p[2], model.topicLabels)
                    w.append(k.toString()).append(',').append(fmt(corr)).append(',').appendLine(topicsB64)
                }
            }
        }
    }

    /**
     * Close writers, then (optionally) globally sort & rewrite -Fun/-Var to keep them aligned.
     * Toggle: -Dnbs.csv.finalRewrite=false to skip the rewrite during exploratory runs.
     * If -Dnbs.csv.top.live=false, write the final TOP file here.
     */
    fun closeStreams(model: DatasetModel) {
        val keyStreams = StreamKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val t0 = System.nanoTime()
        openStreams.remove(keyStreams)?.let { h ->
            runCatching { h.funWriter.flush(); h.funWriter.close() }
            runCatching { h.varWriter.flush(); h.varWriter.close() }
        }
        val t1 = System.nanoTime()

        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val rows = funVarBuffers.remove(key)

        if (doFinalRewrite && rows != null && rows.isNotEmpty()) {
            val sortStart = System.nanoTime()
            val sorted = when (model.targetToAchieve) {
                Constants.TARGET_WORST ->
                    rows.sortedWith(compareBy<FunVarRow>({ it.k }, { -it.corr }))  // K asc, corr desc
                else ->
                    rows.sortedWith(compareBy<FunVarRow>({ it.k }, { it.corr }))   // K asc, corr asc
            }
            val sortEnd = System.nanoTime()

            val funPath = getFunctionValuesFilePath(model)
            val varPath = getVariableValuesFilePath(model)

            val writeStart = System.nanoTime()
            File(funPath).bufferedWriter(Charsets.UTF_8, writerBufferSize).use { fw ->
                for (r in sorted) { fw.append(r.k.toString()).append(' ').append(fmt(r.corr)).append('\n') }
            }
            File(varPath).bufferedWriter(Charsets.UTF_8, writerBufferSize).use { vw ->
                for (r in sorted) { vw.appendLine(r.varLine) }
            }
            val writeEnd = System.nanoTime()

            logger.info(
                "[CSVView.closeStreams] writersClosed={}ms sort={}ms write={}ms rows={}",
                (t1 - t0) / 1_000_000, (sortEnd - sortStart) / 1_000_000,
                (writeEnd - writeStart) / 1_000_000, sorted.size
            )
        } else {
            logger.info(
                "[CSVView.closeStreams] writersClosed={}ms finalRewrite={}; bufferedRows={}",
                (t1 - t0) / 1_000_000, doFinalRewrite, rows?.size ?: 0
            )
        }

        // Write TOP once here if live writes were disabled
        if (!topLive) {
            val cache = topBlocks[key].orEmpty()
            if (cache.isNotEmpty() && model.targetToAchieve != Constants.TARGET_AVERAGE) {
                val outPath = getTopSolutionsFilePath(model)
                val outFile = File(outPath)
                outFile.parentFile?.mkdirs()
                File(outPath).bufferedWriter(Charsets.UTF_8, writerBufferSize).use { w ->
                    w.appendLine("Cardinality,Correlation,TopicsB64")
                    for ((k, lines) in cache.toSortedMap()) {
                        for (line in lines) {
                            val p = line.split(',', limit = 3)
                            if (p.size < 3) continue
                            val corr = p[1].trim().toDoubleOrNull() ?: continue
                            val topicsB64 = fieldToB64(p[2], model.topicLabels)
                            w.append(k.toString()).append(',').append(fmt(corr)).append(',').appendLine(topicsB64)
                        }
                    }
                }
            }
        }

        // Cleanup state
        topBlocks.remove(key)
        flushCounters.remove(key)
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
            File(path).bufferedWriter(Charsets.UTF_8, writerBufferSize).use { fw ->
                for (s in allSolutions) {
                    val k = s.getCardinality().toInt()
                    val corr = fmt(s.getCorrelation())
                    fw.append(k.toString()).append(' ').append(corr).append('\n')
                }
            }
        }.onFailure { logger.warn("FUN CSV write failed", it) }

        // VAR: packed Base64 ("B64:<...>")
        runCatching {
            val path = getVariableValuesFilePath(model)
            File(path).bufferedWriter(Charsets.UTF_8, writerBufferSize).use { vw ->
                val enc = b64Encoder
                for (s in allSolutions) {
                    val bss = s as BestSubsetSolution
                    val mask = bss.retrieveTopicStatus()
                    val b64 = enc.encodeToString(packMaskToLEBytes(mask))
                    vw.append("B64:").appendLine(b64)
                }
            }
        }.onFailure { logger.warn("VAR CSV write failed", it) }

        // TOP (Best/Worst only): header + rows; topics as Base64
        if (actualTarget != Constants.TARGET_AVERAGE) {
            runCatching {
                val path = getTopSolutionsFilePath(model)
                File(path).bufferedWriter(Charsets.UTF_8, writerBufferSize).use { w ->
                    w.appendLine("Cardinality,Correlation,TopicsB64")
                    val enc = b64Encoder
                    for (s in topSolutions) {
                        val bss = s as BestSubsetSolution
                        val k = bss.getCardinality().toInt()
                        val corr = fmt(bss.getCorrelation())
                        val mask = bss.retrieveTopicStatus()
                        val b64 = enc.encodeToString(packMaskToLEBytes(mask))
                        w.append(k.toString()).append(',').append(corr).append(',').append("B64:").appendLine(b64)
                    }
                }
            }.onFailure { logger.warn("TOP CSV write failed", it) }
        }
    }
}
