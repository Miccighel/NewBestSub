package it.uniud.newbestsub.dataset.view

import com.opencsv.CSVWriter
import it.uniud.newbestsub.dataset.*
import it.uniud.newbestsub.dataset.model.CardinalityResult
import it.uniud.newbestsub.problem.BestSubsetSolution
import it.uniud.newbestsub.problem.getCardinality
import it.uniud.newbestsub.utils.Constants
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.binarysolution.BinarySolution
import java.io.File
import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
import java.util.Base64
import java.util.Locale

/**
 * CSVView
 * =======
 *
 * Streaming‑first CSV writer mirroring the Parquet view.
 *
 * ## Responsibilities
 * - **FUN/VAR streaming**: append live into buffered writers; keep an in‑memory buffer for a final rewrite
 * - **Final rewrite** (optional): globally sort by `(K, correlation)` and rewrite both files aligned
 *   - BEST  → `(K asc, corr asc)`
 *   - WORST → `(K asc, corr desc)`
 *   - AVERAGE rows are one‑per‑K; effective order is `K asc`
 * - **Topics** in `-Var` and `-Top`: base64‑packed masks as `"B64:<base64>"` (no padding)
 * - **Top solutions**: replace‑batch semantics; header announces `TopicsB64`
 *   - Set `-Dnbs.csv.top.live=false` to buffer all batches and write once on close
 *
 * ## Filesystem
 * - Files live under the per‑run **CSV** subdirectory returned by [ViewPaths.ensureCsvDir]
 *
 * ## Toggles
 * - `-Dnbs.csv.top.live` (default `true`): live write of `-Top` on each batch
 * - `-Dnbs.csv.finalRewrite` (default `true`): global sort & rewrite of FUN/VAR on close
 * - `-Dnbs.csv.flushEvery` (default `256`): throttled flush frequency
 * - `-Dnbs.csv.buffer` (default `262144`): writer buffer size in bytes
 */

class CSVView {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /* -------- Path helpers (CSV subfolder) -------- */

    fun getFunctionValuesFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) +
                ViewPaths.csvNameNoTs(
                    ViewPaths.fileBaseParts(model, model.targetToAchieve),
                    Constants.FUNCTION_VALUES_FILE_SUFFIX
                )

    fun getVariableValuesFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) +
                ViewPaths.csvNameNoTs(
                    ViewPaths.fileBaseParts(model, model.targetToAchieve),
                    Constants.VARIABLE_VALUES_FILE_SUFFIX
                )

    fun getTopSolutionsFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) +
                ViewPaths.csvNameNoTs(
                    ViewPaths.fileBaseParts(model, model.targetToAchieve),
                    Constants.TOP_SOLUTIONS_FILE_SUFFIX
                )

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

    private val base64Encoder = Base64.getEncoder().withoutPadding()

    data class StreamHandles(
        val funWriter: BufferedWriter,
        val varWriter: BufferedWriter
    )

    private data class StreamKey(val dataset: String, val execution: Int, val target: String)

    private val openStreams = mutableMapOf<StreamKey, StreamHandles>()

    private data class TopKey(val dataset: String, val execution: Int, val target: String)

    private val topBlocks: MutableMap<TopKey, MutableMap<Int, List<String>>> = mutableMapOf()

    private data class FunVarRow(val k: Int, val naturalCorrelation: Double, val funLine: String, val varLine: String)

    private val funVarBuffers: MutableMap<TopKey, MutableList<FunVarRow>> = mutableMapOf()

    /* --------- Lightweight formatting & parsing --------- */

    private val funSplitter = Regex("[,\\s]+")

    private val decimalFormat = DecimalFormat("0.000000", DecimalFormatSymbols(Locale.ROOT))
    private fun fmt(x: Double): String = decimalFormat.format(x)

    private val flushEvery: Int =
        System.getProperty("nbs.csv.flushEvery", "256").toIntOrNull()?.coerceAtLeast(1) ?: 256
    private val flushCounters: MutableMap<TopKey, Int> = mutableMapOf()

    private val topLive: Boolean =
        !System.getProperty("nbs.csv.top.live", "true").equals("false", ignoreCase = true)
    private val doFinalRewrite: Boolean =
        !System.getProperty("nbs.csv.finalRewrite", "true").equals("false", ignoreCase = true)
    private val writerBufferSize: Int =
        System.getProperty("nbs.csv.buffer", "262144").toIntOrNull()?.coerceAtLeast(8192) ?: 262_144

    /* --------- Cached label→index maps for fieldToB64 --------- */

    private data class LabelsKey(val identity: Int, val size: Int, val first: String?)

    private val labelIndexCache = mutableMapOf<LabelsKey, Map<String, Int>>()

    private fun indexByLabel(labels: Array<String>): Map<String, Int> {
        val key = LabelsKey(System.identityHashCode(labels), labels.size, labels.firstOrNull())
        return labelIndexCache.getOrPut(key) { labels.withIndex().associate { it.value to it.index } }
    }

    /**
     * Normalize topics field to canonical "B64:<...>".
     * Accepts: already "B64:", label list, numeric indices, or bitstring.
     */
    private fun fieldToB64(raw: String, labels: Array<String>): String {
        val t = raw.trim()
        if (t.startsWith("B64:")) return t
        return try {
            if (t.any { it == '|' || it == ';' || it == ',' || it == ' ' || it == '[' }) {
                val tokens = t.removePrefix("[").removeSuffix("]")
                    .split(Regex("[,;\\s|]+"))
                    .filter { it.isNotBlank() }
                val byLabel = indexByLabel(labels)
                val mask = BooleanArray(labels.size)
                var matched = 0
                for (token in tokens) {
                    val idx = byLabel[token] ?: token.toIntOrNull()
                    if (idx != null && idx in 0 until mask.size) {
                        mask[idx] = true; matched++
                    }
                }
                if (matched == 0) {
                    logger.warn(
                        "CSVView fieldToB64] topics field did not match labels/indices; emitting empty mask. raw='{}'",
                        raw
                    )
                }
                "B64:" + base64Encoder.encodeToString(packMaskToLEBytes(mask))
            } else {
                val mask = BooleanArray(labels.size)
                val n = minOf(labels.size, t.length)
                for (i in 0 until n) mask[i] = (t[i] == '1')
                "B64:" + base64Encoder.encodeToString(packMaskToLEBytes(mask))
            }
        } catch (_: Exception) {
            logger.warn("CSVView fieldToB64] parse failed; emitting empty mask. raw='{}'", raw)
            "B64:"
        }
    }

    /** Pack a boolean mask into little-endian longs and return raw bytes. */
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
            for (i in 0 until java.lang.Long.BYTES) {
                out[off + i] = (x and 0xFF).toByte(); x = x ushr 8
            }
            off += java.lang.Long.BYTES
        }
        return out
    }

    /** Robust FUN line parser: accepts "K corr" / "K,corr" / any mix of commas/whitespace. */
    private fun parseFunLine(line: String): Pair<Int, Double>? {
        val parts = line.trim().split(funSplitter)
        if (parts.size < 2) return null
        val k = parts[0].toDoubleOrNull()?.toInt() ?: return null
        val corrNatural = parts[1].toDoubleOrNull() ?: return null
        return k to corrNatural
    }

    /**
     * Open and cache large-buffer appender for FUN/VAR of the current run/target.
     * Ensures clean files exist, then returns append-mode writers.
     */
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

            openFresh(funPath).use { }
            openFresh(varPath).use { }
            val funWriter = openAppend(funPath).buffered(writerBufferSize)
            val varWriter = openAppend(varPath).buffered(writerBufferSize)

            StreamHandles(funWriter = funWriter, varWriter = varWriter)
        }
    }

    /**
     * Append one streamed FUN/VAR row, buffering for a possible final rewrite.
     * `ev.functionValuesCsvLine` contains `(K,naturalCorr)`; do not convert here.
     */
    fun onAppendCardinality(model: DatasetModel, ev: CardinalityResult) {
        val handles = openStreams(model)
        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)

        val (k, naturalCorr) = parseFunLine(ev.functionValuesCsvLine) ?: return

        // Canonical CSV output. If we’ll do a final rewrite, avoid fmt() in the hot path.
        val funLine = if (doFinalRewrite) buildString(24) {
            append(k); append(','); append(naturalCorr)
        } else buildString(24) {
            append(k); append(','); append(fmt(naturalCorr))
        }

        val varLine = fieldToB64(ev.variableValuesCsvLine, model.topicLabels)

        handles.funWriter.append(funLine).append('\n')
        handles.varWriter.append(varLine).append('\n')

        val cnt = (flushCounters[key] ?: 0) + 1
        if (cnt % flushEvery == 0) {
            runCatching { handles.funWriter.flush() }
            runCatching { handles.varWriter.flush() }
        }
        flushCounters[key] = cnt

        val buf = funVarBuffers.getOrPut(key) { mutableListOf() }
        buf += FunVarRow(k = k, naturalCorrelation = naturalCorr, funLine = funLine, varLine = varLine)
    }

    /**
     * Merge/replace cached TOP blocks and optionally write the whole file live.
     * Producer provides lines sorted by target: BEST desc, WORST asc.
     */
    fun onReplaceTopBatch(model: DatasetModel, blocks: Map<Int, List<String>>) {
        if (blocks.isEmpty()) return
        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val cache = topBlocks.getOrPut(key) { mutableMapOf() }
        cache.putAll(blocks)

        if (!topLive) return  // defer write to closeStreams()

        // Live rewrite of entire TOP file
        val outPath = getTopSolutionsFilePath(model)
        val outFile = File(outPath)
        outFile.parentFile?.mkdirs()
        outFile.bufferedWriter(Charsets.UTF_8, writerBufferSize).use { w ->
            w.appendLine("Cardinality,Correlation,TopicsB64")
            for ((k, lines) in cache.toSortedMap()) {
                for (line in lines) {
                    val p = line.split(',', limit = 3)
                    if (p.size < 3) continue
                    val corrNatural = p[1].trim().toDoubleOrNull() ?: continue
                    val topicsB64 = fieldToB64(p[2], model.topicLabels)
                    w.append(k.toString()).append(',').append(fmt(corrNatural)).append(',').appendLine(topicsB64)
                }
            }
        }
    }

    /**
     * Close writers, then (optionally) globally sort & rewrite FUN/VAR to keep them aligned.
     * Sorting uses **natural** correlation kept in memory buffers.
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
                    rows.sortedWith(compareBy({ it.k }, { -it.naturalCorrelation }))  // K asc, corr desc
                else ->
                    rows.sortedWith(compareBy({ it.k }, { it.naturalCorrelation }))   // K asc, corr asc
            }
            val sortEnd = System.nanoTime()

            val funPath = getFunctionValuesFilePath(model)
            val varPath = getVariableValuesFilePath(model)

            val writeStart = System.nanoTime()
            File(funPath).bufferedWriter(Charsets.UTF_8, writerBufferSize).use { fw ->
                for (r in sorted) {
                    fw.append(r.k.toString()).append(',').append(fmt(r.naturalCorrelation)).append('\n')
                }
            }
            File(varPath).bufferedWriter(Charsets.UTF_8, writerBufferSize).use { vw ->
                for (r in sorted) {
                    vw.appendLine(r.varLine)
                }
            }
            val writeEnd = System.nanoTime()

            logger.info(
                "writersClosed={}ms sort={}ms write={}ms rows={}",
                (t1 - t0) / 1_000_000, (sortEnd - sortStart) / 1_000_000,
                (writeEnd - writeStart) / 1_000_000, sorted.size
            )
        } else {
            logger.info(
                "writersClosed={}ms finalRewrite={}; bufferedRows={}",
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
                            val corrNatural = p[1].trim().toDoubleOrNull() ?: continue
                            val topicsB64 = fieldToB64(p[2], model.topicLabels)
                            w.append(k.toString()).append(',').append(fmt(corrNatural)).append(',').appendLine(topicsB64)
                        }
                    }
                }
            }
        }

        // Cleanup state
        labelIndexCache.clear()
        topBlocks.remove(key)
        flushCounters.remove(key)
    }

    /* ---------------- Final snapshot (non-streamed) ---------------- */

    /**
     * Write a full snapshot of FUN/VAR (and TOP when applicable) to CSV.
     * Uses **natural** correlation via `model.naturalCorrOf(...)`.
     */
    fun printSnapshot(
        model: DatasetModel,
        allSolutions: List<BinarySolution>,
        topSolutions: List<BinarySolution>,
        actualTarget: String
    ) {
        /* FUN */
        runCatching {
            val path = getFunctionValuesFilePath(model)
            File(path).bufferedWriter(Charsets.UTF_8, writerBufferSize).use { fw ->
                for (s in allSolutions) {
                    val k = s.getCardinality().toInt()
                    val corrNatural = fmt(model.naturalCorrOf(s as BestSubsetSolution))
                    fw.append(k.toString()).append(',').append(corrNatural).append('\n')
                }
            }
        }.onFailure { logger.warn("FUN CSV write failed", it) }

        /* VAR: packed Base64 ("B64:<...>") */
        runCatching {
            val path = getVariableValuesFilePath(model)
            File(path).bufferedWriter(Charsets.UTF_8, writerBufferSize).use { vw ->
                val enc = base64Encoder
                for (s in allSolutions) {
                    val bss = s as BestSubsetSolution
                    val mask = bss.retrieveTopicStatus()
                    val b64 = enc.encodeToString(packMaskToLEBytes(mask))
                    vw.append("B64:").appendLine(b64)
                }
            }
        }.onFailure { logger.warn("VAR CSV write failed", it) }

        /* TOP (Best/Worst only): header + rows; correlation is natural */
        if (actualTarget != Constants.TARGET_AVERAGE) {
            runCatching {
                val path = getTopSolutionsFilePath(model)
                File(path).bufferedWriter(Charsets.UTF_8, writerBufferSize).use { w ->
                    w.appendLine("Cardinality,Correlation,TopicsB64")
                    val enc = base64Encoder
                    for (s in topSolutions) {
                        val bss = s as BestSubsetSolution
                        val k = bss.getCardinality().toInt()
                        val corrNatural = fmt(model.naturalCorrOf(bss))
                        val mask = bss.retrieveTopicStatus()
                        val b64 = enc.encodeToString(packMaskToLEBytes(mask))
                        w.append(k.toString()).append(',').append(corrNatural).append(',').append("B64:").appendLine(b64)
                    }
                }
            }.onFailure { logger.warn("TOP CSV write failed", it) }
        }
    }
}
