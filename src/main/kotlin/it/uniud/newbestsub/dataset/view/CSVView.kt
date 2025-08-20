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
 *  - -Var: store **pipe-delimited topic labels** for bits set to 1 (no brackets).
 *  - -Top: replace-batch semantics; we store **pipe-delimited** topic labels (no brackets).
 *
 * Files are written under the per-run container folder, in the **CSV** subdirectory.
 */
class CSVView {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /* -------- Path helpers (CSV subfolder) -------- */

    fun getFunctionValuesFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) + ViewPaths.csvNameNoTs(ViewPaths.fileBaseParts(model, model.targetToAchieve), Constants.FUNCTION_VALUES_FILE_SUFFIX)

    fun getVariableValuesFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) + ViewPaths.csvNameNoTs(ViewPaths.fileBaseParts(model, model.targetToAchieve), Constants.VARIABLE_VALUES_FILE_SUFFIX)

    fun getTopSolutionsFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) + ViewPaths.csvNameNoTs(ViewPaths.fileBaseParts(model, model.targetToAchieve), Constants.TOP_SOLUTIONS_FILE_SUFFIX)

    fun getInfoFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val token = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureCsvDir(model) + ViewPaths.csvNameNoTs(ViewPaths.fileBaseParts(model, token), Constants.INFO_FILE_SUFFIX)
    }

    fun getAggregatedDataFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val token = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureCsvDir(model) + ViewPaths.csvNameNoTs(ViewPaths.fileBaseParts(model, token), Constants.AGGREGATED_DATA_FILE_SUFFIX)
    }

    /* -------- CSV writer for tables used by controller (aggregate/info) -------- */

    fun writeCsv(data: List<Array<String>>, resultPath: String) {
        Files.newBufferedWriter(Paths.get(resultPath), Charsets.UTF_8).use { bw ->
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

    private val topBlocks: MutableMap<TopKey, MutableMap<Int, List<String>>> = mutableMapOf()

    private data class FunVarRow(val k: Int, val corr: Double, val funLine: String, val varLine: String)

    private val funVarBuffers: MutableMap<TopKey, MutableList<FunVarRow>> = mutableMapOf()

    /** Locale-stable double format (dot decimal). */
    private fun fmt(x: Double): String = String.format(Locale.ROOT, "%.6f", x)

    /** Throttle streaming flushes to reduce I/O overhead. */
    private companion object {
        private const val FLUSH_EVERY = 128
    }
    private val flushCounters: MutableMap<TopKey, Int> = mutableMapOf()

    /** Convert incoming VAR line (bits or labels) to **pipe-delimited label list**. */
    private fun toLabelsLine(raw: String, labels: Array<String>): String {
        val t = raw.trim()
        if (t.isEmpty()) return ""
        if (t.indexOf('|') >= 0) return t

        val sb = StringBuilder()
        var first = true

        // "1 0 1 0" style
        if (t.indexOf(' ') >= 0 || t.indexOf('\t') >= 0) {
            val parts = t.split(Regex("\\s+"))
            val n = minOf(parts.size, labels.size)
            for (i in 0 until n) if (parts[i] == "1") {
                if (!first) sb.append('|') else first = false
                sb.append(labels[i])
            }
            return sb.toString()
        }

        // "101001" compact style
        val n = minOf(t.length, labels.size)
        for (i in 0 until n) if (t[i] == '1') {
            if (!first) sb.append('|') else first = false
            sb.append(labels[i])
        }
        return sb.toString()
    }

    /** Normalize topics text (drop brackets/spaces, replace with '|'). */
    private fun normalizeTopics(raw: String): String =
        raw.trim()
            .removePrefix("[").removeSuffix("]")
            .split(Regex("[,\\s]+"))
            .filter { it.isNotEmpty() }
            .joinToString("|")

    fun openStreams(model: DatasetModel): StreamHandles {
        val key = model.targetToAchieve to model.datasetName
        return openStreams.getOrPut(key) {
            /* Fresh files, then append mode. */
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
     * We rebuild the lines to enforce **6-digit** precision and pipe-delimited labels.
     */
    fun onAppendCardinality(model: DatasetModel, ev: CardinalityResult) {
        val h = openStreams(model)
        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)

        val (k, corr) = parseFunLine(ev.functionValuesCsvLine) ?: return
        val funLine = "$k ${fmt(corr)}"
        val varLine = toLabelsLine(ev.variableValuesCsvLine, model.topicLabels)

        // Stream live (throttled)
        h.funWriter.write(funLine); h.funWriter.newLine()
        h.varWriter.write(varLine); h.varWriter.newLine()
        val cnt = (flushCounters[key] ?: 0) + 1
        if (cnt % FLUSH_EVERY == 0) {
            try { h.funWriter.flush() } catch (_: Exception) {}
            try { h.varWriter.flush() } catch (_: Exception) {}
        }
        flushCounters[key] = cnt

        // Buffer for final global sort
        val buf = funVarBuffers.getOrPut(key) { mutableListOf() }
        buf += FunVarRow(k = k, corr = corr, funLine = funLine, varLine = varLine)
    }

    /**
     * Replace the K blocks and rewrite the entire -Top CSV ordered by K asc.
     * We normalize topics to pipe-delimited labels and format correlations with 6 digits.
     */
    fun onReplaceTopBatch(model: DatasetModel, blocks: Map<Int, List<String>>) {
        if (blocks.isEmpty()) return

        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val cache = topBlocks.getOrPut(key) { mutableMapOf() }
        cache.putAll(blocks)

        val outPath = getTopSolutionsFilePath(model)
        val outFile = File(outPath)
        outFile.parentFile?.mkdirs()
        outFile.bufferedWriter(Charsets.UTF_8).use { w ->
            w.appendLine("Cardinality,Correlation,Topics")
            for ((k, lines) in cache.toSortedMap()) {
                for (line in lines) {
                    val p = line.split(',', limit = 3)
                    if (p.size < 3) continue
                    val corr = p[1].trim().toDoubleOrNull() ?: continue
                    val topics = normalizeTopics(p[2])
                    w.append(k.toString()).append(',').append(fmt(corr)).append(',').appendLine(topics)
                }
            }
        }
    }

    /**
     * Close writers, then globally sort & rewrite -Fun/-Var to keep them aligned.
     */
    fun closeStreams(model: DatasetModel) {
        val keyOfOpen = model.targetToAchieve to model.datasetName
        openStreams.remove(keyOfOpen)?.let { h ->
            try { h.funWriter.flush(); h.funWriter.close() } catch (_: Exception) {}
            try { h.varWriter.flush(); h.varWriter.close() } catch (_: Exception) {}
        }
        flushCounters.remove(TopKey(model.datasetName, model.currentExecution, model.targetToAchieve))

        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        funVarBuffers.remove(key)?.let { rows ->
            if (rows.isNotEmpty()) {
                val funPath = getFunctionValuesFilePath(model)
                val varPath = getVariableValuesFilePath(model)

                val sorted = rows.sortedWith(compareBy<FunVarRow>({ it.k }, { it.corr }))

                File(funPath).bufferedWriter(Charsets.UTF_8).use { fw ->
                    sorted.forEach { r -> fw.append(r.k.toString()).append(' ').append(fmt(r.corr)).append('\n') }
                }
                File(varPath).bufferedWriter(Charsets.UTF_8).use { vw ->
                    sorted.forEach { r -> vw.appendLine(r.varLine) }
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
            File(path).bufferedWriter(Charsets.UTF_8).use { fw ->
                for (s in allSolutions) {
                    val k = s.getCardinality().toInt()
                    val corr = fmt(s.getCorrelation())
                    fw.append(k.toString()).append(' ').append(corr).append('\n')
                }
            }
        }.onFailure { logger.warn("FUN CSV write failed", it) }

        // VAR: labels only (pipe-delimited)
        runCatching {
            val labels = model.topicLabels
            val path = getVariableValuesFilePath(model)
            File(path).bufferedWriter(Charsets.UTF_8).use { vw ->
                for (s in allSolutions) {
                    val flags = (s as BestSubsetSolution).retrieveTopicStatus()
                    val line = buildString {
                        var first = true
                        if (flags is BooleanArray) {
                            for (i in flags.indices) if (flags[i]) {
                                if (!first) append('|') else first = false
                                append(labels[i])
                            }
                        } else {
                            val arr = flags as Array<Boolean>
                            for (i in arr.indices) if (arr[i]) {
                                if (!first) append('|') else first = false
                                append(labels[i])
                            }
                        }
                    }
                    vw.appendLine(line)
                }
            }
        }.onFailure { logger.warn("VAR CSV write failed", it) }

        // TOP (Best/Worst only): header + rows; topics pipe-delimited
        if (actualTarget != Constants.TARGET_AVERAGE) {
            runCatching {
                val path = getTopSolutionsFilePath(model)
                File(path).bufferedWriter(Charsets.UTF_8).use { w ->
                    w.appendLine("Cardinality,Correlation,Topics")
                    for (s in topSolutions) {
                        val bss = s as BestSubsetSolution
                        val k = bss.getCardinality().toInt()
                        val corr = fmt(bss.getCorrelation())
                        val topics = bss.getTopicLabelsFromTopicStatus()
                        w.append(k.toString()).append(',').append(corr).append(',')
                            .appendLine(normalizeTopics(topics))
                    }
                }
            }.onFailure { logger.warn("TOP CSV write failed", it) }
        }
    }
}
