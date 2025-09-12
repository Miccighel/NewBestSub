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
import kotlin.math.min

/**
 * CSVView.
 *
 * Streaming oriented CSV writer mirroring the Parquet view behavior.
 *
 * Responsibilities
 * - FUN/VAR streaming: append rows to buffered writers and keep an in memory buffer for a final rewrite.
 * - Final rewrite (optional): global sort by (K, correlation) and rewrite both files aligned.
 *   - BEST  → (K ascending, correlation ascending).
 *   - WORST → (K ascending, correlation descending).
 *   - AVERAGE rows are one per K; effective order is K ascending.
 * - Topics in -Var and -Top: base64 packed masks with prefix "B64:<base64>" (no padding).
 * - Top solutions: replace batch semantics; header announces TopicsB64.
 *   - Set -Dnbs.csv.top.live=false to buffer all batches and write once on close.
 *
 * Filesystem
 * - Files are placed under the per run CSV subdirectory returned by [ViewPaths.ensureCsvDir].
 *
 * Toggles (System properties)
 * - nbs.csv.top.live (default true): live write of -Top on each batch.
 * - nbs.csv.finalRewrite (default true): global sort and rewrite of FUN/VAR on close.
 * - nbs.csv.flushEvery (default 256): throttled flush frequency.
 * - nbs.csv.buffer (default 262144): writer buffer size in bytes.
 */
class CSVView {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /* Path helpers (CSV subfolder) */

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

    /* CSV writer for controller tables (aggregate/info) */

    /**
     * Write a complete CSV table to the given path.
     * The input is a list of rows where each row is an array of fields.
     */
    fun writeCsv(rows: List<Array<String>>, resultPath: String) {
        Files.newBufferedWriter(Paths.get(resultPath), Charsets.UTF_8).use { bufferedWriter ->
            CSVWriter(bufferedWriter).use { csvWriter -> csvWriter.writeAll(rows) }
        }
    }

    /* Streaming support */

    private val base64Encoder = Base64.getEncoder().withoutPadding()

    data class StreamHandles(
        val functionWriter: BufferedWriter,
        val variableWriter: BufferedWriter
    )

    private data class StreamKey(val datasetName: String, val executionIndex: Int, val targetToken: String)

    private val openStreamsByKey = mutableMapOf<StreamKey, StreamHandles>()

    private data class TopKey(val datasetName: String, val executionIndex: Int, val targetToken: String)

    private val topBlocksByKey: MutableMap<TopKey, MutableMap<Int, List<String>>> = mutableMapOf()

    private data class FunVarRow(
        val cardinality: Int,
        val naturalCorrelation: Double,
        val functionCsv: String,
        val variableCsv: String
    )

    private val funVarBuffersByKey: MutableMap<TopKey, MutableList<FunVarRow>> = mutableMapOf()

    /* Lightweight formatting and parsing */

    private val decimalFormat = DecimalFormat("0.000000", DecimalFormatSymbols(Locale.ROOT))
    private fun formatCorrelation(value: Double): String = decimalFormat.format(value)

    private val flushEvery: Int =
        System.getProperty("nbs.csv.flushEvery", "256").toIntOrNull()?.coerceAtLeast(1) ?: 256
    private val flushCountersByKey: MutableMap<TopKey, Int> = mutableMapOf()

    private val writeTopLive: Boolean =
        !System.getProperty("nbs.csv.top.live", "true").equals("false", ignoreCase = true)
    private val performFinalRewrite: Boolean =
        !System.getProperty("nbs.csv.finalRewrite", "true").equals("false", ignoreCase = true)
    private val writerBufferSizeBytes: Int =
        System.getProperty("nbs.csv.buffer", "262144").toIntOrNull()?.coerceAtLeast(8192) ?: 262_144

    /* Cached label → index maps for fieldToB64 */

    private data class LabelsKey(val identityHash: Int, val size: Int, val firstLabel: String?)

    private val labelIndexCacheByKey = mutableMapOf<LabelsKey, Map<String, Int>>()

    private fun indexByLabel(labels: Array<String>): Map<String, Int> {
        val cacheKey = LabelsKey(System.identityHashCode(labels), labels.size, labels.firstOrNull())
        return labelIndexCacheByKey.getOrPut(cacheKey) { labels.withIndex().associate { it.value to it.index } }
    }

    /**
     * Normalize a Topics field to the canonical "B64:<...>" representation.
     * Accepts: already "B64:", a label list, numeric indices, or a compact bitstring "101001…".
     */
    private fun fieldToB64(rawField: String, topicLabels: Array<String>): String {
        val trimmed = rawField.trim()
        if (trimmed.isEmpty()) return "B64:"
        if (trimmed.startsWith("B64:")) return trimmed

        /* Fast path for a pure bitstring. Tight loop index variable by convention. */
        var isPureBitstring = true
        run {
            var charIndex = 0
            while (charIndex < trimmed.length) { /* hot loop index */
                val ch = trimmed[charIndex]
                if (ch != '0' && ch != '1') {
                    isPureBitstring = false
                    break
                }
                charIndex++
            }
        }
        if (isPureBitstring) {
            val mask = BooleanArray(topicLabels.size)
            val copyLength = min(topicLabels.size, trimmed.length)
            var bitIndex = 0
            while (bitIndex < copyLength) { /* hot loop index */
                mask[bitIndex] = trimmed[bitIndex] == '1'
                bitIndex++
            }
            return "B64:" + base64Encoder.encodeToString(packMaskToLittleEndianBytes(mask))
        }

        /* Token list path: labels and/or numeric indices. Accept common separators and optional [..] wrapper. */
        val tokens = trimmed.removePrefix("[").removeSuffix("]")
            .split(Regex("[,;\\s|]+"))
            .filter { it.isNotBlank() }

        if (tokens.isEmpty()) return "B64:"

        val mask = BooleanArray(topicLabels.size)
        var matchedCount = 0

        val allTokensAreNumeric = tokens.all { token -> token.all { ch -> ch in '0'..'9' } }
        if (allTokensAreNumeric) {
            for (token in tokens) {
                val parsedIndex = token.toIntOrNull()
                if (parsedIndex != null && parsedIndex in 0 until mask.size && !mask[parsedIndex]) {
                    mask[parsedIndex] = true
                    matchedCount++
                }
            }
        } else {
            val indexByTopicLabel = indexByLabel(topicLabels)
            for (token in tokens) {
                val parsedIndex = indexByTopicLabel[token] ?: token.toIntOrNull()
                if (parsedIndex != null && parsedIndex in 0 until mask.size && !mask[parsedIndex]) {
                    mask[parsedIndex] = true
                    matchedCount++
                }
            }
        }

        if (matchedCount == 0) {
            logger.warn(
                "Topics field did not match labels or indices; emitting empty mask. raw='{}'",
                rawField
            )
        }
        return "B64:" + base64Encoder.encodeToString(packMaskToLittleEndianBytes(mask))
    }

    /**
     * Pack a boolean mask into little endian 64 bit words and return the raw bytes.
     * The output byte array length is 8 * ceil(maskLength / 64).
     */
    private fun packMaskToLittleEndianBytes(mask: BooleanArray): ByteArray {
        val wordCount = (mask.size + 63) ushr 6
        val packedWords = LongArray(wordCount)

        var bitInWord = 0
        var wordIndex = 0
        var wordAccumulator = 0L
        var maskIndex = 0

        while (maskIndex < mask.size) { /* hot loop index */
            if (mask[maskIndex]) wordAccumulator = wordAccumulator or (1L shl bitInWord)
            bitInWord++
            if (bitInWord == 64) {
                packedWords[wordIndex++] = wordAccumulator
                wordAccumulator = 0L
                bitInWord = 0
            }
            maskIndex++
        }
        if (bitInWord != 0) packedWords[wordIndex] = wordAccumulator

        val outBytes = ByteArray(wordCount * java.lang.Long.BYTES)
        var outOffset = 0
        for (word in packedWords) {
            var x = word
            var byteIndex = 0
            while (byteIndex < java.lang.Long.BYTES) { /* hot loop index */
                outBytes[outOffset + byteIndex] = (x and 0xFF).toByte()
                x = x ushr 8
                byteIndex++
            }
            outOffset += java.lang.Long.BYTES
        }
        return outBytes
    }

    /**
     * Robust FUN line parser without regex or String.split.
     * Accepts "K,corr" or "K corr". Returns null on parse failure.
     */
    private fun parseFunLine(line: String): Pair<Int, Double>? {
        val trimmed = line.trim()
        if (trimmed.isEmpty()) return null

        var index = 0
        val length = trimmed.length

        /* Parse K (integer). */
        var sign = 1
        if (index < length && (trimmed[index] == '+' || trimmed[index] == '-')) {
            if (trimmed[index] == '-') sign = -1
            index++
        }
        var kValue = 0
        var haveK = false
        while (index < length) { /* hot loop index */
            val c = trimmed[index]
            if (c in '0'..'9') {
                kValue = kValue * 10 + (c.code - 48)
                haveK = true
                index++
            } else {
                break
            }
        }
        if (!haveK) return null
        kValue *= sign

        /* Skip separators. */
        while (index < length && (trimmed[index] == ',' || trimmed[index].isWhitespace())) index++
        if (index >= length) return null

        /* Parse correlation (double). */
        val start = index
        while (index < length) { /* hot loop index */
            val c = trimmed[index]
            if (c.isWhitespace()) break
            index++
        }
        val corrString = trimmed.substring(start, index)
        val corrValue = corrString.toDoubleOrNull() ?: return null
        return kValue to corrValue
    }

    /**
     * Open and cache large buffer appenders for FUN and VAR of the current run and target.
     * Ensures clean files exist, then returns append mode writers.
     */
    fun openStreams(model: DatasetModel): StreamHandles {
        val streamKey = StreamKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        return openStreamsByKey.getOrPut(streamKey) {
            fun openFresh(path: String) = Files.newBufferedWriter(
                Paths.get(path),
                Charsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            )

            fun openAppend(path: String) = Files.newBufferedWriter(
                Paths.get(path),
                Charsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
            )

            val functionPath = getFunctionValuesFilePath(model)
            val variablePath = getVariableValuesFilePath(model)

            openFresh(functionPath).use { }
            openFresh(variablePath).use { }
            val functionWriter = openAppend(functionPath).buffered(writerBufferSizeBytes)
            val variableWriter = openAppend(variablePath).buffered(writerBufferSizeBytes)

            StreamHandles(functionWriter = functionWriter, variableWriter = variableWriter)
        }
    }

    /**
     * Append one streamed FUN and VAR row, buffering for a possible final rewrite.
     * The event function line contains (K, naturalCorrelation). No conversion is performed here.
     */
    fun onAppendCardinality(model: DatasetModel, event: CardinalityResult) {
        val streamHandles = openStreams(model)
        val topKey = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)

        val parsed = parseFunLine(event.functionValuesCsvLine) ?: return
        val cardinality = parsed.first
        val naturalCorrelation = parsed.second

        /* Canonical CSV output. If a final rewrite will occur, avoid decimal formatting on the hot path. */
        val functionCsv = if (performFinalRewrite) {
            buildString(24) { append(cardinality); append(','); append(naturalCorrelation) }
        } else {
            buildString(24) { append(cardinality); append(','); append(formatCorrelation(naturalCorrelation)) }
        }

        val variableCsv = fieldToB64(event.variableValuesCsvLine, model.topicLabels)

        streamHandles.functionWriter.append(functionCsv).append('\n')
        streamHandles.variableWriter.append(variableCsv).append('\n')

        val nextCount = (flushCountersByKey[topKey] ?: 0) + 1
        if (nextCount % flushEvery == 0) {
            runCatching { streamHandles.functionWriter.flush() }
            runCatching { streamHandles.variableWriter.flush() }
        }
        flushCountersByKey[topKey] = nextCount

        val buffer = funVarBuffersByKey.getOrPut(topKey) { mutableListOf() }
        buffer += FunVarRow(
            cardinality = cardinality,
            naturalCorrelation = naturalCorrelation,
            functionCsv = functionCsv,
            variableCsv = variableCsv
        )
    }

    /**
     * Merge or replace cached TOP blocks and optionally write the whole file live.
     * The producer provides lines sorted by target: BEST descending, WORST ascending.
     */
    fun onReplaceTopBatch(model: DatasetModel, blocks: Map<Int, List<String>>) {
        if (blocks.isEmpty()) return
        val topKey = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val cache = topBlocksByKey.getOrPut(topKey) { mutableMapOf() }
        cache.putAll(blocks)

        if (!writeTopLive) return  // defer write to closeStreams()

        /* Live rewrite of the entire TOP file. */
        val outPath = getTopSolutionsFilePath(model)
        val outFile = File(outPath)
        outFile.parentFile?.mkdirs()
        outFile.bufferedWriter(Charsets.UTF_8, writerBufferSizeBytes).use { writer ->
            writer.appendLine("Cardinality,Correlation,TopicsB64")
            for ((cardinality, lines) in cache.toSortedMap()) {
                for (line in lines) {
                    val parts = line.split(',', limit = 3)
                    if (parts.size < 3) continue
                    val corrNatural = parts[1].trim().toDoubleOrNull() ?: continue
                    val topicsB64 = fieldToB64(parts[2], model.topicLabels)
                    writer.append(cardinality.toString())
                        .append(',')
                        .append(formatCorrelation(corrNatural))
                        .append(',')
                        .appendLine(topicsB64)
                }
            }
        }
    }

    /**
     * Close writers and optionally globally sort and rewrite FUN and VAR to keep them aligned.
     * Sorting uses the natural correlation preserved in memory buffers.
     */
    fun closeStreams(model: DatasetModel) {
        val streamKey = StreamKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val tOpenCloseStart = System.nanoTime()
        openStreamsByKey.remove(streamKey)?.let { handles ->
            runCatching { handles.functionWriter.flush(); handles.functionWriter.close() }
            runCatching { handles.variableWriter.flush(); handles.variableWriter.close() }
        }
        val tOpenCloseEnd = System.nanoTime()

        val topKey = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val bufferedRows = funVarBuffersByKey.remove(topKey)

        if (performFinalRewrite && !bufferedRows.isNullOrEmpty()) {
            val sortStart = System.nanoTime()
            val sorted = when (model.targetToAchieve) {
                Constants.TARGET_WORST ->
                    bufferedRows.sortedWith(compareBy({ it.cardinality }, { -it.naturalCorrelation }))
                else ->
                    bufferedRows.sortedWith(compareBy({ it.cardinality }, { it.naturalCorrelation }))
            }
            val sortEnd = System.nanoTime()

            val functionPath = getFunctionValuesFilePath(model)
            val variablePath = getVariableValuesFilePath(model)

            val writeStart = System.nanoTime()
            File(functionPath).bufferedWriter(Charsets.UTF_8, writerBufferSizeBytes).use { functionWriter ->
                for (row in sorted) {
                    functionWriter.append(row.cardinality.toString())
                        .append(',')
                        .append(formatCorrelation(row.naturalCorrelation))
                        .append('\n')
                }
            }
            File(variablePath).bufferedWriter(Charsets.UTF_8, writerBufferSizeBytes).use { variableWriter ->
                for (row in sorted) {
                    variableWriter.appendLine(row.variableCsv)
                }
            }
            val writeEnd = System.nanoTime()

            logger.info(
                "writersClosed={}ms sort={}ms write={}ms rows={}",
                (tOpenCloseEnd - tOpenCloseStart) / 1_000_000,
                (sortEnd - sortStart) / 1_000_000,
                (writeEnd - writeStart) / 1_000_000,
                sorted.size
            )
        } else {
            logger.info(
                "writersClosed={}ms finalRewrite={}; bufferedRows={}",
                (tOpenCloseEnd - tOpenCloseStart) / 1_000_000,
                performFinalRewrite,
                bufferedRows?.size ?: 0
            )
        }

        /* Write TOP once here if live writes were disabled. */
        if (!writeTopLive) {
            val cache = topBlocksByKey[topKey].orEmpty()
            if (cache.isNotEmpty() && model.targetToAchieve != Constants.TARGET_AVERAGE) {
                val outPath = getTopSolutionsFilePath(model)
                val outFile = File(outPath)
                outFile.parentFile?.mkdirs()
                File(outPath).bufferedWriter(Charsets.UTF_8, writerBufferSizeBytes).use { writer ->
                    writer.appendLine("Cardinality,Correlation,TopicsB64")
                    for ((cardinality, lines) in cache.toSortedMap()) {
                        for (line in lines) {
                            val parts = line.split(',', limit = 3)
                            if (parts.size < 3) continue
                            val corrNatural = parts[1].trim().toDoubleOrNull() ?: continue
                            val topicsB64 = fieldToB64(parts[2], model.topicLabels)
                            writer.append(cardinality.toString())
                                .append(',')
                                .append(formatCorrelation(corrNatural))
                                .append(',')
                                .appendLine(topicsB64)
                        }
                    }
                }
            }
        }

        /* Cleanup state. */
        labelIndexCacheByKey.clear()
        topBlocksByKey.remove(topKey)
        flushCountersByKey.remove(topKey)
    }

    /* Final snapshot (non streamed) */

    /**
     * Write a full snapshot of FUN and VAR (and TOP when applicable) to CSV.
     * Uses the natural correlation via model.naturalCorrOf(...).
     */
    fun printSnapshot(
        model: DatasetModel,
        allSolutions: List<BinarySolution>,
        topSolutions: List<BinarySolution>,
        actualTarget: String
    ) {
        /* FUN */
        runCatching {
            val functionPath = getFunctionValuesFilePath(model)
            File(functionPath).bufferedWriter(Charsets.UTF_8, writerBufferSizeBytes).use { functionWriter ->
                for (solution in allSolutions) {
                    val cardinality = solution.getCardinality().toInt()
                    val corrNatural = formatCorrelation(model.naturalCorrOf(solution as BestSubsetSolution))
                    functionWriter.append(cardinality.toString()).append(',').append(corrNatural).append('\n')
                }
            }
        }.onFailure { logger.warn("FUN CSV write failed", it) }

        /* VAR: packed Base64 ("B64:<...>"). */
        runCatching {
            val variablePath = getVariableValuesFilePath(model)
            File(variablePath).bufferedWriter(Charsets.UTF_8, writerBufferSizeBytes).use { variableWriter ->
                val encoder = base64Encoder
                for (solution in allSolutions) {
                    val bestSubset = solution as BestSubsetSolution
                    val mask = bestSubset.retrieveTopicStatus()
                    val base64 = encoder.encodeToString(packMaskToLittleEndianBytes(mask))
                    variableWriter.append("B64:").appendLine(base64)
                }
            }
        }.onFailure { logger.warn("VAR CSV write failed", it) }

        /* TOP (Best/Worst only): header + rows; correlation is natural. */
        if (actualTarget != Constants.TARGET_AVERAGE) {
            runCatching {
                val topPath = getTopSolutionsFilePath(model)
                File(topPath).bufferedWriter(Charsets.UTF_8, writerBufferSizeBytes).use { writer ->
                    writer.appendLine("Cardinality,Correlation,TopicsB64")
                    val encoder = base64Encoder
                    for (solution in topSolutions) {
                        val bestSubset = solution as BestSubsetSolution
                        val cardinality = bestSubset.getCardinality().toInt()
                        val corrNatural = formatCorrelation(model.naturalCorrOf(bestSubset))
                        val mask = bestSubset.retrieveTopicStatus()
                        val base64 = encoder.encodeToString(packMaskToLittleEndianBytes(mask))
                        writer.append(cardinality.toString())
                            .append(',')
                            .append(corrNatural)
                            .append(',')
                            .append("B64:")
                            .appendLine(base64)
                    }
                }
            }.onFailure { logger.warn("TOP CSV write failed", it) }
        }
    }
}
