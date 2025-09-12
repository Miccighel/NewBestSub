package it.uniud.newbestsub.dataset.view

import it.uniud.newbestsub.dataset.DatasetModel
import it.uniud.newbestsub.dataset.view.ViewPaths
import it.uniud.newbestsub.dataset.model.CardinalityResult
import it.uniud.newbestsub.problem.BestSubsetSolution
import it.uniud.newbestsub.utils.Constants
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.binarysolution.BinarySolution
import kotlin.math.round
import java.util.Locale
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32
import org.apache.parquet.schema.Types

/* Minimal NIO fallback (no Hadoop winutils). */
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
import java.util.Base64

/**
 * ParquetView.
 *
 * Streaming oriented Parquet writer for FUN, VAR, and TOP tables, plus generic
 * final tables (Aggregated and Info). Mirrors CSVView semantics.
 *
 * Responsibilities
 * - Streamed FUN/VAR buffering followed by a single aligned write.
 * - Optional TOP batches merge and write by increasing K.
 * - Generic table writer that does not depend on the CSV layer.
 *
 * Platform notes
 * - On Windows without winutils, falls back to a pure NIO OutputFile implementation.
 * - Compression codec can be selected via system property `-Dnbs.parquet.codec`.
 */
class ParquetView {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /* Paths (Parquet subfolder). */

    private fun funParquetPath(model: DatasetModel): String =
        ViewPaths.ensureParquetDir(model) +
            ViewPaths.parquetNameNoTs(
                ViewPaths.fileBaseParts(model, model.targetToAchieve),
                Constants.FUNCTION_VALUES_FILE_SUFFIX
            )

    private fun varParquetPath(model: DatasetModel): String =
        ViewPaths.ensureParquetDir(model) +
            ViewPaths.parquetNameNoTs(
                ViewPaths.fileBaseParts(model, model.targetToAchieve),
                Constants.VARIABLE_VALUES_FILE_SUFFIX
            )

    private fun topParquetPath(model: DatasetModel): String =
        ViewPaths.ensureParquetDir(model) +
            ViewPaths.parquetNameNoTs(
                ViewPaths.fileBaseParts(model, model.targetToAchieve),
                Constants.TOP_SOLUTIONS_FILE_SUFFIX
            )

    fun getAggregatedDataParquetPath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val token = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureParquetDir(model) +
            ViewPaths.parquetNameNoTs(ViewPaths.fileBaseParts(model, token), Constants.AGGREGATED_DATA_FILE_SUFFIX)
    }

    fun getInfoParquetPath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val token = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureParquetDir(model) +
            ViewPaths.parquetNameNoTs(ViewPaths.fileBaseParts(model, token), Constants.INFO_FILE_SUFFIX)
    }

    /* Schemas. */

    private val schemaFun: MessageType = Types.buildMessage()
        .required(INT32).named("K")
        .required(DOUBLE).named("Correlation")
        .named("Fun")

    private val schemaVar: MessageType = Types.buildMessage()
        .required(INT32).named("K")
        .required(BINARY).`as`(LogicalTypeAnnotation.stringType()).named("TopicsB64")
        .named("Var")

    private val schemaTop: MessageType = Types.buildMessage()
        .required(INT32).named("K")
        .required(DOUBLE).named("Correlation")
        .required(BINARY).`as`(LogicalTypeAnnotation.stringType()).named("TopicsB64")
        .named("Top")

    private fun buildUtf8Schema(messageName: String, columnNames: List<String>): MessageType {
        val builder = Types.buildMessage()
        columnNames.forEach { columnName ->
            builder.required(BINARY).`as`(LogicalTypeAnnotation.stringType()).named(columnName)
        }
        return builder.named(messageName)
    }

    /* Compression and platform helpers. */

    private fun isWindows(): Boolean =
        System.getProperty("os.name").lowercase(Locale.ROOT).contains("win")

    private fun chooseCodec(): CompressionCodecName {
        when (System.getProperty("nbs.parquet.codec")?.uppercase(Locale.ROOT)) {
            "SNAPPY" -> return CompressionCodecName.SNAPPY
            "GZIP" -> return CompressionCodecName.GZIP
            "UNCOMPRESSED" -> return CompressionCodecName.UNCOMPRESSED
        }
        return if (isWindows()) CompressionCodecName.GZIP else CompressionCodecName.SNAPPY
    }

    /**
     * Ensure a writable directory for xerial-snappy temporary files.
     * The selection prefers a sibling of the output directory, then java.io.tmpdir, then user.home.
     */
    private fun ensureSnappyTemp(outPathStr: String) {
        if (System.getProperty("org.xerial.snappy.tempdir") != null) return
        val candidates = listOfNotNull(
            runCatching { Paths.get(outPathStr).parent?.resolve("tmp-snappy") }.getOrNull(),
            runCatching { Paths.get(System.getProperty("java.io.tmpdir", "")).resolve("tmp-snappy") }.getOrNull(),
            runCatching { Paths.get(System.getProperty("user.home", ".")).resolve(".snappy") }.getOrNull()
        )
        for (candidate in candidates) {
            try {
                Files.createDirectories(candidate)
                System.setProperty("org.xerial.snappy.tempdir", candidate.toString())
                return
            } catch (_: Exception) {
                /* Ignore and try next candidate. */
            }
        }
    }

    private fun hasWinutils(): Boolean {
        fun check(dir: String?): Boolean =
            !dir.isNullOrBlank() && Files.exists(Paths.get(dir, "bin", "winutils.exe"))
        return check(System.getenv("HADOOP_HOME")) || check(System.getProperty("hadoop.home.dir"))
    }

    /**
     * OutputFile implementation backed by NIO for Windows environments without winutils.
     * Writes with overwrite semantics.
     */
    private class NioOutputFile(private val nioPath: java.nio.file.Path) : OutputFile {
        override fun create(blockSizeHint: Long): PositionOutputStream = createOrOverwrite(blockSizeHint)

        override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream {
            Files.createDirectories(nioPath.parent)
            val channel = FileChannel.open(
                nioPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            )
            val singleByteBuffer = ByteBuffer.allocate(1)
            return object : PositionOutputStream() {
                private var position: Long = 0
                override fun getPos(): Long = position
                override fun write(b: Int) {
                    singleByteBuffer.clear()
                    singleByteBuffer.put(0, b.toByte())
                    channel.write(singleByteBuffer)
                    position += 1
                }
                override fun write(b: ByteArray, off: Int, len: Int) {
                    val byteBuffer = ByteBuffer.wrap(b, off, len)
                    val written = channel.write(byteBuffer)
                    position += written.toLong()
                }
                override fun flush() {}
                override fun close() { channel.close() }
            }
        }

        override fun supportsBlockSize(): Boolean = false
        override fun defaultBlockSize(): Long = 0L
    }

    /**
     * ParquetWriter factory using RawLocalFS or NIO fallback with OVERWRITE mode.
     * Applies page size, row group size, dictionary encoding, and compression.
     */
    private fun openWriter(pathStr: String, schema: MessageType): ParquetWriter<Group> {
        val codec = chooseCodec()
        if (codec == CompressionCodecName.SNAPPY) runCatching { ensureSnappyTemp(pathStr) }

        val useNioFallback = isWindows() && !hasWinutils()

        val builder = if (useNioFallback) {
            val outFile: OutputFile = NioOutputFile(Paths.get(pathStr))
            ExampleParquetWriter.builder(outFile)
        } else {
            val hadoopConf = Configuration().apply {
                set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                setBoolean("fs.file.impl.disable.cache", true)
            }
            val outFile = HadoopOutputFile.fromPath(Path(pathStr), hadoopConf)
            ExampleParquetWriter.builder(outFile).withConf(hadoopConf)
        }

        return builder
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(codec)
            .withPageSize(128 * 1024)
            .withRowGroupSize(8 * 1024 * 1024L)
            .withDictionaryEncoding(true)
            .withType(schema)
            .build()
    }

    /* Local formatting helpers. */

    private fun round6(value: Double): Double = round(value * 1_000_000.0) / 1_000_000.0

    private val decimalFormat = DecimalFormat("0.000000", DecimalFormatSymbols(Locale.ROOT))
    private fun fmt6(value: Double): String = decimalFormat.format(value)

    private val nonAlNumbersUnderscoreRegex = Regex("[^A-Za-z0-9_]")
    private val tokenSplitRegex = Regex("[,;\\s|]+")

    /**
     * Normalize a cell string: trim, keep plain strings as is, and format decimals to 6 digits.
     */
    private fun normalizeCell(cellText: String): String {
        val trimmed = cellText.trim()
        if (trimmed.isEmpty()) return ""
        val looksDecimal = trimmed.indexOfAny(charArrayOf('.', 'e', 'E')) >= 0
        if (!looksDecimal) return trimmed
        val numeric = trimmed.toDoubleOrNull() ?: return trimmed
        return fmt6(numeric)
    }

    /**
     * Sanitize column names to `[A-Za-z_][A-Za-z0-9_]*` and ensure uniqueness.
     * Empty or invalid names default to `col`, `col_2`, and so on.
     */
    private fun sanitizeAndUniq(rawNames: List<String>): List<String> {
        val used = mutableSetOf<String>()
        return rawNames.map { raw ->
            var base = raw.trim().ifEmpty { "col" }.replace(nonAlNumbersUnderscoreRegex, "_")
            if (base.firstOrNull()?.isDigit() == true) base = "_$base"
            var name = base
            var suffix = 2
            while (!used.add(name)) {
                name = "${base}_$suffix"
                suffix++
            }
            name
        }
    }

    /* Encode helpers (shared encoder and cached label map). */

    private val base64Encoder = Base64.getEncoder().withoutPadding()

    private data class LabelsKey(val identityHash: Int, val size: Int, val firstLabel: String?)

    private val labelIndexCache = mutableMapOf<LabelsKey, Map<String, Int>>()

    private fun indexByLabel(labels: Array<String>): Map<String, Int> {
        val key = LabelsKey(System.identityHashCode(labels), labels.size, labels.firstOrNull())
        return labelIndexCache.getOrPut(key) { labels.withIndex().associate { it.value to it.index } }
    }

    /**
     * Convert an incoming topics field into the canonical "B64:<...>" representation.
     * Accepts labels, numeric indices, bitstrings, or already encoded values.
     */
    private fun fieldToB64(raw: String, labels: Array<String>): String {
        val trimmed = raw.trim()
        if (trimmed.isEmpty()) return "B64:"
        if (trimmed.startsWith("B64:")) return trimmed

        /* Fast path for true bitstrings. Tight loop index variables by convention. */
        var isBitstring = true
        run {
            var index = 0
            while (index < trimmed.length) {
                val ch = trimmed[index]
                if (ch != '0' && ch != '1') {
                    isBitstring = false
                    break
                }
                index++
            }
        }
        if (isBitstring) {
            val mask = BooleanArray(labels.size)
            val copyLength = minOf(labels.size, trimmed.length)
            var bitIndex = 0
            while (bitIndex < copyLength) {
                mask[bitIndex] = trimmed[bitIndex] == '1'
                bitIndex++
            }
            return "B64:" + toBase64(mask)
        }

        /* Token path (handles single token too: label or numeric index). */
        val tokens = if (
            trimmed.any { it == '|' || it == ';' || it == ',' || it.isWhitespace() || it == '[' || it == ']' }
        ) {
            trimmed.removePrefix("[").removeSuffix("]").split(tokenSplitRegex).filter { it.isNotBlank() }
        } else {
            listOf(trimmed)
        }
        if (tokens.isEmpty()) return "B64:"

        val mask = BooleanArray(labels.size)
        var matchedCount = 0
        val allNumeric = tokens.all { token -> token.all { ch -> ch in '0'..'9' } }

        if (allNumeric) {
            for (token in tokens) {
                val index = token.toIntOrNull()
                if (index != null && index in 0 until mask.size && !mask[index]) {
                    mask[index] = true
                    matchedCount++
                }
            }
        } else {
            val indexByTopicLabel = indexByLabel(labels)
            for (token in tokens) {
                val index = indexByTopicLabel[token] ?: token.toIntOrNull()
                if (index != null && index in 0 until mask.size && !mask[index]) {
                    mask[index] = true
                    matchedCount++
                }
            }
        }

        if (matchedCount == 0) {
            logger.warn(
                "ParquetView fieldToB64] topics field did not match labels or indices; emitting empty mask. raw='{}'",
                raw
            )
        }
        return "B64:" + toBase64(mask)
    }

    /**
     * Pack a boolean mask into little endian 64 bit words and Base64 encode the bytes.
     * The output length is 8 * ceil(maskLength / 64).
     */
    private fun toBase64(mask: BooleanArray): String {
        val wordCount = (mask.size + 63) ushr 6
        val packedWords = LongArray(wordCount)

        var bitInWord = 0
        var wordIndex = 0
        var wordAccumulator = 0L
        var maskIndex = 0

        /* Tight loop indices by convention. */
        while (maskIndex < mask.size) {
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
            while (byteIndex < java.lang.Long.BYTES) {
                outBytes[outOffset + byteIndex] = (x and 0xFF).toByte()
                x = x ushr 8
                byteIndex++
            }
            outOffset += java.lang.Long.BYTES
        }
        return base64Encoder.encodeToString(outBytes)
    }

    /* Streaming state. */

    private data class ViewKey(val datasetName: String, val executionIndex: Int, val targetToken: String)

    private data class FunVarRow(
        val cardinality: Int,
        val naturalCorrelation: Double,
        val topicsMaskB64: String
    )

    private val funVarBuffersByKey: MutableMap<ViewKey, MutableList<FunVarRow>> = mutableMapOf()

    private data class TopRow(val cardinality: Int, val naturalCorrelation: Double, val topicsMaskB64: String)

    private val topBlocksByKey: MutableMap<ViewKey, MutableMap<Int, List<TopRow>>> = mutableMapOf()

    /* Snapshot (non streamed). */

    /**
     * Write a full snapshot of FUN, VAR, and TOP (when applicable).
     * Uses the natural correlation as provided by the model.
     */
    fun printSnapshot(
        model: DatasetModel,
        allSolutions: List<BinarySolution>,
        topSolutions: List<BinarySolution>,
        actualTarget: String
    ) {
        if (System.getProperty("nbs.parquet.enabled", "true").equals("false", ignoreCase = true)) {
            logger.info("disabled via -Dnbs.parquet.enabled=false (printSnapshot skipped)")
            return
        }

        runCatching {
            val funFactory = SimpleGroupFactory(schemaFun)
            val varFactory = SimpleGroupFactory(schemaVar)
            openWriter(funParquetPath(model), schemaFun).use { funWriter ->
                openWriter(varParquetPath(model), schemaVar).use { varWriter ->
                    for (solution in allSolutions) {
                        val bestSubset = solution as BestSubsetSolution
                        val cardinality = bestSubset.getCardinality().toInt()
                        val naturalCorrelation = round6(model.naturalCorrOf(bestSubset))
                        val mask = bestSubset.retrieveTopicStatus()
                        funWriter.write(funFactory.newGroup().append("K", cardinality).append("Correlation", naturalCorrelation))
                        varWriter.write(varFactory.newGroup().append("K", cardinality).append("TopicsB64", "B64:" + toBase64(mask)))
                    }
                }
            }
        }.onFailure { logger.warn("FUN/VAR Parquet write (snapshot) failed", it) }

        if (actualTarget != Constants.TARGET_AVERAGE) {
            runCatching {
                val topFactory = SimpleGroupFactory(schemaTop)
                openWriter(topParquetPath(model), schemaTop).use { topWriter ->
                    for (solution in topSolutions) {
                        val bestSubset = solution as BestSubsetSolution
                        topWriter.write(
                            topFactory.newGroup()
                                .append("K", bestSubset.getCardinality().toInt())
                                .append("Correlation", round6(model.naturalCorrOf(bestSubset)))
                                .append("TopicsB64", "B64:" + toBase64(bestSubset.retrieveTopicStatus()))
                        )
                    }
                }
            }.onFailure { logger.warn("TOP Parquet write (snapshot) failed", it) }
        }
    }

    /* Streaming hooks. */

    /**
     * Buffer one streamed FUN/VAR row with topics normalized to B64.
     * The event correlation is the natural correlation used for ordering.
     */
    fun onAppendCardinality(model: DatasetModel, event: CardinalityResult) {
        if (System.getProperty("nbs.parquet.enabled", "true").equals("false", ignoreCase = true)) return
        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val buffer = funVarBuffersByKey.getOrPut(viewKey) { mutableListOf() }

        val naturalCorrelation = event.correlation
        val topicsMaskB64 = fieldToB64(event.variableValuesCsvLine, model.topicLabels)

        buffer += FunVarRow(
            cardinality = event.cardinality,
            naturalCorrelation = naturalCorrelation,
            topicsMaskB64 = topicsMaskB64
        )
    }

    /**
     * Merge or replace per K TOP blocks.
     * Lines are "K,Corr,Topics", where Corr is natural, and the producer already orders by target.
     */
    fun onReplaceTopBatch(model: DatasetModel, blocks: Map<Int, List<String>>) {
        if (System.getProperty("nbs.parquet.enabled", "true").equals("false", ignoreCase = true)) return
        if (blocks.isEmpty()) return

        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val cache = topBlocksByKey.getOrPut(viewKey) { mutableMapOf() }

        for ((fixedCardinality, lines) in blocks) {
            val parsedRows: List<TopRow> = lines.mapNotNull { line ->
                val parts = line.split(',', limit = 3)
                if (parts.size < 3) return@mapNotNull null
                val corrNatural = parts[1].trim().toDoubleOrNull() ?: return@mapNotNull null
                TopRow(
                    cardinality = fixedCardinality,
                    naturalCorrelation = corrNatural,
                    topicsMaskB64 = fieldToB64(parts[2], model.topicLabels)
                )
            }
            if (parsedRows.isNotEmpty()) cache[fixedCardinality] = parsedRows
        }
    }

    /**
     * Flush all buffered rows to Parquet and clear internal state.
     *
     * Ordering
     * - FUN/VAR sorted by (K ascending, correlation ascending) except WORST which uses correlation descending.
     * - TOP written by increasing K and producer provided order within each block.
     */
    fun closeStreams(model: DatasetModel) {
        val enabled = !System.getProperty("nbs.parquet.enabled", "true").equals("false", ignoreCase = true)
        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)

        if (!enabled) {
            funVarBuffersByKey.remove(viewKey)
            topBlocksByKey.remove(viewKey)
            labelIndexCache.clear()
            logger.info("skipped (disabled).")
            return
        }

        val totalStartNs = System.nanoTime()

        /* Sort FUN/VAR by natural correlation with K as primary key. */
        val sortStartNs = System.nanoTime()
        val orderedRows: List<FunVarRow> = run {
            val rows = funVarBuffersByKey[viewKey].orEmpty()
            if (rows.isEmpty()) emptyList() else {
                val comparator =
                    if (model.targetToAchieve == Constants.TARGET_WORST)
                        compareBy<FunVarRow> { it.cardinality }.thenByDescending { it.naturalCorrelation }
                    else
                        compareBy<FunVarRow> { it.cardinality }.thenBy { it.naturalCorrelation }
                rows.sortedWith(comparator)
            }
        }
        val sortEndNs = System.nanoTime()

        /* Write FUN and VAR. */
        val funVarStartNs = System.nanoTime()
        runCatching {
            val funFactory = SimpleGroupFactory(schemaFun)
            val varFactory = SimpleGroupFactory(schemaVar)
            openWriter(funParquetPath(model), schemaFun).use { funWriter ->
                openWriter(varParquetPath(model), schemaVar).use { varWriter ->
                    var rowCount = 0
                    for (row in orderedRows) {
                        funWriter.write(funFactory.newGroup().append("K", row.cardinality).append("Correlation", round6(row.naturalCorrelation)))
                        varWriter.write(varFactory.newGroup().append("K", row.cardinality).append("TopicsB64", row.topicsMaskB64))
                        rowCount++
                    }
                    logger.info("FUN/VAR rows={} writeOk", rowCount)
                }
            }
        }.onFailure { logger.warn("FUN/VAR Parquet write (streamed) failed", it) }
        val funVarEndNs = System.nanoTime()

        /* Write TOP if applicable. */
        val topStartNs = System.nanoTime()
        if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
            runCatching {
                val cache = topBlocksByKey[viewKey].orEmpty().toSortedMap()
                val topFactory = SimpleGroupFactory(schemaTop)
                var rowCount = 0
                openWriter(topParquetPath(model), schemaTop).use { topWriter ->
                    for ((_, block) in cache) {
                        for (row in block) {
                            topWriter.write(
                                topFactory.newGroup()
                                    .append("K", row.cardinality)
                                    .append("Correlation", round6(row.naturalCorrelation))
                                    .append("TopicsB64", row.topicsMaskB64)
                            )
                            rowCount++
                        }
                    }
                }
                logger.info("TOP rows={} writeOk", rowCount)
            }.onFailure { logger.warn("TOP Parquet write (streamed) failed", it) }
        }
        val topEndNs = System.nanoTime()

        /* Cleanup state. */
        funVarBuffersByKey.remove(viewKey)
        topBlocksByKey.remove(viewKey)
        labelIndexCache.clear()

        val totalEndNs = System.nanoTime()
        logger.info(
            "sort={}ms FUN+VAR={}ms TOP={}ms TOTAL={}ms",
            (sortEndNs - sortStartNs) / 1_000_000,
            (funVarEndNs - funVarStartNs) / 1_000_000,
            (topEndNs - topStartNs) / 1_000_000,
            (totalEndNs - totalStartNs) / 1_000_000
        )
    }

    /**
     * Write a generic UTF 8 table as Parquet without involving the CSV layer.
     * The first row is treated as a header. Column names are sanitized and deduplicated.
     */
    fun writeTable(rows: List<Array<String>>, outPath: String) {
        if (rows.isEmpty()) return
        if (System.getProperty("nbs.parquet.enabled", "true").equals("false", ignoreCase = true)) {
            logger.info("skipped (disabled): {}", outPath)
            return
        }

        val header = rows.first().map { it }
        val dataRows = rows.drop(1)
        val columnNames = sanitizeAndUniq(header)

        val schema = buildUtf8Schema("Table", columnNames)
        val groupFactory = SimpleGroupFactory(schema)

        runCatching {
            openWriter(outPath, schema).use { writer ->
                for (row in dataRows) {
                    val group = groupFactory.newGroup()
                    var columnIndex = 0
                    while (columnIndex < columnNames.size) { /* tight loop index by convention */
                        group.append(columnNames[columnIndex], normalizeCell(row.getOrNull(columnIndex) ?: ""))
                        columnIndex++
                    }
                    writer.write(group)
                }
            }
        }.onFailure { logger.warn("Parquet table write failed for $outPath", it) }
    }
}