package it.uniud.newbestsub.dataset.view

import it.uniud.newbestsub.dataset.DatasetModel
import it.uniud.newbestsub.dataset.model.CardinalityResult
import it.uniud.newbestsub.problem.BestSubsetSolution
import it.uniud.newbestsub.problem.getCardinality
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

// Minimal NIO fallback (no Hadoop winutils)
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
import java.util.Base64

/**
 * # ParquetView
 *
 * Streaming‑first Parquet writer for **FUN**, **VAR**, and **TOP** tables, plus
 * generic “final” tables (Aggregated/Info). Mirrors [CSVView] semantics.
 *
 * ## Responsibilities
 * - Resolve deterministic output paths via [ViewPaths].
 * - Stream or snapshot write:
 *   - **FUN**: `(K, Correlation)`
 *   - **VAR**: `(K, TopicsB64)` where topics are a Base64‑packed boolean mask.
 *   - **TOP** (BEST/WORST only): `(K, Correlation, TopicsB64)`.
 * - Write arbitrary final tables (header‑driven UTF‑8 schema).
 *
 * ## Platform
 * - Compression: `-Dnbs.parquet.codec=SNAPPY|GZIP|UNCOMPRESSED`; default SNAPPY (non‑Windows), GZIP (Windows).
 * - SNAPPY tempdir auto‑provisioning when needed.
 * - Windows without `winutils.exe`: falls back to a pure‑NIO [OutputFile].
 *
 * ## Ordering
 * - On close: FUN/VAR rows sorted by `(K asc, corr asc)` except WORST `(K asc, corr desc)`.
 *
 * ## Safety
 * - Toggle: `-Dnbs.parquet.enabled=false` makes methods no‑ops.
 * - Internal buffers cleared on [closeStreams].
 */

class ParquetView {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /* ---------------- Paths (Parquet subfolder) ---------------- */

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

    /* ---------------- Schemas ---------------- */

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

    private fun buildUtf8Schema(messageName: String, cols: List<String>): MessageType {
        val b = Types.buildMessage()
        cols.forEach { c -> b.required(BINARY).`as`(LogicalTypeAnnotation.stringType()).named(c) }
        return b.named(messageName)
    }

    /* ---------------- Compression / platform helpers ---------------- */

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

    private fun ensureSnappyTemp(outPathStr: String) {
        if (System.getProperty("org.xerial.snappy.tempdir") != null) return
        val candidates = listOfNotNull(
            runCatching { Paths.get(outPathStr).parent?.resolve("tmp-snappy") }.getOrNull(),
            runCatching { Paths.get(System.getProperty("java.io.tmpdir", "")).resolve("tmp-snappy") }.getOrNull(),
            runCatching { Paths.get(System.getProperty("user.home", ".")).resolve(".snappy") }.getOrNull()
        )
        for (p in candidates) {
            try {
                Files.createDirectories(p)
                System.setProperty("org.xerial.snappy.tempdir", p.toString())
                return
            } catch (_: Exception) { /* ignore */ }
        }
    }

    private fun hasWinutils(): Boolean {
        fun check(dir: String?): Boolean =
            !dir.isNullOrBlank() && Files.exists(Paths.get(dir, "bin", "winutils.exe"))
        return check(System.getenv("HADOOP_HOME")) || check(System.getProperty("hadoop.home.dir"))
    }

    /* ---------------- OutputFile fallback (Windows, no winutils) ---------------- */

    private class NioOutputFile(private val path: java.nio.file.Path) : OutputFile {
        override fun create(blockSizeHint: Long) = createOrOverwrite(blockSizeHint)
        override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream {
            Files.createDirectories(path.parent)
            val ch = FileChannel.open(
                path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
            )
            val singleByte = ByteBuffer.allocate(1) // reused per stream instance
            return object : PositionOutputStream() {
                private var pos = 0L
                override fun getPos(): Long = pos
                override fun write(b: Int) {
                    singleByte.clear()
                    singleByte.put(0, b.toByte())
                    ch.write(singleByte); pos += 1
                }
                override fun write(b: ByteArray, off: Int, len: Int) {
                    val bb = ByteBuffer.wrap(b, off, len)
                    val n = ch.write(bb); pos += n.toLong()
                }
                override fun flush() {}
                override fun close() { ch.close() }
            }
        }
        override fun supportsBlockSize(): Boolean = false
        override fun defaultBlockSize(): Long = 0L
    }

    /* ---------------- Writer factory (RawLocalFS or NIO fallback, OVERWRITE) ---------------- */

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

    /* ---------------- Local formatting helpers ---------------- */

    private fun round6(x: Double): Double = round(x * 1_000_000.0) / 1_000_000.0

    private val decimalFormat = DecimalFormat("0.000000", DecimalFormatSymbols(Locale.ROOT))
    private fun fmt6(x: Double): String = decimalFormat.format(x)

    private val nonAlNumbersUnderscoreRegex = Regex("[^A-Za-z0-9_]")
    private val tokenSplitRegex = Regex("[,;\\s|]+")

    private fun normalizeCell(cellText: String): String {
        val t = cellText.trim()
        if (t.isEmpty()) return ""
        val looksDecimal = t.indexOfAny(charArrayOf('.', 'e', 'E')) >= 0
        if (!looksDecimal) return t
        val v = t.toDoubleOrNull() ?: return t
        return fmt6(v)
    }

    private fun sanitizeAndUniq(rawNames: List<String>): List<String> {
        val used = mutableSetOf<String>()
        return rawNames.map { raw ->
            var base = raw.trim().ifEmpty { "col" }.replace(nonAlNumbersUnderscoreRegex, "_")
            if (base.firstOrNull()?.isDigit() == true) base = "_$base"
            var name = base
            var idx = 2
            while (!used.add(name)) { name = "${base}_$idx"; idx++ }
            name
        }
    }

    /* ------------- Encode helpers (shared encoder + cached labels map) ------------- */

    private val base64Encoder = Base64.getEncoder().withoutPadding()

    private data class LabelsKey(val identity: Int, val size: Int, val first: String?)
    private val labelIndexCache = mutableMapOf<LabelsKey, Map<String, Int>>()

    private fun indexByLabel(labels: Array<String>): Map<String, Int> {
        val key = LabelsKey(System.identityHashCode(labels), labels.size, labels.firstOrNull())
        return labelIndexCache.getOrPut(key) { labels.withIndex().associate { it.value to it.index } }
    }

    /**
     * Convert incoming topics field into canonical "B64:<...>".
     * Accepts labels, numeric indices, bitstrings, or already-encoded "B64:".
     */
    private fun fieldToB64(raw: String, labels: Array<String>): String {
        val t = raw.trim()
        if (t.startsWith("B64:")) return t
        return try {
            if (t.any { it == '|' || it == ';' || it == ',' || it == ' ' || it == '[' }) {
                val tokens = t.removePrefix("[").removeSuffix("]").split(tokenSplitRegex).filter { it.isNotBlank() }
                val byLabel = indexByLabel(labels)
                val mask = BooleanArray(labels.size)
                var matched = 0
                for (token in tokens) {
                    val idx = byLabel[token] ?: token.toIntOrNull()
                    if (idx != null && idx in 0 until mask.size) { mask[idx] = true; matched++ }
                }
                if (matched == 0) logger.warn("ParquetView fieldToB64] topics field did not match labels/indices; emitting empty mask. raw='{}'", raw)
                "B64:" + toBase64(mask)
            } else {
                val mask = BooleanArray(labels.size)
                val n = minOf(labels.size, t.length)
                for (i in 0 until n) mask[i] = (t[i] == '1')
                "B64:" + toBase64(mask)
            }
        } catch (_: Exception) {
            logger.warn("ParquetView fieldToB64] parse failed; emitting empty mask. raw='{}'", raw)
            "B64:"
        }
    }

    private fun toBase64(mask: BooleanArray): String {
        val words = (mask.size + 63) ushr 6
        val packed = LongArray(words)
        var bitInWord = 0
        var wordIndex = 0
        var accumulator = 0L
        for (i in mask.indices) {
            if (mask[i]) accumulator = accumulator or (1L shl bitInWord)
            bitInWord++
            if (bitInWord == 64) {
                packed[wordIndex++] = accumulator; accumulator = 0L; bitInWord = 0
            }
        }
        if (bitInWord != 0) packed[wordIndex] = accumulator
        val out = ByteArray(words * java.lang.Long.BYTES)
        var offset = 0
        for (word in packed) {
            var x = word
            for (i in 0 until java.lang.Long.BYTES) { out[offset + i] = (x and 0xFF).toByte(); x = x ushr 8 }
            offset += java.lang.Long.BYTES
        }
        return base64Encoder.encodeToString(out)
    }

    /* ---------------- Streaming state ---------------- */

    private data class ViewKey(val dataset: String, val execution: Int, val target: String)
    private data class FunVarRow(val k: Int, val naturalCorrelation: Double, val topicsMaskB64: String)
    private val funVarBuffers: MutableMap<ViewKey, MutableList<FunVarRow>> = mutableMapOf()

    private data class TopRow(val k: Int, val naturalCorrelation: Double, val topicsMaskB64: String)
    private val topBlocks: MutableMap<ViewKey, MutableMap<Int, List<TopRow>>> = mutableMapOf()

    /* ---------------- Snapshot (non-streamed) ---------------- */

    /**
     * Write a full snapshot of FUN/VAR (and TOP when applicable) to Parquet.
     * Uses **natural** correlation via `model.naturalCorrOf(...)`.
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
            openWriter(funParquetPath(model), schemaFun).use { funW ->
                openWriter(varParquetPath(model), schemaVar).use { varW ->
                    for (s in allSolutions) {
                        val k = s.getCardinality().toInt()
                        val corrNatural = round6(model.naturalCorrOf(s as BestSubsetSolution))
                        val mask = s.retrieveTopicStatus()
                        funW.write(funFactory.newGroup().append("K", k).append("Correlation", corrNatural))
                        varW.write(varFactory.newGroup().append("K", k).append("TopicsB64", "B64:" + toBase64(mask)))
                    }
                }
            }
        }.onFailure { logger.warn("FUN/VAR Parquet write (snapshot) failed", it) }

        if (actualTarget != Constants.TARGET_AVERAGE) {
            runCatching {
                val factory = SimpleGroupFactory(schemaTop)
                openWriter(topParquetPath(model), schemaTop).use { w ->
                    for (s in topSolutions) {
                        val bss = s as BestSubsetSolution
                        w.write(
                            factory.newGroup()
                                .append("K", bss.getCardinality().toInt())
                                .append("Correlation", round6(model.naturalCorrOf(bss)))
                                .append("TopicsB64", "B64:" + toBase64(bss.retrieveTopicStatus()))
                        )
                    }
                }
            }.onFailure { logger.warn("TOP Parquet write (snapshot) failed", it) }
        }
    }

    /* ---------------- Streaming hooks ---------------- */

    /**
     * Buffer one streamed FUN/VAR row (topics normalized to B64) for later write.
     * `ev.correlation` is already **natural**. Do NOT convert here.
     */
    fun onAppendCardinality(model: DatasetModel, ev: CardinalityResult) {
        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val buf = funVarBuffers.getOrPut(viewKey) { mutableListOf() }

        val naturalCorrelation = ev.correlation
        val topicsMaskB64 = fieldToB64(ev.variableValuesCsvLine, model.topicLabels)

        buf += FunVarRow(k = ev.cardinality, naturalCorrelation = naturalCorrelation, topicsMaskB64 = topicsMaskB64)
    }

    /**
     * Merge/replace per-K TOP blocks.
     * Lines are "K,Corr,Topics", where Corr is **natural** and producer already ordered by target
     * (BEST → desc, WORST → asc).
     */
    fun onReplaceTopBatch(model: DatasetModel, blocks: Map<Int, List<String>>) {
        if (blocks.isEmpty()) return
        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val cache = topBlocks.getOrPut(viewKey) { mutableMapOf() }

        for ((kFixed, lines) in blocks) {
            val parsed: List<TopRow> = lines.mapNotNull { line ->
                val p = line.split(',', limit = 3)
                if (p.size < 3) return@mapNotNull null
                val corrNatural = p[1].trim().toDoubleOrNull() ?: return@mapNotNull null
                TopRow(k = kFixed, naturalCorrelation = corrNatural, topicsMaskB64 = fieldToB64(p[2], model.topicLabels))
            }
            if (parsed.isNotEmpty()) cache[kFixed] = parsed
        }
    }

    /**
     * Flush all buffered rows to Parquet and clear internal state.
     * Ordering:
     * - FUN/VAR sorted by (K asc, corr asc) except WORST (K asc, corr desc).
     * - TOP written by ascending K and producer-provided block order.
     */
    fun closeStreams(model: DatasetModel) {
        if (System.getProperty("nbs.parquet.enabled", "true").equals("false", ignoreCase = true)) {
            val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)
            funVarBuffers.remove(viewKey); topBlocks.remove(viewKey); labelIndexCache.clear()
            logger.info("skipped (disabled).")
            return
        }

        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val totStart = System.nanoTime()

        val sortStart = System.nanoTime()
        val orderedRows: List<FunVarRow> = run {
            val rows = funVarBuffers[viewKey].orEmpty()
            if (rows.isEmpty()) emptyList()
            else {
                val cmp =
                    if (model.targetToAchieve == Constants.TARGET_WORST)
                        compareBy<FunVarRow> { it.k }.thenByDescending { it.naturalCorrelation }
                    else
                        compareBy<FunVarRow> { it.k }.thenBy { it.naturalCorrelation }
                rows.sortedWith(cmp)
            }
        }
        val sortEnd = System.nanoTime()

        val funVarStart = System.nanoTime()
        runCatching {
            val funFactory = SimpleGroupFactory(schemaFun)
            val varFactory = SimpleGroupFactory(schemaVar)
            openWriter(funParquetPath(model), schemaFun).use { funW ->
                openWriter(varParquetPath(model), schemaVar).use { varW ->
                    var count = 0
                    for (r in orderedRows) {
                        funW.write(funFactory.newGroup().append("K", r.k).append("Correlation", round6(r.naturalCorrelation)))
                        varW.write(varFactory.newGroup().append("K", r.k).append("TopicsB64", r.topicsMaskB64))
                        count++
                    }
                    logger.info("FUN/VAR rows={} writeOk", count)
                }
            }
        }.onFailure { logger.warn("FUN/VAR Parquet write (streamed) failed", it) }
        val funVarEnd = System.nanoTime()

        val topStart = System.nanoTime()
        if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
            runCatching {
                val cache = topBlocks[viewKey].orEmpty().toSortedMap()
                val factory = SimpleGroupFactory(schemaTop)
                var count = 0
                openWriter(topParquetPath(model), schemaTop).use { w ->
                    for ((_, block) in cache) {
                        for (row in block) {
                            w.write(
                                factory.newGroup()
                                    .append("K", row.k)
                                    .append("Correlation", round6(row.naturalCorrelation))
                                    .append("TopicsB64", row.topicsMaskB64)
                            )
                            count++
                        }
                    }
                }
                logger.info("TOP rows={} writeOk", count)
            }.onFailure { logger.warn("TOP Parquet write (streamed) failed", it) }
        }
        val topEnd = System.nanoTime()

        // Clean up state
        funVarBuffers.remove(viewKey)
        topBlocks.remove(viewKey)
        labelIndexCache.clear()

        val totEnd = System.nanoTime()
        logger.info(
            "sort={}ms FUN+VAR={}ms TOP={}ms TOTAL={}ms",
            (sortEnd - sortStart) / 1_000_000,
            (funVarEnd - funVarStart) / 1_000_000,
            (topEnd - topStart) / 1_000_000,
            (totEnd - totStart) / 1_000_000
        )
    }

    /* ---------------- Final-table writer (no CSV dependency) ---------------- */

    fun writeTable(rows: List<Array<String>>, outPath: String) {
        if (rows.isEmpty()) return
        if (System.getProperty("nbs.parquet.enabled", "true").equals("false", ignoreCase = true)) {
            logger.info("skipped (disabled): {}", outPath)
            return
        }

        val header = rows.first().map { it }
        val dataRows = rows.drop(1)
        val colNames = sanitizeAndUniq(header)

        val schema = buildUtf8Schema("Table", colNames)
        val factory = SimpleGroupFactory(schema)

        runCatching {
            openWriter(outPath, schema).use { w ->
                for (r in dataRows) {
                    val g = factory.newGroup()
                    var i = 0
                    while (i < colNames.size) {
                        g.append(colNames[i], normalizeCell(r.getOrNull(i) ?: ""))
                        i++
                    }
                    w.write(g)
                }
            }
        }.onFailure { logger.warn("Parquet table write failed for $outPath", it) }
    }


}
