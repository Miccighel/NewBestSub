package it.uniud.newbestsub.dataset.view

import it.uniud.newbestsub.dataset.DatasetModel
import it.uniud.newbestsub.dataset.CardinalityResult
import it.uniud.newbestsub.problem.BestSubsetSolution
import it.uniud.newbestsub.problem.getCardinality
import it.uniud.newbestsub.problem.getCorrelation
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

    /* ---------------- Schemas (annotate all string-like as UTF‑8) ---------------- */

    /** Parquet schema for FUN: K:int32, Correlation:double */
    private val SCHEMA_FUN: MessageType = Types.buildMessage()
        .required(INT32).named("K")
        .required(DOUBLE).named("Correlation")
        .named("Fun")

    /** Parquet schema for VAR: K:int32, TopicsB64:utf8 (Base64-packed mask) */
    private val SCHEMA_VAR: MessageType = Types.buildMessage()
        .required(INT32).named("K")
        .required(BINARY).`as`(LogicalTypeAnnotation.stringType()).named("TopicsB64")
        .named("Var")

    /** Parquet schema for TOP: K:int32, Correlation:double, TopicsB64:utf8 */
    private val SCHEMA_TOP: MessageType = Types.buildMessage()
        .required(INT32).named("K")
        .required(DOUBLE).named("Correlation")
        .required(BINARY).`as`(LogicalTypeAnnotation.stringType()).named("TopicsB64")
        .named("Top")

    /** Generic UTF-8 schema builder for Final tables (columns derived from header). */
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

    /** Ensure a writable temp dir for snappy-java when using SNAPPY (helps on Windows). */
    private fun ensureSnappyTemp(outPathStr: String) {
        if (System.getProperty("org.xerial.snappy.tempdir") != null) return
        val candidates = listOfNotNull(
            runCatching { Paths.get(outPathStr).parent?.resolve("tmp-snappy") }.getOrNull(),
            runCatching { Paths.get(System.getProperty("java.io.tmpdir", "")).resolve("tmp-snappy") }.getOrNull(),
            runCatching { Paths.get(System.getProperty("user.home", "."))?.resolve(".snappy") }.getOrNull()
        )
        for (p in candidates) {
            try {
                Files.createDirectories(p)
                System.setProperty("org.xerial.snappy.tempdir", p.toString())
                return
            } catch (_: Exception) {
            }
        }
    }

    private fun hasWinutils(): Boolean {
        fun check(dir: String?): Boolean =
            !dir.isNullOrBlank() && Files.exists(Paths.get(dir, "bin", "winutils.exe"))
        return check(System.getenv("HADOOP_HOME")) || check(System.getProperty("hadoop.home.dir"))
    }

    /* -------- NIO OutputFile (fallback when winutils is missing) -------- */

    private class NioOutputFile(private val path: java.nio.file.Path) : OutputFile {
        override fun create(blockSizeHint: Long) = createOrOverwrite(blockSizeHint)
        override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream {
            Files.createDirectories(path.parent)
            val ch = FileChannel.open(
                path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
            )
            // allocate the 1-byte buffer ONCE per stream instance
            val singleByte = ByteBuffer.allocate(1)
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
            .withRowGroupSize(8 * 1024 * 1024)
            .withDictionaryEncoding(true) // good for string-like columns (TopicsB64)
            .withType(schema)
            .build()
    }

    /* ---------------- Local formatting helpers ---------------- */

    private fun round6(x: Double): Double = round(x * 1_000_000.0) / 1_000_000.0

    private fun normalizeCell(cellText: String): String {
        val t = cellText.trim()
        if (t.isEmpty()) return ""
        val looksDecimal = t.contains('.') || t.contains('e') || t.contains('E')
        if (!looksDecimal) return t
        val v = t.toDoubleOrNull() ?: return t
        return String.format(Locale.ROOT, "%.6f", v)
    }

    private fun sanitizeAndUniq(rawNames: List<String>): List<String> {
        val used = mutableSetOf<String>()
        return rawNames.map { raw ->
            var base = raw.trim().ifEmpty { "col" }.replace(Regex("[^A-Za-z0-9_]"), "_")
            if (base.firstOrNull()?.isDigit() == true) base = "_$base"
            var name = base
            var idx = 2
            while (!used.add(name)) { name = "${base}_$idx"; idx++ }
            name
        }
    }

    /** Convert any incoming topics field (labels/indices/bits/B64) → "B64:<...>" */
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
                    if (idx is Int && idx >= 0 && idx < mask.size) {
                        mask[idx] = true; matched++
                    }
                }
                if (matched == 0) logger.warn("ParquetView fieldToB64] TOP topics field did not match labels/indices; emitting empty mask. raw='{}'", raw)
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
        return java.util.Base64.getEncoder().withoutPadding().encodeToString(out)
    }

    /* ---------------- Streaming state ---------------- */

    private data class ViewKey(val dataset: String, val execution: Int, val target: String)
    private data class FunVarRow(val k: Int, val corrExternal: Double, val topicsB64: String)
    private val funVarBuffers: MutableMap<ViewKey, MutableList<FunVarRow>> = mutableMapOf()

    private data class TopRow(val k: Int, val corrExternal: Double, val topicsB64: String)
    private val topBlocks: MutableMap<ViewKey, MutableMap<Int, List<TopRow>>> = mutableMapOf()

    /* ---------------- Snapshot (non-streamed) ---------------- */

    fun printSnapshot(
        model: DatasetModel,
        allSolutions: List<BinarySolution>,
        topSolutions: List<BinarySolution>,
        actualTarget: String
    ) {
        if (System.getProperty("nbs.parquet.enabled", "true").equals("false", ignoreCase = true)) {
            logger.info("[ParquetView] disabled via -Dnbs.parquet.enabled=false (printSnapshot skipped)")
            return
        }

        /* FUN + VAR in a single pass (avoid looping twice over allSolutions) */
        runCatching {
            val funFactory = SimpleGroupFactory(SCHEMA_FUN)
            val varFactory = SimpleGroupFactory(SCHEMA_VAR)
            openWriter(funParquetPath(model), SCHEMA_FUN).use { funW ->
                openWriter(varParquetPath(model), SCHEMA_VAR).use { varW ->
                    for (s in allSolutions) {
                        val k = s.getCardinality().toInt()
                        val corr = round6(s.getCorrelation())
                        val mask = (s as BestSubsetSolution).retrieveTopicStatus()
                        val funRow = funFactory.newGroup().append("K", k).append("Correlation", corr)
                        val varRow = varFactory.newGroup().append("K", k).append("TopicsB64", "B64:" + toBase64(mask))
                        funW.write(funRow)
                        varW.write(varRow)
                    }
                }
            }
        }.onFailure { logger.warn("FUN/VAR Parquet write (snapshot) failed", it) }

        /* TOP (Best/Worst only) */
        if (actualTarget != Constants.TARGET_AVERAGE) {
            runCatching {
                val factory = SimpleGroupFactory(SCHEMA_TOP)
                openWriter(topParquetPath(model), SCHEMA_TOP).use { w ->
                    for (s in topSolutions) {
                        val bss = s as BestSubsetSolution
                        val g = factory.newGroup()
                            .append("K", bss.getCardinality().toInt())
                            .append("Correlation", round6(bss.getCorrelation()))
                            .append("TopicsB64", "B64:" + toBase64(bss.retrieveTopicStatus()))
                        w.write(g)
                    }
                }
            }.onFailure { logger.warn("TOP Parquet write (snapshot) failed", it) }
        }
    }

    /* ---------------- Streaming hooks ---------------- */

    fun onAppendCardinality(model: DatasetModel, ev: CardinalityResult) {
        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val buf = funVarBuffers.getOrPut(viewKey) { mutableListOf() }

        val corrExternal = when (model.targetToAchieve) {
            Constants.TARGET_BEST -> -ev.correlation
            else -> ev.correlation
        }
        val topicsB64 = fieldToB64(ev.variableValuesCsvLine, model.topicLabels)

        buf += FunVarRow(k = ev.cardinality, corrExternal = corrExternal, topicsB64 = topicsB64)
    }

    fun onReplaceTopBatch(model: DatasetModel, blocks: Map<Int, List<String>>) {
        if (blocks.isEmpty()) return
        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val cache = topBlocks.getOrPut(viewKey) { mutableMapOf() }

        for ((kFixed, lines) in blocks) {
            val parsed: List<TopRow> = lines.mapNotNull { line ->
                val p = line.split(',', limit = 3)
                if (p.size < 3) return@mapNotNull null
                val corr = p[1].trim().toDoubleOrNull() ?: return@mapNotNull null
                TopRow(k = kFixed, corrExternal = corr, topicsB64 = fieldToB64(p[2], model.topicLabels))
            }
            if (parsed.size == 10) cache[kFixed] = parsed
        }
    }

    fun closeStreams(model: DatasetModel) {
        if (System.getProperty("nbs.parquet.enabled", "true").equals("false", ignoreCase = true)) {
            val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)
            funVarBuffers.remove(viewKey); topBlocks.remove(viewKey)
            logger.info("[ParquetView.closeStreams] skipped (disabled).")
            return
        }

        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val totStart = System.nanoTime()

        /* Compute ordered FUN/VAR rows ONCE (shared by both writers) */
        val sortStart = System.nanoTime()
        val orderedRows: List<FunVarRow> = run {
            val rows = funVarBuffers[viewKey].orEmpty()
            if (rows.isEmpty()) emptyList()
            else {
                val cmp =
                    if (model.targetToAchieve == Constants.TARGET_WORST)
                        compareBy<FunVarRow>({ it.k }).thenByDescending { it.corrExternal }
                    else
                        compareBy<FunVarRow>({ it.k }).thenBy { it.corrExternal }
                rows.sortedWith(cmp)
            }
        }
        val sortEnd = System.nanoTime()

        /* FUN + VAR: dual-writer pass (avoid duplicating sort + loops) */
        val funVarStart = System.nanoTime()
        runCatching {
            val funFactory = SimpleGroupFactory(SCHEMA_FUN)
            val varFactory = SimpleGroupFactory(SCHEMA_VAR)
            openWriter(funParquetPath(model), SCHEMA_FUN).use { funW ->
                openWriter(varParquetPath(model), SCHEMA_VAR).use { varW ->
                    var count = 0
                    for (r in orderedRows) {
                        funW.write(funFactory.newGroup().append("K", r.k).append("Correlation", round6(r.corrExternal)))
                        varW.write(varFactory.newGroup().append("K", r.k).append("TopicsB64", r.topicsB64))
                        count++
                    }
                    logger.info("[ParquetView.closeStreams] FUN/VAR rows={} writeOk", count)
                }
            }
        }.onFailure { logger.warn("FUN/VAR Parquet write (streamed) failed", it) }
        val funVarEnd = System.nanoTime()

        /* TOP */
        val topStart = System.nanoTime()
        if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
            runCatching {
                val cache = topBlocks[viewKey].orEmpty().toSortedMap() // K asc
                val factory = SimpleGroupFactory(SCHEMA_TOP)
                var count = 0
                openWriter(topParquetPath(model), SCHEMA_TOP).use { w ->
                    for ((_, block) in cache) {
                        for (row in block) {
                            w.write(
                                factory.newGroup()
                                    .append("K", row.k)
                                    .append("Correlation", round6(row.corrExternal))
                                    .append("TopicsB64", row.topicsB64)
                            )
                            count++
                        }
                    }
                }
                logger.info("[ParquetView.closeStreams] TOP rows={} writeOk", count)
            }.onFailure { logger.warn("TOP Parquet write (streamed) failed", it) }
        }
        val topEnd = System.nanoTime()

        // Clean up state
        funVarBuffers.remove(viewKey)
        topBlocks.remove(viewKey)

        val totEnd = System.nanoTime()
        logger.info(
            "[ParquetView.closeStreams] sort={}ms FUN+VAR={}ms TOP={}ms TOTAL={}ms",
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
            logger.info("[ParquetView.writeTable] skipped (disabled): {}", outPath)
            return
        }

        val header = rows.first().map { it ?: "" }
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
