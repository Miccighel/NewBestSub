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

// NIO OutputFile fallback (Windows without winutils)
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

/**
 * ParquetView
 * ===========
 *
 * Streaming-first Parquet writer:
 *  - **-Fun/-Var**: buffer rows during streaming; on close, sort globally by (K asc, corr asc|desc)
 *    and write Parquet siblings. Correlations stored with **6 digits** of precision.
 *    - **-Var** stores a compact **Base64 bitmask** (field: BitsB64), **no "B64:" prefix**.
 *  - **-Top**: replace-batch semantics; we keep a per-K 10-row block cache and write it at close.
 *    - **-Top** stores **TopicsB64** (Base64 bitmask), **no "B64:" prefix**.
 *
 * Non-streamed path:
 *  - `printSnapshot(...)` writes final Parquet directly from the provided solution lists.
 *
 * Helpers:
 *  - `writeTable(rows, outPath)` writes small "Final" tables (Aggregated/Info) to Parquet
 *    with a dynamic UTF-8 schema inferred from the header row. Decimal-looking cells are
 *    normalized to **6 digits**; everything is stored as UTF-8 strings for schema stability.
 *
 * Cross-platform compression:
 *  - Default **GZIP on Windows**, **SNAPPY elsewhere** (override via -Dnbs.parquet.codec=SNAPPY|GZIP|UNCOMPRESSED).
 *  - If SNAPPY is used, we try to set a writable temp dir for the native lib.
 *
 * On Windows without winutils.exe, we use a fallback NIO OutputFile that doesn't require Hadoop local FS.
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

    /** Parquet schema for FUN: K:int32, Correlation:double */
    private fun schemaFun(): MessageType = Types.buildMessage()
        .required(INT32).named("K")
        .required(DOUBLE).named("Correlation")
        .named("Fun")

    /**
     * Parquet schema for VAR (compact):
     *  - K:int32
     *  - BitsB64:utf8   (Base64 bitmask WITHOUT "B64:" prefix)
     */
    private fun schemaVar(): MessageType = Types.buildMessage()
        .required(INT32).named("K")
        .required(BINARY).`as`(LogicalTypeAnnotation.stringType()).named("BitsB64")
        .named("Var")

    /**
     * Parquet schema for TOP (compact):
     *  - K:int32
     *  - Correlation:double
     *  - TopicsB64:utf8 (Base64 bitmask WITHOUT "B64:" prefix)
     */
    private fun schemaTop(): MessageType = Types.buildMessage()
        .required(INT32).named("K")
        .required(DOUBLE).named("Correlation")
        .required(BINARY).`as`(LogicalTypeAnnotation.stringType()).named("TopicsB64")
        .named("Top")

    /** Generic UTF-8 schema builder for Final tables (columns derived from header). */
    private fun buildUtf8Schema(messageName: String, cols: List<String>): MessageType {
        val b = Types.buildMessage()
        cols.forEach { c ->
            b.required(BINARY).`as`(LogicalTypeAnnotation.stringType()).named(c)
        }
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
                // try next candidate
            }
        }
    }

    /** Check if winutils.exe is available (HADOOP_HOME or -Dhadoop.home.dir). */
    private fun hasWinutils(): Boolean {
        fun check(dir: String?): Boolean =
            !dir.isNullOrBlank() && Files.exists(Paths.get(dir, "bin", "winutils.exe"))
        return check(System.getenv("HADOOP_HOME")) || check(System.getProperty("hadoop.home.dir"))
    }

    /* --------- Minimal NIO OutputFile (fallback when winutils is missing) --------- */

    private class NioOutputFile(private val path: java.nio.file.Path) : OutputFile {
        override fun create(blockSizeHint: Long) = createOrOverwrite(blockSizeHint)
        override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream {
            Files.createDirectories(path.parent)
            val ch = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            )
            return object : PositionOutputStream() {
                private var pos = 0L
                override fun getPos(): Long = pos
                override fun write(b: Int) {
                    val bb = ByteBuffer.allocate(1)
                    bb.put(0, b.toByte())
                    ch.write(bb)
                    pos += 1
                }
                override fun write(b: ByteArray, off: Int, len: Int) {
                    val bb = ByteBuffer.wrap(b, off, len)
                    val n = ch.write(bb)
                    pos += n.toLong()
                }
                override fun flush() { /* no-op */ }
                override fun close() { ch.close() }
            }
        }
        override fun supportsBlockSize(): Boolean = false
        override fun defaultBlockSize(): Long = 0L
    }

    /* ---------------- Writer factory (RawLocalFS or NIO fallback, OVERWRITE) ---------------- */

    private fun openWriter(pathStr: String, schema: MessageType): ParquetWriter<Group> {
        val codec = chooseCodec()
        if (codec == CompressionCodecName.SNAPPY) {
            runCatching { ensureSnappyTemp(pathStr) }
        }

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
            .withType(schema)
            .build()
    }

    /* ---------------- Local formatting + compact bitmask helpers ---------------- */

    /** Round a double to 6 digits for storage (CSV & Parquet policy). */
    private fun round6(x: Double): Double = round(x * 1_000_000.0) / 1_000_000.0

    /** Keep integers as-is; format decimals to 6 digits with dot-locale for Final tables. */
    private fun normalizeCell(cellText: String): String {
        val t = cellText.trim()
        if (t.isEmpty()) return ""
        val looksDecimal = t.contains('.') || t.contains('e') || t.contains('E')
        if (!looksDecimal) return t
        val v = t.toDoubleOrNull() ?: return t
        return String.format(Locale.ROOT, "%.6f", v)
    }

    /** Make Parquet-friendly, unique column names (sanitize + de-dup). */
    private fun sanitizeAndUniq(rawNames: List<String>): List<String> {
        val used = mutableSetOf<String>()
        return rawNames.map { raw ->
            var base = raw.trim().ifEmpty { "col" }
            base = base.replace(Regex("[^A-Za-z0-9_]"), "_")
            if (base.firstOrNull()?.isDigit() == true) base = "_$base"
            var name = base
            var idx = 2
            while (!used.add(name)) {
                name = "${base}_$idx"
                idx++
            }
            name
        }
    }

    /** Normalize topics text like "[401 423]" → "401|423". */
    private fun normalizeTopics(rawTopics: String): String =
        rawTopics.trim()
            .removePrefix("[").removeSuffix("]")
            .split(Regex("[,\\s]+"))
            .filter { it.isNotEmpty() }
            .joinToString("|")

    /* ---- Base64 packing (little-endian 64-bit words, LSB-first bits) ---- */

    /** BooleanArray → Base64 payload (WITHOUT "B64:" prefix). */
    private fun booleanMaskToBase64(mask: BooleanArray): String {
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
        return java.util.Base64.getEncoder().withoutPadding().encodeToString(bytes)
    }

    /** Convert labels (pipe-delimited) → Base64 payload (WITHOUT "B64:" prefix). */
    private fun labelsPipeToBase64(labelsPipe: String, topicLabels: Array<String>): String {
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
        return booleanMaskToBase64(mask)
    }

    /** Robust converter: B64 / bitstrings / labels / indices → bare Base64 payload. */
    private fun topicsFieldToBase64(raw: String, topicLabels: Array<String>): String {
        val t = raw.trim()
        if (t.startsWith("B64:")) return t.substring(4)

        // raw 0/1 bitstring (spaces allowed)
        val bitsOnly = t.replace(" ", "")
        if (bitsOnly.isNotEmpty() && bitsOnly.all { it == '0' || it == '1' }) {
            val n = topicLabels.size
            val mask = BooleanArray(n)
            val m = kotlin.math.min(bitsOnly.length, n)
            for (i in 0 until m) if (bitsOnly[i] == '1') mask[i] = true
            return booleanMaskToBase64(mask)
        }

        // Bracketed or delimited labels/indices
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
        return booleanMaskToBase64(mask)
    }

    /** Accepts either "B64:..." or labels/bitstrings; returns bare Base64 for VAR rows. */
    private fun anyVarToBase64(raw: String, topicLabels: Array<String>): String {
        val t = raw.trim()
        return when {
            t.startsWith("B64:") -> t.substring(4)
            t.contains('|') || t.contains(',') || t.contains(' ') || t.contains('[') -> {
                val pipe = if (t.contains('|')) t else normalizeTopics(t)
                labelsPipeToBase64(pipe, topicLabels)
            }
            // raw bitstring without spaces?
            t.all { it == '0' || it == '1' } -> {
                val n = topicLabels.size
                val mask = BooleanArray(n)
                val m = kotlin.math.min(t.length, n)
                for (i in 0 until m) if (t[i] == '1') mask[i] = true
                booleanMaskToBase64(mask)
            }
            else -> t // assume already bare Base64
        }
    }

    /* ---------------- Streaming state ---------------- */

    private data class ViewKey(val dataset: String, val execution: Int, val target: String)

    /** Buffered rows for -Fun/-Var streaming (we write Parquet at close). */
    private data class FunVarRow(val k: Int, val corrExternal: Double, val base64Bits: String)

    private val funVarBuffers: MutableMap<ViewKey, MutableList<FunVarRow>> = mutableMapOf()

    /** Cached 10-row blocks for -Top streaming (we write Parquet at close). */
    private data class TopRow(val k: Int, val corrExternal: Double, val topicsB64: String)

    private val topBlocks: MutableMap<ViewKey, MutableMap<Int, List<TopRow>>> = mutableMapOf()

    /* ---------------- Snapshot (non-streamed) ---------------- */

    fun printSnapshot(
        model: DatasetModel,
        allSolutions: List<BinarySolution>,
        topSolutions: List<BinarySolution>,
        actualTarget: String
    ) {
        /* FUN */
        runCatching {
            val schema = schemaFun()
            val factory = SimpleGroupFactory(schema)
            openWriter(funParquetPath(model), schema).use { w ->
                for (s in allSolutions) {
                    val g = factory.newGroup()
                        .append("K", s.getCardinality().toInt())
                        .append("Correlation", round6(s.getCorrelation()))
                    w.write(g)
                }
            }
        }.onFailure { logger.warn("FUN Parquet write failed", it) }

        /* VAR (compact Base64) */
        runCatching {
            val schema = schemaVar()
            val factory = SimpleGroupFactory(schema)
            openWriter(varParquetPath(model), schema).use { w ->
                for (s in allSolutions) {
                    val flags = (s as BestSubsetSolution).retrieveTopicStatus()
                    val b64 = booleanMaskToBase64(flags)                  // no "B64:" prefix
                    val g = factory.newGroup()
                        .append("K", s.getCardinality().toInt())
                        .append("BitsB64", b64)
                    w.write(g)
                }
            }
        }.onFailure { logger.warn("VAR Parquet write failed", it) }

        /* TOP (Best/Worst only, compact TopicsB64) */
        if (actualTarget != Constants.TARGET_AVERAGE) {
            runCatching {
                val schema = schemaTop()
                val factory = SimpleGroupFactory(schema)
                openWriter(topParquetPath(model), schema).use { w ->
                    for (s in topSolutions) {
                        val bss = s as BestSubsetSolution
                        val flags = bss.retrieveTopicStatus()
                        val topicsB64 = booleanMaskToBase64(flags)        // no "B64:" prefix
                        val g = factory.newGroup()
                            .append("K", bss.getCardinality().toInt())
                            .append("Correlation", round6(bss.getCorrelation()))
                            .append("TopicsB64", topicsB64)
                        w.write(g)
                    }
                }
            }.onFailure { logger.warn("TOP Parquet write failed", it) }
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

        // Source may already be "B64:..." (new CSVView/model), or legacy labels/bitstrings.
        val base64Bits = anyVarToBase64(ev.variableValuesCsvLine, model.topicLabels)

        buf += FunVarRow(k = ev.cardinality, corrExternal = corrExternal, base64Bits = base64Bits)
    }

    fun onReplaceTopBatch(model: DatasetModel, blocks: Map<Int, List<String>>) {
        if (blocks.isEmpty()) return

        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val cache = topBlocks.getOrPut(viewKey) { mutableMapOf() }

        for ((kFixed, lines) in blocks) {
            val parsed: List<TopRow> = lines.mapNotNull { line ->
                // incoming lines are "K,Correlation,Topics" where Topics may be B64, labels, indices, or bitstrings
                val p = line.split(',', limit = 3)
                if (p.size < 3) return@mapNotNull null
                val corr = p[1].trim().toDoubleOrNull() ?: return@mapNotNull null
                val topicsB64 = topicsFieldToBase64(p[2], model.topicLabels)
                TopRow(k = kFixed, corrExternal = corr, topicsB64 = topicsB64)
            }
            if (parsed.size == 10) cache[kFixed] = parsed
        }
    }

    fun closeStreams(model: DatasetModel) {
        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)

        /* FUN */
        runCatching {
            val rows = funVarBuffers[viewKey]?.sortedWith(
                if (model.targetToAchieve == Constants.TARGET_WORST)
                    compareBy<FunVarRow>({ it.k }).thenByDescending { it.corrExternal }
                else
                    compareBy<FunVarRow>({ it.k }).thenBy { it.corrExternal }
            ).orEmpty()

            val schema = schemaFun()
            val factory = SimpleGroupFactory(schema)
            openWriter(funParquetPath(model), schema).use { w ->
                for (r in rows) {
                    val g = factory.newGroup()
                        .append("K", r.k)
                        .append("Correlation", round6(r.corrExternal))
                    w.write(g)
                }
            }
        }.onFailure { logger.warn("FUN Parquet write (streamed) failed", it) }

        /* VAR (compact Base64) */
        runCatching {
            val rows = funVarBuffers[viewKey]?.sortedWith(
                if (model.targetToAchieve == Constants.TARGET_WORST)
                    compareBy<FunVarRow>({ it.k }).thenByDescending { it.corrExternal }
                else
                    compareBy<FunVarRow>({ it.k }).thenBy { it.corrExternal }
            ).orEmpty()

            val schema = schemaVar()
            val factory = SimpleGroupFactory(schema)
            openWriter(varParquetPath(model), schema).use { w ->
                for (r in rows) {
                    val g = factory.newGroup()
                        .append("K", r.k)
                        .append("BitsB64", r.base64Bits) // bare Base64, no "B64:" prefix
                    w.write(g)
                }
            }
        }.onFailure { logger.warn("VAR Parquet write (streamed) failed", it) }

        /* TOP (compact TopicsB64) */
        if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
            runCatching {
                val cache = topBlocks[viewKey].orEmpty().toSortedMap() // K asc
                val schema = schemaTop()
                val factory = SimpleGroupFactory(schema)
                openWriter(topParquetPath(model), schema).use { w ->
                    for ((_, block) in cache) {
                        for (row in block) {
                            val g = factory.newGroup()
                                .append("K", row.k)
                                .append("Correlation", round6(row.corrExternal))
                                .append("TopicsB64", row.topicsB64) // bare Base64, no prefix
                            w.write(g)
                        }
                    }
                }
            }.onFailure { logger.warn("TOP Parquet write (streamed) failed", it) }
        }

        // Clean up state
        funVarBuffers.remove(viewKey)
        topBlocks.remove(viewKey)
    }

    /* ---------------- Final-table writer (no CSV dependency) ---------------- */

    fun writeTable(rows: List<Array<String>>, outPath: String) {
        if (rows.isEmpty()) return

        val header = rows.first().map { it ?: "" }
        val dataRows = rows.drop(1)
        val colNames = sanitizeAndUniq(header)

        val schema = buildUtf8Schema("Table", colNames)
        val factory = SimpleGroupFactory(schema)

        runCatching {
            openWriter(outPath, schema).use { w ->
                for (r in dataRows) {
                    val normCells = Array(colNames.size) { idx ->
                        val raw = r.getOrNull(idx) ?: ""
                        normalizeCell(raw)
                    }
                    val g = factory.newGroup()
                    for (i in colNames.indices) g.append(colNames[i], normCells[i])
                    w.write(g)
                }
            }
        }.onFailure { logger.warn("Parquet table write failed for $outPath", it) }
    }
}
