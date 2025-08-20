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

/**
 * ParquetView
 * ===========
 *
 * Streaming-first Parquet writer:
 *  - **-Fun/-Var**: buffer rows during streaming; on close, sort globally by (K asc, corr asc)
 *    and write Parquet siblings. Correlations stored with **6 digits** of precision.
 *    - **-Var** stores **pipe-delimited topic labels** for bits set to 1 (e.g., "401|423|446").
 *  - **-Top**: replace-batch semantics; we keep a per-K 10-row block cache and write it at close.
 *
 * Non-streamed path:
 *  - `printSnapshot(...)` writes final Parquet directly from the provided solution lists.
 *
 * Helpers:
 *  - `writeTable(rows, outPath)` writes small "Final" tables (Aggregated/Info) to Parquet
 *    with a dynamic UTF-8 schema inferred from the header row. Decimal-looking cells are
 *    normalized to **6 digits**; everything is stored as UTF-8 strings for schema stability.
 *
 * Files land under the per-run container folder, in the **Parquet** subdirectory.
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

    /** Parquet schema for VAR: K:int32, Labels:utf8 (pipe-delimited labels for ones) */
    private fun schemaVar(): MessageType = Types.buildMessage()
        .required(INT32).named("K")
        .required(BINARY).`as`(LogicalTypeAnnotation.stringType()).named("Labels")
        .named("Var")

    /** Parquet schema for TOP: K:int32, Correlation:double, Topics:utf8 (pipe-delimited) */
    private fun schemaTop(): MessageType = Types.buildMessage()
        .required(INT32).named("K")
        .required(DOUBLE).named("Correlation")
        .required(BINARY).`as`(LogicalTypeAnnotation.stringType()).named("Topics")
        .named("Top")

    /** Generic UTF-8 schema builder for Final tables (columns derived from header). */
    private fun buildUtf8Schema(messageName: String, cols: List<String>): MessageType {
        val b = Types.buildMessage()
        cols.forEach { c ->
            b.required(BINARY).`as`(LogicalTypeAnnotation.stringType()).named(c)
        }
        return b.named(messageName)
    }

    /* ---------------- Writer factory (RawLocalFileSystem, Snappy, OVERWRITE) ---------------- */

    private fun openWriter(pathStr: String, schema: MessageType): ParquetWriter<Group> {
        val hadoopConf = Configuration().apply {
            // Avoid .crc sidecars on local disks.
            set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            setBoolean("fs.file.impl.disable.cache", true)
        }
        val output = HadoopOutputFile.fromPath(Path(pathStr), hadoopConf)
        return ExampleParquetWriter.builder(output)
            .withConf(hadoopConf)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withType(schema)
            .build()
    }

    /* ---------------- Local formatting helpers ---------------- */

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

    /** Convert incoming VAR bitstring to **pipe-delimited labels**. */
    private fun toLabelsLine(rawLine: String, labels: Array<String>): String {
        val t = rawLine.trim()
        if (t.isEmpty()) return ""
        if (t.indexOf('|') >= 0) return t

        val sb = StringBuilder()
        var first = true

        // Space-separated bits (e.g., "1 0 1 0")
        if (t.indexOf(' ') >= 0 || t.indexOf('\t') >= 0) {
            val parts = t.split(Regex("\\s+"))
            val n = minOf(parts.size, labels.size)
            for (i in 0 until n) if (parts[i] == "1") {
                if (!first) sb.append('|') else first = false
                sb.append(labels[i])
            }
            return sb.toString()
        }

        // Compact bits (e.g., "101001")
        val n = minOf(t.length, labels.size)
        for (i in 0 until n) if (t[i] == '1') {
            if (!first) sb.append('|') else first = false
            sb.append(labels[i])
        }
        return sb.toString()
    }

    /** Normalize topic strings like "[401 423]" → "401|423". */
    private fun normalizeTopics(rawTopics: String): String =
        rawTopics.trim()
            .removePrefix("[").removeSuffix("]")
            .split(Regex("[,\\s]+"))
            .filter { it.isNotEmpty() }
            .joinToString("|")

    /* ---------------- Streaming state ---------------- */

    private data class ViewKey(val dataset: String, val execution: Int, val target: String)

    /** Buffered rows for -Fun/-Var streaming (we write Parquet at close). */
    private data class FunVarRow(val k: Int, val corrExternal: Double, val labelsLine: String)

    private val funVarBuffers: MutableMap<ViewKey, MutableList<FunVarRow>> = mutableMapOf()

    /** Cached 10-row blocks for -Top streaming (we write Parquet at close). */
    private data class TopRow(val k: Int, val corrExternal: Double, val topicsPipe: String)

    private val topBlocks: MutableMap<ViewKey, MutableMap<Int, List<TopRow>>> = mutableMapOf()

    /* ---------------- Snapshot (non-streamed) ---------------- */

    /**
     * Write Parquet siblings directly from final solution lists.
     * Correlations are already in external view (model fixed them before returning).
     */
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

        /* VAR */
        runCatching {
            val labels = model.topicLabels
            val schema = schemaVar()
            val factory = SimpleGroupFactory(schema)
            openWriter(varParquetPath(model), schema).use { w ->
                for (s in allSolutions) {
                    val flags = (s as BestSubsetSolution).retrieveTopicStatus()
                    val ones = buildString {
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
                    val g = factory.newGroup()
                        .append("K", s.getCardinality().toInt())
                        .append("Labels", ones)
                    w.write(g)
                }
            }
        }.onFailure { logger.warn("VAR Parquet write failed", it) }

        /* TOP (Best/Worst only) */
        if (actualTarget != Constants.TARGET_AVERAGE) {
            runCatching {
                val schema = schemaTop()
                val factory = SimpleGroupFactory(schema)
                openWriter(topParquetPath(model), schema).use { w ->
                    for (s in topSolutions) {
                        val bss = s as BestSubsetSolution
                        val g = factory.newGroup()
                            .append("K", bss.getCardinality().toInt())
                            .append("Correlation", round6(bss.getCorrelation()))
                            .append("Topics", normalizeTopics(bss.getTopicLabelsFromTopicStatus()))
                        w.write(g)
                    }
                }
            }.onFailure { logger.warn("TOP Parquet write failed", it) }
        }
    }

    /* ---------------- Streaming hooks ---------------- */

    /**
     * Append an improved K row (external-view correlation) to in-memory buffers.
     * Note: the model streams internal corr for BEST as negative; we flip to external.
     */
    fun onAppendCardinality(model: DatasetModel, ev: CardinalityResult) {
        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val buf = funVarBuffers.getOrPut(viewKey) { mutableListOf() }

        val corrExternal = when (model.targetToAchieve) {
            Constants.TARGET_BEST  -> -ev.correlation   // BEST is negated internally
            else                   ->  ev.correlation
        }
        val labelsLine = toLabelsLine(ev.variableValuesCsvLine, model.topicLabels)

        buf += FunVarRow(k = ev.cardinality, corrExternal = corrExternal, labelsLine = labelsLine)
    }

    /**
     * Cache 10-row blocks for -Top with external-view correlation and pipe-delimited topics.
     * We only store complete 10-row blocks per K.
     */
    fun onReplaceTopBatch(model: DatasetModel, blocks: Map<Int, List<String>>) {
        if (blocks.isEmpty()) return

        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val cache = topBlocks.getOrPut(viewKey) { mutableMapOf() }

        for ((kFixed, lines) in blocks) {
            val parsed: List<TopRow> = lines.mapNotNull { line ->
                val p = line.split(',', limit = 3)
                if (p.size < 3) return@mapNotNull null
                val corr = p[1].trim().toDoubleOrNull() ?: return@mapNotNull null
                TopRow(k = kFixed, corrExternal = corr, topicsPipe = normalizeTopics(p[2]))
            }
            if (parsed.size == 10) cache[kFixed] = parsed
        }
    }

    /**
     * Close streaming: write FUN/VAR (globally sorted) and TOP from cached blocks.
     */
    fun closeStreams(model: DatasetModel) {
        val viewKey = ViewKey(model.datasetName, model.currentExecution, model.targetToAchieve)

        /* FUN */
        runCatching {
            val rows = funVarBuffers[viewKey]?.sortedWith(
                compareBy<FunVarRow>({ it.k }, { it.corrExternal })
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

        /* VAR */
        runCatching {
            val rows = funVarBuffers[viewKey]?.sortedWith(
                compareBy<FunVarRow>({ it.k }, { it.corrExternal })
            ).orEmpty()

            val schema = schemaVar()
            val factory = SimpleGroupFactory(schema)
            openWriter(varParquetPath(model), schema).use { w ->
                for (r in rows) {
                    val g = factory.newGroup()
                        .append("K", r.k)
                        .append("Labels", r.labelsLine)
                    w.write(g)
                }
            }
        }.onFailure { logger.warn("VAR Parquet write (streamed) failed", it) }

        /* TOP */
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
                                .append("Topics", row.topicsPipe)
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

    /**
     * Write a small table (e.g., AggregatedData/Info) to Parquet.
     *  - rows[0] is the header
     *  - every column is UTF-8 (stable schema)
     *  - decimal-looking cells normalized to **6 digits** (dot-locale)
     */
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
