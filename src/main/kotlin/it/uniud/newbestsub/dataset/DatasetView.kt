package it.uniud.newbestsub.dataset

import com.opencsv.CSVWriter
import it.uniud.newbestsub.problem.BestSubsetSolution
import it.uniud.newbestsub.problem.getCardinality
import it.uniud.newbestsub.problem.getCorrelation
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools
import it.uniud.newbestsub.utils.Tools.folderBaseName
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.fileoutput.SolutionListOutput
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext
import java.io.File
import java.io.FileWriter
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

/**
 * DatasetView
 * ===========
 *
 * Responsibilities:
 *  - Compute canonical output paths (shared by Best/Worst/Average of the same run).
 *  - Synchronous "print" of final results (-Fun / -Var / -Top).
 *  - Streaming I/O helpers:
 *      * -Fun / -Var: append rows live for visibility AND keep an in-memory buffer.
 *        On close, we sort the buffered rows globally by **(K asc, corr asc)** and
 *        rewrite both files in the same (sorted) order to keep them aligned.
 *      * -Top:       **REPLACE-BATCH** semantics via `replaceTopBatch(...)`: every time
 *        we get a batch of updated K blocks, we rebuild the entire -Top file: header
 *        + all known K-blocks ordered by K asc. Each K block is exactly 10 rows.
 */
class DatasetView {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    companion object {
        /** Single run folder cached so Best/Worst/Average land together */
        @Volatile
        private var cachedOutDirPath: String? = null
    }

    /* ---------------- Public path helpers (use these in the controller) ---------------- */

    fun getFunctionValuesFilePath(model: DatasetModel): String =
        ensureRunDir(model) + csvNameNoTs(fileBaseParts(model, model.targetToAchieve), Constants.FUNCTION_VALUES_FILE_SUFFIX)

    fun getVariableValuesFilePath(model: DatasetModel): String =
        ensureRunDir(model) + csvNameNoTs(fileBaseParts(model, model.targetToAchieve), Constants.VARIABLE_VALUES_FILE_SUFFIX)

    fun getTopSolutionsFilePath(model: DatasetModel): String =
        ensureRunDir(model) + csvNameNoTs(fileBaseParts(model, model.targetToAchieve), Constants.TOP_SOLUTIONS_FILE_SUFFIX)

    fun getInfoFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val target = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ensureRunDir(model) + csvNameNoTs(fileBaseParts(model, target), Constants.INFO_FILE_SUFFIX)
    }

    fun getAggregatedDataFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val target = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ensureRunDir(model) + csvNameNoTs(fileBaseParts(model, target), Constants.AGGREGATED_DATA_FILE_SUFFIX)
    }

    /* -------- Merged helpers -------- */

    fun getFunctionValuesMergedFilePath(model: DatasetModel): String =
        ensureRunDir(model) + csvNameNoTsMerged(fileBaseParts(model, model.targetToAchieve), Constants.FUNCTION_VALUES_FILE_SUFFIX)

    fun getVariableValuesMergedFilePath(model: DatasetModel): String =
        ensureRunDir(model) + csvNameNoTsMerged(fileBaseParts(model, model.targetToAchieve), Constants.VARIABLE_VALUES_FILE_SUFFIX)

    fun getTopSolutionsMergedFilePath(model: DatasetModel): String =
        ensureRunDir(model) + csvNameNoTsMerged(fileBaseParts(model, model.targetToAchieve), Constants.TOP_SOLUTIONS_FILE_SUFFIX)

    fun getAggregatedDataMergedFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val target = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ensureRunDir(model) + csvNameNoTsMerged(fileBaseParts(model, target), Constants.AGGREGATED_DATA_FILE_SUFFIX)
    }

    fun getInfoMergedFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val target = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ensureRunDir(model) + csvNameNoTsMerged(fileBaseParts(model, target), Constants.INFO_FILE_SUFFIX)
    }

    /* ---------------- Main printing for FUN / VAR / TOP (final snapshot) ---------------- */

    fun print(
        runResult: Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>>,
        model: DatasetModel
    ) {
        val (allSolutions, topSolutions, executionInfo) = runResult
        val (actualTarget, threadName, computingTime) = executionInfo
        val populationHelper = SolutionListOutput(allSolutions)

        logger.info("Starting to print result for execution on \"$threadName\" with target \"$actualTarget\" completed in ${computingTime}ms.")

        /* Single run folder shared by Best/Worst/Average */
        val outDirPath = ensureRunDir(model)

        /* Filenames have NO timestamp, but do include the executed target */
        val base = fileBaseParts(model, actualTarget)
        val funPath = outDirPath + csvNameNoTs(base, Constants.FUNCTION_VALUES_FILE_SUFFIX)
        val varPath = outDirPath + csvNameNoTs(base, Constants.VARIABLE_VALUES_FILE_SUFFIX)
        val topPath = outDirPath + csvNameNoTs(base, Constants.TOP_SOLUTIONS_FILE_SUFFIX)

        /* FUN / VAR (jMetal writer; no headers) */
        populationHelper
            .setVarFileOutputContext(DefaultFileOutputContext(varPath))
            .setFunFileOutputContext(DefaultFileOutputContext(funPath))
            .print()

        /* TOP only for Best/Worst (header + rows) */
        if (actualTarget != Constants.TARGET_AVERAGE) {
            val rows = mutableListOf(arrayOf("Cardinality", "Correlation", "Topics"))
            topSolutions.forEach { sol ->
                sol as BestSubsetSolution
                rows.add(
                    arrayOf(
                        sol.getCardinality().toString(),
                        sol.getCorrelation().toString(),
                        sol.getTopicLabelsFromTopicStatus()
                    )
                )
            }
            writeCsv(rows, topPath)
        }

        logger.info("Result for execution on \"$threadName\" with target \"$actualTarget\" available at:")
        logger.info("\"$varPath\"")
        logger.info("\"$funPath\"")
        if (actualTarget != Constants.TARGET_AVERAGE) logger.info("\"$topPath\"")
        logger.info("Print completed.")
    }

    fun writeCsv(data: List<Array<String>>, resultPath: String) {
        CSVWriter(FileWriter(resultPath)).use { writer -> writer.writeAll(data) }
    }

    /* ---------------- STREAMING SUPPORT ----------------
     * - FUN/VAR: append mode for visibility + in-memory buffering.
     *            On close, sort globally by (K asc, corr asc) and rewrite both files.
     * - TOP:     replace-batch mode via replaceTopBatch(...).
     * --------------------------------------------------- */

    data class StreamHandles(
        val funWriter: java.io.BufferedWriter,
        val varWriter: java.io.BufferedWriter
    )

    /** One handle set per (target, dataset) */
    private val openStreams = mutableMapOf<Pair<String, String>, StreamHandles>()

    /**
     * In-memory cache for -Top: (dataset, execution, target) -> (K -> 10 CSV lines).
     * Each Top update replaces the K block and we rewrite the entire -Top file sorted by K.
     */
    private data class TopKey(val dataset: String, val execution: Int, val target: String)
    private val topBlocks: MutableMap<TopKey, MutableMap<Int, List<String>>> = mutableMapOf()

    /**
     * In-memory buffer for -Fun/-Var streamed rows so that we can globally sort by
     * (K asc, corr asc) at the end and rewrite both files aligned.
     */
    private data class FunVarRow(val k: Int, val corr: Double, val funLine: String, val varLine: String)
    private val funVarBuffers: MutableMap<TopKey, MutableList<FunVarRow>> = mutableMapOf()

    fun openStreams(model: DatasetModel): StreamHandles {
        val key = model.targetToAchieve to model.datasetName
        return openStreams.getOrPut(key) {
            /* Ensure directory exists; open files in CREATE+TRUNCATE mode for a clean run, then append. */
            ensureRunDir(model)

            fun openFresh(path: String) = Files.newBufferedWriter(
                Paths.get(path),
                Charset.forName("UTF-8"),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            )

            val funPath = getFunctionValuesFilePath(model)
            val varPath = getVariableValuesFilePath(model)

            // Start fresh, then re-open in APPEND for the live stream
            openFresh(funPath).use { }
            openFresh(varPath).use { }

            fun openAppend(path: String) = Files.newBufferedWriter(
                Paths.get(path),
                Charset.forName("UTF-8"),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
            )

            StreamHandles(
                funWriter = openAppend(funPath),
                varWriter = openAppend(varPath)
            )
        }
    }

    /** Parse a jMetal-style FUN line "K corr" (e.g., "1.0 0.2419...") into (K, corr). */
    private fun parseFunLine(line: String): Pair<Int, Double>? {
        val parts = line.trim().split(Regex("\\s+"))
        if (parts.size < 2) return null
        val k = parts[0].toDoubleOrNull()?.toInt() ?: return null
        val corr = parts[1].toDoubleOrNull() ?: return null
        return k to corr
    }

    /**
     * Append a single improved row to -Fun/-Var (for live visibility) AND buffer it
     * so we can sort globally on close and rewrite both files aligned.
     *
     * The model may emit later improvements for smaller K after larger K have already
     * been appended; the final rewrite pass fixes global ordering.
     */
    fun appendCardinality(model: DatasetModel, ev: CardinalityResult) {
        val h = openStreams(model)

        // Stream live
        h.funWriter.write(ev.functionValuesCsvLine); h.funWriter.newLine(); h.funWriter.flush()
        h.varWriter.write(ev.variableValuesCsvLine); h.varWriter.newLine(); h.varWriter.flush()

        // Buffer for final global sort
        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val (k, corr) = parseFunLine(ev.functionValuesCsvLine) ?: return
        val buf = funVarBuffers.getOrPut(key) { mutableListOf() }
        buf += FunVarRow(k = k, corr = corr, funLine = ev.functionValuesCsvLine, varLine = ev.variableValuesCsvLine)
    }

    /**
     * Replace the K blocks contained in `blocks` and rewrite the whole -Top file
     * (header + all known blocks), ordered by K asc. Incoming lists must be exactly
     * 10 pre-sorted CSV lines per K.
     */
    fun replaceTopBatch(model: DatasetModel, blocks: Map<Int, List<String>>) {
        if (blocks.isEmpty()) return

        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        val cache = topBlocks.getOrPut(key) { mutableMapOf() }
        cache.putAll(blocks)

        // Rebuild -Top ordered by K asc (header + each block in order)
        val outPath = getTopSolutionsFilePath(model)
        val outFile = File(outPath)
        outFile.parentFile?.mkdirs()
        outFile.bufferedWriter(Charsets.UTF_8).use { w ->
            w.appendLine("Cardinality,Correlation,Topics")
            cache.toSortedMap().forEach { (_, lines) -> lines.forEach { w.appendLine(it) } }
        }
    }

    /**
     * Finalization hook:
     *  - Close stream writers
     *  - Perform the **global sort & rewrite** of -Fun and -Var by (K asc, corr asc)
     *    so both files are aligned and stable.
     */
    fun closeStreams(model: DatasetModel) {
        val k1 = model.targetToAchieve to model.datasetName
        // Close stream writers first to release handles
        openStreams.remove(k1)?.let { h ->
            try {
                h.funWriter.close()
            } catch (_: Exception) {}
            try {
                h.varWriter.close()
            } catch (_: Exception) {}
        }

        // Final sort + rewrite for FUN/VAR so both are globally ordered and aligned
        val key = TopKey(model.datasetName, model.currentExecution, model.targetToAchieve)
        funVarBuffers.remove(key)?.let { rows ->
            if (rows.isNotEmpty()) {
                val funPath = getFunctionValuesFilePath(model)
                val varPath = getVariableValuesFilePath(model)

                // Sort by (K asc, corr asc)
                val sorted = rows.sortedWith(compareBy<FunVarRow>({ it.k }, { it.corr }))

                // Rewrite both files atomically (no headers)
                File(funPath).bufferedWriter(Charsets.UTF_8).use { fw ->
                    sorted.forEach { fw.appendLine(it.funLine) }
                }
                File(varPath).bufferedWriter(Charsets.UTF_8).use { vw ->
                    sorted.forEach { vw.appendLine(it.varLine) }
                }
            }
        }
    }

    /* ---------------- Internals ---------------- */

    /**
     * Build the shared container folder via Tools (canonical logic) and cache it
     * so Best/Worst/Average of the same run write into the same directory.
     */
    private fun ensureRunDir(model: DatasetModel): String {
        cachedOutDirPath?.let { return it }

        val containerFolder = Tools.buildContainerFolderName(
            datasetName = model.datasetName,
            correlationMethod = model.correlationMethod,
            numberOfTopics = model.numberOfTopics,
            numberOfSystems = model.numberOfSystems,
            populationSize = model.populationSize,
            numberOfIterations = model.numberOfIterations,
            numberOfRepetitions = model.numberOfRepetitions,
            expansionCoefficient = model.expansionCoefficient,
            includePercentiles = (model.targetToAchieve == Constants.TARGET_AVERAGE),
            percentiles = model.percentiles.keys.sorted()
        )

        val out = Constants.NEWBESTSUB_PATH +
                "res" + Constants.PATH_SEPARATOR +
                containerFolder + Constants.PATH_SEPARATOR

        File(out).mkdirs()

        cachedOutDirPath = out
        return out
    }

    /**
     * Assemble the core name parts used by all files for a given run/target.
     * Percentiles token is added only for AVERAGE.
     */
    private fun fileBaseParts(model: DatasetModel, target: String): Array<String> {
        val includePct = (target == Constants.TARGET_AVERAGE)
        val pctKeys = model.percentiles.keys.sorted()
        val pctPart: String? = if (includePct && pctKeys.isNotEmpty())
            "pe${pctKeys.first()}_${pctKeys.last()}"
        else null

        val parts = mutableListOf<String>()
        parts += model.datasetName
        parts += model.correlationMethod
        parts += target
        if (model.numberOfTopics > 0) parts += "top${model.numberOfTopics}"
        if (model.numberOfSystems > 0) parts += "sys${model.numberOfSystems}"
        if (model.populationSize > 0) parts += "po${model.populationSize}"
        if (model.numberOfIterations > 0) parts += "i${model.numberOfIterations}"
        if (model.numberOfRepetitions > 0) parts += "r${model.numberOfRepetitions}"
        if (model.expansionCoefficient > 0) parts += "mx${model.expansionCoefficient}"
        if (model.currentExecution > 0) parts += "ex${model.currentExecution}"
        if (pctPart != null) parts += pctPart

        return parts.toTypedArray()
    }

    private fun csvNameNoTs(baseParts: Array<String>, suffix: String): String =
        folderBaseName(*baseParts) + Constants.FILE_NAME_SEPARATOR + suffix + Constants.CSV_FILE_EXTENSION

    private fun csvNameNoTsMerged(baseParts: Array<String>, suffix: String): String =
        folderBaseName(*baseParts) + Constants.FILE_NAME_SEPARATOR + suffix +
                Constants.FILE_NAME_SEPARATOR + Constants.MERGED_RESULT_FILE_SUFFIX + Constants.CSV_FILE_EXTENSION
}
