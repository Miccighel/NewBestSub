package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.dataset.model.CardinalityResult
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.dataset.view.CSVView
import it.uniud.newbestsub.dataset.view.ParquetView
import it.uniud.newbestsub.dataset.view.ViewPaths
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.binarysolution.BinarySolution

/**
 * DatasetView
 * ===========
 *
 * Composite façade that fans out to:
 *  - CSVView     (streaming-first CSV; owns buffering + canonical sort/rewrite)
 *  - ParquetView (streaming-first Parquet; owns its *own* buffering and final write)
 *
 * Public API unchanged:
 *  - print(runResult, model)
 *  - appendCardinality(model, event)
 *  - replaceTopBatch(model, blocks)
 *  - closeStreams(model)
 *
 * Path helpers:
 *  - CSV helpers delegated to CSVView (same names/behavior as before).
 *  - Parquet helpers provided here for logging/UX convenience.
 *  - All *merged* artifacts now live under /CSV/ and /Parquet/ subfolders.
 */
class DatasetView {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    private val csvView = CSVView()
    private val parquetView = ParquetView()

    /* ---------------- Public CSV path helpers (unchanged) ---------------- */

    fun getFunctionValuesFilePath(model: DatasetModel): String =
        csvView.getFunctionValuesFilePath(model)

    fun getVariableValuesFilePath(model: DatasetModel): String =
        csvView.getVariableValuesFilePath(model)

    fun getTopSolutionsFilePath(model: DatasetModel): String =
        csvView.getTopSolutionsFilePath(model)

    fun getInfoFilePath(model: DatasetModel, isTargetAll: Boolean = false): String =
        csvView.getInfoFilePath(model, isTargetAll)

    fun getAggregatedDataFilePath(model: DatasetModel, isTargetAll: Boolean = false): String =
        csvView.getAggregatedDataFilePath(model, isTargetAll)

    /* ---------------- Public CSV merged path helpers (now under /CSV/) ---------------- */

    fun getFunctionValuesMergedFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) +
            ViewPaths.csvNameNoTsMerged(
                ViewPaths.fileBaseParts(model, model.targetToAchieve),
                Constants.FUNCTION_VALUES_FILE_SUFFIX
            )

    fun getVariableValuesMergedFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) +
            ViewPaths.csvNameNoTsMerged(
                ViewPaths.fileBaseParts(model, model.targetToAchieve),
                Constants.VARIABLE_VALUES_FILE_SUFFIX
            )

    fun getTopSolutionsMergedFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) +
            ViewPaths.csvNameNoTsMerged(
                ViewPaths.fileBaseParts(model, model.targetToAchieve),
                Constants.TOP_SOLUTIONS_FILE_SUFFIX
            )

    fun getAggregatedDataMergedFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val targetToken = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureCsvDir(model) +
            ViewPaths.csvNameNoTsMerged(
                ViewPaths.fileBaseParts(model, targetToken),
                Constants.AGGREGATED_DATA_FILE_SUFFIX
            )
    }

    fun getInfoMergedFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val targetToken = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureCsvDir(model) +
            ViewPaths.csvNameNoTsMerged(
                ViewPaths.fileBaseParts(model, targetToken),
                Constants.INFO_FILE_SUFFIX
            )
    }

    /* ---------------- Public Parquet path helpers (per-run, under /Parquet/) ---------------- */

    fun getFunctionValuesParquetPath(model: DatasetModel): String =
        ViewPaths.ensureParquetDir(model) +
            ViewPaths.parquetNameNoTs(
                ViewPaths.fileBaseParts(model, model.targetToAchieve),
                Constants.FUNCTION_VALUES_FILE_SUFFIX
            )

    fun getVariableValuesParquetPath(model: DatasetModel): String =
        ViewPaths.ensureParquetDir(model) +
            ViewPaths.parquetNameNoTs(
                ViewPaths.fileBaseParts(model, model.targetToAchieve),
                Constants.VARIABLE_VALUES_FILE_SUFFIX
            )

    fun getTopSolutionsParquetPath(model: DatasetModel): String =
        ViewPaths.ensureParquetDir(model) +
            ViewPaths.parquetNameNoTs(
                ViewPaths.fileBaseParts(model, model.targetToAchieve),
                Constants.TOP_SOLUTIONS_FILE_SUFFIX
            )

    /** Final-table Parquet paths (mirror CSV Final tables; also under /Parquet/). */
    fun getAggregatedDataParquetPath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val targetToken = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureParquetDir(model) +
            ViewPaths.parquetNameNoTs(
                ViewPaths.fileBaseParts(model, targetToken),
                Constants.AGGREGATED_DATA_FILE_SUFFIX
            )
    }

    fun getInfoParquetPath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val targetToken = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureParquetDir(model) +
            ViewPaths.parquetNameNoTs(
                ViewPaths.fileBaseParts(model, targetToken),
                Constants.INFO_FILE_SUFFIX
            )
    }

    /* ---------------- Public Parquet merged path helpers (under /Parquet/) ---------------- */

    fun getAggregatedDataMergedParquetPath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val targetToken = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        val base = ViewPaths.fileBaseParts(model, targetToken)
        val suffixWithMerged = Constants.AGGREGATED_DATA_FILE_SUFFIX +
            Constants.FILE_NAME_SEPARATOR + Constants.MERGED_RESULT_FILE_SUFFIX
        return ViewPaths.ensureParquetDir(model) + ViewPaths.parquetNameNoTs(base, suffixWithMerged)
    }

    fun getInfoMergedParquetPath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val targetToken = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        val base = ViewPaths.fileBaseParts(model, targetToken)
        val suffixWithMerged = Constants.INFO_FILE_SUFFIX +
            Constants.FILE_NAME_SEPARATOR + Constants.MERGED_RESULT_FILE_SUFFIX
        return ViewPaths.ensureParquetDir(model) + ViewPaths.parquetNameNoTs(base, suffixWithMerged)
    }

    fun getFunctionValuesMergedParquetPath(model: DatasetModel): String {
        val base = ViewPaths.fileBaseParts(model, model.targetToAchieve)
        val suffixWithMerged = Constants.FUNCTION_VALUES_FILE_SUFFIX +
            Constants.FILE_NAME_SEPARATOR + Constants.MERGED_RESULT_FILE_SUFFIX
        return ViewPaths.ensureParquetDir(model) + ViewPaths.parquetNameNoTs(base, suffixWithMerged)
    }

    fun getVariableValuesMergedParquetPath(model: DatasetModel): String {
        val base = ViewPaths.fileBaseParts(model, model.targetToAchieve)
        val suffixWithMerged = Constants.VARIABLE_VALUES_FILE_SUFFIX +
            Constants.FILE_NAME_SEPARATOR + Constants.MERGED_RESULT_FILE_SUFFIX
        return ViewPaths.ensureParquetDir(model) + ViewPaths.parquetNameNoTs(base, suffixWithMerged)
    }

    fun getTopSolutionsMergedParquetPath(model: DatasetModel): String {
        val base = ViewPaths.fileBaseParts(model, model.targetToAchieve)
        val suffixWithMerged = Constants.TOP_SOLUTIONS_FILE_SUFFIX +
            Constants.FILE_NAME_SEPARATOR + Constants.MERGED_RESULT_FILE_SUFFIX
        return ViewPaths.ensureParquetDir(model) + ViewPaths.parquetNameNoTs(base, suffixWithMerged)
    }

    /* ---------------- Snapshot print (final, non-streamed) ---------------- */

    fun print(
        runResult: Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>>,
        model: DatasetModel
    ) {
        val (allSolutions, topSolutions, executionInfo) = runResult
        val (actualTarget, threadName, computingTimeMs) = executionInfo

        logger.info(
            "Starting to print result for execution on \"$threadName\" with target " +
                "\"$actualTarget\" completed in ${computingTimeMs}ms."
        )

        // CSV snapshot
        csvView.printSnapshot(model, allSolutions, topSolutions, actualTarget)

        // Parquet snapshot (decoupled; same data, built directly from the lists/model)
        parquetView.printSnapshot(model, allSolutions, topSolutions, actualTarget)

        logger.info("Print completed for target \"$actualTarget\".")
    }

    /* ---------------- Streaming fan-out (CSV + Parquet) ---------------- */

    fun appendCardinality(model: DatasetModel, event: CardinalityResult) {
        csvView.onAppendCardinality(model, event)
        parquetView.onAppendCardinality(model, event)
    }

    fun replaceTopBatch(model: DatasetModel, blocks: Map<Int, List<String>>) {
        csvView.onReplaceTopBatch(model, blocks)
        parquetView.onReplaceTopBatch(model, blocks)
    }

    /**
     * Finalization:
     *  - CSV: close writers, global sort & rewrite FUN/VAR, finalize TOP blocks.
     *  - Parquet: global sort & write FUN/VAR + finalize TOP blocks (from its own buffers).
     *
     * No CSV ↔ Parquet coupling: both maintain their own state.
     */
    fun closeStreams(model: DatasetModel) {
        // 1) finalize CSV files
        csvView.closeStreams(model)

        // 3) finalize Parquet independently
        parquetView.closeStreams(model)
    }

    /* ---------------- Controller convenience (CSV/Parquet table writers) ---------------- */

    fun writeCsv(csvRows: List<Array<String>>, resultCsvPath: String) =
        csvView.writeCsv(csvRows, resultCsvPath)

    /** Generic table writer for Parquet Final tables (header-driven UTF-8 schema, 6-digit decimals). */
    fun writeParquet(rows: List<Array<String>>, resultParquetPath: String) =
        parquetView.writeTable(rows, resultParquetPath)
}
