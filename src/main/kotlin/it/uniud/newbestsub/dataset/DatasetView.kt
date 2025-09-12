package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.dataset.model.CardinalityResult
import it.uniud.newbestsub.dataset.view.CSVView
import it.uniud.newbestsub.dataset.view.ParquetView
import it.uniud.newbestsub.dataset.view.ViewPaths
import it.uniud.newbestsub.utils.Constants
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.binarysolution.BinarySolution

/**
 * DatasetView
 *
 * Composite façade that fans out output operations to two independent backends:
 *
 * - CSVView — streaming-first CSV (owns buffering and canonical sort/rewrite).
 * - ParquetView — streaming-first Parquet (keeps its own buffers and performs the final write).
 *
 * Public surface
 * - print: final snapshot writer (CSV + Parquet).
 * - appendCardinality: streaming append of per-generation cardinalities (FUN/VAR).
 * - replaceTopBatch: streaming update of top-solutions blocks (TOP).
 * - closeStreams: finalize and write sorted/merged artifacts for both backends.
 *
 * Path helpers
 * - CSV path helpers are delegated to CSVView and preserve the original naming scheme.
 * - Parquet helpers are provided here for logging/UX convenience and mirror CSV layouts.
 * - All merged artifacts are emitted under /CSV/ and /Parquet/ subfolders.
 */
class DatasetView {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    private val csvView = CSVView()
    private val parquetView = ParquetView()

    /* Public CSV path helpers (per-run, under /CSV/) */

    /** Absolute path to the per-run CSV with function/objective values (FUN). */
    fun getFunctionValuesFilePath(model: DatasetModel): String =
        csvView.getFunctionValuesFilePath(model)

    /** Absolute path to the per-run CSV with variable values (VAR). */
    fun getVariableValuesFilePath(model: DatasetModel): String =
        csvView.getVariableValuesFilePath(model)

    /** Absolute path to the per-run CSV with top solutions (TOP). */
    fun getTopSolutionsFilePath(model: DatasetModel): String =
        csvView.getTopSolutionsFilePath(model)

    /**
     * @param model Dataset context used to compute the base parts.
     * @param isTargetAll When true, uses the ALL token instead of the model’s target.
     * @return Absolute path to the per-run CSV with run metadata (INFO).
     */
    fun getInfoFilePath(model: DatasetModel, isTargetAll: Boolean = false): String =
        csvView.getInfoFilePath(model, isTargetAll)

    /**
     * @param model Dataset context used to compute the base parts.
     * @param isTargetAll When true, uses the ALL token instead of the model’s target.
     * @return Absolute path to the per-run CSV with aggregated/final table.
     */
    fun getAggregatedDataFilePath(model: DatasetModel, isTargetAll: Boolean = false): String =
        csvView.getAggregatedDataFilePath(model, isTargetAll)

    /* Public CSV merged path helpers (under /CSV/) */

    /** Absolute path to the merged CSV FUN table under /CSV/. */
    fun getFunctionValuesMergedFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) +
                ViewPaths.csvNameNoTsMerged(
                    ViewPaths.fileBaseParts(model, model.targetToAchieve),
                    Constants.FUNCTION_VALUES_FILE_SUFFIX
                )

    /** Absolute path to the merged CSV VAR table under /CSV/. */
    fun getVariableValuesMergedFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) +
                ViewPaths.csvNameNoTsMerged(
                    ViewPaths.fileBaseParts(model, model.targetToAchieve),
                    Constants.VARIABLE_VALUES_FILE_SUFFIX
                )

    /** Absolute path to the merged CSV TOP table under /CSV/. */
    fun getTopSolutionsMergedFilePath(model: DatasetModel): String =
        ViewPaths.ensureCsvDir(model) +
                ViewPaths.csvNameNoTsMerged(
                    ViewPaths.fileBaseParts(model, model.targetToAchieve),
                    Constants.TOP_SOLUTIONS_FILE_SUFFIX
                )

    /**
     * @return Absolute path to the merged CSV FINAL table under /CSV/.
     */
    fun getAggregatedDataMergedFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val targetToken = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureCsvDir(model) +
                ViewPaths.csvNameNoTsMerged(
                    ViewPaths.fileBaseParts(model, targetToken),
                    Constants.AGGREGATED_DATA_FILE_SUFFIX
                )
    }

    /**
     * @return Absolute path to the merged CSV INFO table under /CSV/.
     */
    fun getInfoMergedFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val targetToken = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureCsvDir(model) +
                ViewPaths.csvNameNoTsMerged(
                    ViewPaths.fileBaseParts(model, targetToken),
                    Constants.INFO_FILE_SUFFIX
                )
    }

    /* Public Parquet path helpers (per-run, under /Parquet/) */

    /** Absolute path to the per-run Parquet FUN file. */
    fun getFunctionValuesParquetPath(model: DatasetModel): String =
        ViewPaths.ensureParquetDir(model) +
                ViewPaths.parquetNameNoTs(
                    ViewPaths.fileBaseParts(model, model.targetToAchieve),
                    Constants.FUNCTION_VALUES_FILE_SUFFIX
                )

    /** Absolute path to the per-run Parquet VAR file. */
    fun getVariableValuesParquetPath(model: DatasetModel): String =
        ViewPaths.ensureParquetDir(model) +
                ViewPaths.parquetNameNoTs(
                    ViewPaths.fileBaseParts(model, model.targetToAchieve),
                    Constants.VARIABLE_VALUES_FILE_SUFFIX
                )

    /** Absolute path to the per-run Parquet TOP file. */
    fun getTopSolutionsParquetPath(model: DatasetModel): String =
        ViewPaths.ensureParquetDir(model) +
                ViewPaths.parquetNameNoTs(
                    ViewPaths.fileBaseParts(model, model.targetToAchieve),
                    Constants.TOP_SOLUTIONS_FILE_SUFFIX
                )

    /** Mirrors the CSV FINAL table location under /Parquet/. */
    fun getAggregatedDataParquetPath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val targetToken = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureParquetDir(model) +
                ViewPaths.parquetNameNoTs(
                    ViewPaths.fileBaseParts(model, targetToken),
                    Constants.AGGREGATED_DATA_FILE_SUFFIX
                )
    }

    /** Mirrors the CSV INFO table location under /Parquet/. */
    fun getInfoParquetPath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val targetToken = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ViewPaths.ensureParquetDir(model) +
                ViewPaths.parquetNameNoTs(
                    ViewPaths.fileBaseParts(model, targetToken),
                    Constants.INFO_FILE_SUFFIX
                )
    }

    /* Public Parquet merged path helpers (under /Parquet/) */

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

    /** Absolute path to the merged Parquet FUN table. */
    fun getFunctionValuesMergedParquetPath(model: DatasetModel): String {
        val base = ViewPaths.fileBaseParts(model, model.targetToAchieve)
        val suffixWithMerged = Constants.FUNCTION_VALUES_FILE_SUFFIX +
                Constants.FILE_NAME_SEPARATOR + Constants.MERGED_RESULT_FILE_SUFFIX
        return ViewPaths.ensureParquetDir(model) + ViewPaths.parquetNameNoTs(base, suffixWithMerged)
    }

    /** Absolute path to the merged Parquet VAR table. */
    fun getVariableValuesMergedParquetPath(model: DatasetModel): String {
        val base = ViewPaths.fileBaseParts(model, model.targetToAchieve)
        val suffixWithMerged = Constants.VARIABLE_VALUES_FILE_SUFFIX +
                Constants.FILE_NAME_SEPARATOR + Constants.MERGED_RESULT_FILE_SUFFIX
        return ViewPaths.ensureParquetDir(model) + ViewPaths.parquetNameNoTs(base, suffixWithMerged)
    }

    /** Absolute path to the merged Parquet TOP table. */
    fun getTopSolutionsMergedParquetPath(model: DatasetModel): String {
        val base = ViewPaths.fileBaseParts(model, model.targetToAchieve)
        val suffixWithMerged = Constants.TOP_SOLUTIONS_FILE_SUFFIX +
                Constants.FILE_NAME_SEPARATOR + Constants.MERGED_RESULT_FILE_SUFFIX
        return ViewPaths.ensureParquetDir(model) + ViewPaths.parquetNameNoTs(base, suffixWithMerged)
    }

    /* Snapshot print (final, non-streamed) */

    /**
     * Write final snapshot artifacts for a completed run to both CSV and Parquet.
     *
     * @param runResult Triple of:
     *  - first = all solutions evaluated,
     *  - second = selected top solutions,
     *  - third = (actualTarget, threadName, computingTimeMs).
     *
     * Notes
     * - The heavy lifting for formatting is delegated to CSVView and ParquetView.
     * - This method only orchestrates fan-out and logs a concise lifecycle.
     */
    fun print(
        runResult: Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>>,
        model: DatasetModel
    ) {
        val (allSolutions, topSolutions, executionInfo) = runResult
        val (actualTarget, threadName, computingTimeMs) = executionInfo

        logger.info(
            "Starting to print result for execution on \"{}\" with target \"{}\" completed in {} ms.",
            threadName, actualTarget, computingTimeMs
        )

        csvView.printSnapshot(model, allSolutions, topSolutions, actualTarget)
        parquetView.printSnapshot(model, allSolutions, topSolutions, actualTarget)

        logger.info("Print completed for target \"{}\".", actualTarget)
    }

    /* Streaming fan-out (CSV + Parquet) */

    /**
     * Forward one cardinality event into both backends.
     * Small, hot-path method: avoid extra allocations or string formatting here.
     */
    fun appendCardinality(model: DatasetModel, event: CardinalityResult) {
        csvView.onAppendCardinality(model, event)
        parquetView.onAppendCardinality(model, event)
    }

    /**
     * Replace a batch of top solutions blocks (per K) in both backends.
     *
     * @param blocks Map K → listOf(csvLine). Each list should contain up to
     *               Constants.TOP_SOLUTIONS_NUMBER pre-ordered rows.
     *
     * The CSV backend rewrites the whole TOP file in a stable K order.
     * The Parquet backend buffers and writes on close.
     */
    fun replaceTopBatch(model: DatasetModel, blocks: Map<Int, List<String>>) {
        csvView.onReplaceTopBatch(model, blocks)
        parquetView.onReplaceTopBatch(model, blocks)
    }

    /**
     * Finalize both backends.
     *
     * CSV
     * - Closes writers.
     * - Globally sorts and rewrites FUN/VAR to keep them aligned.
     * - Optionally rewrites TOP once if live updates were disabled.
     *
     * Parquet
     * - Sorts and writes FUN/VAR from its buffers.
     * - Writes TOP blocks from its own state.
     *
     * Backends are independent; failures in one are logged and do not block the other.
     */
    fun closeStreams(model: DatasetModel) {
        csvView.closeStreams(model)
        parquetView.closeStreams(model)
    }

    /* Controller convenience (CSV/Parquet table writers) */

    /** Thin CSV passthrough for generic tables. */
    fun writeCsv(csvRows: List<Array<String>>, resultCsvPath: String) =
        csvView.writeCsv(csvRows, resultCsvPath)

    /** Generic Parquet table writer (header-driven UTF-8 schema, 6-digit decimals). */
    fun writeParquet(rows: List<Array<String>>, resultParquetPath: String) =
        parquetView.writeTable(rows, resultParquetPath)
}
