package it.uniud.newbestsub.utils

import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.*

object Constants {

    /* ----------------------------------------------------------------------------------------------------------------
     * Filesystem paths
     * ---------------------------------------------------------------------------------------------------------------- */
    val PATH_SEPARATOR: String = System.getProperty("file.separator").toString()
    val BASE_PATH: String = "${Paths.get("").toAbsolutePath().parent}$PATH_SEPARATOR"
    const val NEWBESTSUB_NAME: String = "NewBestSub"
    val NEWBESTSUB_PATH: String = "${Paths.get("").toAbsolutePath()}$PATH_SEPARATOR"
    val NEWBESTSUB_INPUT_PATH: String = "${NEWBESTSUB_PATH}data$PATH_SEPARATOR"
    val NEWBESTSUB_OUTPUT_PATH: String = "${NEWBESTSUB_PATH}res$PATH_SEPARATOR"

    /* ----------------------------------------------------------------------------------------------------------------
     * Experiments folder structure
     * ---------------------------------------------------------------------------------------------------------------- */
    const val NEWBESTSUB_EXPERIMENTS_NAME: String = "NewBestSub-Experiments"
    val NEWBESTSUB_EXPERIMENTS_PATH: String = "$BASE_PATH$NEWBESTSUB_EXPERIMENTS_NAME$PATH_SEPARATOR"

    /* Single timestamp token for this run (format yyyy-MM-dd-HH-mm-ss, 24h clock).
     * Used in folder/file names to ensure uniqueness per execution. */
    val RUN_TIMESTAMP: String = SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(Date())

    val NEWBESTSUB_EXPERIMENTS_INPUT_PATH: String =
        "${NEWBESTSUB_EXPERIMENTS_PATH}data$PATH_SEPARATOR$NEWBESTSUB_NAME$PATH_SEPARATOR$RUN_TIMESTAMP"

    /* ----------------------------------------------------------------------------------------------------------------
     * LogManager
     * ---------------------------------------------------------------------------------------------------------------- */
    const val LOG_FILE_NAME: String = "Execution"
    const val LOG_FILE_SUFFIX: String = ".log"

    /* Controls logging frequency (in percent milestones).
     * Example: LOGGING_FACTOR = 1 → log every 1% progress. */
    const val LOGGING_FACTOR: Int = 1

    /* ----------------------------------------------------------------------------------------------------------------
     * Targets (evaluation modes)
     * ---------------------------------------------------------------------------------------------------------------- */
    const val TARGET_BEST: String = "Best"
    const val TARGET_WORST: String = "Worst"
    const val TARGET_AVERAGE: String = "Average"
    const val TARGET_ALL: String = "All"
    const val CARDINALITY_NOT_AVAILABLE: String = "X"

    /* ----------------------------------------------------------------------------------------------------------------
     * Correlation methods
     * ---------------------------------------------------------------------------------------------------------------- */
    const val CORRELATION_PEARSON: String = "Pearson"
    const val CORRELATION_KENDALL: String = "Kendall"

    /* ----------------------------------------------------------------------------------------------------------------
     * File naming components
     * ---------------------------------------------------------------------------------------------------------------- */
    const val FUNCTION_VALUES_FILE_SUFFIX: String = "Fun"
    const val VARIABLE_VALUES_FILE_SUFFIX: String = "Var"
    const val TOP_SOLUTIONS_NUMBER: Int = 10
    val TOP_SOLUTIONS_FILE_SUFFIX: String = "Top-$TOP_SOLUTIONS_NUMBER-Solutions"
    const val AGGREGATED_DATA_FILE_SUFFIX: String = "Final"
    const val INFO_FILE_SUFFIX: String = "Info"
    const val MERGED_RESULT_FILE_SUFFIX: String = "Merged"
    const val FILE_NAME_SEPARATOR: String = "-"
    const val CSV_FILE_EXTENSION: String = ".csv"

    /* ----------------------------------------------------------------------------------------------------------------
     * Streaming throttling
     * ----------------------------------------------------------------------------------------------------------------
     * Used in per-generation streaming to reduce output size and I/O load.
     * GENERATION_WARMUP → skip the first N generations entirely.
     * GENERATION_STRIDE → then log/stream every Nth generation.
     */
    const val GENERATION_WARMUP: Int = 20
    const val GENERATION_STRIDE: Int = 5
}
