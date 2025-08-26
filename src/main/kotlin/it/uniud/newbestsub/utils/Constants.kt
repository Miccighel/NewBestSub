package it.uniud.newbestsub.utils

import java.nio.file.FileSystems
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.*

/**
 * Global constants and immutable configuration values for NewBestSub.
 *
 * This object centralizes filesystem paths, naming conventions, evaluation modes,
 * correlation methods, and various configuration parameters used across the project.
 *
 * Conventions:
 * - Paths always include a trailing [PATH_SEPARATOR].
 * - File/folder names are constructed from tokens separated by [FILE_NAME_SEPARATOR].
 * - Run-specific outputs use [RUN_TIMESTAMP] to guarantee uniqueness per execution.
 */
object Constants {

    // -----------------------------------------------------------------------------------------------------------------
    // Filesystem paths
    // -----------------------------------------------------------------------------------------------------------------

    /** System-dependent path separator (e.g., "/" on Unix, "\\" on Windows). */
    val PATH_SEPARATOR: String = FileSystems.getDefault().separator.toString()

    /** Parent directory of the current working directory (absolute). */
    val BASE_PATH: String = "${Paths.get("").toAbsolutePath().parent}$PATH_SEPARATOR"

    /** Project name token: `"NewBestSub"`. */
    const val NEWBESTSUB_NAME: String = "NewBestSub"

    /** Absolute path to the current working directory (with trailing [PATH_SEPARATOR]). */
    val NEWBESTSUB_PATH: String = "${Paths.get("").toAbsolutePath()}$PATH_SEPARATOR"

    /** Default input directory under the project (`<project>/data/`). */
    val NEWBESTSUB_INPUT_PATH: String = "${NEWBESTSUB_PATH}data$PATH_SEPARATOR"

    /** Default output directory under the project (`<project>/res/`). */
    val NEWBESTSUB_OUTPUT_PATH: String = "${NEWBESTSUB_PATH}res$PATH_SEPARATOR"

    // -----------------------------------------------------------------------------------------------------------------
    // Experiments folder structure
    // -----------------------------------------------------------------------------------------------------------------

    /** Base name of the experiments' folder. */
    const val NEWBESTSUB_EXPERIMENTS_NAME: String = "NewBestSub-Experiments"

    /** Absolute path to the experiments folder (`<base>/<NewBestSub-Experiments>/`). */
    val NEWBESTSUB_EXPERIMENTS_PATH: String = "$BASE_PATH$NEWBESTSUB_EXPERIMENTS_NAME$PATH_SEPARATOR"

    /**
     * Single timestamp token for this run.
     *
     * Format: `yyyy-MM-dd-HH-mm-ss` (24h clock).
     * Used in folder/file names to ensure uniqueness per execution.
     */
    val RUN_TIMESTAMP: String = SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(Date())

    /** Path for experiment inputs: `<experiments>/data/NewBestSub/<RUN_TIMESTAMP>`. */
    val NEWBESTSUB_EXPERIMENTS_INPUT_PATH: String =
        "${NEWBESTSUB_EXPERIMENTS_PATH}data$PATH_SEPARATOR$NEWBESTSUB_NAME$PATH_SEPARATOR$RUN_TIMESTAMP"

    // -----------------------------------------------------------------------------------------------------------------
    // LogManager
    // -----------------------------------------------------------------------------------------------------------------

    /** Base filename (prefix) for execution logs. */
    const val LOG_FILE_NAME: String = "Execution"

    /** Log file extension. */
    const val LOG_FILE_SUFFIX: String = ".log"

    /**
     * Controls logging frequency (in percentage milestones).
     *
     * Example:
     * - `LOGGING_FACTOR = 1` → log every 1% progress
     * - `LOGGING_FACTOR = 5` → log every 5% progress
     */
    const val LOGGING_FACTOR: Int = 1

    // -----------------------------------------------------------------------------------------------------------------
    // Targets (evaluation modes)
    // -----------------------------------------------------------------------------------------------------------------

    /** Evaluation target: best system subset. */
    const val TARGET_BEST: String = "Best"

    /** Evaluation target: worst system subset. */
    const val TARGET_WORST: String = "Worst"

    /** Evaluation target: average system subset. */
    const val TARGET_AVERAGE: String = "Average"

    /** Evaluation target: all available systems. */
    const val TARGET_ALL: String = "All"

    /** Placeholder for unavailable cardinality values. */
    const val CARDINALITY_NOT_AVAILABLE: String = "X"

    // -----------------------------------------------------------------------------------------------------------------
    // Correlation methods
    // -----------------------------------------------------------------------------------------------------------------

    /** Pearson correlation identifier. */
    const val CORRELATION_PEARSON: String = "Pearson"

    /** Kendall correlation identifier. */
    const val CORRELATION_KENDALL: String = "Kendall"

    // -----------------------------------------------------------------------------------------------------------------
    // File naming components
    // -----------------------------------------------------------------------------------------------------------------

    /** Suffix used for function values files. */
    const val FUNCTION_VALUES_FILE_SUFFIX: String = "Fun"

    /** Suffix used for variable values files. */
    const val VARIABLE_VALUES_FILE_SUFFIX: String = "Var"

    /** Number of top solutions saved per run. */
    const val TOP_SOLUTIONS_NUMBER: Int = 10

    /** Suffix for top solutions files (`Top-<N>-Solutions`). */
    const val TOP_SOLUTIONS_FILE_SUFFIX: String = "Top-$TOP_SOLUTIONS_NUMBER-Solutions"

    /** Suffix for aggregated data files. */
    const val AGGREGATED_DATA_FILE_SUFFIX: String = "Final"

    /** Suffix for run info files. */
    const val INFO_FILE_SUFFIX: String = "Info"

    /** Suffix for merged result files. */
    const val MERGED_RESULT_FILE_SUFFIX: String = "Merged"

    /** Separator used in file names (typically `"-"`). */
    const val FILE_NAME_SEPARATOR: String = "-"

    /** File extension for CSV outputs. */
    const val CSV_FILE_EXTENSION: String = ".csv"

    // -----------------------------------------------------------------------------------------------------------------
    // Streaming throttling
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Generation warm-up: number of generations to skip completely before streaming begins.
     */
    const val GENERATION_WARMUP: Int = 20

    /**
     * Generation stride: after the warm-up, stream/log every N-th generation.
     *
     * Used to reduce I/O load and output size during long evolutionary runs.
     */
    const val GENERATION_STRIDE: Int = 5
}
