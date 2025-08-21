package it.uniud.newbestsub.utils

import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.*

object Constants {

    val PATH_SEPARATOR = System.getProperty("file.separator").toString()
    val BASE_PATH = "${Paths.get("").toAbsolutePath().parent}$PATH_SEPARATOR"
    val NEWBESTSUB_NAME = "NewBestSub"
    val NEWBESTSUB_PATH = "${Paths.get("").toAbsolutePath()}$PATH_SEPARATOR"
    val NEWBESTSUB_INPUT_PATH = "${NEWBESTSUB_PATH}data$PATH_SEPARATOR"

    /* Single timestamp token for this run; used at the END of the run folder name */
    val RUN_TIMESTAMP: String? = SimpleDateFormat("yyyy-MM-dd-hh-mm-ss").format(Date())

    val LOG_FILE_NAME = "Execution"
    val LOG_FILE_SUFFIX = ".log"

    /* Output base dir */
    val NEWBESTSUB_OUTPUT_PATH = "${NEWBESTSUB_PATH}res$PATH_SEPARATOR"

    val LOGGING_FACTOR = 2
    val TARGET_BEST = "Best"
    val TARGET_WORST = "Worst"
    val TARGET_AVERAGE = "Average"
    val TARGET_ALL = "All"
    val CARDINALITY_NOT_AVAILABLE = "X"
    val CORRELATION_PEARSON = "Pearson"
    val CORRELATION_KENDALL = "Kendall"
    val FUNCTION_VALUES_FILE_SUFFIX = "Fun"
    val VARIABLE_VALUES_FILE_SUFFIX = "Var"
    val TOP_SOLUTIONS_NUMBER = 10
    val TOP_SOLUTIONS_FILE_SUFFIX = "Top-$TOP_SOLUTIONS_NUMBER-Solutions"
    val AGGREGATED_DATA_FILE_SUFFIX = "Final"
    val INFO_FILE_SUFFIX = "Info"
    val MERGED_RESULT_FILE_SUFFIX = "Merged"
    val FILE_NAME_SEPARATOR = "-"
    val CSV_FILE_EXTENSION = ".csv"

    val NEWBESTSUB_EXPERIMENTS_NAME = "NewBestSub-Experiments"
    val NEWBESTSUB_EXPERIMENTS_PATH = "$BASE_PATH$NEWBESTSUB_EXPERIMENTS_NAME$PATH_SEPARATOR"
    val NEWBESTSUB_EXPERIMENTS_INPUT_PATH =
        "${NEWBESTSUB_EXPERIMENTS_PATH}data$PATH_SEPARATOR$NEWBESTSUB_NAME$PATH_SEPARATOR$RUN_TIMESTAMP"
}
