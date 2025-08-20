package it.uniud.newbestsub.utils

import it.uniud.newbestsub.dataset.Parameters
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.core.LoggerContext
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.security.MessageDigest

object Tools {

    /*
     * stringComparison
     * ----------------
     * Hamming-like distance between two strings. Compares character-by-character
     * up to the length of the shorter string, then counts any extra trailing
     * characters in the longer string as differences.
     *
     * Rationale:
     *  - The previous implementation assumed equal length and could throw
     *    IndexOutOfBoundsException. This version is total and deterministic.
     */
    fun stringComparison(firstString: String, secondString: String): Int {
        var distance = 0
        val minLen = minOf(firstString.length, secondString.length)
        var i = 0
        while (i < minLen) {
            if (secondString[i] != firstString[i]) distance++
            i++
        }
        distance += kotlin.math.abs(firstString.length - secondString.length)
        return distance
    }

    /*
     * getMean
     * -------
     * Mean over a run vector restricted to columns flagged as true in useColumns.
     * Throws if no columns are selected to avoid silent NaN/Infinity.
     */
    fun getMean(run: DoubleArray, useColumns: BooleanArray): Double {
        var sum = 0.0
        var count = 0
        val n = minOf(run.size, useColumns.size)
        var i = 0
        while (i < n) {
            if (useColumns[i]) {
                sum += run[i]
                count++
            }
            i++
        }
        require(count > 0) { "getMean: no columns selected (count == 0)" }
        return sum / count.toDouble()
    }

    /* ---------------- Sanitize & Name Builders ---------------- */

    /* Replace path-unfriendly chars, collapse whitespace/dashes, and trim. */
    fun sanitizeForFile(part: String): String = part
        .replace("[\\\\/:*?\"<>|]".toRegex(), "-")
        .replace("\\s+".toRegex(), "-")
        .replace("-{2,}".toRegex(), "-")
        .trim('-')

    /* Join arbitrary parts (ignores null/blank). Falls back to "Run" if empty. */
    fun folderBaseName(vararg parts: Any?): String =
        parts.asSequence()
            .filterNotNull()
            .map { it.toString() }
            .map { sanitizeForFile(it) }
            .filter { it.isNotBlank() }
            .joinToString(Constants.FILE_NAME_SEPARATOR)
            .ifBlank { "Run" }

    /*
     * buildParamsToken
     * ----------------
     * Build the *params token* (NO timestamp) common to outputs/logs:
     *  - Exclude target so Best/Worst/Average share the same container
     *  - Include only params whose numeric value > 0
     *  - Include percentiles ONLY when includePercentiles == true and list is not empty,
     *    formatted as "pe<first>_<last>"
     */
    fun buildParamsToken(
        datasetName: String,
        correlationMethod: String,
        numberOfTopics: Int,
        numberOfSystems: Int,
        populationSize: Int,
        numberOfIterations: Int,
        numberOfRepetitions: Int,
        expansionCoefficient: Int,
        includePercentiles: Boolean,
        percentiles: List<Int>
    ): String {
        val parts = mutableListOf<String>()
        parts += datasetName
        parts += correlationMethod
        if (numberOfTopics > 0) parts += "top$numberOfTopics"
        if (numberOfSystems > 0) parts += "sys$numberOfSystems"
        if (populationSize > 0) parts += "po$populationSize"
        if (numberOfIterations > 0) parts += "i$numberOfIterations"
        if (numberOfRepetitions > 0) parts += "r$numberOfRepetitions"
        if (expansionCoefficient > 0) parts += "mx$expansionCoefficient"

        if (includePercentiles && percentiles.isNotEmpty()) {
            val first = percentiles.first()
            val last = percentiles.last()
            parts += "pe${first}_${last}"
        }

        return folderBaseName(*parts.toTypedArray())
    }

    /*
     * buildContainerFolderName
     * ------------------------
     * Build the shared *container folder name* used for outputs:
     *   <params-token>-time<RUN_TIMESTAMP>
     */
    fun buildContainerFolderName(
        datasetName: String,
        correlationMethod: String,
        numberOfTopics: Int,
        numberOfSystems: Int,
        populationSize: Int,
        numberOfIterations: Int,
        numberOfRepetitions: Int,
        expansionCoefficient: Int,
        includePercentiles: Boolean,
        percentiles: List<Int>
    ): String {
        val token = buildParamsToken(
            datasetName,
            correlationMethod,
            numberOfTopics,
            numberOfSystems,
            populationSize,
            numberOfIterations,
            numberOfRepetitions,
            expansionCoefficient,
            includePercentiles,
            percentiles
        )
        return token + Constants.FILE_NAME_SEPARATOR + "time" + Constants.RUN_TIMESTAMP
    }

    /* ---------------- Deterministic Seed Helper ---------------- */

    /*
     * stableSeedFrom
     * --------------
     * Deterministically derive a 64-bit seed from key Parameters when
     * the user did not supply an explicit seed.
     *
     * Notes:
     *  - currentExecution is intentionally NOT included so that batch runs (--mr)
     *    share the same master seed. If you need per-execution variability,
     *    supply an explicit seed or add currentExecution here.
     */
    fun stableSeedFrom(p: Parameters): Long {
        val signature = buildString {
            append(p.datasetName).append('|')
            append(p.correlationMethod).append('|')
            append(p.targetToAchieve).append('|')
            append(p.numberOfIterations).append('|')
            append(p.numberOfRepetitions).append('|')
            append(p.populationSize).append('|')
            append(p.percentiles.joinToString(","))
        }
        val sha = MessageDigest.getInstance("SHA-256").digest(signature.toByteArray())
        return ByteBuffer.wrap(sha, 0, 8).long
    }

    /* ---------------- Logging Helpers ---------------- */

    /* Path to the *bootstrap* log file (used from program start). */
    fun getBootstrapLogFilePath(): String =
        Paths.get(
            Constants.NEWBESTSUB_PATH,
            "log",
            "${Constants.LOG_FILE_NAME}${Constants.RUN_TIMESTAMP}${Constants.LOG_FILE_SUFFIX}"
        ).toString()

    /*
     * Build the final *log filename* (NO subfolder):
     * "<params-token>-time<RUN_TIMESTAMP>.log"
     */
    fun buildFinalLogFilePathFromParams(paramsToken: String): String =
        Paths.get(
            Constants.NEWBESTSUB_PATH,
            "log",
            "${paramsToken}-time${Constants.RUN_TIMESTAMP}${Constants.LOG_FILE_SUFFIX}"
        ).toString()

    /*
     * promoteBootstrapLogToParamNamedFile
     * -----------------------------------
     * Copy the bootstrap log into the final parameterized log file, switch Log4j
     * to that file, and then delete the bootstrap file so we don't leave clutter.
     *
     * Order matters:
     *  1) copy  ->  2) switch appender  ->  3) delete bootstrap
     * This minimizes the window where new messages could land in the old file.
     */
    fun promoteBootstrapLogToParamNamedFile(
        paramsToken: String,
        desiredLevel: Level? = null
    ) {
        val srcPath = Paths.get(getBootstrapLogFilePath())
        val dstPath = Paths.get(buildFinalLogFilePathFromParams(paramsToken))

        Files.createDirectories(dstPath.parent)

        if (Files.exists(srcPath)) {
            /* Copy bootstrap contents to the final file */
            Files.copy(srcPath, dstPath, StandardCopyOption.REPLACE_EXISTING)
        } else {
            /* Ensure destination exists so the appender can append */
            if (!Files.exists(dstPath)) {
                Files.createFile(dstPath)
            }
        }

        /* Switch Log4j to write to the final parameterized file */
        switchLogFile(dstPath.toString())

        /* Apply requested level after reconfiguration (if any) */
        desiredLevel?.let {
            updateLogger(LogManager.getLogger(LogManager.ROOT_LOGGER_NAME), it)
        }

        /* Best-effort cleanup of the bootstrap file (ignore errors) */
        try {
            if (Files.exists(srcPath)) {
                Files.deleteIfExists(srcPath)
            }
        } catch (_: Exception) {
            /* Intentionally ignored — logging must not fail due to cleanup. */
        }
    }

    /*
     * Switch the active Log4j file appender to a new base path.
     * Assumes your log4j2 config uses ${sys:baseLogFileName}.
     */
    fun switchLogFile(newBasePath: String) {
        System.setProperty("baseLogFileName", newBasePath)
        val ctx = LogManager.getContext(false) as LoggerContext
        ctx.reconfigure()
    }

    /*
     * Update the root logger level and refresh loggers.
     * (Does not change the file path; use switchLogFile for that.)
     */
    fun updateLogger(logger: Logger, level: Level): Logger {
        val currentContext = (LogManager.getContext(false) as LoggerContext)
        val currentConfiguration = currentContext.configuration
        val loggerConfig = currentConfiguration.getLoggerConfig(LogManager.ROOT_LOGGER_NAME)
        loggerConfig.level = level
        currentContext.updateLoggers()
        return logger
    }
}
