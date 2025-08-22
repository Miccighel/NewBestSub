package it.uniud.newbestsub.utils

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.core.LoggerContext
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

object LogManager {

    /* ----------------------------------------------------------------------------------------------------------------
     * Log directory & path helpers
     * ---------------------------------------------------------------------------------------------------------------- */

    fun ensureLogDir(): String {
        val directoryPath = Paths.get(Constants.NEWBESTSUB_PATH, "log").toString() + Constants.PATH_SEPARATOR
        Files.createDirectories(Paths.get(directoryPath))
        return directoryPath
    }

    /*
     * getBootstrapLogFilePath
     * -----------------------
     * Path to the *bootstrap* log file used from program start, before
     * parameters are known. We ensure the directory exists.
     */
    fun getBootstrapLogFilePath(): String {
        val directoryPath = ensureLogDir()
        return directoryPath +
            Constants.LOG_FILE_NAME +
            Constants.RUN_TIMESTAMP +
            Constants.LOG_FILE_SUFFIX
    }

    /*
     * buildFinalLogFilePathFromParams
     * -------------------------------
     * Build the final *log filename* (NO subfolder suffix):
     * "<params-token>-time<RUN_TIMESTAMP>.log"
     */
    fun buildFinalLogFilePathFromParams(paramsToken: String): String {
        val directoryPath = ensureLogDir()
        return directoryPath +
            paramsToken +
            Constants.FILE_NAME_SEPARATOR +
            "time" +
            Constants.RUN_TIMESTAMP +
            Constants.LOG_FILE_SUFFIX
    }

    /* ----------------------------------------------------------------------------------------------------------------
     * Log4j operations (mechanics)
     * ---------------------------------------------------------------------------------------------------------------- */

    /*
     * switchActiveLogFile
     * -------------------
     * Switch the active Log4j file appender to a new base path.
     * Assumes your log4j2 config uses ${sys:baseLogFileName}.
     */
    fun switchActiveLogFile(newAbsoluteBasePath: String) {
        System.setProperty("baseLogFileName", newAbsoluteBasePath)
        val loggerContext = LogManager.getContext(false) as LoggerContext
        loggerContext.reconfigure()
    }

    /*
     * updateRootLoggerLevel
     * ---------------------
     * Update the root logger level and refresh loggers.
     * (Does not change the file path; use switchActiveLogFile for that.)
     */
    fun updateRootLoggerLevel(level: Level): Logger {
        val loggerContext = LogManager.getContext(false) as LoggerContext
        val configuration = loggerContext.configuration
        val rootConfig = configuration.getLoggerConfig(LogManager.ROOT_LOGGER_NAME)
        rootConfig.level = level
        loggerContext.updateLoggers()
        return LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
    }

    /* ----------------------------------------------------------------------------------------------------------------
     * High-level workflow
     * ---------------------------------------------------------------------------------------------------------------- */

    /*
     * promoteBootstrapToParameterizedLog
     * ----------------------------------
     * Copy the bootstrap log into the final parameterized log file, switch Log4j
     * to that file, and then delete the bootstrap file so we don't leave clutter.
     *
     * Order matters:
     *  1) copy  ->  2) switch appender  ->  3) delete bootstrap
     * This minimizes the window where new messages could land in the old file.
     */
    fun promoteBootstrapToParameterizedLog(
        paramsToken: String,
        desiredRootLevel: Level? = null
    ) {
        val sourcePath = Paths.get(getBootstrapLogFilePath())
        val destinationPath = Paths.get(buildFinalLogFilePathFromParams(paramsToken))

        Files.createDirectories(destinationPath.parent)

        if (Files.exists(sourcePath)) {
            /* Copy bootstrap contents to the final file */
            Files.copy(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING)
        } else {
            /* Ensure destination exists so the appender can append */
            if (!Files.exists(destinationPath)) {
                Files.createFile(destinationPath)
            }
        }

        /* Switch Log4j to write to the final parameterized file */
        switchActiveLogFile(destinationPath.toString())

        /* Apply requested level after reconfiguration (if any) */
        desiredRootLevel?.let { updateRootLoggerLevel(it) }

        /* Best-effort cleanup of the bootstrap file (ignore errors) */
        try {
            if (Files.exists(sourcePath)) {
                Files.deleteIfExists(sourcePath)
            }
        } catch (_: Exception) {
            /* Intentionally ignored — logging must not fail due to cleanup. */
        }
    }
}
