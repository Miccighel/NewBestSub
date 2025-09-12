package it.uniud.newbestsub.utils

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.LoggerContext
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

/**
 * Logging utilities: path helpers, log file switching, and root level updates.
 *
 * This helper assumes your Log4j2 configuration references the system property
 * `baseLogFileName` (e.g., using `${sys:baseLogFileName}` in the `RollingFile` appender).
 *
 * Notes:
 * • This object’s name intentionally matches Log4j’s type name. Calls to Log4j's static helpers
 *   are referenced with fully qualified names to avoid ambiguity.
 * • All path helpers ensure the `${Constants.NEWBESTSUB_PATH}/log/` directory exists.
 */
object LogManager {

    /**
     * Ensure the log directory exists under `NEWBESTSUB_PATH/log/` and return its absolute path
     * with a trailing path separator.
     *
     * Idempotent: creating existing directories is a no-op.
     *
     * @return Absolute path to the log directory, ending with [Constants.PATH_SEPARATOR].
     */
    fun ensureLogDir(): String {
        val directoryPath = Paths.get(Constants.NEWBESTSUB_PATH, "log").toString() + Constants.PATH_SEPARATOR
        Files.createDirectories(Paths.get(directoryPath))
        return directoryPath
    }

    /**
     * Absolute path of the bootstrap log file used from program start,
     * before parameters are known. Ensures the log directory exists.
     *
     * Filename pattern:
     *   `<logDir>/<base><RUN_TIMESTAMP><suffix>`
     * where:
     *   base   = [Constants.LOG_FILE_NAME]
     *   suffix = [Constants.LOG_FILE_SUFFIX]
     */
    fun getBootstrapLogFilePath(): String {
        val directoryPath = ensureLogDir()
        return directoryPath +
            Constants.LOG_FILE_NAME +
            Constants.RUN_TIMESTAMP +
            Constants.LOG_FILE_SUFFIX
    }

    /**
     * Absolute path of the final parameterized log file (no subfolder).
     *
     * Filename pattern:
     *   `<logDir>/<params-token>-time<RUN_TIMESTAMP>.log`
     *
     * @param paramsToken Token built from run parameters (see filename builder utilities).
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

    /**
     * Switch the active Log4j2 file appender to write to [newAbsoluteBasePath].
     *
     * Preconditions:
     * • The Log4j2 configuration must reference the `baseLogFileName` system property
     *   (e.g., in the file appender `fileName`/`filePattern`).
     *
     * Steps:
     * 1) Set `baseLogFileName` system property.
     * 2) Reconfigure the current [LoggerContext].
     *
     * Propagation:
     * • Reconfiguration affects the whole JVM logging context.
     */
    fun switchActiveLogFile(newAbsoluteBasePath: String) {
        System.setProperty("baseLogFileName", newAbsoluteBasePath)
        val loggerContext = org.apache.logging.log4j.LogManager.getContext(false) as LoggerContext
        loggerContext.reconfigure()
    }

    /**
     * Update the root logger level and refresh all loggers in the current context.
     *
     * This does not change the output file; use [switchActiveLogFile] for path changes.
     *
     * @param level New root [Level].
     * @return The refreshed root logger.
     */
    fun updateRootLoggerLevel(level: Level): org.apache.logging.log4j.Logger {
        val loggerContext = org.apache.logging.log4j.LogManager.getContext(false) as LoggerContext
        val configuration = loggerContext.configuration
        val rootConfig = configuration.getLoggerConfig(org.apache.logging.log4j.LogManager.ROOT_LOGGER_NAME)
        rootConfig.level = level
        loggerContext.updateLoggers()
        return org.apache.logging.log4j.LogManager.getLogger(org.apache.logging.log4j.LogManager.ROOT_LOGGER_NAME)
    }

    /**
     * Promote the bootstrap log to the final parameterized log.
     *
     * Workflow:
     * 1) If the bootstrap file exists, copy its contents to the final parameterized path;
     *    otherwise, ensure the destination file exists so the appender can append to it.
     * 2) Switch Log4j2 to write to the new parameterized file (via [switchActiveLogFile]).
     * 3) Optionally apply a new root level (via [updateRootLoggerLevel]).
     * 4) Best-effort delete of the old bootstrap file (errors ignored).
     *
     * Ordering minimizes the window where messages could land in the old file.
     *
     * @param paramsToken Parameter token used to build the final log filename.
     * @param desiredRootLevel Optional root level to apply after switching files.
     */
    fun promoteBootstrapToParameterizedLog(
        paramsToken: String,
        desiredRootLevel: Level? = null
    ) {
        val sourcePath = Paths.get(getBootstrapLogFilePath())
        val destinationPath = Paths.get(buildFinalLogFilePathFromParams(paramsToken))

        Files.createDirectories(destinationPath.parent)

        /*
         * If a bootstrap log exists, copy its contents so the final file starts with all early messages.
         * Otherwise, create an empty destination file to guarantee the appender has a valid target.
         */
        if (Files.exists(sourcePath)) {
            Files.copy(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING)
        } else {
            if (!Files.exists(destinationPath)) {
                Files.createFile(destinationPath)
            }
        }

        /*
         * Atomically switch the active file in the Log4j2 context. After this point, all subsequent
         * log events will be appended to the final parameterized file.
         */
        switchActiveLogFile(destinationPath.toString())

        /* Apply the requested root level after the reconfiguration, if provided. */
        desiredRootLevel?.let { updateRootLoggerLevel(it) }

        /*
         * Clean up the bootstrap file. Any failure here is intentionally ignored to avoid
         * disrupting the main execution flow due to log housekeeping.
         */
        try {
            if (Files.exists(sourcePath)) {
                Files.deleteIfExists(sourcePath)
            }
        } catch (_: Exception) {
            /* Ignored by design: logging must not fail due to clean-up. */
        }
    }
}
