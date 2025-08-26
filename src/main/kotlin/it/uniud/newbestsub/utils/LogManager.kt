package it.uniud.newbestsub.utils

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
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
 * - This object’s name intentionally matches Log4j’s type name. Calls to Log4j's static helpers
 *   are fully qualified through imports (e.g., [LogManager.getContext]) and remain unambiguous.
 * - All path helpers ensure the `${Constants.NEWBESTSUB_PATH}/log/` directory exists.
 */
object LogManager {

    // -----------------------------------------------------------------------------------------------------------------
    // Log directory & path helpers
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Ensures that the log directory exists under `NEWBESTSUB_PATH/log/` and returns its absolute path
     * with a trailing path separator.
     *
     * @return Absolute path to the log directory, ending with [Constants.PATH_SEPARATOR].
     */
    fun ensureLogDir(): String {
        val directoryPath = Paths.get(Constants.NEWBESTSUB_PATH, "log").toString() + Constants.PATH_SEPARATOR
        Files.createDirectories(Paths.get(directoryPath))
        return directoryPath
    }

    /**
     * Returns the absolute path of the **bootstrap** log file used from program start,
     * before parameters are known. Ensures the log directory exists.
     *
     * Filename pattern:
     * ```
     * <logDir>/<base><RUN_TIMESTAMP><suffix>
     * ```
     * where:
     * - `base`  = [Constants.LOG_FILE_NAME]
     * - `suffix` = [Constants.LOG_FILE_SUFFIX]
     *
     * @return Absolute path to the bootstrap log file.
     * @see ensureLogDir
     */
    fun getBootstrapLogFilePath(): String {
        val directoryPath = ensureLogDir()
        return directoryPath +
            Constants.LOG_FILE_NAME +
            Constants.RUN_TIMESTAMP +
            Constants.LOG_FILE_SUFFIX
    }

    /**
     * Builds the absolute path of the **final parameterized** log file (no subfolder).
     *
     * Filename pattern:
     * ```
     * <logDir>/<params-token>-time<RUN_TIMESTAMP>.log
     * ```
     *
     * @param paramsToken The token built from run parameters (see your filename builder utilities).
     * @return Absolute path to the final parameterized log file.
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

    // -----------------------------------------------------------------------------------------------------------------
    // Log4j operations (mechanics)
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Switches the active Log4j2 file appender to write to [newAbsoluteBasePath].
     *
     * Preconditions:
     * - Your Log4j2 configuration must reference the `baseLogFileName` system property
     *   (e.g., in the file appender `fileName`/`filePattern`).
     *
     * Steps:
     * 1. Set `baseLogFileName` system property.
     * 2. Reconfigure the current [LoggerContext].
     *
     * @param newAbsoluteBasePath Absolute base path used by the file appender.
     */
    fun switchActiveLogFile(newAbsoluteBasePath: String) {
        System.setProperty("baseLogFileName", newAbsoluteBasePath)
        val loggerContext = LogManager.getContext(false) as LoggerContext
        loggerContext.reconfigure()
    }

    /**
     * Updates the root logger level and refreshes all loggers in the current context.
     *
     * This does **not** change the output file; use [switchActiveLogFile] for path changes.
     *
     * @param level New root [Level].
     * @return The refreshed root [Logger].
     */
    fun updateRootLoggerLevel(level: Level): Logger {
        val loggerContext = LogManager.getContext(false) as LoggerContext
        val configuration = loggerContext.configuration
        val rootConfig = configuration.getLoggerConfig(LogManager.ROOT_LOGGER_NAME)
        rootConfig.level = level
        loggerContext.updateLoggers()
        return LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)
    }

    // -----------------------------------------------------------------------------------------------------------------
    // High-level workflow
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Promotes the bootstrap log to the final parameterized log:
     *
     * Workflow:
     * 1. Copy the bootstrap log to the final parameterized path (if the bootstrap exists),
     *    otherwise ensure the destination file exists.
     * 2. Switch Log4j2 to write to the new parameterized file (via [switchActiveLogFile]).
     * 3. Optionally apply a new root level (via [updateRootLoggerLevel]).
     * 4. Best-effort delete of the old bootstrap file (errors ignored).
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

        if (Files.exists(sourcePath)) {
            // Copy bootstrap contents to the final file
            Files.copy(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING)
        } else {
            // Ensure destination exists so the appender can append
            if (!Files.exists(destinationPath)) {
                Files.createFile(destinationPath)
            }
        }

        // Switch Log4j to write to the final parameterized file
        switchActiveLogFile(destinationPath.toString())

        // Apply requested level after reconfiguration (if any)
        desiredRootLevel?.let { updateRootLoggerLevel(it) }

        // Best-effort cleanup of the bootstrap file (ignore errors)
        try {
            if (Files.exists(sourcePath)) {
                Files.deleteIfExists(sourcePath)
            }
        } catch (_: Exception) {
            // Intentionally ignored — logging must not fail due to clean up.
        }
    }
}
