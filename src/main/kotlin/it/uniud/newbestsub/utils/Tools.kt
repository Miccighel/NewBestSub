package it.uniud.newbestsub.utils

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.core.LoggerContext

object Tools {

    fun stringComparison(firstString: String, secondString: String): Int {
        var distance = 0
        var i = 0
        while (i < firstString.length) {
            if (secondString[i] != firstString[i]) distance++; i++
        }
        return distance
    }

    fun getMean(run: DoubleArray, useColumns: BooleanArray): Double {

        var mean = 0.0
        var numberOfUsedCols = 0

        useColumns.forEachIndexed { _, value ->
            if (value) {
                numberOfUsedCols++
            }
        }
        useColumns.forEachIndexed { index, value ->
            if (value) {
                mean += run[index]
            }
        }

        mean /= numberOfUsedCols.toDouble()

        return mean
    }

    fun updateLogger(logger: Logger, level: Level): Logger {
        val currentContext = (LogManager.getContext(false) as LoggerContext)
        val currentConfiguration = currentContext.configuration
        val loggerConfig = currentConfiguration.getLoggerConfig(LogManager.ROOT_LOGGER_NAME)
        loggerConfig.level = level
        currentContext.updateLoggers()
        return logger
    }

}
