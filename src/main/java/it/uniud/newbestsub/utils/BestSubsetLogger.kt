package it.uniud.newbestsub.utils

import java.io.File
import java.io.IOException

import java.util.logging.*

object BestSubsetLogger {

    private lateinit var fileHandler: FileHandler
    private lateinit var currentLevel: Level

    private var textFormatter  = BestSubsetFormatter()
    private var streamHandler  = StreamHandler(System.out, textFormatter)
    private var logger =  Logger.getLogger(this.javaClass.name)

    var modality = "Debug"
        set(value) {
            logger.useParentHandlers = false

            when (value) {
                "Debug" -> {
                    logger.level = Level.FINE
                    currentLevel = Level.FINE
                }
                "File" -> {
                    logger.level = Level.FINER
                    currentLevel = Level.FINER
                }
            }

            val logDirectory = File(Constants.LOG_PATH)
            if (!logDirectory.exists()) {
                logDirectory.mkdir()
            }

            try {
                fileHandler = FileHandler(Constants.LOG_PATH + Constants.LOG_FILE_NAME)
            } catch (exception: IOException) {
                println(exception.message)
            }

            fileHandler.formatter = textFormatter
            fileHandler.level = Level.FINER
            logger.addHandler(fileHandler)
            streamHandler.formatter = textFormatter
            streamHandler.level = Level.FINE
            logger.addHandler(streamHandler)
        }

    fun log(message: String) {
        logger.log(currentLevel, message)
    }
}

