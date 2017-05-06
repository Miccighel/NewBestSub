package it.uniud.newbestsub.utils

import java.io.File
import java.io.IOException

import java.util.logging.*

class BestSubsetLogger {

    companion object {

        private lateinit var fileHandler: FileHandler
        private lateinit var streamHandler: StreamHandler
        private lateinit var textFormatter: BestSubsetFormatter
        private lateinit var currentLevel: Level
        private lateinit var modality: String
        private  var logger = Logger.getLogger("BestSubsetLogger")

        fun loadModality(modalityToUse: String) {

            modality = modalityToUse
            logger.useParentHandlers = false

            when (modality) {
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
            if (!logDirectory.exists())
                logDirectory.mkdir()

            try {
                fileHandler = FileHandler(Constants.LOG_PATH + Constants.LOG_FILE_NAME)
            } catch (exception: IOException) {
                println(exception.message)
            }

            textFormatter = BestSubsetFormatter()

            fileHandler.formatter = textFormatter
            fileHandler.level = Level.FINER
            logger.addHandler(fileHandler)
            streamHandler = StreamHandler(System.out, textFormatter)
            streamHandler.formatter = textFormatter
            streamHandler.level = Level.FINE
            logger.addHandler(streamHandler)

        }

        fun log(message: String) {
            logger.log(currentLevel, message)
        }
    }

}