package it.uniud.newbestsub.program

import it.uniud.newbestsub.dataset.DatasetController
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools.updateLogger
import org.apache.commons.cli.*
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.io.File
import java.io.FileNotFoundException

object Program {

    @JvmStatic fun main(arguments: Array<String>) {

        val commandLine: CommandLine
        val parser: CommandLineParser
        val options = loadCommandLineOptions()
        val datasetController: DatasetController
        val datasetPath: String
        val chosenCorrelationMethod: String
        val targetToAchieve: String
        var numberOfIterations: Int
        var resultPath: String
        val loggingLevel: Level
        var logger: Logger

        try {

            parser = DefaultParser()
            commandLine = parser.parse(options, arguments)
            datasetPath = Constants.INPUT_PATH + commandLine.getOptionValue("fi") + ".csv"

            if (!File(datasetPath).exists()) throw FileNotFoundException("Dataset file does not exists. Be sure that path is correct.") else {

                resultPath = commandLine.getOptionValue("fi") + "-"

                if (commandLine.getOptionValue("l") == "Verbose" || commandLine.getOptionValue("l") == "Limited" || commandLine.getOptionValue("l") == "Off") {
                    when (commandLine.getOptionValue("l")) {
                        "Verbose" -> loggingLevel = Level.DEBUG
                        "Limited" -> loggingLevel = Level.INFO
                        "Off" -> loggingLevel = Level.OFF
                        else -> loggingLevel = Level.INFO
                    }
                } else throw ParseException("Value for the option <<l>> or <<log>> is wrong. Check the usage section below.")

                if (commandLine.getOptionValue("c") == "Pearson" || commandLine.getOptionValue("c") == "Kendall") {
                    chosenCorrelationMethod = commandLine.getOptionValue("c")
                    resultPath = resultPath + chosenCorrelationMethod + "-"
                } else throw ParseException("Value for the option <<c>> or <<corr>> is wrong. Check the usage section below.")

                if (commandLine.getOptionValue("t") == "Best" || commandLine.getOptionValue("t") == "Worst" || commandLine.getOptionValue("t") == "Average" || commandLine.getOptionValue("t") == "All") {
                    targetToAchieve = commandLine.getOptionValue("t")
                    numberOfIterations = 0
                    if (targetToAchieve != "Average") {
                        if (!commandLine.hasOption("i")) throw ParseException("Value for the option <<i>> or <<iter>> is missing. Check the usage section below.")
                        try {
                            numberOfIterations = Integer.parseInt(commandLine.getOptionValue("i"))
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<i>> or <<iter>> is not an integer. Check the usage section below")
                        }
                    } else {
                        if (commandLine.hasOption("i")) throw ParseException("Option <<i>> or <<iter>> is not necessary. Please remove it and launch the program again. Check the usage section below.")
                    }
                    if(targetToAchieve != "All") {
                        resultPath += targetToAchieve
                        System.setProperty("baseLogFileName", Constants.LOG_PATH + resultPath + ".log")
                    } else System.setProperty("baseLogFileName", Constants.LOG_PATH + resultPath + "All.log")
                    logger = updateLogger(LogManager.getLogger(),loggingLevel)
                    logger.info("NewBestSub execution started.")
                    datasetController = DatasetController()
                    datasetController.loadData(datasetPath)
                    when (targetToAchieve) {
                        "All" -> datasetController.solve(chosenCorrelationMethod, numberOfIterations, resultPath)
                        "Average" -> datasetController.solve(chosenCorrelationMethod, targetToAchieve, 0, resultPath)
                        else -> datasetController.solve(chosenCorrelationMethod, targetToAchieve, numberOfIterations, resultPath)
                    }
                    logger.info("NewBestSub execution terminated.")
                } else throw ParseException("Value for the option <<t>> or <<target>> is wrong. Check the usage section below.")
            }

        } catch (exception: ParseException) {
            println(exception.message)
            val formatter = HelpFormatter()
            formatter.printHelp("NewBestSub", options)
        } catch (exception: FileNotFoundException) {
            println(exception.message)
        }

    }

    fun loadCommandLineOptions(): Options {

        val options = Options()
        var source = Option.builder("fi").longOpt("fileIn").desc("Relative path to the CSV dataset file (do not use any extension in filename) [REQUIRED].").hasArg().argName("SourceFile").required().build()
        options.addOption(source)
        source = Option.builder("c").longOpt("corr").desc("Indicates the method that must be used to compute correlations. Available methods: Pearson, Kendall. [REQUIRED]").hasArg().argName("Method").required().build()
        options.addOption(source)
        source = Option.builder("t").longOpt("target").desc("Indicates the target that must be achieved. Available targets: Best, Worst, Average, All. [REQUIRED]").hasArg().argName("Target").required().build()
        options.addOption(source)
        source = Option.builder("l").longOpt("log").desc("Indicates the required level of logging. Available levels: Verbose, Limited, Off. [REQUIRED]").required().hasArg().argName("Log Level").build()
        options.addOption(source)
        source = Option.builder("i").longOpt("iter").desc("Indicates the number of iterations to be done. It must be an integer value. It is mandatory only if the selected target is: Best, Worst, All. [OPTIONAL]").hasArg().argName("Number").build()
        options.addOption(source)
        return options

    }

}


