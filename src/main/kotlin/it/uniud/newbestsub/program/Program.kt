package it.uniud.newbestsub.program

import it.uniud.newbestsub.dataset.DatasetController
import it.uniud.newbestsub.dataset.Parameters
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
        var numberOfRepetitions: Int
        var populationSize: Int
        var percentiles: List<Int>
        val expansionCoefficient: Int
        val numberOfExecutions: Int
        var resultPath: String
        val loggingLevel: Level
        var logger: Logger

        System.setProperty("baseLogFileName", "${Constants.LOG_PATH}/Program.log")
        logger = updateLogger(LogManager.getLogger(), Level.INFO)

        try {

            parser = DefaultParser()
            commandLine = parser.parse(options, arguments)
            datasetPath = Constants.INPUT_PATH + commandLine.getOptionValue("fi") + ".csv"

            if (!File(datasetPath).exists()) throw FileNotFoundException("Dataset file does not exists. Be sure that path is correct.") else {

                resultPath = "${commandLine.getOptionValue("fi")}-"

                if (commandLine.getOptionValue("l") == "Verbose" || commandLine.getOptionValue("l") == "Limited" || commandLine.getOptionValue("l") == "Off") {
                    when (commandLine.getOptionValue("l")) {
                        "Verbose" -> loggingLevel = Level.DEBUG
                        "Limited" -> loggingLevel = Level.INFO
                        "Off" -> loggingLevel = Level.OFF
                        else -> loggingLevel = Level.INFO
                    }
                } else throw ParseException("Value for the option <<l>> or <<log>> is wrong. Check the usage section below.")

                if (commandLine.getOptionValue("c") == Constants.CORRELATION_PEARSON || commandLine.getOptionValue("c") == Constants.CORRELATION_KENDALL) {

                    chosenCorrelationMethod = commandLine.getOptionValue("c")
                    resultPath = "$resultPath$chosenCorrelationMethod-"

                } else throw ParseException("Value for the option <<c>> or <<corr>> is wrong. Check the usage section below.")

                if (commandLine.getOptionValue("t") == Constants.TARGET_BEST || commandLine.getOptionValue("t") == Constants.TARGET_WORST || commandLine.getOptionValue("t") == Constants.TARGET_AVERAGE || commandLine.getOptionValue("t") == Constants.TARGET_ALL) {

                    targetToAchieve = commandLine.getOptionValue("t")
                    numberOfIterations = 0
                    populationSize = 0
                    numberOfRepetitions = 0
                    percentiles = emptyList()

                    if (targetToAchieve != Constants.TARGET_AVERAGE) {

                        if (!commandLine.hasOption("i")) throw ParseException("Value for the option <<i>> or <<iter>> is missing. Check the usage section below.")
                        try {
                            numberOfIterations = Integer.parseInt(commandLine.getOptionValue("i"))
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<i>> or <<iter>> is not an integer. Check the usage section below")
                        }

                        if (!commandLine.hasOption("po")) throw ParseException("Value for the option <<po>> or <<pop>> is missing. Check the usage section below.")
                        try {
                            populationSize = Integer.parseInt(commandLine.getOptionValue("po"))
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<po>> or <<pop>> is not an integer. Check the usage section below")
                        }

                        resultPath += "$numberOfIterations-"
                        resultPath += "$populationSize-"

                    }

                    if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {

                        if (!commandLine.hasOption("r")) throw ParseException("Value for the option <<r>> or <<rep>> is missing. Check the usage section below.")
                        try {
                            numberOfRepetitions = Integer.parseInt(commandLine.getOptionValue("r"))
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<r>> or <<rep>> is not an integer. Check the usage section below")
                        }

                        val percentilesToParse = commandLine.getOptionValues("pe").toList()
                        if (percentilesToParse.size > 2) throw ParseException("Value for the option <<pe>> or <<perc>> is wrong. Check the usage section below.")
                        try {
                            val firstPercentile = Integer.parseInt(percentilesToParse[0])
                            val lastPercentile = Integer.parseInt(percentilesToParse[1])
                            for (currentPercentile in firstPercentile..lastPercentile)
                                percentiles = percentiles.plus(currentPercentile)
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<pe>> or <<perc>> is not an integer. Check the usage section below")
                        }

                        resultPath += "$numberOfRepetitions-"

                    }

                    if (targetToAchieve != Constants.TARGET_ALL) {
                        resultPath += targetToAchieve
                        System.setProperty("baseLogFileName", "${Constants.LOG_PATH}$resultPath.log")
                        resultPath += "-"
                    } else System.setProperty("baseLogFileName", "${Constants.LOG_PATH}$resultPath${Constants.TARGET_ALL}.log")

                    logger = updateLogger(LogManager.getLogger(), loggingLevel)
                    logger.info("NewBestSub execution started.")

                    datasetController = DatasetController(targetToAchieve)
                    datasetController.load(datasetPath)

                    if (commandLine.hasOption('m')) {

                        try {
                            numberOfExecutions = Integer.parseInt(commandLine.getOptionValue("m"))
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<m>> or <<mrg>> is not an integer. Check the usage section below")
                        }

                        (1..numberOfExecutions).forEach{
                            currentExecution ->
                            logger.info("Execution number: $currentExecution")
                            val currentExecutionResultPath = "$resultPath${datasetController.models[0].numberOfTopics}-$currentExecution-"
                            datasetController.solve(Parameters(chosenCorrelationMethod, targetToAchieve, numberOfIterations, numberOfRepetitions, populationSize, percentiles), currentExecutionResultPath)
                            datasetController.save(currentExecutionResultPath)
                        }

                        datasetController.merge()

                    } else
                        datasetController.solve(Parameters(chosenCorrelationMethod, targetToAchieve, numberOfIterations, numberOfRepetitions, populationSize, percentiles), "$resultPath${datasetController.models[0].numberOfTopics}-")

                    if (commandLine.hasOption('e')) {

                        try {
                            expansionCoefficient = Integer.parseInt(commandLine.getOptionValue('e'))
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<e>> or <<exp>> is not an integer. Check the usage section below")
                        }
                        val trueTopicNumber = datasetController.models[0].numberOfTopics

                        logger.info("Base execution (true data)")

                        while (datasetController.models[0].numberOfTopics + expansionCoefficient < Constants.MAXIMUM_EXPANSION) {
                            logger.info("Data expansion: <New Topic Number: ${datasetController.models[0].numberOfTopics + expansionCoefficient}, Earlier Topic Number: ${datasetController.models[0].numberOfTopics}, Expansion Coefficient: $expansionCoefficient, Maximum Expansion: ${Constants.MAXIMUM_EXPANSION}, Original Topic Number: $trueTopicNumber>")
                            datasetController.expand(expansionCoefficient)
                            datasetController.solve(Parameters(chosenCorrelationMethod, targetToAchieve, numberOfIterations, numberOfRepetitions, populationSize, percentiles), "$resultPath${datasetController.models[0].numberOfTopics}-")
                        }

                    }

                    logger.info("NewBestSub execution terminated.")

                } else throw ParseException("Value for the option <<t>> or <<target>> is wrong. Check the usage section below.")
            }

        } catch (exception: ParseException) {
            logger.error(exception.message)
            val formatter = HelpFormatter()
            formatter.printHelp("NewBestSub", options)
            logger.error("End of the usage section.")
            logger.info("NewBestSub execution terminated.")
        } catch (exception: FileNotFoundException) {
            logger.error(exception.message)
            logger.info("NewBestSub execution terminated.")
        } catch (exception: FileSystemException) {
            logger.error(exception.message)
            logger.error(exception.stackTrace)
            logger.info("NewBestSub execution terminated.")
        }

    }

    fun loadCommandLineOptions(): Options {

        val options = Options()
        var source = Option.builder("fi").longOpt("fileIn").desc("Relative path to the CSV dataset file (do not use any extension in filename) [REQUIRED].").hasArg().argName("SourceFile").required().build()
        options.addOption(source)
        source = Option.builder("c").longOpt("corr").desc("Indicates the method that must be used to compute correlations. Available methods: Pearson, Kendall. [REQUIRED]").hasArg().argName("Method").required().build()
        options.addOption(source)
        source = Option.builder("t").longOpt("targ").desc("Indicates the target that must be achieved. Available targets: Best, Worst, Average, All. [REQUIRED]").hasArg().argName("Target").required().build()
        options.addOption(source)
        source = Option.builder("l").longOpt("log").desc("Indicates the required level of logging. Available levels: Verbose, Limited, Off. [REQUIRED]").required().hasArg().argName("Logging Level").build()
        options.addOption(source)
        source = Option.builder("i").longOpt("iter").desc("Indicates the number of iterations to be done. It is mandatory only if the selected target is: Best, Worst, All. [OPTIONAL]").hasArg().argName("Value").build()
        options.addOption(source)
        source = Option.builder("r").longOpt("rep").desc("Indicates the number of repetitions to be done to compute a single cardinality during Average experiment. It must be an integer value. It is mandatory only if the selected target is: Average, All. [OPTIONAL]").hasArg().argName("Value").build()
        options.addOption(source)
        source = Option.builder("po").longOpt("pop").desc("Indicates the size of the initial population to be generated. It must be an integer value. It is mandatory only if the selected target is: Best, Worst, All. [OPTIONAL]").hasArg().argName("Value").build()
        options.addOption(source)
        source = Option.builder("pe").longOpt("perc").desc("Indicates the set of percentiles to be calculated. There must be two comma separated integer values. It is mandatory only if the selected target is: Average, All. [OPTIONAL]").hasArgs().valueSeparator(',').argName("Percentile").build()
        options.addOption(source)
        source = Option.builder("e").longOpt("exp").desc("Indicates the number of fake topics to be added at each iteration. It must be an integer value. [OPTIONAL]").hasArg().argName("Value").build()
        options.addOption(source)
        source = Option.builder("m").longOpt("mrg").desc("Indicates the number of executions of the program to do. The results of the executions will be merged together. It must be an integer value. [OPTIONAL]").hasArg().argName("Value").build()
        options.addOption(source)
        return options

    }

}


