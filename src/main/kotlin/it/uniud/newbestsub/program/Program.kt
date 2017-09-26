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

    @JvmStatic
    fun main(arguments: Array<String>) {

        val commandLine: CommandLine
        val parser: CommandLineParser
        val options = loadCommandLineOptions()
        val datasetController: DatasetController
        val datasetPath: String
        val datasetName: String
        val correlationMethod: String
        val targetToAchieve: String
        var numberOfIterations: Int
        var numberOfRepetitions: Int
        var populationSize: Int
        var percentiles: List<Int>
        val topicExpansionCoefficient: Int
        val systemExpansionCoefficient: Int
        val numberOfExecutions: Int
        val loggingLevel: Level
        var logger: Logger

        System.setProperty("baseLogFileName", "${Constants.LOG_PATH}${Constants.LOG_FILE_NAME}${Constants.LOG_FILE_SUFFIX}")
        logger = updateLogger(LogManager.getLogger("it.uniud.newbestsub.program.Program"), Level.INFO)

        try {

            parser = DefaultParser()
            commandLine = parser.parse(options, arguments)
            datasetName = commandLine.getOptionValue("fi")
            datasetPath = "${Constants.NEWBESTSUB_INPUT_PATH}$datasetName.csv"

            if (!File(datasetPath).exists()) throw FileNotFoundException("Dataset file does not exists Path: \"$datasetPath\"") else {

                if (commandLine.getOptionValue("l") == "Verbose" || commandLine.getOptionValue("l") == "Limited" || commandLine.getOptionValue("l") == "Off") {
                    when (commandLine.getOptionValue("l")) {
                        "Verbose" -> loggingLevel = Level.DEBUG
                        "Limited" -> loggingLevel = Level.INFO
                        "Off" -> loggingLevel = Level.OFF
                        else -> loggingLevel = Level.INFO
                    }
                } else throw ParseException("Value for the option <<l>> or <<log>> is wrong. Check the usage section below.")

                if (commandLine.getOptionValue("c") == Constants.CORRELATION_PEARSON || commandLine.getOptionValue("c") == Constants.CORRELATION_KENDALL) {

                    correlationMethod = commandLine.getOptionValue("c")

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
                            if (numberOfIterations <= 0) throw ParseException("Value for the option <<i>> or <<iter>> must be a positive value. Check the usage section below")
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<i>> or <<iter>> is not an integer. Check the usage section below")
                        }

                        if (!commandLine.hasOption("po")) throw ParseException("Value for the option <<po>> or <<pop>> is missing. Check the usage section below.")
                        try {
                            populationSize = Integer.parseInt(commandLine.getOptionValue("po"))
                            if (populationSize <= 0) throw ParseException("Value for the option <<p>> or <<po>> must be a positive value. Check the usage section below")
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<po>> or <<pop>> is not an integer. Check the usage section below")
                        }

                    }

                    logger = updateLogger(LogManager.getLogger(), loggingLevel)
                    logger.info("${Constants.NEWBESTSUB_NAME} execution started.")
                    logger.info("Base path:")
                    logger.info("\"${Constants.BASE_PATH}\"")
                    logger.info("${Constants.NEWBESTSUB_NAME} path:")
                    logger.info("\"${Constants.NEWBESTSUB_PATH}\"")
                    logger.info("${Constants.NEWBESTSUB_NAME} input path:")
                    logger.info("\"${Constants.NEWBESTSUB_INPUT_PATH}\"")
                    logger.info("${Constants.NEWBESTSUB_NAME} output path:")
                    logger.info("\"${Constants.NEWBESTSUB_OUTPUT_PATH}\"")
                    logger.info("${Constants.NEWBESTSUB_NAME} log path:")
                    logger.info("\"${Constants.LOG_PATH}\"")

                    if (commandLine.hasOption("copy")) {

                        logger.info("${Constants.NEWBESTSUB_EXPERIMENTS_NAME} path:")
                        logger.info("\"${Constants.NEWBESTSUB_EXPERIMENTS_PATH}\"")
                        logger.info("${Constants.NEWBESTSUB_EXPERIMENTS_NAME} input path:")
                        logger.info("\"${Constants.NEWBESTSUB_EXPERIMENTS_INPUT_PATH}\"")

                        logger.info("Checking if ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} exists.")

                        val errorMessage = "${Constants.NEWBESTSUB_EXPERIMENTS_NAME} not found. Please, place its folder the same one where ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} is located. Path: \"${Constants.BASE_PATH}\""
                        if (!File(Constants.NEWBESTSUB_EXPERIMENTS_PATH).exists()) throw FileNotFoundException(errorMessage) else {
                            logger.info("${Constants.NEWBESTSUB_EXPERIMENTS_NAME} detected.")
                        }

                    }

                    if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {

                        if (!commandLine.hasOption("r")) throw ParseException("Value for the option <<r>> or <<rep>> is missing. Check the usage section below.")
                        try {
                            numberOfRepetitions = Integer.parseInt(commandLine.getOptionValue("r"))
                            if (numberOfRepetitions <= 0) throw ParseException("Value for the option <<r>> or <<rep>> must be a positive value. Check the usage section below")
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<r>> or <<rep>> is not an integer. Check the usage section below")
                        }

                        if (!commandLine.hasOption("pe")) throw ParseException("Value for the option <<pe>> or <<perc>> is missing. Check the usage section below.")
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

                    }

                    datasetController = DatasetController(targetToAchieve)
                    datasetController.load(datasetPath)

                    if (commandLine.hasOption('m')) {

                        try {
                            numberOfExecutions = Integer.parseInt(commandLine.getOptionValue("m"))
                            if (numberOfExecutions <= 0) throw ParseException("Value for the option <<m>> or <<mrg>> must be a positive value. Check the usage section below")
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<m>> or <<mrg>> is not an integer. Check the usage section below")
                        }

                        (1..numberOfExecutions).forEach { currentExecution ->
                            logger.info("Execution number: $currentExecution/$numberOfExecutions")
                            datasetController.solve(Parameters(datasetName, correlationMethod, targetToAchieve, numberOfIterations, numberOfRepetitions, populationSize, currentExecution, percentiles))
                        }

                        datasetController.merge(numberOfExecutions)

                    }

                    if (commandLine.hasOption("et")) {

                        try {
                            topicExpansionCoefficient = Integer.parseInt(commandLine.getOptionValue("et"))
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<et>> or <<expt>> is not an integer. Check the usage section below")
                        }
                        val trueTopicNumber = datasetController.models[0].numberOfTopics

                        logger.info("Base execution (true data)")

                        datasetController.solve(Parameters(datasetName, correlationMethod, targetToAchieve, numberOfIterations, numberOfRepetitions, populationSize, 0, percentiles))
                        while (datasetController.models[0].numberOfTopics + topicExpansionCoefficient < Constants.MAXIMUM_TOPIC_EXPANSION) {
                            logger.info("Data expansion: <New Topic Number: ${datasetController.models[0].numberOfTopics + topicExpansionCoefficient}, Earlier Topic Number: ${datasetController.models[0].numberOfTopics}, Expansion Coefficient: $topicExpansionCoefficient, Maximum Expansion: ${Constants.MAXIMUM_TOPIC_EXPANSION}, True Topic Number: $trueTopicNumber>")
                            datasetController.expandTopics(topicExpansionCoefficient)
                            datasetController.solve(Parameters(datasetName, correlationMethod, targetToAchieve, numberOfIterations, numberOfRepetitions, populationSize, 0, percentiles))
                        }
                    }

                    if (commandLine.hasOption("es")) {

                        try {
                            systemExpansionCoefficient = Integer.parseInt(commandLine.getOptionValue("es"))
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<es>> or <<exps> is not an integer. Check the usage section below")
                        }
                        val trueNumberOfSystems = datasetController.models[0].numberOfSystems

                        datasetController.models.forEach { model ->
                            model.numberOfSystems = 0
                        }

                        logger.info("Base execution (true data)")

                        while (datasetController.models[0].numberOfSystems + systemExpansionCoefficient < Constants.MAXIMUM_SYSTEM_EXPANSION) {
                            logger.info("Data expansion: <New System Number: ${datasetController.models[0].numberOfSystems + systemExpansionCoefficient}, Earlier System Number: ${datasetController.models[0].numberOfSystems}, Expansion Coefficient: $systemExpansionCoefficient, Maximum Expansion: ${Constants.MAXIMUM_SYSTEM_EXPANSION}, Initial System Number: $systemExpansionCoefficient, True System Number: $trueNumberOfSystems>")
                            datasetController.expandSystems(systemExpansionCoefficient, trueNumberOfSystems)
                            datasetController.solve(Parameters(datasetName, correlationMethod, targetToAchieve, numberOfIterations, numberOfRepetitions, populationSize, 0, percentiles))
                        }
                    }

                    if (!commandLine.hasOption("et") && !commandLine.hasOption("es") && !commandLine.hasOption('m')) datasetController.solve(Parameters(datasetName, correlationMethod, targetToAchieve, numberOfIterations, numberOfRepetitions, populationSize, 0, percentiles))
                    if (commandLine.hasOption("copy")) datasetController.copy()

                    logger.info("NewBestSub execution terminated.")

                } else throw ParseException("Value for the option <<t>> or <<target>> is wrong. Check the usage section below.")
            }

        } catch (exception: ParseException) {

            logger.error(exception.message)
            val formatter = HelpFormatter()
            formatter.printHelp(Constants.NEWBESTSUB_NAME, options)
            logger.error("End of the usage section.")
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")

        } catch (exception: FileNotFoundException) {

            logger.error(exception.message)
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")

        } catch (exception: FileSystemException) {

            logger.error(exception.message)
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")

        }

    }

    fun loadCommandLineOptions(): Options {

        val options = Options()
        var source = Option.builder("fi").longOpt("fileIn").desc("Relative path to the CSV dataset file (do not use any extension in filename) [REQUIRED].").hasArg().argName("Source File").required().build()
        options.addOption(source)
        source = Option.builder("c").longOpt("corr").desc("Strategy that must be used to compute correlations. Available methods: Pearson, Kendall. [REQUIRED]").hasArg().argName("Correlation").required().build()
        options.addOption(source)
        source = Option.builder("t").longOpt("targ").desc("Target that must be achieved. Available targets: Best, Worst, Average, All. [REQUIRED]").hasArg().argName("Target").required().build()
        options.addOption(source)
        source = Option.builder("l").longOpt("log").desc("Required level of logging. Available levels: Verbose, Limited, Off. [REQUIRED]").required().hasArg().argName("Logging Level").build()
        options.addOption(source)
        source = Option.builder("i").longOpt("iter").desc("Number of iterations to be done. It is mandatory only if the selected target is: Best, Worst, All. [OPTIONAL]").hasArg().argName("Number of Iterations").build()
        options.addOption(source)
        source = Option.builder("r").longOpt("rep").desc("Number of repetitions to be done to compute a single cardinality during Average experiment. It must be a positive integer value. It is mandatory only if the selected target is: Average, All. [OPTIONAL]").hasArg().argName("Number of Repetitions").build()
        options.addOption(source)
        source = Option.builder("po").longOpt("pop").desc("Size of the initial population to be generated. It must be an integer value. It must be greater or equal than/to of the number of topics of the data set. It is mandatory only if the selected target is: Best, Worst, All. [OPTIONAL]").hasArg().argName("Population Size").build()
        options.addOption(source)
        source = Option.builder("pe").longOpt("perc").desc("Set of percentiles to be calculated. There must be two comma separated integer values. Example: \"-pe 1,100\". It is mandatory only if the selected target is: Average, All. [OPTIONAL]").hasArgs().valueSeparator(',').argName("Percentile").build()
        options.addOption(source)
        source = Option.builder("et").longOpt("expt").desc("Number of fake topics to be added at each iteration. It must be a positive integer value. [OPTIONAL]").hasArg().argName("Expansion Coefficient (Topics)").build()
        options.addOption(source)
        source = Option.builder("es").longOpt("exps").desc("Number of fake systems to be added at each iteration. It must be a positive integer value. [OPTIONAL]").hasArg().argName("Expansion Coefficient (Systems)").build()
        options.addOption(source)
        source = Option.builder("m").longOpt("mrg").desc("Number of executions of the program to do. The results of the executions will be merged together. It must be a positive integer value. [OPTIONAL]").hasArg().argName("Value").build()
        options.addOption(source)
        source = Option.builder("copy").desc("NewBestSub should search the folder or NewBestSub-Experiments and copy the results of the current execution inside its data folders. The following structure into filesystem must be respected: \"baseFolder/NewBestSub/..\" and \"baseFolder/NewBestSub-Experiments/..\" otherwise, an exception will be raised. [OPTIONAL]").build()
        options.addOption(source)
        return options

    }

}


