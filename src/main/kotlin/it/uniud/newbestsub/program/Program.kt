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
        val numberOfIterations: Int
        var populationSize: Int
        var percentiles: List<Int>
        val expansionCoefficient: Int
        var resultPath: String
        val loggingLevel: Level
        val logger: Logger

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
                    numberOfIterations = Integer.parseInt(commandLine.getOptionValue("i"))
                    populationSize = 0
                    percentiles = emptyList()

                    if (targetToAchieve != Constants.TARGET_AVERAGE) {

                        if (!commandLine.hasOption("po")) throw ParseException("Value for the option <<po>> or <<pop>> is missing. Check the usage section below.")
                        try {
                            populationSize = Integer.parseInt(commandLine.getOptionValue("po"))
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<po>> or <<pop>> is not an integer. Check the usage section below")
                        }

                    }

                    if (targetToAchieve != Constants.TARGET_ALL) {
                        resultPath += targetToAchieve
                        System.setProperty("baseLogFileName", "${Constants.LOG_PATH}$resultPath.log")
                        resultPath += "-"
                    } else System.setProperty("baseLogFileName", "${Constants.LOG_PATH}$resultPath${Constants.TARGET_ALL}.log")


                    if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {

                        val percentilesToParse = commandLine.getOptionValues("pe").toList()
                        try {
                            percentiles = percentilesToParse.zip(List(percentilesToParse.size, { "" }), { percentileToParse, _ -> Integer.parseInt(percentileToParse) })
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<pe>> or <<perc>> is not an integer. Check the usage section below")
                        }

                    }

                    logger = updateLogger(LogManager.getLogger(), loggingLevel)
                    logger.info("NewBestSub execution started.")

                    datasetController = DatasetController(targetToAchieve)
                    datasetController.loadData(datasetPath)
                    datasetController.solve(Parameters(chosenCorrelationMethod, targetToAchieve, numberOfIterations, populationSize, percentiles), "$resultPath${datasetController.models[0].numberOfTopics}-")

                    if (commandLine.hasOption('e')) {

                        expansionCoefficient = commandLine.getOptionValue('e').toInt()
                        val trueTopicNumber = datasetController.models[0].numberOfTopics

                        logger.info("Base execution (true data)")

                        while (datasetController.models[0].numberOfTopics + expansionCoefficient < Constants.MAXIMUM_EXPANSION) {
                            logger.info("Data expansion: <New Topic Number: ${datasetController.models[0].numberOfTopics + expansionCoefficient}, Earlier Topic Number: ${datasetController.models[0].numberOfTopics}, Expansion Coefficient: $expansionCoefficient, Maximum Expansion: ${Constants.MAXIMUM_EXPANSION}, Original Topic Number: $trueTopicNumber>")
                            datasetController.expandData(expansionCoefficient)
                            datasetController.solve(Parameters(chosenCorrelationMethod, targetToAchieve, numberOfIterations, populationSize, percentiles), "$resultPath${datasetController.models[0].numberOfTopics}-")
                        }

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
        source = Option.builder("t").longOpt("targ").desc("Indicates the target that must be achieved. Available targets: Best, Worst, Average, All. [REQUIRED]").hasArg().argName("Target").required().build()
        options.addOption(source)
        source = Option.builder("l").longOpt("log").desc("Indicates the required level of logging. Available levels: Verbose, Limited, Off. [REQUIRED]").required().hasArg().argName("Logging Level").build()
        options.addOption(source)
        source = Option.builder("i").longOpt("iter").desc("Indicates the number of iterations to be done. It must be an integer value. [REQUIRED]").required().hasArg().argName("Value").build()
        options.addOption(source)
        source = Option.builder("po").longOpt("pop").desc("Indicates the size of the initial population to be generated. It must be an integer value. It is mandatory only if the selected target is: Best, Worst, All. [OPTIONAL]").hasArg().argName("Value").build()
        options.addOption(source)
        source = Option.builder("pe").longOpt("perc").desc("Indicates the set of percentiles to be calculated . It must be a list of comma separated integer values. It is mandatory only if the selected target is: Average, All. [OPTIONAL]").hasArgs().valueSeparator(',').argName("Percentile").build()
        options.addOption(source)
        source = Option.builder("e").longOpt("exp").desc("Indicates the number of fake topics to be added at each iteration. It must be an integer value. [OPTIONAL]").hasArg().argName("Value").build()
        options.addOption(source)
        return options

    }

}


