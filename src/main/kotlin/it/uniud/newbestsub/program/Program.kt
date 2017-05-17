package it.uniud.newbestsub.program

import it.uniud.newbestsub.dataset.DatasetController
import it.uniud.newbestsub.utils.BestSubsetLogger
import it.uniud.newbestsub.utils.Constants
import org.apache.commons.cli.*
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
        val loggingModality: String
        var resultPath: String

        println("SYSTEM - NewBestSub is starting.")
        println("SYSTEM - Wait for the program to complete...")

        try {

            parser = DefaultParser()
            commandLine = parser.parse(options, arguments)
            datasetPath = Constants.INPUT_PATH + commandLine.getOptionValue("fi") + ".csv"

            if (!File(datasetPath).exists()) throw FileNotFoundException("Dataset file does not exists. Be sure that path is correct.") else {

                resultPath = commandLine.getOptionValue("fi") + "-"

                if (commandLine.getOptionValue("l") == "Debug" || commandLine.getOptionValue("l") == "File") {
                    loggingModality = commandLine.getOptionValue("l")
                } else throw ParseException("Value for the option <<l>> or <<log>> is wrong. Check the usage section below.")

                if (commandLine.getOptionValue("c") == "Pearson" || commandLine.getOptionValue("c") == "Kendall") {
                    chosenCorrelationMethod = commandLine.getOptionValue("c")
                    resultPath = resultPath + chosenCorrelationMethod + "-"
                } else throw ParseException("Value for the option <<c>> or <<corr>> is wrong. Check the usage section below.")

                if (commandLine.getOptionValue("t") == "Best" || commandLine.getOptionValue("t") == "Worst" || commandLine.getOptionValue("t") == "Average" || commandLine.getOptionValue("t") == "All") {

                    targetToAchieve = commandLine.getOptionValue("t")

                    if (commandLine.getOptionValue("t") != "Average") {

                        if (!commandLine.hasOption("i")) throw ParseException("Value for the option <<i>> or <<iter>> is missing. Check the usage section below.")
                        try {
                            numberOfIterations = Integer.parseInt(commandLine.getOptionValue("i"))
                            datasetController = DatasetController()
                            datasetController.loadData(datasetPath)
                            if (commandLine.getOptionValue("t") == "All") datasetController.solve(chosenCorrelationMethod, numberOfIterations, resultPath)
                            else {
                                resultPath += targetToAchieve
                                datasetController.solve(chosenCorrelationMethod, targetToAchieve, numberOfIterations, resultPath)
                            }
                        } catch (exception: NumberFormatException) {
                            throw ParseException("Value for the option <<i>> or <<iter>> is not an integer. Check the usage section below")
                        }

                    } else {

                        if (commandLine.hasOption("i")) throw ParseException("Option <<i>> or <<iter>> is not necessary. Please remove it and launch the program again. Check the usage section below.")
                        resultPath += targetToAchieve
                        datasetController = DatasetController()
                        datasetController.loadData(datasetPath)
                        datasetController.solve(chosenCorrelationMethod, targetToAchieve, 0, resultPath)
                    }

                } else throw ParseException("Value for the option <<t>> or <<target>> is wrong. Check the usage section below.")
            }

        } catch (exception: ParseException) {
            println("EXCEPTION (System) - ${exception.message}")
            val formatter = HelpFormatter()
            formatter.printHelp("NewBestSub", options)
        } catch (exception: FileNotFoundException) {
            println("EXCEPTION (System) - ${exception.message}")
        }

        println("SYSTEM - NewBestSub is closing.")

    }

    fun loadCommandLineOptions(): Options {

        val options = Options()
        var source = Option.builder("fi").longOpt("fileIn").desc("Relative path to the CSV dataset file (do not use any extension in filename) [REQUIRED].").hasArg().argName("SourceFile").required().build()
        options.addOption(source)
        source = Option.builder("c").longOpt("corr").desc("Indicates the method that must be used to compute correlations. Available methods: Pearson, Kendall. [REQUIRED]").hasArg().argName("Method").required().build()
        options.addOption(source)
        source = Option.builder("t").longOpt("target").desc("Indicates the target that must be achieved. Available targets: Best, Worst, Average, All. [REQUIRED]").hasArg().argName("Target").required().build()
        options.addOption(source)
        source = Option.builder("l").longOpt("log").desc("Indicates the required level of logging. Available levels: Debug, File. [REQUIRED]").required().hasArg().argName("Log Level").build()
        options.addOption(source)
        source = Option.builder("i").longOpt("iter").desc("Indicates the number of iterations to be done. It must be an integer value. It is mandatory only if the selected target is: Best, Worst, All. [OPTIONAL]").hasArg().argName("Number").build()
        options.addOption(source)
        return options

    }
}


