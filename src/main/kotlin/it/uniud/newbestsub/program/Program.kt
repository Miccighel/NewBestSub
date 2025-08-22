package it.uniud.newbestsub.program

import it.uniud.newbestsub.dataset.DatasetController
import it.uniud.newbestsub.dataset.Parameters
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools
import it.uniud.newbestsub.utils.LogManager
import it.uniud.newbestsub.utils.RandomBridge
import org.apache.commons.cli.*
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.Logger
import org.uma.jmetal.util.errorchecking.JMetalException
import java.io.File
import java.io.FileNotFoundException
import java.nio.file.FileSystemException
import org.apache.commons.cli.help.HelpFormatter

object Program {

    @JvmStatic
    fun main(arguments: Array<String>) {

        println("Program started.")

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
        val topicMaximumExpansionCoefficient: Int
        val systemMaximumExpansionCoefficient: Int
        val numberOfExecutions: Int
        val loggingLevel: Level
        var logger: Logger

        /* -------- Bootstrap logging to a timestamped file (no params yet) -------- */
        val bootstrapLogPath = LogManager.getBootstrapLogFilePath()
        System.setProperty("baseLogFileName", bootstrapLogPath)
        logger = LogManager.updateRootLoggerLevel(Level.INFO)

        try {
            parser = DefaultParser()
            commandLine = parser.parse(options, arguments)
            datasetName = commandLine.getOptionValue("fi")
            datasetPath = "${Constants.NEWBESTSUB_INPUT_PATH}$datasetName.csv"

            if (!File(datasetPath).exists()) throw FileNotFoundException("Dataset file does not exists Path: \"$datasetPath\"") else {

                /* LogManager level */
                loggingLevel = when (commandLine.getOptionValue("l")) {
                    "Verbose" -> Level.DEBUG
                    "Limited" -> Level.INFO
                    "Off" -> Level.OFF
                    else -> throw ParseException("Value for the option <<l>> or <<log>> is wrong. Check the usage section below.")
                }

                /* Correlation method */
                correlationMethod = when (commandLine.getOptionValue("c")) {
                    Constants.CORRELATION_PEARSON, Constants.CORRELATION_KENDALL -> commandLine.getOptionValue("c")
                    else -> throw ParseException("Value for the option <<c>> or <<corr>> is wrong. Check the usage section below.")
                }

                /* Target */
                targetToAchieve = when (commandLine.getOptionValue("t")) {
                    Constants.TARGET_BEST, Constants.TARGET_WORST, Constants.TARGET_AVERAGE, Constants.TARGET_ALL ->
                        commandLine.getOptionValue("t")

                    else -> throw ParseException("Value for the option <<t>> or <<target>> is wrong. Check the usage section below.")
                }

                /* Defaults (filled depending on target) */
                numberOfIterations = 0
                populationSize = 0
                numberOfRepetitions = 0
                percentiles = emptyList()

                if (targetToAchieve != Constants.TARGET_AVERAGE) {
                    if (!commandLine.hasOption("i")) throw ParseException("Value for the option <<i>> or <<iter>> is missing. Check the usage section below.")
                    numberOfIterations = commandLine.getOptionValue("i").toIntOrNull()
                        ?: throw ParseException("Value for the option <<i>> or <<iter>> is not an integer. Check the usage section below")
                    if (numberOfIterations <= 0) throw ParseException("Value for the option <<i>> or <<iter>> must be a positive value.")

                    if (!commandLine.hasOption("po")) throw ParseException("Value for the option <<po>> or <<pop>> is missing. Check the usage section below.")
                    populationSize = commandLine.getOptionValue("po").toIntOrNull()
                        ?: throw ParseException("Value for the option <<po>> or <<pop>> is not an integer. Check the usage section below")
                    if (populationSize <= 0) throw ParseException("Value for the option <<po>> or <<pop>> must be a positive value.")
                }

                if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {
                    if (!commandLine.hasOption("r")) throw ParseException("Value for the option <<r>> or <<rep>> is missing. Check the usage section below.")
                    numberOfRepetitions = commandLine.getOptionValue("r").toIntOrNull()
                        ?: throw ParseException("Value for the option <<r>> or <<rep>> is not an integer. Check the usage section below")
                    if (numberOfRepetitions <= 0) throw ParseException("Value for the option <<r>> or <<rep>> must be a positive value.")

                    if (!commandLine.hasOption("pe")) throw ParseException("Value for the option <<pe>> or <<perc>> is missing. Check the usage section below.")
                    val percentilesToParse = commandLine.getOptionValues("pe").toList()
                    if (percentilesToParse.size > 2) throw ParseException("Value for the option <<pe>> or <<perc>> is wrong. Check the usage section below.")
                    try {
                        val firstPercentile = percentilesToParse[0].toInt()
                        val lastPercentile = percentilesToParse[1].toInt()
                        percentiles = (firstPercentile..lastPercentile).toList()
                    } catch (_: NumberFormatException) {
                        throw ParseException("Value for the option <<pe>> or <<perc>> is not an integer. Check the usage section below")
                    }
                }

                /* Apply requested logging level (still pointing to bootstrap file) */
                logger = LogManager.updateRootLoggerLevel(loggingLevel)

                /* Controller + dataset load (to know topics/systems/etc.) */
                datasetController = DatasetController(targetToAchieve)
                datasetController.load(datasetPath)

                /* Build the final log filename with params (percentiles only for Average) and switch to it */
                run {
                    val model = datasetController.models[0]
                    val paramsToken = Tools.buildParamsToken(
                        datasetName = datasetName,
                        correlationMethod = correlationMethod,
                        numberOfTopics = model.numberOfTopics,
                        numberOfSystems = model.numberOfSystems,
                        populationSize = populationSize,
                        numberOfIterations = numberOfIterations,
                        numberOfRepetitions = numberOfRepetitions,
                        expansionCoefficient = model.expansionCoefficient,
                        includePercentiles = (targetToAchieve == Constants.TARGET_AVERAGE),
                        percentiles = percentiles
                    )
                    LogManager.promoteBootstrapToParameterizedLog(paramsToken, loggingLevel)
                }

                logger.info("${Constants.NEWBESTSUB_NAME} execution started.")
                logger.info("Base path:")
                logger.info("\"${Constants.BASE_PATH}\"")
                logger.info("${Constants.NEWBESTSUB_NAME} path:")
                logger.info("\"${Constants.NEWBESTSUB_PATH}\"")
                logger.info("${Constants.NEWBESTSUB_NAME} input path:")
                logger.info("\"${Constants.NEWBESTSUB_INPUT_PATH}\"")
                logger.info("${Constants.NEWBESTSUB_NAME} output path:")
                logger.info("\"${Constants.NEWBESTSUB_OUTPUT_PATH}\"")
                logger.info("${Constants.NEWBESTSUB_NAME} log path (directory):")
                logger.info("\"${Constants.NEWBESTSUB_PATH}log${Constants.PATH_SEPARATOR}\"")

                if (commandLine.hasOption("copy")) {
                    logger.info("${Constants.NEWBESTSUB_EXPERIMENTS_NAME} path:")
                    logger.info("\"${Constants.NEWBESTSUB_EXPERIMENTS_PATH}\"")
                    logger.info("${Constants.NEWBESTSUB_EXPERIMENTS_NAME} input path:")
                    logger.info("\"${Constants.NEWBESTSUB_EXPERIMENTS_INPUT_PATH}\"")

                    logger.info("Checking if ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} exists.")

                    val errorMessage =
                        "${Constants.NEWBESTSUB_EXPERIMENTS_NAME} not found. Please, place its folder the same one where ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} is located. Path: \"${Constants.BASE_PATH}\""
                    if (!File(Constants.NEWBESTSUB_EXPERIMENTS_PATH).exists()) throw FileNotFoundException(errorMessage) else {
                        logger.info("${Constants.NEWBESTSUB_EXPERIMENTS_NAME} detected.")
                    }
                }

                if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {
                    logger.info("Percentiles: ${percentiles.joinToString(", ")}. [Experiment: Average]")
                }

                /* --------------------------- Deterministic mode ---------------------------
                 * We support:
                 *  - --det / --deterministic      → enable reproducibility
                 *  - --sd <long> / --seed <long> → explicit master seed (implies deterministic)
                 *
                 * If deterministic and no seed is provided, we derive a stable seed from the key
                 * parameters (ignores currentExecution so that multi-run batches share one seed).
                 * We also install our RNG bridge early so that jMetal’s singleton uses it.
                 * ------------------------------------------------------------------------ */
                val deterministicRequested = commandLine.hasOption("det") || commandLine.hasOption("sd")
                val seedFromCli: Long? = if (commandLine.hasOption("sd")) {
                    commandLine.getOptionValue("sd")?.toLongOrNull()
                        ?: throw ParseException("Value for the option <<sd>> or <<seed>> is not a valid long. Check the usage section below.")
                } else null
                if (seedFromCli != null) System.setProperty("nbs.seed.cli", seedFromCli.toString())


                val masterSeed: Long? = if (deterministicRequested) {
                    val seedTemplate = Parameters(
                        datasetName = datasetName,
                        correlationMethod = correlationMethod,
                        targetToAchieve = targetToAchieve,
                        numberOfIterations = numberOfIterations,
                        numberOfRepetitions = numberOfRepetitions,
                        populationSize = populationSize,
                        currentExecution = 0,              /* fixed to avoid changing seed across batch runs */
                        percentiles = percentiles,
                        deterministic = true,
                        seed = null
                    )
                    val derived = seedFromCli ?: Tools.stableSeedFrom(seedTemplate)
                    RandomBridge.installDeterministic(derived)
                    logger.info("Deterministic mode: ON (masterSeed=$derived)")
                    derived
                } else {
                    logger.info("Deterministic mode: OFF")
                    null
                }

                if (commandLine.hasOption("mr")) {

                    try {
                        numberOfExecutions = Integer.parseInt(commandLine.getOptionValue("mr"))
                        if (numberOfExecutions <= 0) throw ParseException("Value for the option <<me>> or <<mrg>> must be a positive value. Check the usage section below")
                    } catch (_: NumberFormatException) {
                        throw ParseException("Value for the option <<mr>> or <<mrg>> is not an integer. Check the usage section below")
                    }

                    (1..numberOfExecutions).forEach { currentExecution ->
                        logger.info("Execution number: $currentExecution/$numberOfExecutions")
                        datasetController.solve(
                            Parameters(
                                datasetName,
                                correlationMethod,
                                targetToAchieve,
                                numberOfIterations,
                                numberOfRepetitions,
                                populationSize,
                                currentExecution,
                                percentiles,
                                deterministicRequested,
                                masterSeed
                            )
                        )
                    }

                    datasetController.merge(numberOfExecutions)
                }

                val parseMaximumExpansionCoefficient = {
                    val maximumExpansionCoefficient: Int

                    if (!commandLine.hasOption("mx")) throw ParseException("Value for the option <<mx>> or <<max>> is missing. Check the usage section below.")
                    try {
                        maximumExpansionCoefficient = Integer.parseInt(commandLine.getOptionValue("mx"))
                        if (maximumExpansionCoefficient <= 0) throw ParseException("Value for the option <<mx>> or <<max>> must be a positive value. Check the usage section below")
                    } catch (_: NumberFormatException) {
                        throw ParseException("Value for the option <<m>> or <<max>> is not an integer. Check the usage section below")
                    }

                    if (populationSize <= maximumExpansionCoefficient) throw ParseException("Value for the option <<p>> or <<pop>> must be greater than value for the option <<mx>> or <<max>>. Check the usage section below")

                    maximumExpansionCoefficient
                }

                val resultCleaner = {
                    logger.info("Cleaning of useless results from last expansion started.")
                    datasetController.clean(datasetController.aggregatedDataResultPaths, "Cleaning aggregated data at paths:")
                    datasetController.clean(datasetController.functionValuesResultPaths, "Cleaning function values at paths:")
                    datasetController.clean(datasetController.variableValuesResultPaths, "Cleaning variable values at paths:")
                    datasetController.clean(datasetController.topSolutionsResultPaths, "Cleaning top solutions at paths:")
                    logger.info("Cleaning completed.")
                }

                if (commandLine.hasOption("et")) {

                    try {
                        topicExpansionCoefficient = Integer.parseInt(commandLine.getOptionValue("et"))
                    } catch (_: NumberFormatException) {
                        throw ParseException("Value for the option <<et>> or <<expt>> is not an integer. Check the usage section below")
                    }

                    topicMaximumExpansionCoefficient = parseMaximumExpansionCoefficient.invoke()
                    val trueTopicNumber = datasetController.models[0].numberOfTopics

                    logger.info("Base execution (true data)")

                    datasetController.solve(
                        Parameters(
                            datasetName, correlationMethod, targetToAchieve,
                            numberOfIterations, numberOfRepetitions, populationSize, 0, percentiles,
                            deterministicRequested, masterSeed
                        )
                    )
                    resultCleaner.invoke()
                    while (datasetController.models[0].numberOfTopics + topicExpansionCoefficient <= topicMaximumExpansionCoefficient) {
                        logger.info("Data expansion: <New Topic Number: ${datasetController.models[0].numberOfTopics + topicExpansionCoefficient}, Earlier Topic Number: ${datasetController.models[0].numberOfTopics}, Expansion Coefficient: $topicExpansionCoefficient, Maximum Expansion: $topicMaximumExpansionCoefficient, True Topic Number: $trueTopicNumber>")
                        datasetController.expandTopics(topicExpansionCoefficient)
                        datasetController.solve(
                            Parameters(
                                datasetName, correlationMethod, targetToAchieve,
                                numberOfIterations, numberOfRepetitions, populationSize, 0, percentiles,
                                deterministicRequested, masterSeed
                            )
                        )
                        resultCleaner.invoke()
                    }
                }

                if (commandLine.hasOption("es")) {

                    try {
                        systemExpansionCoefficient = Integer.parseInt(commandLine.getOptionValue("es"))
                    } catch (_: NumberFormatException) {
                        throw ParseException("Value for the option <<es>> or <<exps> is not an integer. Check the usage section below")
                    }

                    systemMaximumExpansionCoefficient = parseMaximumExpansionCoefficient.invoke()
                    val trueNumberOfSystems = datasetController.models[0].numberOfSystems

                    datasetController.models.forEach { model ->
                        model.numberOfSystems = 0
                    }

                    logger.info("Base execution (true data)")

                    while (datasetController.models[0].numberOfSystems + systemExpansionCoefficient <= systemMaximumExpansionCoefficient) {
                        logger.info("Data expansion: <New System Number: ${datasetController.models[0].numberOfSystems + systemExpansionCoefficient}, Earlier System Number: ${datasetController.models[0].numberOfSystems}, Expansion Coefficient: $systemExpansionCoefficient, Maximum Expansion: $systemMaximumExpansionCoefficient, Initial System Number: $systemExpansionCoefficient, True System Number: $trueNumberOfSystems>")
                        datasetController.expandSystems(systemExpansionCoefficient, trueNumberOfSystems)
                        datasetController.solve(
                            Parameters(
                                datasetName, correlationMethod, targetToAchieve,
                                numberOfIterations, numberOfRepetitions, populationSize, 0, percentiles,
                                deterministicRequested, masterSeed
                            )
                        )
                        resultCleaner.invoke()
                    }
                }

                if (!commandLine.hasOption("et") && !commandLine.hasOption("es") && !commandLine.hasOption("mr")) {
                    datasetController.solve(
                        Parameters(
                            datasetName,
                            correlationMethod,
                            targetToAchieve,
                            numberOfIterations,
                            numberOfRepetitions,
                            populationSize,
                            0,
                            percentiles,
                            deterministicRequested,
                            masterSeed
                        )
                    )
                }
                if (commandLine.hasOption("copy")) datasetController.copy()

                logger.info("NewBestSub execution terminated.")
            }

        } catch (exception: ParseException) {

            logger.error(exception.message)
            val formatter = HelpFormatter.builder().get()
            formatter.printHelp(Constants.NEWBESTSUB_NAME, "", options, "", true)
            logger.error("End of the usage section.")
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")

        } catch (exception: FileNotFoundException) {

            logger.error(exception.message)
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")

        } catch (exception: FileSystemException) {

            logger.error(exception.message)
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")

        } catch (exception: JMetalException) {

            logger.error(exception.message)
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")

        } catch (_: OutOfMemoryError) {

            logger.error("NewBestSub hasn't enough heap space to go further. Please, launch it with option -XX:-UseGCOverheadLimit.")
            logger.info("Example: java -jar NewBestSub-1.0.jar -UseGCOverheadLimit -fi \"AH99-Top96\" -c \"Pearson\" -pop 2000 -r 5000 -i 1000000 -t \"All\" -pe 1,100 -l Limited")
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")
        }
    }

    fun loadCommandLineOptions(): Options {

        val options = Options()

        var source = Option.builder("fi").longOpt("fileIn")
            .desc("Relative path to the CSV dataset file (do not use any extension in filename) [REQUIRED].")
            .hasArg().argName("Source File").required().get()
        options.addOption(source)

        source = Option.builder("c").longOpt("corr")
            .desc("Strategy that must be used to compute correlations. Available methods: Pearson, Kendall. [REQUIRED]")
            .hasArg().argName("Correlation").required().get()
        options.addOption(source)

        source = Option.builder("t").longOpt("targ")
            .desc("Target that must be achieved. Available targets: Best, Worst, Average, All. [REQUIRED]")
            .hasArg().argName("Target").required().get()
        options.addOption(source)

        source = Option.builder("l").longOpt("log")
            .desc("Required level of logging. Available levels: Verbose, Limited, Off. [REQUIRED]")
            .required().hasArg().argName("LogManager Level").get()
        options.addOption(source)

        source = Option.builder("i").longOpt("iter")
            .desc("Number of iterations to be done. It is mandatory only if the selected target is: Best, Worst, All. [OPTIONAL]")
            .hasArg().argName("Number of Iterations").get()
        options.addOption(source)

        source = Option.builder("r").longOpt("rep")
            .desc("Number of repetitions to be done to compute a single cardinality during Average experiment. It must be a positive integer value. It is mandatory only if the selected target is: Average, All. [OPTIONAL]")
            .hasArg().argName("Number of Repetitions").get()
        options.addOption(source)

        source = Option.builder("po").longOpt("pop")
            .desc("Size of the initial population to be generated. It must be an integer value. It must be greater or equal than/to of the number of topics of the data set. It must be greater than the value for the option <<mx>> or <<max>> if this one is used.a It is mandatory only if the selected target is: Best, Worst, All. [OPTIONAL]")
            .hasArg().argName("Population Size").get()
        options.addOption(source)

        source = Option.builder("pe").longOpt("perc")
            .desc("Set of percentiles to be calculated. There must be two comma separated integer values. Example: \"-pe 1,100\". It is mandatory only if the selected target is: Average, All. [OPTIONAL]")
            .hasArgs().valueSeparator(',').argName("Percentile").get()
        options.addOption(source)

        source = Option.builder("et").longOpt("expt")
            .desc("Number of fake topics to be added at each iteration. It must be a positive integer value. [OPTIONAL]")
            .hasArg().argName("Expansion Coefficient (Topics)").get()
        options.addOption(source)

        source = Option.builder("es").longOpt("exps")
            .desc("Number of fake systems to be added at each iteration. It must be a positive integer value. [OPTIONAL]")
            .hasArg().argName("Expansion Coefficient (Systems)").get()
        options.addOption(source)

        source = Option.builder("mx").longOpt("max")
            .desc("Maximum number of fake topics or systems to reach using expansion options. [OPTIONAL]")
            .hasArg().argName("Maximum Expansion Coefficient").get()
        options.addOption(source)

        source = Option.builder("mr").longOpt("mrg")
            .desc("Number of executions of the program to do. The results of the executions will be merged together. It must be a positive integer value. [OPTIONAL]")
            .hasArg().argName("Value").get()
        options.addOption(source)

        source = Option.builder("copy")
            .desc("NewBestSub should search the folder or NewBestSub-Experiments and copy the results of the current execution inside its data folders. The following structure into filesystem must be respected: \"baseFolder/NewBestSub/..\" and \"baseFolder/NewBestSub-Experiments/..\" otherwise, an exception will be raised. [OPTIONAL]")
            .get()
        options.addOption(source)

        /* --- Deterministic execution flags (new) --- */
        source = Option.builder("det").longOpt("deterministic")
            .desc("Enable deterministic, reproducible execution. If used without --seed, a stable seed is derived from key parameters. [OPTIONAL]")
            .get()
        options.addOption(source)

        source = Option.builder("sd").longOpt("seed")
            .desc("Explicit master seed for deterministic execution (long). Implies --deterministic. [OPTIONAL]")
            .hasArg().argName("Seed").get()
        options.addOption(source)

        return options
    }
}
