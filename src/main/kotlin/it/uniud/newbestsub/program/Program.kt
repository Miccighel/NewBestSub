package it.uniud.newbestsub.program

import it.uniud.newbestsub.dataset.DatasetController
import it.uniud.newbestsub.dataset.Parameters
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools
import it.uniud.newbestsub.utils.LogManager
import it.uniud.newbestsub.utils.RandomBridge
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.apache.commons.cli.help.HelpFormatter
import org.apache.commons.cli.help.TextHelpAppendable
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.Logger
import org.uma.jmetal.util.errorchecking.JMetalException
import java.io.File
import java.io.FileNotFoundException
import java.nio.file.FileSystemException

/**
 * Main program entry point for NewBestSub.
 *
 * Responsibilities:
 * - Parse CLI arguments and validate them.
 * - Initialize logging: bootstrap log → parameterized log.
 * - Load dataset and build parameter tokens.
 * - Manage deterministic seeding via RandomBridge.
 * - Run experiments (Best/Worst/Average/All), expansions, and merging.
 * - Copy results into NewBestSub-Experiments/ when requested.
 * - Catch and log errors (CLI, FS, runtime validation, jMetal, OOM) with actionable hints.
 */
object Program {

    @JvmStatic
    fun main(arguments: Array<String>) {

        /* Ensure Snappy extracts its native lib under target/tmp-snappy */
        initSnappyTempDir()

        val options = loadCommandLineOptions()

        /* -------- Bootstrap logging to a timestamped file (no params yet) -------- */
        val bootstrapLogPath = LogManager.getBootstrapLogFilePath()
        System.setProperty("baseLogFileName", bootstrapLogPath)
        var logger: Logger = LogManager.updateRootLoggerLevel(Level.INFO)

        /* -------- Catch uncaught exceptions from worker threads (e.g., coroutines) -------- */
        Thread.setDefaultUncaughtExceptionHandler { t, e ->
            when (e) {
                is ParseException -> {
                    // Runtime validation-style ParseException (e.g., -po too small)
                    logger.error("Runtime configuration error on thread {}: {}", t.name, e.message ?: "ParseException", e)
                    if (e.message?.contains("<<p>>", true) == true || e.message?.contains("<<po>>", true) == true) {
                        logger.info("Hint: set -po (population) ≥ number of topics (see dataset header / logs).")
                    }
                    logger.info("${Constants.NEWBESTSUB_NAME} execution terminated due to configuration error.")
                }
                else -> {
                    logger.error("Uncaught exception on thread {}: {}", t.name, e.message ?: "unknown", e)
                    logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")
                }
            }
        }

        try {
            /* --------------------------- 1) CLI parsing --------------------------- */
            val parser: CommandLineParser = DefaultParser()
            val commandLine: CommandLine = parser.parse(options, arguments)

            val datasetName = commandLine.getOptionValue("fi")
            val datasetPath = "${Constants.NEWBESTSUB_INPUT_PATH}$datasetName.csv"

            if (!File(datasetPath).exists()) {
                throw FileNotFoundException("Dataset file does not exists Path: \"$datasetPath\"")
            }

            /* LogManager level */
            val loggingLevel: Level = when (commandLine.getOptionValue("l")) {
                "Verbose" -> Level.DEBUG
                "Limited" -> Level.INFO
                "Off"     -> Level.OFF
                else -> throw ParseException("Value for the option <<l>> or <<log>> is wrong. Check the usage section below.")
            }
            logger = LogManager.updateRootLoggerLevel(loggingLevel)

            /* Correlation method */
            val correlationMethod: String = when (commandLine.getOptionValue("c")) {
                Constants.CORRELATION_PEARSON, Constants.CORRELATION_KENDALL -> commandLine.getOptionValue("c")
                else -> throw ParseException("Value for the option <<c>> or <<corr>> is wrong. Check the usage section below.")
            }

            /* Target */
            val targetToAchieve: String = when (commandLine.getOptionValue("t")) {
                Constants.TARGET_BEST, Constants.TARGET_WORST, Constants.TARGET_AVERAGE, Constants.TARGET_ALL -> commandLine.getOptionValue("t")
                else -> throw ParseException("Value for the option <<t>> or <<target>> is wrong. Check the usage section below.")
            }

            /* Defaults (filled depending on target) */
            var numberOfIterations = 0
            var populationSize = 0
            var numberOfRepetitions = 0
            var percentiles: List<Int> = emptyList()

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
                    val lastPercentile  = percentilesToParse[1].toInt()
                    percentiles = (firstPercentile..lastPercentile).toList()
                } catch (_: NumberFormatException) {
                    throw ParseException("Value for the option <<pe>> or <<perc>> is not an integer. Check the usage section below")
                }
            }

            /* --------------------------- 2) Controller + dataset load --------------------------- */
            val datasetController = DatasetController(targetToAchieve)
            datasetController.load(datasetPath)

            /* PRE-VALIDATE: for Best/Worst/All, enforce -po ≥ #topics (fail early on main thread) */
            run {
                if (targetToAchieve != Constants.TARGET_AVERAGE) {
                    val topics = datasetController.models[0].numberOfTopics
                    if (populationSize < topics) {
                        // Mirror the message style thrown deeper to keep consistency
                        throw ParseException(
                            "Value for the option <<p>> or <<po>> must be greater or equal than/to $topics. " +
                            "Current value is $populationSize. Check the usage section below."
                        )
                    }
                }
            }

            /* Build the final log filename with params and switch to it */
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
            logger.info("Base path:\n\"${Constants.BASE_PATH}\"")
            logger.info("${Constants.NEWBESTSUB_NAME} input path:\n\"${Constants.NEWBESTSUB_INPUT_PATH}\"")
            logger.info("${Constants.NEWBESTSUB_NAME} output path:\n\"${Constants.NEWBESTSUB_OUTPUT_PATH}\"")
            logger.info("${Constants.NEWBESTSUB_NAME} log path (directory):\n\"${Constants.NEWBESTSUB_PATH}log${Constants.PATH_SEPARATOR}\"")

            if (commandLine.hasOption("copy")) {
                logger.info("${Constants.NEWBESTSUB_EXPERIMENTS_NAME} path:\n\"${Constants.NEWBESTSUB_EXPERIMENTS_PATH}\"")
                logger.info("${Constants.NEWBESTSUB_EXPERIMENTS_NAME} input path:\n\"${Constants.NEWBESTSUB_EXPERIMENTS_INPUT_PATH}\"")
                val errorMessage =
                    "${Constants.NEWBESTSUB_EXPERIMENTS_NAME} not found. Please, place its folder the same one where ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} is located. Path: \"${Constants.BASE_PATH}\""
                if (!File(Constants.NEWBESTSUB_EXPERIMENTS_PATH).exists()) throw FileNotFoundException(errorMessage)
                logger.info("${Constants.NEWBESTSUB_EXPERIMENTS_NAME} detected.")
            }

            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {
                logger.info("Percentiles: ${percentiles.joinToString(", ")}. [Experiment: Average]")
            }

            /* --------------------------- Deterministic mode --------------------------- */
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
                    currentExecution = 0,
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

            /* --------------------------- Helpers --------------------------- */
            val resultCleaner = {
                logger.info("Cleaning of useless results from last expansion started.")
                datasetController.clean(datasetController.aggregatedDataResultPaths, "Cleaning aggregated data at paths:")
                datasetController.clean(datasetController.functionValuesResultPaths, "Cleaning function values at paths:")
                datasetController.clean(datasetController.variableValuesResultPaths, "Cleaning variable values at paths:")
                datasetController.clean(datasetController.topSolutionsResultPaths, "Cleaning top solutions at paths:")
                logger.info("Cleaning completed.")
            }

            val parseMaximumExpansionCoefficient = {
                val maximumExpansionCoefficient: Int
                if (!options.hasOption("mx") && !options.hasLongOption("max")) {
                    // This lambda is only called when CLI had -mx; keep old error string for consistency.
                }
                if (!commandLine.hasOption("mx")) throw ParseException("Value for the option <<mx>> or <<max>> is missing. Check the usage section below.")
                try {
                    maximumExpansionCoefficient = Integer.parseInt(commandLine.getOptionValue("mx"))
                    if (maximumExpansionCoefficient <= 0) throw ParseException("Value for the option <<mx>> or <<max>> must be a positive value. Check the usage section below")
                } catch (_: NumberFormatException) {
                    throw ParseException("Value for the option <<m>> or <<max>> is not an integer. Check the usage section below")
                }
                if (populationSize <= maximumExpansionCoefficient) {
                    throw ParseException("Value for the option <<p>> or <<pop>> must be greater than value for the option <<mx>> or <<max>>. Check the usage section below")
                }
                maximumExpansionCoefficient
            }

            /* --------------------------- 3) Run modes --------------------------- */
            if (commandLine.hasOption("mr")) {
                val numberOfExecutions = try {
                    Integer.parseInt(commandLine.getOptionValue("mr")).also {
                        if (it <= 0) throw ParseException("Value for the option <<me>> or <<mrg>> must be a positive value. Check the usage section below")
                    }
                } catch (_: NumberFormatException) {
                    throw ParseException("Value for the option <<mr>> or <<mrg>> is not an integer. Check the usage section below")
                }

                (1..numberOfExecutions).forEach { currentExecution ->
                    logger.info("Execution number: $currentExecution/$numberOfExecutions")
                    runSolveWithRuntimeValidationHints(
                        datasetController, logger,
                        Parameters(
                            datasetName, correlationMethod, targetToAchieve,
                            numberOfIterations, numberOfRepetitions, populationSize,
                            currentExecution, percentiles, deterministicRequested, masterSeed
                        )
                    )
                }
                datasetController.merge(numberOfExecutions)
            }

            if (commandLine.hasOption("et")) {
                val topicExpansionCoefficient = try {
                    Integer.parseInt(commandLine.getOptionValue("et"))
                } catch (_: NumberFormatException) {
                    throw ParseException("Value for the option <<et>> or <<expt>> is not an integer. Check the usage section below")
                }
                val topicMaximumExpansionCoefficient = parseMaximumExpansionCoefficient.invoke()
                val trueTopicNumber = datasetController.models[0].numberOfTopics

                logger.info("Base execution (true data)")
                runSolveWithRuntimeValidationHints(
                    datasetController, logger,
                    Parameters(
                        datasetName, correlationMethod, targetToAchieve,
                        numberOfIterations, numberOfRepetitions, populationSize, 0, percentiles,
                        deterministicRequested, masterSeed
                    )
                )
                resultCleaner.invoke()

                while (datasetController.models[0].numberOfTopics + topicExpansionCoefficient <= topicMaximumExpansionCoefficient) {
                    logger.info(
                        "Data expansion: <New Topic Number: ${datasetController.models[0].numberOfTopics + topicExpansionCoefficient}, " +
                        "Earlier Topic Number: ${datasetController.models[0].numberOfTopics}, Expansion Coefficient: $topicExpansionCoefficient, " +
                        "Maximum Expansion: $topicMaximumExpansionCoefficient, True Topic Number: $trueTopicNumber>"
                    )
                    datasetController.expandTopics(topicExpansionCoefficient)
                    runSolveWithRuntimeValidationHints(
                        datasetController, logger,
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
                val systemExpansionCoefficient = try {
                    Integer.parseInt(commandLine.getOptionValue("es"))
                } catch (_: NumberFormatException) {
                    throw ParseException("Value for the option <<es>> or <<exps> is not an integer. Check the usage section below")
                }
                val systemMaximumExpansionCoefficient = parseMaximumExpansionCoefficient.invoke()
                val trueNumberOfSystems = datasetController.models[0].numberOfSystems

                datasetController.models.forEach { model -> model.numberOfSystems = 0 }
                logger.info("Base execution (true data)")

                while (datasetController.models[0].numberOfSystems + systemExpansionCoefficient <= systemMaximumExpansionCoefficient) {
                    logger.info(
                        "Data expansion: <New System Number: ${datasetController.models[0].numberOfSystems + systemExpansionCoefficient}, " +
                        "Earlier System Number: ${datasetController.models[0].numberOfSystems}, Expansion Coefficient: $systemExpansionCoefficient, " +
                        "Maximum Expansion: $systemMaximumExpansionCoefficient, Initial System Number: $systemExpansionCoefficient, True System Number: $trueNumberOfSystems>"
                    )
                    datasetController.expandSystems(systemExpansionCoefficient, trueNumberOfSystems)
                    runSolveWithRuntimeValidationHints(
                        datasetController, logger,
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
                runSolveWithRuntimeValidationHints(
                    datasetController, logger,
                    Parameters(
                        datasetName, correlationMethod, targetToAchieve,
                        numberOfIterations, numberOfRepetitions, populationSize, 0, percentiles,
                        deterministicRequested, masterSeed
                    )
                )
            }

            if (commandLine.hasOption("copy")) datasetController.copy()
            logger.info("NewBestSub execution terminated.")

        } catch (exception: ParseException) {
            /* Distinguish CLI parse vs runtime validation by message heuristics. */
            val msg = exception.message ?: ""
            val looksLikeRuntimeConfig =
                msg.contains("<<p>>", true) || msg.contains("<<po>>", true) ||
                msg.contains("greater or equal", true) || msg.contains("greater than", true)

            if (looksLikeRuntimeConfig) {
                // Runtime validation error (e.g., -po too small for #topics)
                logger.error("Configuration error: {}", msg, exception)
                if (msg.contains("<<p>>", true) || msg.contains("<<po>>", true)) {
                    logger.info("Hint: set -po (population) ≥ number of topics of the dataset.")
                }
                logger.info("${Constants.NEWBESTSUB_NAME} execution terminated due to configuration error.")
            } else {
                // Genuine CLI parse error → pretty help
                logger.error("Invalid command line arguments: {}", msg.ifBlank { "unknown ParseException" }, exception)
                printNiceHelp(options, exception.message)
                logger.info("${Constants.NEWBESTSUB_NAME} execution terminated due to invalid arguments.")
            }

        } catch (exception: FileNotFoundException) {
            val cwd = File(".").absoluteFile.normalize().path
            logger.error("File not found (cwd: {}): {}", cwd, exception.message ?: "unknown", exception)
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")
        } catch (exception: FileSystemException) {
            logger.error(
                "Filesystem error (file='{}', other='{}', reason='{}').",
                exception.file, exception.otherFile, exception.reason, exception
            )
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")
        } catch (exception: JMetalException) {
            logger.error("jMetal error: {}", exception.message ?: "unknown JMetalException", exception)
            val msg = exception.message ?: ""
            if (msg.contains("must be greater or equal", true)) {
                logger.info("Hint: set -po (population) ≥ number of topics (current dataset topics logged above).")
            }
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")
        } catch (e: OutOfMemoryError) {
            val kindHint = when {
                e.message?.contains("GC overhead limit exceeded", true) == true ->
                    "GC overhead limit exceeded. You can disable it with -XX:-UseGCOverheadLimit, but it is usually better to increase -Xmx and/or reduce workload size."
                e.message?.contains("Metaspace", true) == true ->
                    "Metaspace exhausted. Increase -XX:MaxMetaspaceSize or reduce dynamic class generation."
                e.message?.contains("Direct buffer memory", true) == true ->
                    "Direct buffer memory exhausted. Increase -XX:MaxDirectMemorySize or reduce off-heap buffers."
                e.message?.contains("Requested array size exceeds VM limit", true) == true ->
                    "Requested array size exceeds VM limit. Reduce problem size or chunk processing."
                else ->
                    "Java heap space exhausted. Increase -Xmx or reduce population/iterations/repetitions."
            }
            logger.error("${Constants.NEWBESTSUB_NAME} ran out of memory. $kindHint", e)
            logger.info(
                "Try: increase heap (for example, -Xmx32g), or reduce -po / -i / -r. Also consider -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=heapdump.hprof for diagnostics."
            )
            logger.info(
                "Example: java -Xms32g -Xmx32g -XX:+UseG1GC -XX:+UseStringDeduplication -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=heapdump.hprof -jar NewBestSub-2.0-jar-with-dependencies.jar -fi \"mmlu\" -c \"Pearson\" -po 20000 -i 100000 -r 2000 -t \"All\" -pe 1,100 -log Limited"
            )
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated due to OutOfMemoryError.")
        }
    }

    /**
     * Run `solve` with a small try/catch that turns model-time ParseExceptions
     * into friendly hints, without invoking the CLI help printer.
     */
    private fun runSolveWithRuntimeValidationHints(
        controller: DatasetController,
        logger: Logger,
        params: Parameters
    ) {
        try {
            controller.solve(params)
        } catch (e: ParseException) {
            val msg = e.message ?: "ParseException"
            val poCase = msg.contains("<<p>>", true) || msg.contains("<<po>>", true)
            if (poCase) {
                logger.error("Configuration error during solve: {}", msg, e)
                logger.info("Hint: -po (population) must be ≥ number of topics for this dataset.")
                logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")
            } else {
                // Unknown runtime ParseException → still log cleanly.
                logger.error("Runtime ParseException during solve: {}", msg, e)
                logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")
            }
            // Stop further workflow after a hard validation failure.
            throw e
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
            .desc("Size of the initial population to be generated. It must be an integer value. It must be greater or equal to the number of topics in the dataset. It must be greater than the value for the option <<mx>> or <<max>> if this one is used. It is mandatory only if the selected target is: Best, Worst, All. [OPTIONAL]")
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
            .desc("Search NewBestSub-Experiments and copy the results of the current execution into its data folders. Folder structure required: baseFolder/NewBestSub/... and baseFolder/NewBestSub-Experiments/.... [OPTIONAL]")
            .get()
        options.addOption(source)

        /* Deterministic execution flags */
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

    /** Pretty help and usage printer using the new Commons CLI help API. */
    private fun printNiceHelp(options: Options, cause: String?) {
        val out = TextHelpAppendable(java.io.PrintWriter(System.out, true))

        val header = buildString {
            appendLine("NewBestSub")
            if (!cause.isNullOrBlank()) {
                appendLine(); appendLine("Cause: $cause")
            }
            appendLine()
            appendLine("Targets:")
            appendLine("  Best/Worst : NSGA-II over (K, corr) with streaming FUN/VAR/TOP.")
            appendLine("  Average    : one row per K from random subsets; percentiles computed from repeats.")
            appendLine()
        }

        val footer = buildString {
            appendLine()
            appendLine("Examples")
            appendLine("--------")
            appendLine("Best (Pearson):")
            appendLine("  java -Xms32g -Xmx32g -jar NewBestSub-2.0-jar-with-dependencies.jar \\")
            appendLine("    -fi \"mmlu\" -c \"Pearson\" -t \"Best\" -po 20000 -i 100000 -l Limited --det")
            appendLine()
            appendLine("Average (1..100 percentiles):")
            appendLine("  java -Xms32g -Xmx32g -jar NewBestSub-2.0-jar-with-dependencies.jar \\")
            appendLine("    -fi \"mmlu\" -c \"Pearson\" -t \"Average\" -r 2000 -pe 1,100 -l Limited --det")
            appendLine()
            appendLine("All (Best/Worst + Average):")
            appendLine("  java -Xms32g -Xmx32g -jar NewBestSub-2.0-jar-with-dependencies.jar \\")
            appendLine("    -fi \"mmlu\" -c \"Pearson\" -t \"All\" -po 20000 -i 100000 -r 2000 -pe 1,100 -l Limited")
            appendLine()
            appendLine("Tips")
            appendLine("----")
            appendLine("• Ensure -po (population) ≥ number of topics.")
            appendLine("• Average/All require -r and -pe; Best/Worst require -po and -i.")
            appendLine("• Use --det / --seed for reproducible runs.")
        }

        val priority = listOf(
            "fi", "c", "t", "l",
            "i", "po",
            "r", "pe",
            "det", "sd",
            "mr", "et", "es", "mx",
            "copy"
        )
        val optionOrder = Comparator<Option> { a, b ->
            val ia = priority.indexOf(a.opt).let { if (it == -1) Int.MAX_VALUE else it }
            val ib = priority.indexOf(b.opt).let { if (it == -1) Int.MAX_VALUE else it }
            if (ia != ib) ia - ib else a.opt.compareTo(b.opt)
        }

        val formatter = HelpFormatter.builder()
            .setHelpAppendable(out)
            .setComparator(optionOrder)
            .get()
            .apply { syntaxPrefix = "Usage: " }

        formatter.printHelp(
            "${Constants.NEWBESTSUB_NAME} [options]",
            header,
            options,
            footer,
            /* autoUsage = */ true
        )
    }

    /** Initialize Snappy's native temp directory under `target/tmp-snappy`. */
    private fun initSnappyTempDir() {
        val key = "org.xerial.snappy.tempdir"
        if (System.getProperty(key).isNullOrBlank()) {
            val dir = java.nio.file.Paths.get(
                Constants.NEWBESTSUB_PATH,
                "target", "tmp-snappy"
            )
            try { java.nio.file.Files.createDirectories(dir) } catch (_: Throwable) { /* best effort */ }
            System.setProperty(key, dir.toAbsolutePath().normalize().toString())
        }
    }
}
