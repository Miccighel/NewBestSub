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

            /* ParseException
             * --------------
             * Bad CLI arguments: log full error + show usage with examples.
             */
            logger.error("Invalid command line arguments: {}", exception.message ?: "unknown ParseException", exception)
            printNiceHelp(options, exception.message)
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated due to invalid arguments.")

        } catch (exception: FileNotFoundException) {

            /* FileNotFoundException
             * ---------------------
             * Missing input or required directory. Include context + stacktrace.
             */
            val cwd = File(".").absoluteFile.normalize().path
            logger.error("File not found (cwd: {}): {}", cwd, exception.message ?: "unknown", exception)
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")

        } catch (exception: FileSystemException) {

            /* FileSystemException
             * -------------------
             * IO/FS error: show file, other file (if any) and reason.
             */
            logger.error(
                "Filesystem error (file='{}', other='{}', reason='{}').",
                exception.file, exception.otherFile, exception.reason, exception
            )
            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")

        } catch (exception: JMetalException) {

            /* JMetalException
             * ---------------
             * Algorithm/runtime error. Keep the full stacktrace and add a small hint when recognizable.
             */
            logger.error("jMetal error: {}", exception.message ?: "unknown JMetalException", exception)

            val msg = exception.message ?: ""
            if (msg.contains("must be greater or equal", ignoreCase = true)) {
                logger.info("Hint: set -po (population) ≥ number of topics (current dataset topics logged above).")
            }

            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated.")

        } catch (e: OutOfMemoryError) {

            /* OutOfMemoryError
             * -----------------
             * Provide a targeted hint based on the error kind + full stacktrace.
             */
            val kindHint = when {
                e.message?.contains("GC overhead limit exceeded", ignoreCase = true) == true ->
                    "GC overhead limit exceeded. You can disable it with -XX:-UseGCOverheadLimit, " +
                            "but it’s usually better to increase -Xmx and/or reduce workload size."

                e.message?.contains("Metaspace", ignoreCase = true) == true ->
                    "Metaspace exhausted. Increase -XX:MaxMetaspaceSize or reduce dynamic class generation."

                e.message?.contains("Direct buffer memory", ignoreCase = true) == true ->
                    "Direct buffer memory exhausted. Increase -XX:MaxDirectMemorySize or reduce off-heap buffers."

                e.message?.contains("Requested array size exceeds VM limit", ignoreCase = true) == true ->
                    "Requested array size exceeds VM limit. Reduce problem size or chunk processing."

                else ->
                    "Java heap space exhausted. Increase -Xmx or reduce population/iterations/repetitions."
            }

            logger.error("${Constants.NEWBESTSUB_NAME} ran out of memory. $kindHint", e)

            logger.info(
                "Try: increase heap (e.g., -Xmx32g), or reduce -po / -i / -r. " +
                        "Also consider -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=heapdump.hprof for diagnostics."
            )
            logger.info(
                "Example: java -Xms32g -Xmx32g -XX:+UseG1GC -XX:+UseStringDeduplication " +
                        "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=heapdump.hprof " +
                        "-jar NewBestSub-2.0-jar-with-dependencies.jar -fi \"mmlu\" -c \"Pearson\" " +
                        "-po 20000 -i 100000 -r 2000 -t \"All\" -pe 1,100 -log Limited"
            )

            logger.info("${Constants.NEWBESTSUB_NAME} execution terminated due to OutOfMemoryError.")
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

    /* ------------------------------------------------------------------------------------
 * Pretty help/usage
 *  - Wider layout, custom option ordering, clear header + example commands.
 *  - Keeps your comment style and avoids noisy auto-wrapping of descriptions.
 * ---------------------------------------------------------------------------------- */
    private fun buildHelpFormatter(): org.apache.commons.cli.HelpFormatter {
        val columns = System.getenv("COLUMNS")?.toIntOrNull()?.coerceIn(100, 160) ?: 120
        return org.apache.commons.cli.HelpFormatter().apply {
            // Layout tuning
            width = columns
            leftPadding = 2
            descPadding = 4
            syntaxPrefix = "Usage: "
            longOptPrefix = "--"
            optPrefix = "-"

            // Order: required & core options first, then the rest (stable)
            val priority = listOf(
                "fi", "c", "t", "l",     // core required
                "i", "po",             // iterations/population (Best/Worst/All)
                "r", "pe",             // repetitions/percentiles (Average/All)
                "det", "sd",           // deterministic flags
                "mr", "et", "es", "mx",  // multi-run & expansions
                "copy"                // experiments copy
            )
            optionComparator = Comparator { a, b ->
                val ia = priority.indexOf(a.opt).let { if (it == -1) Int.MAX_VALUE else it }
                val ib = priority.indexOf(b.opt).let { if (it == -1) Int.MAX_VALUE else it }
                if (ia != ib) ia - ib else a.opt.compareTo(b.opt)
            }
        }
    }

    /* Print a clean, helpful usage page with a contextual cause and examples. */
    private fun printNiceHelp(options: Options, cause: String?) {
        val formatter = buildHelpFormatter()

        val header = buildString {
            appendLine("NewBestSub – multi-objective best-subset driver")
            if (!cause.isNullOrBlank()) {
                appendLine()
                appendLine("Cause: $cause")
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

        // Wider call signature gives best layout control
        formatter.printHelp(
            /* pw   */ java.io.PrintWriter(System.out, true),
            /* width*/ formatter.width,
            /* cmd  */ "${Constants.NEWBESTSUB_NAME} [options]",
            /* hdr  */ header,
            /* opts */ options,
            /* lpad */ formatter.leftPadding,
            /* dpad */ formatter.descPadding,
            /* ftr  */ footer,
            /* autoUsage */ true
        )
    }

}
