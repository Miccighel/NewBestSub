package it.uniud.newbestsub.dataset

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import it.uniud.newbestsub.utils.Constants
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.apache.commons.lang3.StringUtils
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.binarysolution.BinarySolution
import java.io.*
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.util.*
import kotlin.collections.LinkedHashMap

typealias RunResult = Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>>

/**
 * DatasetController
 *
 * Orchestrates:
 *  - DatasetModel lifecycle (load/expand/solve)
 *  - Streaming consumption (printer coroutine)
 *  - View writing for final snapshots and merged artifacts
 *
 * Efficiency notes:
 *  - Printer uses a Channel with back-pressure.
 *  - We use the **batched** -Top event (TopKReplaceBatch) to avoid rewriting the file
 *    multiple times per generation.
 */
class DatasetController(
    private var targetToAchieve: String
) {

    var models = mutableListOf<DatasetModel>()
    private var view = DatasetView()
    private lateinit var parameters: Parameters
    private lateinit var datasetPath: String
    var aggregatedDataResultPaths = mutableListOf<String>()
    var variableValuesResultPaths = mutableListOf<String>()
    var functionValuesResultPaths = mutableListOf<String>()
    var topSolutionsResultPaths = mutableListOf<String>()
    var infoResultPaths = mutableListOf<String>()
    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    init {
        logger.info("Problem resolution started.")
    }

    fun load(datasetPath: String) {

        this.datasetPath = datasetPath

        logger.info("Dataset loading started.")
        logger.info("Path: \"$datasetPath\".")

        try {
            models.plusAssign(DatasetModel())
            models[0].loadData(this.datasetPath)
            if (targetToAchieve == Constants.TARGET_ALL) {
                models.plusAssign(DatasetModel())
                models[1].loadData(this.datasetPath)
                models.plusAssign(DatasetModel())
                models[2].loadData(this.datasetPath)
            }
        } catch (_: FileNotFoundException) {
            logger.warn("Dataset not found. Is file inside a \"data\" dir.?")
        } catch (exception: IOException) {
            logger.warn(exception.message as String)
        }

        logger.info("Dataset loading for input file \"${models[0].datasetName}\" completed.")
    }

    fun expandTopics(expansionCoefficient: Int) {
        val random = Random()
        val systemLabels = models[0].systemLabels
        val topicLabels = Array(expansionCoefficient) { "${random.nextInt(998 + 1 - 800) + 800} (F)" }
        val randomizedAveragePrecisions = LinkedHashMap<String, DoubleArray>()
        systemLabels.forEach { systemLabel ->
            randomizedAveragePrecisions[systemLabel] = DoubleArray(expansionCoefficient) { Math.random() }
        }
        models.forEach { model -> model.expandTopics(expansionCoefficient, randomizedAveragePrecisions, topicLabels) }
    }

    fun expandSystems(expansionCoefficient: Int, trueNumberOfSystems: Int) {
        val random = Random()
        val systemLabels = Array(expansionCoefficient) { index -> "Sys$index${random.nextInt(998 + 1 - 800) + 800} (F)" }
        val randomizedAveragePrecisions = LinkedHashMap<String, DoubleArray>()
        systemLabels.forEach { systemLabel ->
            randomizedAveragePrecisions[systemLabel] =
                DoubleArray(models[0].numberOfTopics + expansionCoefficient) { Math.random() }
        }
        models.forEach { model ->
            model.expandSystems(expansionCoefficient, trueNumberOfSystems, randomizedAveragePrecisions, systemLabels)
        }
    }

    fun solve(parameters: Parameters) {

        this.parameters = parameters

        logger.info("Printing common execution parameters.")
        logger.info("Dataset name: ${parameters.datasetName}.")
        logger.info("Correlation: ${parameters.correlationMethod}.")
        logger.info("Target: ${parameters.targetToAchieve}.")
        logger.info("[Experiments: Best, Worst] Number of iterations: ${parameters.numberOfIterations}.")
        logger.info("[Experiments: Best, Worst] Population size: ${parameters.populationSize}. ")
        logger.info("[Experiment: Average] Number of repetitions: ${parameters.numberOfRepetitions}.")

        if (parameters.currentExecution > 0) logger.info("Current Execution: ${parameters.currentExecution}.")

        if (parameters.targetToAchieve == Constants.TARGET_ALL || parameters.targetToAchieve == Constants.TARGET_AVERAGE) {
            val pct = parameters.percentiles.joinToString(", ") { "$it%" }
            logger.info("Percentiles: $pct. [Experiment: Average]")
        }

        if (parameters.targetToAchieve == Constants.TARGET_ALL) {

            val bestParameters = parameters.copy(targetToAchieve = Constants.TARGET_BEST)
            val worstParameters = parameters.copy(targetToAchieve = Constants.TARGET_WORST)
            val averageParameters = parameters.copy(targetToAchieve = Constants.TARGET_AVERAGE)

            /* Capacity can be tuned; small buffer applies back-pressure to the solver if I/O is slow. */
            val progress: Channel<ProgressEvent> = Channel(64)

            runBlocking {
                supervisorScope {
                    /* Printer: append/replace rows as they arrive (DatasetView = composite to CSV/Parquet). */
                    val printer = launch(Dispatchers.IO) {
                        for (ev in progress) {
                            /* Lookup the right model by target; all 3 have unique targets */
                            val model = models.first { it.targetToAchieve == ev.target }
                            when (ev) {
                                is CardinalityResult -> view.appendCardinality(model, ev)
                                is TopKReplaceBatch -> view.replaceTopBatch(model, ev.blocks)
                                is RunCompleted -> view.closeStreams(model)
                            }
                        }
                    }

                    /* Launch solvers (Best/Worst/Average) */
                    val jobs = listOf(
                        launch(Dispatchers.Default) { models[0].solve(bestParameters, progress) },
                        launch(Dispatchers.Default) { models[1].solve(worstParameters, progress) },
                        launch(Dispatchers.Default) { models[2].solve(averageParameters, progress) }
                    )
                    jobs.joinAll()
                    progress.close()
                    printer.join()
                }
            }

            aggregatedDataResultPaths.add(view.getAggregatedDataFilePath(models[0], isTargetAll = true))
            models.forEach { model ->
                functionValuesResultPaths.add(view.getFunctionValuesFilePath(model))
                variableValuesResultPaths.add(view.getVariableValuesFilePath(model))
                if (model.targetToAchieve != Constants.TARGET_AVERAGE)
                    topSolutionsResultPaths.add(view.getTopSolutionsFilePath(model))
            }
            infoResultPaths.add(view.getInfoFilePath(models[0], isTargetAll = true))

            logger.info("Data aggregation started.")
            val aggregatedRows = aggregate(models)
            view.writeCsv(aggregatedRows, view.getAggregatedDataFilePath(models[0], isTargetAll = true))
            view.writeParquet(aggregatedRows, view.getAggregatedDataParquetPath(models[0], isTargetAll = true))
            logger.info("Aggregated data available at:")
            logger.info("\"${view.getAggregatedDataFilePath(models[0], isTargetAll = true)}\"")

            logger.info("Execution information gathering started.")
            val infoRows = info(models)
            view.writeCsv(infoRows, view.getInfoFilePath(models[0], isTargetAll = true))
            view.writeParquet(infoRows, view.getInfoParquetPath(models[0], isTargetAll = true))
            logger.info("Execution information available at:")
            logger.info("\"${view.getInfoFilePath(models[0], isTargetAll = true)}\"")

            logger.info("Execution result paths:")
            models.forEach { model ->
                logger.info("\"${view.getFunctionValuesFilePath(model)}\" (Function values CSV)")
                logger.info("\"${view.getVariableValuesFilePath(model)}\" (Variable values CSV)")
                logger.info("\"${view.getFunctionValuesParquetPath(model)}\" (Function values Parquet)")
                logger.info("\"${view.getVariableValuesParquetPath(model)}\" (Variable values Parquet)")
                if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                    logger.info("\"${view.getTopSolutionsFilePath(model)}\" (Top Solutions CSV)")
                    logger.info("\"${view.getTopSolutionsParquetPath(model)}\" (Top Solutions Parquet)")
                }
            }
            logger.info("\"${view.getAggregatedDataFilePath(models[0], isTargetAll = true)}\" (Aggregated data CSV)")
            logger.info("\"${view.getInfoFilePath(models[0], isTargetAll = true)}\" (Info CSV)")

        } else {
            /* Streaming also for single target */
            val progress: Channel<ProgressEvent> = Channel(64)

            runBlocking {
                supervisorScope {
                    /* Printer -> composite DatasetView */
                    val printer = launch(Dispatchers.IO) {
                        for (ev in progress) {
                            val model = models[0]
                            when (ev) {
                                is CardinalityResult -> view.appendCardinality(model, ev)
                                is TopKReplaceBatch -> view.replaceTopBatch(model, ev.blocks)
                                is RunCompleted -> view.closeStreams(model)
                            }
                        }
                    }

                    /* Launch solver with channel */
                    val job = launch(Dispatchers.Default) { models[0].solve(parameters, progress) }
                    job.join()
                    progress.close()
                    printer.join()
                }
            }

            /* Collect paths (files are already appended by the printer) */
            aggregatedDataResultPaths.add(view.getAggregatedDataFilePath(models[0], isTargetAll = false))
            functionValuesResultPaths.add(view.getFunctionValuesFilePath(models[0]))
            variableValuesResultPaths.add(view.getVariableValuesFilePath(models[0]))
            if (models[0].targetToAchieve != Constants.TARGET_AVERAGE)
                topSolutionsResultPaths.add(view.getTopSolutionsFilePath(models[0]))
            infoResultPaths.add(view.getInfoFilePath(models[0], isTargetAll = false))

            logger.info("Data aggregation started.")
            val aggregatedRows = aggregate(models)
            view.writeCsv(aggregatedRows, view.getAggregatedDataFilePath(models[0], isTargetAll = false))
            view.writeParquet(aggregatedRows, view.getAggregatedDataParquetPath(models[0], isTargetAll = false))

            logger.info("Execution information gathering started.")
            val infoRows = info(models)
            view.writeCsv(infoRows, view.getInfoFilePath(models[0], isTargetAll = false))
            view.writeParquet(infoRows, view.getInfoParquetPath(models[0], isTargetAll = false))
            logger.info("Execution information available at:")
            logger.info("\"${view.getInfoFilePath(models[0], isTargetAll = false)}\"")

            logger.info("Execution result paths:")
            logger.info("\"${view.getFunctionValuesFilePath(models[0])}\" (Function values CSV)")
            logger.info("\"${view.getVariableValuesFilePath(models[0])}\" (Variable values CSV)")
            logger.info("\"${view.getFunctionValuesParquetPath(models[0])}\" (Function values Parquet)")
            logger.info("\"${view.getVariableValuesParquetPath(models[0])}\" (Variable values Parquet)")
            if (models[0].targetToAchieve != Constants.TARGET_AVERAGE) {
                logger.info("\"${view.getTopSolutionsFilePath(models[0])}\" (Top Solutions CSV)")
                logger.info("\"${view.getTopSolutionsParquetPath(models[0])}\" (Top Solutions Parquet)")
            }
            logger.info("\"${view.getAggregatedDataFilePath(models[0], isTargetAll = false)}\" (Aggregated data CSV)")
            logger.info("\"${view.getInfoFilePath(models[0], isTargetAll = false)}\" (Info CSV)")
        }

        logger.info("Execution information gathering completed.")
        logger.info("Data aggregation completed.")
        logger.info("Problem resolution completed.")
    }

    /* ------------------------ Aggregation and info (unchanged) ------------------------ */

    private fun aggregate(models: List<DatasetModel>): List<Array<String>> {
        val header = mutableListOf<String>()
        val incompleteData = mutableListOf<Array<String>>()
        val aggregatedData = mutableListOf<Array<String>>()
        val topicLabels = models[0].topicLabels
        var percentiles = linkedMapOf<Int, List<Double>>()

        header.add("Cardinality")
        models.forEach { model ->
            header.add(model.targetToAchieve)
            if (model.targetToAchieve == Constants.TARGET_AVERAGE) percentiles = model.percentiles
        }
        percentiles.keys.forEach { percentile -> header.add("$percentile%") }
        topicLabels.forEach { topicLabel -> header.add(topicLabel) }
        aggregatedData.add(header.toTypedArray())

        logger.info("Starting to print common data between models.")
        logger.info("Topics number: ${models[0].numberOfTopics}")
        logger.info("Systems number: ${models[0].numberOfSystems}")
        logger.info("Print completed.")

        val computedCardinality =
            mutableMapOf(Constants.TARGET_BEST to 0, Constants.TARGET_WORST to 0, Constants.TARGET_AVERAGE to 0)

        (0..models[0].numberOfTopics - 1).forEach { index ->
            val currentCardinality = (index + 1).toDouble()
            val currentLine = LinkedList<String>()
            currentLine.add(currentCardinality.toString())
            models.forEach { model ->
                val correlationValueForCurrentCardinality = model.findCorrelationForCardinality(currentCardinality)
                if (correlationValueForCurrentCardinality != null) {
                    currentLine.add(correlationValueForCurrentCardinality.toString())
                    computedCardinality[model.targetToAchieve] =
                        computedCardinality[model.targetToAchieve]?.plus(1) ?: 0
                } else currentLine.add(Constants.CARDINALITY_NOT_AVAILABLE)
            }
            incompleteData.add(currentLine.toTypedArray())
        }

        if (parameters.targetToAchieve != Constants.TARGET_ALL) {
            logger.info("Total cardinality computed for target \"${parameters.targetToAchieve}\": ${computedCardinality[parameters.targetToAchieve]}/${models[0].numberOfTopics}.")
        } else {
            logger.info("Total cardinality computed for target \"${Constants.TARGET_BEST}\": ${computedCardinality[Constants.TARGET_BEST]}/${models[0].numberOfTopics}.")
            logger.info("Total cardinality computed for target \"${Constants.TARGET_WORST}\": ${computedCardinality[Constants.TARGET_WORST]}/${models[0].numberOfTopics}.")
            logger.info("Total cardinality computed for target \"${Constants.TARGET_AVERAGE}\": ${computedCardinality[Constants.TARGET_AVERAGE]}/${models[0].numberOfTopics}.")
        }

        incompleteData.forEach { aLine ->
            var newDataEntry = aLine
            val currentCardinality = newDataEntry[0].toDouble()
            percentiles.entries.forEach { (_, percentileValues) ->
                newDataEntry = newDataEntry.plus(percentileValues[currentCardinality.toInt() - 1].toString())
            }
            topicLabels.forEach { label ->
                // Build a compact presence code for this topic at the current cardinality:
                // "B" if present in BEST, "W" if present in WORST, "BW" if both, or "N" if in none.
                val flags = StringBuilder()
                models.forEach { model ->
                    val present = model.isTopicInASolutionOfCardinality(label, currentCardinality)
                    when (model.targetToAchieve) {
                        Constants.TARGET_BEST -> if (present) flags.append('B')
                        Constants.TARGET_WORST -> if (present) flags.append('W')
                    }
                }
                if (flags.isEmpty()) flags.append('N')
                newDataEntry = newDataEntry.plus(flags.toString())
            }
            aggregatedData.add(newDataEntry)
        }

        incompleteData.clear()
        return aggregatedData
    }

    private fun info(models: List<DatasetModel>): List<Array<String>> {
        val header = mutableListOf<String>()
        val aggregatedData = mutableListOf<Array<String>>()
        var executionParameters: MutableList<String>

        header.add("Dataset Name")
        header.add("Number of Systems")
        header.add("Number of Topics")
        header.add("Correlation Method")
        header.add("Target to Achieve")
        header.add("Number of Iterations")
        header.add("Population Size")
        header.add("Number of Repetitions")
        header.add("Computing Time")
        aggregatedData.add(header.toTypedArray())

        models.forEach { model ->
            executionParameters = mutableListOf()
            executionParameters.plusAssign(model.datasetName)
            executionParameters.plusAssign(model.numberOfSystems.toString())
            executionParameters.plusAssign(model.numberOfTopics.toString())
            executionParameters.plusAssign(model.correlationMethod)
            executionParameters.plusAssign(model.targetToAchieve)
            executionParameters.plusAssign(model.numberOfIterations.toString())
            executionParameters.plusAssign(model.populationSize.toString())
            executionParameters.plusAssign(model.numberOfRepetitions.toString())
            executionParameters.plusAssign(model.computingTime.toString())
            aggregatedData.add(executionParameters.toTypedArray())
        }
        return aggregatedData
    }

    fun merge(numberOfExecutions: Int) {

        logger.info("Starting to merge results of $numberOfExecutions executions.")

        /* ----------------- tiny helpers ----------------- */

        fun readAllCsvRows(path: String): List<Array<String>> =
            try {
                CSVReader(FileReader(path)).use { it.readAll() ?: emptyList() }
            } catch (e: Exception) {
                logger.warn("Failed to read CSV at '$path': ${e.message}")
                emptyList()
            }

        fun readAllLines(path: String): List<String> =
            try {
                Files.readAllLines(Paths.get(path)).filter { it.isNotBlank() }
            } catch (e: Exception) {
                logger.warn("Failed to read text at '$path': ${e.message}")
                emptyList()
            }

        fun headerOrEmpty(rows: List<Array<String>>): Array<String> =
            if (rows.isNotEmpty()) rows.first() else emptyArray()

        fun dataRows(rows: List<Array<String>>): List<Array<String>> =
            if (rows.size > 1) rows.drop(1) else emptyList()

        fun minDataLen(tables: List<List<Array<String>>>): Int =
            tables.minOfOrNull { (if (it.size > 0) it.size - 1 else 0) } ?: 0

        fun safeDouble(s: String): Double? = runCatching { s.trim().toDouble() }.getOrNull()

        /* ----------------- 1) aggregated tables ----------------- */

        logger.info("Loading aggregated data for all executions.")
        logger.info("Aggregated data paths:")
        aggregatedDataResultPaths.take(numberOfExecutions).forEach { logger.info("\"$it\"") }

        val aggregatedTables = aggregatedDataResultPaths
            .take(numberOfExecutions)
            .map { readAllCsvRows(it) }

        if (aggregatedTables.any { it.isEmpty() }) {
            logger.warn("One or more aggregated CSV files are empty; aborting merge to avoid partial output.")
            return
        }

        val aggregatedHeader = headerOrEmpty(aggregatedTables.first())
        val aggregatedDataRowsPerExec = aggregatedTables.map { dataRows(it) }

        val alignedCardinalityCount = minDataLen(aggregatedTables)
        val totalCardinalities = alignedCardinalityCount

        val loggingFactor = maxOf(1, (totalCardinalities * Constants.LOGGING_FACTOR) / 100)
        var progressCounter = 0

        /* ----------------- 2) per-target per-exec (FUN/VAR/TOP) ----------------- */

        var bestFunctionValues: LinkedList<LinkedList<String>>? = null
        var bestVariableValues: LinkedList<LinkedList<String>>? = null
        var bestTopSolutions: LinkedList<LinkedList<String>>? = null
        var worstFunctionValues: LinkedList<LinkedList<String>>? = null
        var worstVariableValues: LinkedList<LinkedList<String>>? = null
        var worstTopSolutions: LinkedList<LinkedList<String>>? = null
        var averageFunctionValues: LinkedList<LinkedList<String>>? = null
        var averageVariableValues: LinkedList<LinkedList<String>>? = null

        fun prepareExecListsFor(model: DatasetModel) {

            fun execSlicePaths(all: MutableList<String>, expect: Int, label: String): List<String> {
                if (all.size < expect) {
                    logger.warn("Expected at least $expect $label paths, found ${all.size}. Some executions may be missing.")
                }
                return all.take(expect)
            }

            val funPaths = mutableListOf<String>()
            val varPaths = mutableListOf<String>()
            val topPaths = mutableListOf<String>()

            when (model.targetToAchieve) {
                Constants.TARGET_BEST -> {
                    funPaths += functionValuesResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_BEST) }
                    varPaths += variableValuesResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_BEST) }
                    topPaths += topSolutionsResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_BEST) }
                }

                Constants.TARGET_WORST -> {
                    funPaths += functionValuesResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_WORST) }
                    varPaths += variableValuesResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_WORST) }
                    topPaths += topSolutionsResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_WORST) }
                }

                Constants.TARGET_AVERAGE -> {
                    funPaths += functionValuesResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_AVERAGE) }
                    varPaths += variableValuesResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_AVERAGE) }
                }
            }

            val funTables: List<List<String>> = execSlicePaths(funPaths, numberOfExecutions, "FUN")
                .map { readAllLines(it) }
                .map { it.take(alignedCardinalityCount) }

            val varTables: List<List<String>> = execSlicePaths(varPaths, numberOfExecutions, "VAR")
                .map { readAllLines(it) }
                .map { it.take(alignedCardinalityCount) }

            val topTables: List<List<String>> =
                if (model.targetToAchieve != Constants.TARGET_AVERAGE)
                    execSlicePaths(topPaths, numberOfExecutions, "TOP")
                        .map { readAllLines(it).drop(1) } // drop header
                else emptyList()

            fun <T> transposeToLinked(rowsPerExec: List<List<T>>): LinkedList<LinkedList<T>> {
                val h = LinkedList<LinkedList<T>>()
                if (rowsPerExec.isEmpty()) return h
                val minLen = rowsPerExec.minOf { it.size }
                repeat(minLen) { idx ->
                    val col = LinkedList<T>()
                    rowsPerExec.forEach { execRows -> col += execRows[idx] }
                    h += col
                }
                return h
            }

            when (model.targetToAchieve) {
                Constants.TARGET_BEST -> {
                    bestFunctionValues = transposeToLinked(funTables)
                    bestVariableValues = transposeToLinked(varTables)
                    bestTopSolutions = transposeToLinked(topTables)
                }

                Constants.TARGET_WORST -> {
                    worstFunctionValues = transposeToLinked(funTables)
                    worstVariableValues = transposeToLinked(varTables)
                    worstTopSolutions = transposeToLinked(topTables)
                }

                Constants.TARGET_AVERAGE -> {
                    averageFunctionValues = transposeToLinked(funTables)
                    averageVariableValues = transposeToLinked(varTables)
                }
            }
        }

        if (targetToAchieve == Constants.TARGET_ALL) {
            prepareExecListsFor(models[0]) // BEST
            prepareExecListsFor(models[1]) // WORST
            prepareExecListsFor(models[2]) // AVERAGE
        } else {
            prepareExecListsFor(models[0])
        }

        /* ----------------- 3) merge Aggregated + pick per-target rows ----------------- */

        val mergedAggregatedData = LinkedList<Array<String>>().apply { add(aggregatedHeader) }

        val mergedBestFunctionValues = LinkedList<String>()
        val mergedBestVariableValues = LinkedList<String>()
        val mergedWorstFunctionValues = LinkedList<String>()
        val mergedWorstVariableValues = LinkedList<String>()
        val mergedAverageFunctionValues = LinkedList<String>()
        val mergedAverageVariableValues = LinkedList<String>()
        val mergedBestTopSolutions = LinkedList<String>()
        val mergedWorstTopSolutions = LinkedList<String>()

        for (cardIdx in 0 until totalCardinalities) {
            if ((cardIdx % loggingFactor) == 0 && totalCardinalities > 0) {
                logger.info("Results merged for cardinality: ${cardIdx + 1}/$totalCardinalities ($progressCounter%) for $numberOfExecutions total executions.")
                progressCounter += Constants.LOGGING_FACTOR
            }

            val rowsAtI: List<Array<String>> =
                aggregatedDataRowsPerExec.mapNotNull { execRows -> execRows.getOrNull(cardIdx) }
            if (rowsAtI.isEmpty()) continue

            val cardinalityToken = rowsAtI.first()[0]

            var bestAggCorr = Double.NEGATIVE_INFINITY
            var bestExecIdx = -1
            var worstAggCorr = Double.POSITIVE_INFINITY
            var worstExecIdx = -1

            rowsAtI.forEachIndexed { execIdx, row ->
                when (targetToAchieve) {
                    Constants.TARGET_ALL -> {
                        safeDouble(row.getOrNull(1) ?: "")?.let {
                            if (it > bestAggCorr) {
                                bestAggCorr = it; bestExecIdx = execIdx
                            }
                        }
                        safeDouble(row.getOrNull(2) ?: "")?.let {
                            if (it < worstAggCorr) {
                                worstAggCorr = it; worstExecIdx = execIdx
                            }
                        }
                    }

                    Constants.TARGET_BEST -> {
                        safeDouble(row.getOrNull(1) ?: "")?.let {
                            if (it > bestAggCorr) {
                                bestAggCorr = it; bestExecIdx = execIdx
                            }
                        }
                    }

                    Constants.TARGET_WORST -> {
                        safeDouble(row.getOrNull(1) ?: "")?.let {
                            if (it < worstAggCorr) {
                                worstAggCorr = it; worstExecIdx = execIdx
                            }
                        }
                    }

                    Constants.TARGET_AVERAGE -> { /* copy as is */
                    }
                }
            }

            val template = rowsAtI.first()
            val out = Array(template.size) { "" }
            out[0] = cardinalityToken
            when (targetToAchieve) {
                Constants.TARGET_ALL -> {
                    out[1] = if (bestExecIdx >= 0) bestAggCorr.toString() else Constants.CARDINALITY_NOT_AVAILABLE
                    out[2] = if (worstExecIdx >= 0) worstAggCorr.toString() else Constants.CARDINALITY_NOT_AVAILABLE
                    for (col in 3 until template.size) out[col] = template[col]
                }

                Constants.TARGET_BEST -> {
                    out[1] = if (bestExecIdx >= 0) bestAggCorr.toString() else Constants.CARDINALITY_NOT_AVAILABLE
                    for (col in 2 until template.size) out[col] = template[col]
                }

                Constants.TARGET_WORST -> {
                    out[1] = if (worstExecIdx >= 0) worstAggCorr.toString() else Constants.CARDINALITY_NOT_AVAILABLE
                    for (col in 2 until template.size) out[col] = template[col]
                }

                Constants.TARGET_AVERAGE -> {
                    val src = rowsAtI.first()
                    for (col in src.indices) out[col] = src[col]
                }
            }
            mergedAggregatedData += out

            // Stash per-target selections (FUN/VAR/TOP) with null/size guards

            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_BEST) {
                bestFunctionValues?.let { m ->
                    if (cardIdx < m.size) {
                        val execCol = if (bestExecIdx >= 0) bestExecIdx.coerceAtMost(m[cardIdx].size - 1) else 0
                        m[cardIdx].getOrNull(execCol)?.let { mergedBestFunctionValues += it }
                    }
                }
                bestVariableValues?.let { m ->
                    if (cardIdx < m.size) {
                        val execCol = if (bestExecIdx >= 0) bestExecIdx.coerceAtMost(m[cardIdx].size - 1) else 0
                        m[cardIdx].getOrNull(execCol)?.let { mergedBestVariableValues += it }
                    }
                }
                bestTopSolutions?.forEach { perExecLineSet ->
                    val line = perExecLineSet.getOrNull(bestExecIdx) ?: return@forEach
                    val kToken = line.substringBefore(',').trim()
                    if (kToken == (cardIdx + 1).toString()) mergedBestTopSolutions += line
                }
            }

            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_WORST) {
                worstFunctionValues?.let { m ->
                    if (cardIdx < m.size) {
                        val execCol = if (worstExecIdx >= 0) worstExecIdx.coerceAtMost(m[cardIdx].size - 1) else 0
                        m[cardIdx].getOrNull(execCol)?.let { mergedWorstFunctionValues += it }
                    }
                }
                worstVariableValues?.let { m ->
                    if (cardIdx < m.size) {
                        val execCol = if (worstExecIdx >= 0) worstExecIdx.coerceAtMost(m[cardIdx].size - 1) else 0
                        m[cardIdx].getOrNull(execCol)?.let { mergedWorstVariableValues += it }
                    }
                }
                worstTopSolutions?.forEach { perExecLineSet ->
                    val line = perExecLineSet.getOrNull(worstExecIdx) ?: return@forEach
                    val kToken = line.substringBefore(',').trim()
                    if (kToken == (cardIdx + 1).toString()) mergedWorstTopSolutions += line
                }
            }

            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {
                averageFunctionValues?.let { m ->
                    if (cardIdx < m.size) m[cardIdx].firstOrNull()?.let { mergedAverageFunctionValues += it }
                }
                averageVariableValues?.let { m ->
                    if (cardIdx < m.size) m[cardIdx].firstOrNull()?.let { mergedAverageVariableValues += it }
                }
            }
        }

        logger.info("Results merged for cardinality: $totalCardinalities/$totalCardinalities (100%) for $numberOfExecutions total executions.")

        /* ----------------- 4) merge Info ----------------- */

        logger.info("Loading info for all executions.")
        logger.info("Info paths:")
        infoResultPaths.take(numberOfExecutions).forEach { logger.info("\"$it\"") }
        val infoTables: List<List<String>> = infoResultPaths.take(numberOfExecutions).map { readAllLines(it) }

        val mergedInfo = LinkedList<String>()
        val infoHeader = infoTables.firstOrNull()?.firstOrNull()
            ?: "Dataset Name,Number of Systems,Number of Topics,Correlation Method,Target to Achieve,Number of Iterations,Population Size,Number of Repetitions,Computing Time"
        mergedInfo += infoHeader

        fun sumTimes(lines: List<String>): Int =
            lines.drop(1).sumOf { ln ->
                ln.substringAfterLast(',').replace("\"", "").trim().toIntOrNull() ?: 0
            }

        when (targetToAchieve) {
            Constants.TARGET_ALL -> {
                val bestLines = infoTables.getOrNull(0) ?: emptyList()
                val worstLines = infoTables.getOrNull(1) ?: emptyList()
                val avgLines = infoTables.getOrNull(2) ?: emptyList()

                fun mergeInfoRow(src: String, total: Int): String {
                    val parts = src.split(',').toMutableList()
                    if (parts.isNotEmpty()) parts[parts.lastIndex] = total.toString()
                    return parts.joinToString(",")
                }

                if (bestLines.size > 1) mergedInfo += mergeInfoRow(bestLines[1], sumTimes(bestLines))
                if (worstLines.size > 1) mergedInfo += mergeInfoRow(worstLines[1], sumTimes(worstLines))
                if (avgLines.size > 1) mergedInfo += mergeInfoRow(avgLines[1], sumTimes(avgLines))
            }

            else -> {
                val lines = infoTables.firstOrNull() ?: emptyList()
                if (lines.size > 1) {
                    val total = sumTimes(lines)
                    val mergedRow = lines[1].split(',').toMutableList().also {
                        if (it.isNotEmpty()) it[it.lastIndex] = total.toString()
                    }.joinToString(",")
                    mergedInfo += mergedRow
                }
            }
        }

        /* ----------------- 5) write merged CSV + Parquet ----------------- */

        val isAll = (targetToAchieve == Constants.TARGET_ALL)

        // Aggregated
        val mergedAggCsv = view.getAggregatedDataMergedFilePath(models[0], isTargetAll = isAll)
        view.writeCsv(mergedAggregatedData, mergedAggCsv)
        logger.info("Merged aggregated data available at:")
        logger.info("\"$mergedAggCsv\"")

        val mergedAggParquet = view.getAggregatedDataMergedParquetPath(models[0], isTargetAll = isAll)
        view.writeParquet(mergedAggregatedData, mergedAggParquet)

        // Info
        val mergedInfoCsv = view.getInfoMergedFilePath(models[0], isTargetAll = isAll)
        Files.newBufferedWriter(Paths.get(mergedInfoCsv)).use { w ->
            mergedInfo.forEach { ln -> w.appendLine(ln) }
        }
        logger.info("Merged info available at:")
        logger.info("\"$mergedInfoCsv\"")

        val mergedInfoParquet = view.getInfoMergedParquetPath(models[0], isTargetAll = isAll)
        val infoRowsForParquet = mergedInfo.map { it.split(',').toTypedArray() }
        view.writeParquet(infoRowsForParquet, mergedInfoParquet)

        // Per target: FUN / VAR / TOP -> write CSV as before + Parquet siblings.

        fun writeMergedPerTarget(model: DatasetModel) {
            /* ---- CSV ---- */

            // FUN CSV
            val funMergedCsv = view.getFunctionValuesMergedFilePath(model)
            Files.newBufferedWriter(Paths.get(funMergedCsv)).use { w ->
                val lines = when (model.targetToAchieve) {
                    Constants.TARGET_BEST -> mergedBestFunctionValues
                    Constants.TARGET_WORST -> mergedWorstFunctionValues
                    else -> mergedAverageFunctionValues
                }
                lines.forEach { ln -> w.appendLine(ln) }
            }

            // VAR CSV
            val varMergedCsv = view.getVariableValuesMergedFilePath(model)
            Files.newBufferedWriter(Paths.get(varMergedCsv)).use { w ->
                val lines = when (model.targetToAchieve) {
                    Constants.TARGET_BEST -> mergedBestVariableValues
                    Constants.TARGET_WORST -> mergedWorstVariableValues
                    else -> mergedAverageVariableValues
                }
                lines.forEach { ln -> w.appendLine(ln) }
            }

            // TOP CSV (no AVERAGE)
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                val topMergedCsv = view.getTopSolutionsMergedFilePath(model)
                Files.newBufferedWriter(Paths.get(topMergedCsv)).use { w ->
                    w.appendLine("Cardinality,Correlation,Topics")
                    val lines = if (model.targetToAchieve == Constants.TARGET_BEST) mergedBestTopSolutions else mergedWorstTopSolutions
                    lines.forEach { ln -> w.appendLine(ln) }
                }
            }

            /* ---- Parquet ---- */

            // FUN Parquet (header: K,Correlation)
            run {
                val funLines = when (model.targetToAchieve) {
                    Constants.TARGET_BEST -> mergedBestFunctionValues
                    Constants.TARGET_WORST -> mergedWorstFunctionValues
                    else -> mergedAverageFunctionValues
                }
                val rows = ArrayList<Array<String>>(funLines.size + 1)
                rows += arrayOf("K", "Correlation")
                funLines.forEach { ln ->
                    val parts = ln.trim().split(Regex("\\s+"))
                    val kTok = parts.getOrNull(0) ?: ""
                    val corrTok = parts.getOrNull(1) ?: ""
                    rows += arrayOf(kTok, corrTok)
                }
                val out = view.getFunctionValuesMergedParquetPath(model)
                view.writeParquet(rows, out)
            }

            // VAR Parquet (header: K,Topics) – K inferred from row index (1..N)
            run {
                val varLines = when (model.targetToAchieve) {
                    Constants.TARGET_BEST -> mergedBestVariableValues
                    Constants.TARGET_WORST -> mergedWorstVariableValues
                    else -> mergedAverageVariableValues
                }
                val rows = ArrayList<Array<String>>(varLines.size + 1)
                rows += arrayOf("K", "Topics")
                varLines.forEachIndexed { idx, ln ->
                    val kTok = (idx + 1).toString()
                    val topicsTok = ln.trim()
                    rows += arrayOf(kTok, topicsTok)
                }
                val out = view.getVariableValuesMergedParquetPath(model)
                view.writeParquet(rows, out)
            }

            // TOP Parquet (header: K,Correlation,Topics)
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                val rows = ArrayList<Array<String>>()
                rows += arrayOf("K", "Correlation", "Topics")
                val src = if (model.targetToAchieve == Constants.TARGET_BEST) mergedBestTopSolutions else mergedWorstTopSolutions
                src.forEach { ln ->
                    val parts = ln.split(',', limit = 3)
                    val kTok = parts.getOrNull(0)?.trim() ?: ""
                    val corrTok = parts.getOrNull(1)?.trim() ?: ""
                    val topicsTok = parts.getOrNull(2)?.trim() ?: ""
                    rows += arrayOf(kTok, corrTok, topicsTok)
                }
                val out = view.getTopSolutionsMergedParquetPath(model)
                view.writeParquet(rows, out)
            }
        }

        if (targetToAchieve == Constants.TARGET_ALL) {
            writeMergedPerTarget(models[0]) // BEST
            writeMergedPerTarget(models[1]) // WORST
            writeMergedPerTarget(models[2]) // AVERAGE
        } else {
            writeMergedPerTarget(models[0])
        }

        /* ----------------- 6) cleanup non-merged CSV + Parquet ----------------- */

        // Snapshot the original CSV path lists BEFORE mutating them
        val snapAggCsv = aggregatedDataResultPaths.toList()
        val snapFunCsv = functionValuesResultPaths.toList()
        val snapVarCsv = variableValuesResultPaths.toList()
        val snapTopCsv = topSolutionsResultPaths.toList()
        val snapInfoCsv = infoResultPaths.toList()

        // CSV cleanup
        logger.info("Cleaning of not merged CSV results started.")
        clean(aggregatedDataResultPaths, "Cleaning aggregated CSV data at paths:")
        clean(functionValuesResultPaths, "Cleaning function values CSV at paths:")
        clean(variableValuesResultPaths, "Cleaning variable values CSV at paths:")
        clean(topSolutionsResultPaths, "Cleaning top solutions CSV at paths:")
        clean(infoResultPaths, "Cleaning info CSV at paths:")
        logger.info("Cleaning of not merged CSV results completed.")

        // Build Parquet lists from the SNAPSHOTS (so we still have ex1/ex2)
        fun mapCsvToParquet(path: String): String =
            path
                .replace(
                    "${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}",
                    "${Constants.PATH_SEPARATOR}Parquet${Constants.PATH_SEPARATOR}"
                )
                .replace(".csv", ".parquet")

        val aggParquetToClean = snapAggCsv.map(::mapCsvToParquet).toMutableList()
        val funParquetToClean = snapFunCsv.map(::mapCsvToParquet).toMutableList()
        val varParquetToClean = snapVarCsv.map(::mapCsvToParquet).toMutableList()
        val topParquetToClean = snapTopCsv.map(::mapCsvToParquet).toMutableList()
        val infoParquetToClean = snapInfoCsv.map(::mapCsvToParquet).toMutableList()

        // Parquet cleanup
        logger.info("Cleaning of not merged Parquet results started.")
        clean(aggParquetToClean, "Cleaning aggregated Parquet data at paths:")
        clean(funParquetToClean, "Cleaning function values Parquet at paths:")
        clean(varParquetToClean, "Cleaning variable values Parquet at paths:")
        clean(topParquetToClean, "Cleaning top solutions Parquet at paths:")
        clean(infoParquetToClean, "Cleaning info Parquet at paths:")
        logger.info("Cleaning of not merged Parquet results completed.")

        logger.info("Executions result merging completed.")

    }


    fun copy() {

        logger.info("Execution result copy to ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} started.")

        val inputRootPath = Constants.NEWBESTSUB_EXPERIMENTS_INPUT_PATH
        val inputRootDir = File(inputRootPath)

        logger.info("Checking if ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} input dir. exists.")
        if (!inputRootDir.exists()) {
            logger.info("Input dir. not exists.")
            if (inputRootDir.mkdirs()) {
                logger.info("Input dir. created.")
                logger.info("Path: \"$inputRootPath\".")
            }
        } else {
            logger.info("Input dir. already exists.")
            logger.info("Input dir. creation skipped.")
            logger.info("Path: \"$inputRootPath\".")
        }

        /* Destination subfolders mirroring our writers */
        val csvDestDir = Paths.get("$inputRootPath${Constants.PATH_SEPARATOR}CSV")
        val parquetDestDir = Paths.get("$inputRootPath${Constants.PATH_SEPARATOR}Parquet")
        if (!Files.exists(csvDestDir)) Files.createDirectories(csvDestDir)
        if (!Files.exists(parquetDestDir)) Files.createDirectories(parquetDestDir)

        /* Small helper: copy if exists, preserve filename, overwrite */
        val copyIfExists = { srcPathStr: String, destDir: java.nio.file.Path ->
            val src = Paths.get(srcPathStr)
            if (Files.exists(src)) {
                val dst = destDir.resolve(src.fileName)
                Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING)
                logger.info("\"$src\" -> \"$dst\"")
                true
            } else {
                false
            }
        }

        /* ---------- Per-execution CSV artifacts (from collected path lists) ---------- */
        val csvListCopier = { dataList: MutableList<String>, logMessage: String ->
            if (dataList.isNotEmpty()) logger.info(logMessage)
            dataList.forEach { src -> copyIfExists(src, csvDestDir) }
        }

        csvListCopier(aggregatedDataResultPaths, "Aggregated data (CSV) copy started from paths:")
        csvListCopier(functionValuesResultPaths, "Function values (CSV) copy started from paths:")
        csvListCopier(variableValuesResultPaths, "Variable values (CSV) copy started from paths:")
        csvListCopier(topSolutionsResultPaths, "Top solutions (CSV) copy started from paths:")
        csvListCopier(infoResultPaths, "Info (CSV) copy started from paths:")

        /* ---------- Per-execution Parquet artifacts (computed from models) ---------- */
        models.forEach { model ->
            // Aggregated/Info: ALL vs single-target follow your solve() logic
            copyIfExists(
                view.getAggregatedDataParquetPath(model, isTargetAll = (targetToAchieve == Constants.TARGET_ALL)),
                parquetDestDir
            )
            copyIfExists(view.getFunctionValuesParquetPath(model), parquetDestDir)
            copyIfExists(view.getVariableValuesParquetPath(model), parquetDestDir)
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                copyIfExists(view.getTopSolutionsParquetPath(model), parquetDestDir)
            }
            copyIfExists(
                view.getInfoParquetPath(model, isTargetAll = (targetToAchieve == Constants.TARGET_ALL)),
                parquetDestDir
            )
        }

        /* ---------- Merged CSV artifacts ---------- */
        val isAll = (targetToAchieve == Constants.TARGET_ALL)

        val copyMergedCsv = { path: String, label: String ->
            if (copyIfExists(path, csvDestDir)) {
                logger.info("$label copied.")
            }
        }

        copyMergedCsv(view.getAggregatedDataMergedFilePath(models[0], isTargetAll = true), "Merged aggregated data (CSV, ALL)")
        copyMergedCsv(view.getAggregatedDataMergedFilePath(models[0], isTargetAll = false), "Merged aggregated data (CSV, single target)")

        models.forEach { model ->
            copyMergedCsv(view.getFunctionValuesMergedFilePath(model), "Merged function values (CSV) for ${model.targetToAchieve}")
            copyMergedCsv(view.getVariableValuesMergedFilePath(model), "Merged variable values (CSV) for ${model.targetToAchieve}")
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                copyMergedCsv(view.getTopSolutionsMergedFilePath(model), "Merged top solutions (CSV) for ${model.targetToAchieve}")
            }
            copyMergedCsv(view.getInfoMergedFilePath(model, isTargetAll = true), "Merged info (CSV, ALL) for ${model.targetToAchieve}")
            copyMergedCsv(view.getInfoMergedFilePath(model, isTargetAll = false), "Merged info (CSV, single target) for ${model.targetToAchieve}")
        }

        /* ---------- Merged Parquet artifacts ---------- */
        val copyMergedParquet = { path: String, label: String ->
            if (copyIfExists(path, parquetDestDir)) {
                logger.info("$label copied.")
            }
        }

        copyMergedParquet(view.getAggregatedDataMergedParquetPath(models[0], isTargetAll = true), "Merged aggregated data (Parquet, ALL)")
        copyMergedParquet(view.getAggregatedDataMergedParquetPath(models[0], isTargetAll = false), "Merged aggregated data (Parquet, single target)")

        models.forEach { model ->
            copyMergedParquet(view.getFunctionValuesMergedParquetPath(model), "Merged function values (Parquet) for ${model.targetToAchieve}")
            copyMergedParquet(view.getVariableValuesMergedParquetPath(model), "Merged variable values (Parquet) for ${model.targetToAchieve}")
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                copyMergedParquet(view.getTopSolutionsMergedParquetPath(model), "Merged top solutions (Parquet) for ${model.targetToAchieve}")
            }
            copyMergedParquet(view.getInfoMergedParquetPath(model, isTargetAll = true), "Merged info (Parquet, ALL) for ${model.targetToAchieve}")
            copyMergedParquet(view.getInfoMergedParquetPath(model, isTargetAll = false), "Merged info (Parquet, single target) for ${model.targetToAchieve}")
        }

        logger.info("Execution result copy to ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} completed.")
    }

    fun clean(dataList: MutableList<String>, logMessage: String) {
        logger.info(logMessage)
        val toBeRemoved = mutableListOf<String>()
        dataList.forEach { aResultPath ->
            if (Files.exists(Paths.get(aResultPath))) {
                Files.delete(Paths.get(aResultPath))
                toBeRemoved.add(aResultPath)
            }
            logger.info("\"$aResultPath\"")
        }
        dataList.removeAll(toBeRemoved)
    }
}
