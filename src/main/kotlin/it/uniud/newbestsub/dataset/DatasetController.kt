package it.uniud.newbestsub.dataset

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import it.uniud.newbestsub.utils.Constants
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.commons.lang3.StringUtils
import org.apache.logging.log4j.LogManager
import java.io.*
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.util.*
import kotlin.collections.LinkedHashMap

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

    init { logger.info("Problem resolution started.") }

    fun load(datasetPath: String) {

        this.datasetPath = datasetPath

        logger.info("Dataset loading started.")
        logger.info("Path: \"$datasetPath\".")

        /* Legacy NEWBESTSUB_OUTPUT_PATH creation removed – View creates run folder */

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
            var percentilesToFind = ""
            parameters.percentiles.forEach { percentile -> percentilesToFind += "$percentile%, " }
            percentilesToFind = percentilesToFind.substring(0, percentilesToFind.length - 2)
            logger.info("Percentiles: $percentilesToFind. [Experiment: Average]")
        }

        if (parameters.targetToAchieve == Constants.TARGET_ALL) {

            val bestParameters = Parameters(
                parameters.datasetName, parameters.correlationMethod, Constants.TARGET_BEST,
                parameters.numberOfIterations, parameters.numberOfRepetitions,
                parameters.populationSize, parameters.currentExecution, parameters.percentiles
            )
            val worstParameters = bestParameters.copy(targetToAchieve = Constants.TARGET_WORST)
            val averageParameters = bestParameters.copy(targetToAchieve = Constants.TARGET_AVERAGE)

            runBlocking {
                val bestResult = async(Dispatchers.Default) { models[0].solve(bestParameters) }
                val worstResult = async(Dispatchers.Default) { models[1].solve(worstParameters) }
                val averageResult = async(Dispatchers.Default) { models[2].solve(averageParameters) }
                view.print(bestResult.await(), models[0])
                view.print(worstResult.await(), models[1])
                view.print(averageResult.await(), models[2])
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
            view.writeCsv(aggregate(models), view.getAggregatedDataFilePath(models[0], isTargetAll = true))
            logger.info("Aggregated data available at:")
            logger.info("\"${view.getAggregatedDataFilePath(models[0], isTargetAll = true)}\"")

            logger.info("Execution information gathering started.")
            view.writeCsv(info(models), view.getInfoFilePath(models[0], isTargetAll = true))
            logger.info("Execution information available at:")
            logger.info("\"${view.getInfoFilePath(models[0], isTargetAll = true)}\"")

            logger.info("Execution result paths:")
            models.forEach { model ->
                logger.info("\"${view.getFunctionValuesFilePath(model)}\" (Function values)")
                logger.info("\"${view.getVariableValuesFilePath(model)}\" (Variable values)")
                if (model.targetToAchieve != Constants.TARGET_AVERAGE)
                    logger.info("\"${view.getTopSolutionsFilePath(model)}\" (Top Solutions)")
            }
            logger.info("\"${view.getAggregatedDataFilePath(models[0], isTargetAll = true)}\" (Aggregated data)")
            logger.info("\"${view.getInfoFilePath(models[0], isTargetAll = true)}\" (Info)")

        } else {

            val result = models[0].solve(parameters)
            view.print(result, models[0])

            aggregatedDataResultPaths.add(view.getAggregatedDataFilePath(models[0], isTargetAll = false))
            functionValuesResultPaths.add(view.getFunctionValuesFilePath(models[0]))
            variableValuesResultPaths.add(view.getVariableValuesFilePath(models[0]))
            if (models[0].targetToAchieve != Constants.TARGET_AVERAGE)
                topSolutionsResultPaths.add(view.getTopSolutionsFilePath(models[0]))
            infoResultPaths.add(view.getInfoFilePath(models[0], isTargetAll = false))

            logger.info("Data aggregation started.")
            view.writeCsv(aggregate(models), view.getAggregatedDataFilePath(models[0], isTargetAll = false))
            logger.info("Aggregated data available at:")
            logger.info("\"${view.getAggregatedDataFilePath(models[0], isTargetAll = false)}\"")

            logger.info("Execution information gathering started.")
            view.writeCsv(info(models), view.getInfoFilePath(models[0], isTargetAll = false))
            logger.info("Execution information available at:")
            logger.info("\"${view.getInfoFilePath(models[0], isTargetAll = false)}\"")

            logger.info("Execution result paths:")
            logger.info("\"${view.getFunctionValuesFilePath(models[0])}\" (Function values)")
            logger.info("\"${view.getVariableValuesFilePath(models[0])}\" (Variable values)")
            if (models[0].targetToAchieve != Constants.TARGET_AVERAGE)
                logger.info("\"${view.getTopSolutionsFilePath(models[0])}\" (Top Solutions)")
            logger.info("\"${view.getAggregatedDataFilePath(models[0], isTargetAll = false)}\" (Aggregated data)")
            logger.info("\"${view.getInfoFilePath(models[0], isTargetAll = false)}\" (Info)")
        }

        logger.info("Execution information gathering completed.")
        logger.info("Data aggregation completed.")
        logger.info("Problem resolution completed.")
    }

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
            topicLabels.forEach { currentLabel ->
                var topicPresence = ""
                models.forEach { model ->
                    val isTopicInASolutionOfCurrentCard =
                        model.isTopicInASolutionOfCardinality(currentLabel, currentCardinality)
                    when (model.targetToAchieve) {
                        Constants.TARGET_BEST -> if (isTopicInASolutionOfCurrentCard) topicPresence += "B"
                        Constants.TARGET_WORST -> if (isTopicInASolutionOfCurrentCard) topicPresence += "W"
                    }
                }
                if (topicPresence == "") topicPresence += "N"
                newDataEntry = newDataEntry.plus(topicPresence)
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

        var bestFunctionValuesReaders = emptyArray<BufferedReader>()
        var bestFunctionValues = LinkedList<LinkedList<String>>()
        var bestVariableValuesReaders = emptyArray<BufferedReader>()
        var bestVariableValues = LinkedList<LinkedList<String>>()
        var bestTopSolutionsReaders = emptyArray<BufferedReader>()
        var bestTopSolutions = LinkedList<LinkedList<String>>()
        var worstFunctionValuesReaders = emptyArray<BufferedReader>()
        var worstFunctionValues = LinkedList<LinkedList<String>>()
        var worstVariableValuesReaders = emptyArray<BufferedReader>()
        var worstVariableValues = LinkedList<LinkedList<String>>()
        var worstTopSolutionsReaders = emptyArray<BufferedReader>()
        var worstTopSolutions = LinkedList<LinkedList<String>>()
        var averageFunctionValuesReaders = emptyArray<BufferedReader>()
        var averageFunctionValues = LinkedList<LinkedList<String>>()
        var averageVariableValuesReaders = emptyArray<BufferedReader>()
        var averageVariableValues = LinkedList<LinkedList<String>>()
        var readCounter = 0
        val mergedBestFunctionValues = LinkedList<String>()
        val mergedBestVariableValues = LinkedList<String>()
        val mergedWorstFunctionValues = LinkedList<String>()
        val mergedWorstVariableValues = LinkedList<String>()
        val mergedAverageFunctionValues = LinkedList<String>()
        val mergedAverageVariableValues = LinkedList<String>()
        val mergedBestTopSolutions = LinkedList<String>()
        val mergedWorstTopSolutions = LinkedList<String>()

        logger.info("Loading aggregated data for all executions.")
        logger.info("Aggregated data paths:")
        val aggregatedDataReaders = Array(numberOfExecutions) { index ->
            logger.info("\"${aggregatedDataResultPaths[index]}\"")
            CSVReader(FileReader(aggregatedDataResultPaths[index]))
        }
        val aggregatedCardinality = LinkedList<LinkedList<Array<String>>>()

        val dataSetup = { model: DatasetModel ->
            var executionIndex = 0
            val setIndex = { shouldBeIncremented: Boolean, isTopSolutionsScan: Boolean ->
                if (shouldBeIncremented) {
                    if (isTopSolutionsScan)
                        if (targetToAchieve == Constants.TARGET_ALL) executionIndex += 2 else executionIndex += 1
                    else
                        if (targetToAchieve == Constants.TARGET_ALL) executionIndex += 3 else executionIndex += 1
                } else {
                    if (isTopSolutionsScan) {
                        when (model.targetToAchieve) {
                            Constants.TARGET_BEST -> if (targetToAchieve == Constants.TARGET_ALL) executionIndex = -2 else executionIndex = -1
                            Constants.TARGET_WORST -> if (targetToAchieve == Constants.TARGET_ALL) executionIndex = -1
                        }
                    } else {
                        when (model.targetToAchieve) {
                            Constants.TARGET_BEST -> if (targetToAchieve == Constants.TARGET_ALL) executionIndex = -3 else executionIndex = -1
                            Constants.TARGET_WORST -> if (targetToAchieve == Constants.TARGET_ALL) executionIndex = -2 else executionIndex = -1
                            Constants.TARGET_AVERAGE -> executionIndex = -1
                        }
                    }
                }
            }
            setIndex(false, false)
            logger.info("Loading function values for experiment \"${model.targetToAchieve}\" for all executions.")
            logger.info("Function values paths:")
            val functionValuesReaders = Array(numberOfExecutions) {
                setIndex(true, false)
                logger.info("\"${functionValuesResultPaths[executionIndex]}\"")
                Files.newBufferedReader(Paths.get(functionValuesResultPaths[executionIndex]))
            }
            setIndex(false, false)
            val functionValues: LinkedList<LinkedList<String>> = LinkedList()
            logger.info("Loading variable values for experiment \"${model.targetToAchieve}\" for all executions.")
            logger.info("Variable values paths:")
            val variableValuesReaders = Array(numberOfExecutions) {
                setIndex(true, false)
                logger.info("\"${variableValuesResultPaths[executionIndex]}\"")
                Files.newBufferedReader(Paths.get(variableValuesResultPaths[executionIndex]))
            }
            val variableValues: LinkedList<LinkedList<String>> = LinkedList()
            var topSolutionsReaders = emptyArray<BufferedReader>()
            var topSolutions = LinkedList<LinkedList<String>>()
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                setIndex(false, true)
                logger.info("Loading top solutions for experiment \"${model.targetToAchieve}\" for all executions.")
                logger.info("Top solutions paths:")
                topSolutionsReaders = Array(numberOfExecutions) {
                    setIndex(true, true)
                    logger.info("\"${topSolutionsResultPaths[executionIndex]}\"")
                    Files.newBufferedReader(Paths.get(topSolutionsResultPaths[executionIndex]))
                }
                topSolutions = LinkedList()
            }
            when (model.targetToAchieve) {
                Constants.TARGET_BEST -> {
                    bestFunctionValuesReaders = functionValuesReaders
                    bestFunctionValues = functionValues
                    bestVariableValuesReaders = variableValuesReaders
                    bestVariableValues = variableValues
                    bestTopSolutionsReaders = topSolutionsReaders
                    bestTopSolutions = topSolutions
                }
                Constants.TARGET_WORST -> {
                    worstFunctionValuesReaders = functionValuesReaders
                    worstFunctionValues = functionValues
                    worstVariableValuesReaders = variableValuesReaders
                    worstVariableValues = variableValues
                    worstTopSolutionsReaders = topSolutionsReaders
                    worstTopSolutions = topSolutions
                }
                Constants.TARGET_AVERAGE -> {
                    averageFunctionValuesReaders = functionValuesReaders
                    averageFunctionValues = functionValues
                    averageVariableValuesReaders = variableValuesReaders
                    averageVariableValues = variableValues
                }
            }
        }

        if (targetToAchieve == Constants.TARGET_ALL) {
            dataSetup(models[0]); dataSetup(models[1]); dataSetup(models[2])
        } else dataSetup(models[0])

        logger.info("Loading info for all executions.")
        logger.info("Info paths:")
        val infoReaders = Array(numberOfExecutions) { index ->
            logger.info("\"${infoResultPaths[index]}\"")
            Files.newBufferedReader(Paths.get(infoResultPaths[index]))
        }
        val info = LinkedList<LinkedList<String>>()

        while (readCounter < models[0].numberOfTopics + 1) {
            val currentAggregatedCardinality = LinkedList<Array<String>>()
            aggregatedDataReaders.forEach { anAggregatedDataReader ->
                currentAggregatedCardinality.plusAssign(anAggregatedDataReader.readNext())
            }
            aggregatedCardinality.add(currentAggregatedCardinality)
            readCounter++
        }
        aggregatedDataReaders.forEach(CSVReader::close)

        val dataReader = { model: DatasetModel ->
            var functionValuesReaders = emptyArray<BufferedReader>()
            var variableValuesReaders = emptyArray<BufferedReader>()
            var topSolutionsReaders = emptyArray<BufferedReader>()
            val functionValues = LinkedList<LinkedList<String>>()
            val variableValues = LinkedList<LinkedList<String>>()
            val topSolutions = LinkedList<LinkedList<String>>()
            when (model.targetToAchieve) {
                Constants.TARGET_BEST -> { functionValuesReaders = bestFunctionValuesReaders; variableValuesReaders = bestVariableValuesReaders; topSolutionsReaders = bestTopSolutionsReaders }
                Constants.TARGET_WORST -> { functionValuesReaders = worstFunctionValuesReaders; variableValuesReaders = worstVariableValuesReaders; topSolutionsReaders = worstTopSolutionsReaders }
                Constants.TARGET_AVERAGE -> { functionValuesReaders = averageFunctionValuesReaders; variableValuesReaders = averageVariableValuesReaders }
            }
            readCounter = 0
            while (readCounter < model.numberOfTopics) {
                val currentFunctionValue = LinkedList<String>()
                functionValuesReaders.forEach { aFunctionValuesReader -> currentFunctionValue.plusAssign(aFunctionValuesReader.readLine()) }
                functionValues.add(currentFunctionValue)
                readCounter++
            }
            functionValuesReaders.forEach(BufferedReader::close)
            readCounter = 0
            while (readCounter < model.numberOfTopics) {
                val currentVariableValue = LinkedList<String>()
                variableValuesReaders.forEach { aVariableValuesReader -> currentVariableValue.plusAssign(aVariableValuesReader.readLine()) }
                variableValues.add(currentVariableValue)
                readCounter++
            }
            variableValuesReaders.forEach(BufferedReader::close)
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                readCounter = 0
                while (readCounter < model.topSolutions.size) {
                    val currentTopSolution = LinkedList<String>()
                    topSolutionsReaders.forEach { aTopSolutionReader -> currentTopSolution.plusAssign(aTopSolutionReader.readLine()) }
                    topSolutions.add(currentTopSolution)
                    readCounter++
                }
                topSolutionsReaders.forEach(BufferedReader::close)
            }
            when (model.targetToAchieve) {
                Constants.TARGET_BEST -> { bestFunctionValues = functionValues; bestVariableValues = variableValues; bestTopSolutions = topSolutions }
                Constants.TARGET_WORST -> { worstFunctionValues = functionValues; worstVariableValues = variableValues; worstTopSolutions = topSolutions }
                Constants.TARGET_AVERAGE -> { averageFunctionValues = functionValues; averageVariableValues = variableValues }
            }
        }

        if (targetToAchieve == Constants.TARGET_ALL) {
            dataReader(models[0]); dataReader(models[1]); dataReader(models[2])
        } else dataReader(models[0])

        readCounter = 0
        while (readCounter <= 3) {
            val currentInfo = LinkedList<String>()
            infoReaders.forEach { aInfoReader -> currentInfo.plusAssign(aInfoReader.readLine()) }
            info.add(currentInfo)
            readCounter++
        }
        infoReaders.forEach(BufferedReader::close)

        val aggregatedDataHeader = aggregatedCardinality.pop().pop()
        val mergedAggregatedData = LinkedList<Array<String>>()
        mergedAggregatedData.add(aggregatedDataHeader)

        val topSolutionHeader: String
        when (targetToAchieve) {
            Constants.TARGET_BEST -> { topSolutionHeader = bestTopSolutions.pop().pop(); mergedBestTopSolutions.add(topSolutionHeader) }
            Constants.TARGET_WORST -> { topSolutionHeader = worstTopSolutions.pop().pop(); mergedWorstTopSolutions.add(topSolutionHeader) }
            Constants.TARGET_ALL -> {
                topSolutionHeader = bestTopSolutions.pop().pop()
                mergedBestTopSolutions.add(topSolutionHeader); mergedWorstTopSolutions.add(topSolutionHeader)
            }
        }

        val infoHeader = info.pop().pop()
        val mergedInfo = LinkedList<String>()
        mergedInfo.add(infoHeader)

        val loggingFactor = ((aggregatedCardinality.size + 1) * Constants.LOGGING_FACTOR) / 100
        var progressCounter = 0

        aggregatedCardinality.forEachIndexed { index, currentAggregatedCardinality ->
            if (((index) % loggingFactor) == 0 && (aggregatedCardinality.size + 1) > loggingFactor) {
                logger.info("Results merged for cardinality: ${index + 1}/${aggregatedCardinality.size + 1} ($progressCounter%) for $numberOfExecutions total executions.")
                progressCounter += Constants.LOGGING_FACTOR
            }
            var bestAggregatedCorrelation = -10.0
            var bestAggregatedCorrelationIndex = -10
            var worstAggregatedCorrelation = 10.0
            var worstAggregatedCorrelationIndex = 10
            currentAggregatedCardinality.forEachIndexed { anotherIndex, aggregatedCardinalityForAnExecution ->
                val maximumValue = Math.max(bestAggregatedCorrelation, aggregatedCardinalityForAnExecution[1].toDouble())
                if (bestAggregatedCorrelation < maximumValue) {
                    bestAggregatedCorrelation = maximumValue
                    bestAggregatedCorrelationIndex = anotherIndex
                }
                val minimumValue: Double =
                    if (targetToAchieve == Constants.TARGET_ALL)
                        Math.min(worstAggregatedCorrelation, aggregatedCardinalityForAnExecution[2].toDouble())
                    else
                        Math.min(worstAggregatedCorrelation, aggregatedCardinalityForAnExecution[1].toDouble())
                if (worstAggregatedCorrelation > minimumValue) {
                    worstAggregatedCorrelation = minimumValue
                    worstAggregatedCorrelationIndex = anotherIndex
                }
            }
            val mergedDataAggregatedForCurrentAggregatedCardinality = Array(currentAggregatedCardinality[0].size) { "" }
            mergedDataAggregatedForCurrentAggregatedCardinality[0] = currentAggregatedCardinality[0][0]
            when (targetToAchieve) {
                Constants.TARGET_BEST -> mergedDataAggregatedForCurrentAggregatedCardinality[1] = bestAggregatedCorrelation.toString()
                Constants.TARGET_WORST -> mergedDataAggregatedForCurrentAggregatedCardinality[1] = worstAggregatedCorrelation.toString()
            }
            val lowerBound: Int
            val upperBound = currentAggregatedCardinality[0].size - 1
            when (targetToAchieve) {
                Constants.TARGET_ALL -> { mergedDataAggregatedForCurrentAggregatedCardinality[1] = bestAggregatedCorrelation.toString(); mergedDataAggregatedForCurrentAggregatedCardinality[2] = worstAggregatedCorrelation.toString(); lowerBound = 3 }
                Constants.TARGET_AVERAGE -> { lowerBound = 1 }
                else -> { lowerBound = 2 }
            }
            (lowerBound..upperBound).forEach { anotherIndex ->
                mergedDataAggregatedForCurrentAggregatedCardinality[anotherIndex] =
                    currentAggregatedCardinality[0][anotherIndex]
            }
            mergedAggregatedData.add(mergedDataAggregatedForCurrentAggregatedCardinality)
            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_BEST) {
                mergedBestFunctionValues.add(bestFunctionValues[index][bestAggregatedCorrelationIndex])
                mergedBestVariableValues.add(bestVariableValues[index][bestAggregatedCorrelationIndex])
                bestTopSolutions.forEach { aBestTopSolution ->
                    val bestTopSolution = aBestTopSolution[bestAggregatedCorrelationIndex]
                    var bestTopSolutionCardinality = ""
                    try { bestTopSolutionCardinality = bestTopSolution.split(',')[0] } catch (_: Exception) {}
                    bestTopSolutionCardinality = bestTopSolutionCardinality.drop(1).dropLast(3)
                    if (bestTopSolutionCardinality == (index + 1).toString()) mergedBestTopSolutions.add(aBestTopSolution[bestAggregatedCorrelationIndex])
                }
            }
            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_WORST) {
                mergedWorstFunctionValues.add(worstFunctionValues[index][worstAggregatedCorrelationIndex])
                mergedWorstVariableValues.add(worstVariableValues[index][worstAggregatedCorrelationIndex])
                worstTopSolutions.forEach { aWorstTopSolution ->
                    val worstTopSolution = aWorstTopSolution[worstAggregatedCorrelationIndex]
                    var worstTopSolutionCardinality = ""
                    try { worstTopSolutionCardinality = worstTopSolution.split(',')[0] } catch (_: Exception) {}
                    worstTopSolutionCardinality = worstTopSolutionCardinality.drop(1).dropLast(3)
                    if (worstTopSolutionCardinality == (index + 1).toString()) mergedWorstTopSolutions.add(aWorstTopSolution[worstAggregatedCorrelationIndex])
                }
            }
            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {
                mergedAverageFunctionValues.add(averageFunctionValues[index][0])
                mergedAverageVariableValues.add(averageVariableValues[index][0])
            }
        }

        logger.info("Results merged for cardinality: ${aggregatedCardinality.size + 1}/${aggregatedCardinality.size + 1} (100%) for $numberOfExecutions total executions.")

        var bestComputingTime = 0
        var worstComputingTime = 0
        var averageComputingTime = 0
        val mergedBestExecutionInfo: MutableList<String>
        val mergedWorstExecutionInfo: MutableList<String>
        val mergedAverageExecutionInfo: MutableList<String>

        when (targetToAchieve) {
            Constants.TARGET_ALL -> {
                info[0].forEach { infoForABestExecution -> bestComputingTime += infoForABestExecution.split(",").last().replace("\"", "").toInt() }
                mergedBestExecutionInfo = info[0][0].split(",").toMutableList()
                mergedBestExecutionInfo[mergedBestExecutionInfo.lastIndex] = bestComputingTime.toString()
                mergedInfo.add(StringUtils.join(mergedBestExecutionInfo, ","))

                info[1].forEach { infoForAWorstExecution -> worstComputingTime += infoForAWorstExecution.split(",").last().replace("\"", "").toInt() }
                mergedWorstExecutionInfo = info[1][0].split(",").toMutableList()
                mergedWorstExecutionInfo[mergedWorstExecutionInfo.lastIndex] = worstComputingTime.toString()
                mergedInfo.add(StringUtils.join(mergedWorstExecutionInfo, ","))

                info[2].forEach { infoForAnAverageExecution -> averageComputingTime += infoForAnAverageExecution.split(",").last().replace("\"", "").toInt() }
                mergedAverageExecutionInfo = info[2][0].split(",").toMutableList()
                mergedAverageExecutionInfo[mergedAverageExecutionInfo.lastIndex] = averageComputingTime.toString()
                mergedInfo.add(StringUtils.join(mergedAverageExecutionInfo, ","))
            }
            Constants.TARGET_BEST -> {
                info[0].forEach { infoForABestExecution -> bestComputingTime += infoForABestExecution.split(",").last().replace("\"", "").toInt() }
                mergedBestExecutionInfo = info[0][0].split(",").toMutableList()
                mergedBestExecutionInfo[mergedBestExecutionInfo.lastIndex] = bestComputingTime.toString()
                mergedInfo.add(StringUtils.join(mergedBestExecutionInfo, ","))
            }
            Constants.TARGET_WORST -> {
                info[0].forEach { infoForAWorstExecution -> worstComputingTime += infoForAWorstExecution.split(",").last().replace("\"", "").toInt() }
                mergedWorstExecutionInfo = info[0][0].split(",").toMutableList()
                mergedWorstExecutionInfo[mergedWorstExecutionInfo.lastIndex] = worstComputingTime.toString()
                mergedInfo.add(StringUtils.join(mergedWorstExecutionInfo, ","))
            }
            Constants.TARGET_AVERAGE -> {
                info[0].forEach { infoForAnAverageExecution -> averageComputingTime += infoForAnAverageExecution.split(",").last().replace("\"", "").toInt() }
                mergedAverageExecutionInfo = info[0][0].split(",").toMutableList()
                mergedAverageExecutionInfo[mergedAverageExecutionInfo.lastIndex] = averageComputingTime.toString()
                mergedInfo.add(StringUtils.join(mergedAverageExecutionInfo, ","))
            }
        }
        info[0].forEachIndexed { index, _ -> logger.info("Info merged for execution: ${index + 1}/$numberOfExecutions.") }

        /* Merged outputs now also use the View helpers so they end up in the same run folder */
        val isAll = (targetToAchieve == Constants.TARGET_ALL)
        val mergedAggPath = view.getAggregatedDataMergedFilePath(models[0], isTargetAll = isAll)
        logger.info("Merged aggregated data for all executions available at:")
        logger.info("\"$mergedAggPath\"")
        val mergedAggregatedDataWriter = CSVWriter(FileWriter(mergedAggPath))
        mergedAggregatedDataWriter.writeAll(mergedAggregatedData)
        mergedAggregatedDataWriter.close()

        val dataWriter = { model: DatasetModel ->
            val funMergedPath = view.getFunctionValuesMergedFilePath(model)
            logger.info("Merged function values for experiment \"${model.targetToAchieve}\" for all executions available at:")
            logger.info("\"$funMergedPath\"")
            val mergedFunctionValues = when (model.targetToAchieve) {
                Constants.TARGET_BEST -> mergedBestFunctionValues
                Constants.TARGET_WORST -> mergedWorstFunctionValues
                else -> mergedAverageFunctionValues
            }
            val functionValuesDataWriter: BufferedWriter = Files.newBufferedWriter(Paths.get(funMergedPath))
            mergedFunctionValues.forEach { aMergedFunctionValues ->
                functionValuesDataWriter.write(aMergedFunctionValues)
                functionValuesDataWriter.newLine()
            }
            functionValuesDataWriter.close()

            val varMergedPath = view.getVariableValuesMergedFilePath(model)
            logger.info("Merged variable values for experiment \"${model.targetToAchieve}\" for all executions available at:")
            logger.info("\"$varMergedPath\"")
            val mergedVariableValues = when (model.targetToAchieve) {
                Constants.TARGET_BEST -> mergedBestVariableValues
                Constants.TARGET_WORST -> mergedWorstVariableValues
                else -> mergedAverageVariableValues
            }
            val variableValuesDataWriter: BufferedWriter = Files.newBufferedWriter(Paths.get(varMergedPath))
            mergedVariableValues.forEach { aMergedVariableValues ->
                variableValuesDataWriter.write(aMergedVariableValues)
                variableValuesDataWriter.newLine()
            }
            variableValuesDataWriter.close()

            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                val topMergedPath = view.getTopSolutionsMergedFilePath(model)
                logger.info("Merged top solutions for experiment \"${model.targetToAchieve}\" for all executions available at:")
                logger.info("\"$topMergedPath\"")
                val mergedTopSolutions = when (model.targetToAchieve) {
                    Constants.TARGET_BEST -> mergedBestTopSolutions
                    else -> mergedWorstTopSolutions
                }
                val topSolutionsDataWriter: BufferedWriter = Files.newBufferedWriter(Paths.get(topMergedPath))
                mergedTopSolutions.forEach { aMergedTopSolution ->
                    topSolutionsDataWriter.write(aMergedTopSolution)
                    topSolutionsDataWriter.newLine()
                }
                topSolutionsDataWriter.close()
            }
        }

        if (targetToAchieve == Constants.TARGET_ALL) {
            dataWriter(models[0]); dataWriter(models[1]); dataWriter(models[2])
        } else dataWriter(models[0])

        logger.info("Merged info for all executions available at:")
        val infoMergedPath = view.getInfoMergedFilePath(models[0], isTargetAll = isAll)
        logger.info("\"$infoMergedPath\"")
        val infoValuesWriter: BufferedWriter = Files.newBufferedWriter(Paths.get(infoMergedPath))
        mergedInfo.forEach { aMergedInfo ->
            infoValuesWriter.write(aMergedInfo)
            infoValuesWriter.newLine()
        }
        infoValuesWriter.close()

        logger.info("Cleaning of not merged results for all executions started.")
        clean(aggregatedDataResultPaths, "Cleaning aggregated data at paths:")
        clean(functionValuesResultPaths, "Cleaning function values at paths:")
        clean(variableValuesResultPaths, "Cleaning variable values at paths:")
        clean(topSolutionsResultPaths, "Cleaning top solutions at paths:")
        clean(infoResultPaths, "Cleaning info at paths:")
        logger.info("Cleaning of not merged results for all executions completed.")
        logger.info("Executions result merging completed.")
    }

    fun copy() {

        logger.info("Execution result copy to ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} started.")

        val inputPath = Constants.NEWBESTSUB_EXPERIMENTS_INPUT_PATH
        val inputDirectory = File(Constants.NEWBESTSUB_EXPERIMENTS_INPUT_PATH)

        logger.info("Checking if ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} input dir. exists.")
        if (!inputDirectory.exists()) {
            logger.info("Input dir. not exists.")
            if (inputDirectory.mkdirs()) {
                logger.info("Input dir. created.")
                logger.info("Path: \"$inputPath\".")
            }
        } else {
            logger.info("Input dir. already exists.")
            logger.info("Input dir. creation skipped.")
            logger.info("Path: \"$inputPath\".")
        }

        val dataCopier = { dataList: MutableList<String>, logMessage: String ->
            if (dataList.isNotEmpty()) logger.info(logMessage)
            val outputPaths = mutableListOf<String>()
            dataList.forEach { aResultPath ->
                if (Files.exists(Paths.get(aResultPath))) {
                    logger.info("\"$aResultPath\"")
                    Files.copy(
                        Paths.get(aResultPath),
                        Paths.get("$inputPath${Constants.PATH_SEPARATOR}${Paths.get(aResultPath).fileName}"),
                        StandardCopyOption.REPLACE_EXISTING
                    )
                    outputPaths.add(Paths.get(aResultPath).toString())
                }
            }
            if (outputPaths.isNotEmpty()) logger.info("To paths: ")
            outputPaths.forEach(logger::info)
        }

        dataCopier(aggregatedDataResultPaths, "Aggregated data copy started from paths:")
        dataCopier(functionValuesResultPaths, "Function values copy started from paths:")
        dataCopier(variableValuesResultPaths, "Variable values copy started from paths:")
        dataCopier(topSolutionsResultPaths, "Top solutions copy started from paths:")
        dataCopier(infoResultPaths, "Info copy started from paths:")

        val dataMergedCopier = { dataPath: String, logMessage: String ->
            if (Files.exists(Paths.get(dataPath))) {
                logger.info(logMessage)
                logger.info("\"${Paths.get(dataPath)}\"")
                Files.copy(
                    Paths.get(dataPath),
                    Paths.get("$inputPath${Constants.PATH_SEPARATOR}${Paths.get(dataPath).fileName}"),
                    StandardCopyOption.REPLACE_EXISTING
                )
                logger.info("To path: ")
                logger.info(Paths.get("$inputPath${Constants.PATH_SEPARATOR}${Paths.get(dataPath).fileName}"))
            }
        }

        models.forEach { model ->
            val aggMergedAll = view.getAggregatedDataMergedFilePath(model, isTargetAll = true)
            dataMergedCopier(aggMergedAll, "Merged aggregated data copy started from path:")
            val aggMergedSingle = view.getAggregatedDataMergedFilePath(model, isTargetAll = false)
            dataMergedCopier(aggMergedSingle, "Merged aggregated data copy started from path:")

            val funMerged = view.getFunctionValuesMergedFilePath(model)
            dataMergedCopier(funMerged, "Merged function values copy started from path:")

            val varMerged = view.getVariableValuesMergedFilePath(model)
            dataMergedCopier(varMerged, "Merged variable values copy started from path:")

            val topMerged = view.getTopSolutionsMergedFilePath(model)
            dataMergedCopier(topMerged, "Merged top solutions copy started from path:")

            val infoMergedAll = view.getInfoMergedFilePath(model, isTargetAll = true)
            dataMergedCopier(infoMergedAll, "Merged info copy started from path:")
            val infoMergedSingle = view.getInfoMergedFilePath(model, isTargetAll = false)
            dataMergedCopier(infoMergedSingle, "Merged info copy started from path:")
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
