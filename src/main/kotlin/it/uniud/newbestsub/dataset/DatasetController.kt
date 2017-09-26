package it.uniud.newbestsub.dataset

import com.opencsv.CSVReader
import com.opencsv.CSVWriter
import it.uniud.newbestsub.utils.Constants
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.runBlocking
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
    private var aggregatedDataResultPaths = mutableListOf<String>()
    private var variableValuesResultPaths = mutableListOf<String>()
    private var functionValuesResultPaths = mutableListOf<String>()
    private var topSolutionsResultPaths = mutableListOf<String>()
    private var infoResultPaths = mutableListOf<String>()
    private var logger = LogManager.getLogger()

    init {
        logger.info("Problem resolution started.")
    }

    fun load(datasetPath: String) {

        this.datasetPath = datasetPath

        logger.info("Data set loading started.")
        logger.info("Path: \"$datasetPath\".")

        val outputDirectory = File(Constants.NEWBESTSUB_OUTPUT_PATH)
        logger.info("Checking if ${Constants.NEWBESTSUB_NAME} output dir. exists.")
        if (!outputDirectory.exists()) {
            logger.info("Output dir. not exists.")
            if (outputDirectory.mkdirs()) {
                logger.info("Output dir. created.")
                logger.info("Path: \"${Constants.NEWBESTSUB_OUTPUT_PATH}\".")
            }
        } else {
            logger.info("Output dir. already exists.")
            logger.info("Output dir. creation skipped.")
            logger.info("Path:\"${Constants.NEWBESTSUB_OUTPUT_PATH}\".")
        }
        try {
            models.plusAssign(DatasetModel())
            models[0].loadData(this.datasetPath)
            if (targetToAchieve == Constants.TARGET_ALL) {
                models.plusAssign(DatasetModel())
                models[1].loadData(this.datasetPath)
                models.plusAssign(DatasetModel())
                models[2].loadData(this.datasetPath)
            }
        } catch (exception: FileNotFoundException) {
            logger.warn("Data set not found. Is file inside a \"data\" dir.?")
        } catch (exception: IOException) {
            logger.warn(exception.message as String)
        }

        logger.info("Data set loading for input file \"${models[0].datasetName}\" completed.")
    }

    fun expandTopics(expansionCoefficient: Int) {

        val random = Random()
        val systemLabels = models[0].systemLabels
        val topicLabels = Array(expansionCoefficient, { "${random.nextInt(998 + 1 - 800) + 800} (F)" })
        val randomizedAveragePrecisions = LinkedHashMap<String, DoubleArray>()

        systemLabels.forEach { systemLabel ->
            randomizedAveragePrecisions[systemLabel] = DoubleArray(expansionCoefficient, { Math.random() })
        }
        models.forEach { model -> model.expandTopics(expansionCoefficient, randomizedAveragePrecisions, topicLabels) }
    }

    fun expandSystems(expansionCoefficient: Int, trueNumberOfSystems: Int) {

        val random = Random()
        val systemLabels = Array(expansionCoefficient, { "Sys${random.nextInt(998 + 1 - 800) + 800} (F)" })
        val randomizedAveragePrecisions = LinkedHashMap<String, DoubleArray>()

        systemLabels.forEach { systemLabel ->
            randomizedAveragePrecisions[systemLabel] = DoubleArray(models[0].numberOfTopics + expansionCoefficient, { Math.random() })
        }
        models.forEach { model -> model.expandSystems(expansionCoefficient, trueNumberOfSystems, randomizedAveragePrecisions, systemLabels) }

    }

    fun solve(parameters: Parameters) {

        this.parameters = parameters

        logger.info("Printing common execution parameters.")

        logger.info("Data set name: ${parameters.datasetName}.")
        logger.info("Correlation: ${parameters.correlationMethod}.")
        logger.info("Target: ${parameters.targetToAchieve}.")
        logger.info("Number of iterations: ${parameters.numberOfIterations}. [Experiments: Best, Worst]")
        logger.info("Number of repetitions: ${parameters.numberOfRepetitions}.[Experiment: Average]")
        logger.info("Population size: ${parameters.populationSize}. [Experiments: Best, Worst]")

        if (parameters.currentExecution > 0)
            logger.info("Current Execution: ${parameters.currentExecution}.")

        if (parameters.targetToAchieve == Constants.TARGET_ALL || parameters.targetToAchieve == Constants.TARGET_AVERAGE) {
            var percentilesToFind = ""
            parameters.percentiles.forEach { percentile -> percentilesToFind += "$percentile%, " }
            percentilesToFind = percentilesToFind.substring(0, percentilesToFind.length - 2)
            logger.info("Percentiles: $percentilesToFind. [Experiment: Average]")
        }

        if (parameters.targetToAchieve == Constants.TARGET_ALL) {

            val bestParameters = Parameters(parameters.datasetName, parameters.correlationMethod, Constants.TARGET_BEST, parameters.numberOfIterations, parameters.numberOfRepetitions, parameters.populationSize, parameters.currentExecution, parameters.percentiles)
            val worstParameters = Parameters(parameters.datasetName, parameters.correlationMethod, Constants.TARGET_WORST, parameters.numberOfIterations, parameters.numberOfRepetitions, parameters.populationSize, parameters.currentExecution, parameters.percentiles)
            val averageParameters = Parameters(parameters.datasetName, parameters.correlationMethod, Constants.TARGET_AVERAGE, parameters.numberOfIterations, parameters.numberOfRepetitions, parameters.populationSize, parameters.currentExecution, parameters.percentiles)

            val bestResult = { async(CommonPool) { models[0].solve(bestParameters) } }.invoke()
            val worstResult = { async(CommonPool) { models[1].solve(worstParameters) } }.invoke()
            val averageResult = { async(CommonPool) { models[2].solve(averageParameters) } }.invoke()

            runBlocking {
                view.print(bestResult.await(), models[0])
                view.print(worstResult.await(), models[1])
                view.print(averageResult.await(), models[2])
            }

            aggregatedDataResultPaths.add(models[0].getAggregatedDataFilePath(true))
            models.forEach { model ->
                functionValuesResultPaths.add(model.getFunctionValuesFilePath())
                variableValuesResultPaths.add(model.getVariableValuesFilePath())
                topSolutionsResultPaths.add(model.getTopSolutionsFilePath())
            }
            infoResultPaths.add(models[0].getInfoFilePath(true))

            logger.info("Data aggregation started.")
            view.print(aggregate(models), models[0].getAggregatedDataFilePath(true))
            logger.info("Aggregated data available at:")
            logger.info("\"${models[0].getAggregatedDataFilePath(true)}\"")

            logger.info("Execution information gathering started.")
            view.print(info(models), models[0].getInfoFilePath(true))
            logger.info("Execution information available at:")
            logger.info("\"${models[0].getInfoFilePath(true)}\"")

            logger.info("Execution result paths:")
            models.forEach { model ->
                logger.info("\"${model.getFunctionValuesFilePath()}\" (Function values)")
                logger.info("\"${model.getVariableValuesFilePath()}\" (Variable values)")
                if (model.targetToAchieve != Constants.TARGET_AVERAGE) logger.info("\"${model.getTopSolutionsFilePath()}\" (Top Solutions)")
            }
            logger.info("\"${models[0].getAggregatedDataFilePath(true)}\" (Aggregated data)")
            logger.info("\"${models[0].getInfoFilePath(true)}\" (Info)")

        } else {

            val result = models[0].solve(parameters)

            view.print(result, models[0])

            aggregatedDataResultPaths.add(models[0].getAggregatedDataFilePath(false))
            functionValuesResultPaths.add(models[0].getFunctionValuesFilePath())
            variableValuesResultPaths.add(models[0].getVariableValuesFilePath())
            if (models[0].targetToAchieve != Constants.TARGET_AVERAGE) topSolutionsResultPaths.add(models[0].getTopSolutionsFilePath())
            infoResultPaths.add(models[0].getInfoFilePath(false))

            logger.info("Data aggregation started.")
            view.print(aggregate(models), models[0].getAggregatedDataFilePath(false))
            logger.info("Aggregated data available at:")
            logger.info("\"${models[0].getAggregatedDataFilePath(false)}\"")

            logger.info("Execution information gathering started.")
            view.print(info(models), models[0].getInfoFilePath(false))
            logger.info("Execution information available at:")
            logger.info("\"${models[0].getInfoFilePath(false)}\"")

            logger.info("Execution result paths:")
            logger.info("\"${models[0].getFunctionValuesFilePath()}\" (Function values)")
            logger.info("\"${models[0].getVariableValuesFilePath()}\" (Variable values)")
            if (models[0].targetToAchieve != Constants.TARGET_AVERAGE) logger.info("\"${models[0].getTopSolutionsFilePath()}\" (Top Solutions)")
            logger.info("\"${models[0].getAggregatedDataFilePath(false)}\" (Aggregated data)")
            logger.info("\"${models[0].getInfoFilePath(true)}\" (Info)")

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

        val computedCardinality = mutableMapOf(Constants.TARGET_BEST to 0, Constants.TARGET_WORST to 0, Constants.TARGET_AVERAGE to 0)

        (0..models[0].numberOfTopics - 1).forEach { index ->
            val currentCardinality = (index + 1).toDouble()
            val currentLine = LinkedList<String>()
            currentLine.add(currentCardinality.toString())
            models.forEach { model ->
                val correlationValueForCurrentCardinality = model.findCorrelationForCardinality(currentCardinality)
                if (correlationValueForCurrentCardinality != null) {
                    currentLine.add(correlationValueForCurrentCardinality.toString())
                    computedCardinality[model.targetToAchieve] = computedCardinality[model.targetToAchieve]?.plus(1) ?: 0
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
                    val isTopicInASolutionOfCurrentCard = model.isTopicInASolutionOfCardinality(currentLabel, currentCardinality)
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

        header.add("Data set Name")
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
        val aggregatedDataReaders = Array(numberOfExecutions, { index ->
            logger.info("\"${aggregatedDataResultPaths[index]}\"")
            CSVReader(FileReader(aggregatedDataResultPaths[index]))
        })
        val aggregatedCardinality = LinkedList<LinkedList<Array<String>>>()

        val dataSetup = {
            model : DatasetModel ->
            var executionIndex = 0
            val setIndex = {
                shouldBeIncremented: Boolean ->
                if(shouldBeIncremented) {
                    if (targetToAchieve == Constants.TARGET_ALL) executionIndex += 3 else executionIndex += 1
                } else {
                    when(model.targetToAchieve) {
                        Constants.TARGET_BEST -> if (targetToAchieve == Constants.TARGET_ALL) executionIndex = -3 else executionIndex = -1
                        Constants.TARGET_WORST -> if (targetToAchieve == Constants.TARGET_ALL) executionIndex = -2 else executionIndex = -1
                        Constants.TARGET_AVERAGE -> executionIndex = -1
                    }
                }
            }
            setIndex(false)
            logger.info("Loading function values for experiment \"${model.targetToAchieve}\" for all executions.")
            logger.info("Function values paths:")
            val functionValuesReaders = Array(numberOfExecutions, { _ ->
                setIndex(true)
                logger.info("\"${functionValuesResultPaths[executionIndex]}\"")
                Files.newBufferedReader(Paths.get(functionValuesResultPaths[executionIndex]))
            })
            setIndex(false)
            val functionValues : LinkedList<LinkedList<String>> = LinkedList()
            logger.info("Loading variable values for experiment \"${model.targetToAchieve}\" for all executions.")
            logger.info("Variable values paths:")
            val variableValuesReaders = Array(numberOfExecutions, { _ ->
                setIndex(true)
                logger.info("\"${variableValuesResultPaths[executionIndex]}\"")
                Files.newBufferedReader(Paths.get(variableValuesResultPaths[executionIndex]))
            })
            val variableValues : LinkedList<LinkedList<String>> = LinkedList()
            var topSolutionsReaders = emptyArray<BufferedReader>()
            var topSolutions = LinkedList<LinkedList<String>>()
            if(model.targetToAchieve != Constants.TARGET_AVERAGE) {
                setIndex(false)
                logger.info("Loading top solutions for experiment \"${model.targetToAchieve}\" for all executions.")
                logger.info("Top solutions paths:")
                topSolutionsReaders = Array(numberOfExecutions, { _ ->
                    setIndex(true)
                    logger.info("\"${topSolutionsResultPaths[executionIndex]}\"")
                    Files.newBufferedReader(Paths.get(topSolutionsResultPaths[executionIndex]))
                })
                topSolutions = LinkedList()
            }
            when(model.targetToAchieve) {
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
            dataSetup(models[0])
            dataSetup(models[1])
            dataSetup(models[2])
        } else dataSetup(models[0])

        logger.info("Loading info for all executions.")
        logger.info("Info paths:")

        val infoReaders = Array(numberOfExecutions, { index ->
            logger.info("\"${infoResultPaths[index]}\"")
            Files.newBufferedReader(Paths.get(infoResultPaths[index]))
        })
        val info = LinkedList<LinkedList<String>>()

        while (readCounter < models[0].numberOfTopics + 1) {
            val currentAggregatedCardinality = LinkedList<Array<String>>()
            aggregatedDataReaders.forEach { anAggregatedDataReader -> currentAggregatedCardinality.plusAssign(anAggregatedDataReader.readNext()) }
            aggregatedCardinality.add(currentAggregatedCardinality)
            readCounter++
        }
        aggregatedDataReaders.forEach(CSVReader::close)

        val dataReader = {
            model : DatasetModel ->
            var functionValuesReaders = emptyArray<BufferedReader>()
            var variableValuesReaders = emptyArray<BufferedReader>()
            var topSolutionsReaders = emptyArray<BufferedReader>()
            val functionValues = LinkedList<LinkedList<String>>()
            val variableValues = LinkedList<LinkedList<String>>()
            val topSolutions = LinkedList<LinkedList<String>>()
            when(model.targetToAchieve) {
                Constants.TARGET_BEST -> {
                    functionValuesReaders = bestFunctionValuesReaders
                    variableValuesReaders = bestVariableValuesReaders
                    topSolutionsReaders = bestTopSolutionsReaders
                }
                Constants.TARGET_WORST -> {
                    functionValuesReaders = worstFunctionValuesReaders
                    variableValuesReaders = worstVariableValuesReaders
                    topSolutionsReaders = worstTopSolutionsReaders
                }
                Constants.TARGET_AVERAGE -> {
                    functionValuesReaders = averageFunctionValuesReaders
                    variableValuesReaders = averageVariableValuesReaders
                }
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
            if(model.targetToAchieve != Constants.TARGET_AVERAGE) {
                readCounter = 0
                while (readCounter < model.topSolutions.size) {
                    val currentTopSolution = LinkedList<String>()
                    topSolutionsReaders.forEach { aTopSolutionReader -> currentTopSolution.plusAssign(aTopSolutionReader.readLine()) }
                    topSolutions.add(currentTopSolution)
                    readCounter++
                }
                topSolutionsReaders.forEach(BufferedReader::close)
            }
            when(model.targetToAchieve) {
                Constants.TARGET_BEST -> {
                    bestFunctionValues = functionValues
                    bestVariableValues = variableValues
                    bestTopSolutions = topSolutions
                }
                Constants.TARGET_WORST -> {
                    worstFunctionValues = functionValues
                    worstVariableValues = variableValues
                    worstTopSolutions = topSolutions
                }
                Constants.TARGET_AVERAGE -> {
                    averageFunctionValues = functionValues
                    averageVariableValues = variableValues
                }
            }
        }

        if(targetToAchieve == Constants.TARGET_ALL) {
            dataReader(models[0])
            dataReader(models[1])
            dataReader(models[2])
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
            Constants.TARGET_BEST -> {
                topSolutionHeader = bestTopSolutions.pop().pop()
                mergedBestTopSolutions.add(topSolutionHeader)
            }
            Constants.TARGET_WORST -> {
                topSolutionHeader = worstTopSolutions.pop().pop()
                mergedWorstTopSolutions.add(topSolutionHeader)
            }
            Constants.TARGET_ALL -> {
                topSolutionHeader = bestTopSolutions.pop().pop()
                mergedBestTopSolutions.add(topSolutionHeader)
                mergedWorstTopSolutions.add(topSolutionHeader)
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
                val minimumValue: Double
                if (targetToAchieve == Constants.TARGET_ALL)
                    minimumValue = Math.min(worstAggregatedCorrelation, aggregatedCardinalityForAnExecution[2].toDouble())
                else
                    minimumValue = Math.min(worstAggregatedCorrelation, aggregatedCardinalityForAnExecution[1].toDouble())
                if (worstAggregatedCorrelation > minimumValue) {
                    worstAggregatedCorrelation = minimumValue
                    worstAggregatedCorrelationIndex = anotherIndex
                }
            }
            val mergedDataAggregatedForCurrentAggregatedCardinality = Array(currentAggregatedCardinality[0].size, { "" })
            mergedDataAggregatedForCurrentAggregatedCardinality[0] = currentAggregatedCardinality[0][0]
            when (targetToAchieve) {
                Constants.TARGET_BEST -> mergedDataAggregatedForCurrentAggregatedCardinality[1] = bestAggregatedCorrelation.toString()
                Constants.TARGET_WORST -> mergedDataAggregatedForCurrentAggregatedCardinality[1] = worstAggregatedCorrelation.toString()
            }
            val lowerBound: Int
            val upperBound = currentAggregatedCardinality[0].size - 1
            when (targetToAchieve) {
                Constants.TARGET_ALL -> {
                    mergedDataAggregatedForCurrentAggregatedCardinality[1] = bestAggregatedCorrelation.toString()
                    mergedDataAggregatedForCurrentAggregatedCardinality[2] = worstAggregatedCorrelation.toString()
                    lowerBound = 3
                }
                Constants.TARGET_AVERAGE -> {
                    lowerBound = 1
                }
                else -> {
                    lowerBound = 2
                }
            }
            (lowerBound..upperBound).forEach { anotherIndex ->
                mergedDataAggregatedForCurrentAggregatedCardinality[anotherIndex] = currentAggregatedCardinality[0][anotherIndex]
            }
            mergedAggregatedData.add(mergedDataAggregatedForCurrentAggregatedCardinality)
            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_BEST) {
                mergedBestFunctionValues.add(bestFunctionValues[index][bestAggregatedCorrelationIndex])
                mergedBestVariableValues.add(bestVariableValues[index][bestAggregatedCorrelationIndex])
                bestTopSolutions.forEach { aBestTopSolution ->
                    val bestTopSolution = aBestTopSolution[bestAggregatedCorrelationIndex]
                    var bestTopSolutionCardinality = ""
                    try {
                        bestTopSolutionCardinality = bestTopSolution.split(',')[0]
                    } catch (exception: IllegalArgumentException) {
                    }
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
                    try {
                        worstTopSolutionCardinality = worstTopSolution.split(',')[0]
                    } catch (exception: IllegalArgumentException) {
                    }
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

        val mergedAggregatedDataWriter: CSVWriter
        if (targetToAchieve == Constants.TARGET_ALL) {
            logger.info("Merged aggregated data for all executions available at:")
            logger.info("\"${models[0].getAggregatedDataMergedFilePath(true)}\"")
            mergedAggregatedDataWriter = CSVWriter(FileWriter(models[0].getAggregatedDataMergedFilePath(true)))
        } else {
            logger.info("Merged aggregated data for all executions available at:")
            logger.info("\"${models[0].getAggregatedDataMergedFilePath(false)}\"")
            mergedAggregatedDataWriter = CSVWriter(FileWriter(models[0].getAggregatedDataMergedFilePath(false)))
        }
        mergedAggregatedDataWriter.writeAll(mergedAggregatedData)
        mergedAggregatedDataWriter.close()

        val dataWriter = {
            model : DatasetModel ->
            logger.info("Merged function values for experiment \"${model.targetToAchieve}\" for all executions available at:")
            logger.info("\"${model.getFunctionValuesMergedFilePath()}\"")
            var mergedFunctionValues = LinkedList<String>()
            when(model.targetToAchieve) {
                Constants.TARGET_BEST -> mergedFunctionValues = mergedBestFunctionValues
                Constants.TARGET_WORST -> mergedFunctionValues = mergedWorstFunctionValues
                Constants.TARGET_AVERAGE -> mergedFunctionValues = mergedAverageFunctionValues
            }
            val functionValuesDataWriter: BufferedWriter = Files.newBufferedWriter(Paths.get(model.getFunctionValuesMergedFilePath()))
            mergedFunctionValues.forEach { aMergedFunctionValues ->
                functionValuesDataWriter.write(aMergedFunctionValues)
                functionValuesDataWriter.newLine()
            }
            functionValuesDataWriter.close()
            logger.info("Merged variable values for experiment \"${model.targetToAchieve}\" for all executions available at:")
            logger.info("\"${model.getVariableValuesMergedFilePath()}\"")
            var mergedVariableValues = LinkedList<String>()
            when(model.targetToAchieve) {
                Constants.TARGET_BEST -> mergedVariableValues = mergedBestVariableValues
                Constants.TARGET_WORST -> mergedVariableValues = mergedWorstVariableValues
                Constants.TARGET_AVERAGE -> mergedVariableValues = mergedAverageVariableValues
            }
            val variableValuesDataWriter: BufferedWriter = Files.newBufferedWriter(Paths.get(model.getVariableValuesMergedFilePath()))
            mergedVariableValues.forEach { aMergedVariableValues ->
                variableValuesDataWriter.write(aMergedVariableValues)
                variableValuesDataWriter.newLine()
            }
            variableValuesDataWriter.close()
            if(model.targetToAchieve != Constants.TARGET_AVERAGE) {
                logger.info("Merged top solutions for experiment \"${model.targetToAchieve}\" for all executions available at:")
                logger.info("\"${model.getTopSolutionsMergedFilePath()}\"")
                var mergedTopSolutions = LinkedList<String>()
                when(model.targetToAchieve) {
                    Constants.TARGET_BEST -> mergedTopSolutions = mergedBestTopSolutions
                    Constants.TARGET_WORST -> mergedTopSolutions = mergedWorstTopSolutions
                }
                val topSolutionsDataWriter: BufferedWriter = Files.newBufferedWriter(Paths.get(model.getTopSolutionsMergedFilePath()))
                mergedTopSolutions.forEach { aMergedTopSolution ->
                    topSolutionsDataWriter.write(aMergedTopSolution)
                    topSolutionsDataWriter.newLine()
                }
                topSolutionsDataWriter.close()
            }
        }

        if(targetToAchieve == Constants.TARGET_ALL) {
            dataWriter(models[0])
            dataWriter(models[1])
            dataWriter(models[2])
        } else dataWriter(models[0])

        logger.info("Merged info for all executions available at:")
        val infoValuesWriter: BufferedWriter
        if (targetToAchieve == Constants.TARGET_ALL) {
            infoValuesWriter = Files.newBufferedWriter(Paths.get(models[0].getInfoMergedFilePath(true)))
            logger.info("\"${models[0].getInfoMergedFilePath(true)}\"")
        } else {
            infoValuesWriter = Files.newBufferedWriter(Paths.get(models[0].getInfoMergedFilePath(false)))
            logger.info("\"${models[0].getInfoMergedFilePath(false)}\"")
        }
        mergedInfo.forEach { aMergedInfo ->
            infoValuesWriter.write(aMergedInfo)
            infoValuesWriter.newLine()
        }
        infoValuesWriter.close()

        logger.info("Cleaning of not merged results for all executions started.")

        val dataCleaner = { dataList: MutableList<String>, logMessage: String ->
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

        dataCleaner(aggregatedDataResultPaths, "Cleaning aggregated data at paths:")
        dataCleaner(functionValuesResultPaths, "Cleaning function values at paths:")
        dataCleaner(variableValuesResultPaths, "Cleaning variable values at paths:")
        dataCleaner(topSolutionsResultPaths, "Cleaning top solutions at paths:")
        dataCleaner(infoResultPaths, "Cleaning info at paths:")

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
            if (dataList.size > 0) logger.info(logMessage)
            val outputPaths = mutableListOf<String>()
            dataList.forEach { aResultPath ->
                if (Files.exists(Paths.get(aResultPath))) {
                    logger.info("\"$aResultPath\"")
                    Files.copy(Paths.get(aResultPath), Paths.get("$inputPath${Constants.PATH_SEPARATOR}${Paths.get(aResultPath).fileName}"), StandardCopyOption.REPLACE_EXISTING)
                    outputPaths.add(Paths.get(aResultPath).toString())
                }
            }
            if (outputPaths.size > 0) logger.info("To paths: ")
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
                Files.copy(Paths.get(dataPath), Paths.get("$inputPath${Constants.PATH_SEPARATOR}${Paths.get(dataPath).fileName}"), StandardCopyOption.REPLACE_EXISTING)
                logger.info("To path: ")
                logger.info(Paths.get("$inputPath${Constants.PATH_SEPARATOR}${Paths.get(dataPath).fileName}"))
            }
        }

        models.forEach { model ->
            val aggregatedDataMergedFilePathTargetAll = model.getAggregatedDataMergedFilePath(true)
            dataMergedCopier(aggregatedDataMergedFilePathTargetAll, "Merged aggregated data copy started from path:")
            val aggregatedDataMergedFilePathTargetSingle = model.getAggregatedDataMergedFilePath(false)
            dataMergedCopier(aggregatedDataMergedFilePathTargetSingle, "Merged aggregated data copy started from path:")
            val functionValuesMergedFilePath = model.getFunctionValuesMergedFilePath()
            dataMergedCopier(functionValuesMergedFilePath, "Merged function values copy started from path:")
            val variableValuesMergedFilePath = model.getVariableValuesMergedFilePath()
            dataMergedCopier(variableValuesMergedFilePath, "Merged variable values copy started from path:")
            val topSolutionsMergedFilePath = model.getTopSolutionsMergedFilePath()
            dataMergedCopier(topSolutionsMergedFilePath, "Merged top solutions copy started from path:")
            val infoMergedFilePathTargetAll = model.getInfoMergedFilePath(true)
            dataMergedCopier(infoMergedFilePathTargetAll, "Merged info copy started from path:")
            val infoMergedFilePathTargetSingle = model.getInfoMergedFilePath(false)
            dataMergedCopier(infoMergedFilePathTargetSingle, "Merged info copy started from path:")
        }

        logger.info("Execution result copy to ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} completed.")

    }

}