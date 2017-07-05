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
import java.util.*
import kotlin.collections.LinkedHashMap


class DatasetController(

        private var targetToAchieve: String

) {

    var models = mutableListOf<DatasetModel>()
    private var view = DatasetView()
    private lateinit var parameters: Parameters
    private lateinit var datasetPath: String
    private lateinit var resultPath: String
    private var resultPaths = mutableListOf<String>()
    private var logger = LogManager.getLogger()

    init {
        logger.info("Problem resolution started.")
    }

    fun load(datasetPath: String) {

        this.datasetPath = datasetPath

        logger.info("Data set loading started.")
        logger.info("Path: \"$datasetPath\".")

        val outputDirectory = File(Constants.OUTPUT_PATH)
        logger.info("Checking if output dir. exists.")
        if (!outputDirectory.exists()) {
            logger.info("Output dir. not exists.")
            if (outputDirectory.mkdirs()) {
                logger.info("Output dir. created.")
                logger.info("Path: \"${Constants.OUTPUT_PATH}\".")
            }
        } else {
            logger.info("Output dir. already exists.")
            logger.info("Output dir. creation skipped.")
            logger.info("Path:\"${Constants.OUTPUT_PATH}\".")
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

    fun expand(expansionCoefficient: Int) {

        val random = Random()
        val systemLabels = models[0].systemLabels
        val topicLabels = Array(expansionCoefficient, { "${random.nextInt(998 + 1 - 100) + 100} (F)" })
        val randomizedAveragePrecisions = LinkedHashMap<String, DoubleArray>()

        systemLabels.forEach {
            systemLabel ->
            randomizedAveragePrecisions[systemLabel] = DoubleArray(expansionCoefficient, { Math.random() })
        }
        models.forEach { model -> model.expandData(randomizedAveragePrecisions, topicLabels) }
    }

    fun solve(parameters: Parameters, resultPath: String) {

        this.parameters = parameters
        this.resultPath = resultPath
        this.resultPaths.plusAssign(resultPath)

        logger.info("Printing common execution parameters.")

        logger.info("Data set name: ${parameters.datasetName}.")
        logger.info("Correlation: ${parameters.correlationMethod}.")
        logger.info("Target: ${parameters.targetToAchieve}.")
        logger.info("Number of iterations: ${parameters.numberOfIterations}.")
        logger.info("Number of repetitions: ${parameters.numberOfRepetitions}.")
        logger.info("Population size: ${parameters.populationSize}.")

        if (parameters.targetToAchieve == Constants.TARGET_ALL || parameters.targetToAchieve == Constants.TARGET_AVERAGE) {
            var percentilesToFind = ""
            parameters.percentiles.forEach { percentile -> percentilesToFind += "$percentile%, " }
            percentilesToFind = percentilesToFind.substring(0, percentilesToFind.length - 2)
            logger.info("Percentiles: $percentilesToFind.")
        }

        logger.info("Output path:")

        if (parameters.targetToAchieve == Constants.TARGET_ALL) {

            logger.info("\"${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_ALL}-Final.csv\" (Aggregated data)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_BEST}-Fun.csv\" (Target function values)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_BEST}-Var.csv\" (Variable values)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_WORST}-Fun.csv\" (Target function values)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_WORST}-Var.csv\" (Variable values)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_AVERAGE}-Fun.csv\" (Target function values)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_AVERAGE}-Var.csv\" (Variable values)")

            val bestParameters = Parameters(parameters.datasetName, parameters.correlationMethod, Constants.TARGET_BEST, parameters.numberOfIterations, parameters.numberOfRepetitions, parameters.populationSize, parameters.percentiles)
            val worstParameters = Parameters(parameters.datasetName, parameters.correlationMethod, Constants.TARGET_WORST, parameters.numberOfIterations, parameters.numberOfRepetitions, parameters.populationSize, parameters.percentiles)
            val averageParameters = Parameters(parameters.datasetName, parameters.correlationMethod, Constants.TARGET_AVERAGE, parameters.numberOfIterations, parameters.numberOfRepetitions, parameters.populationSize, parameters.percentiles)
            val bestResult = { async(CommonPool) { models[0].solve(bestParameters) } }.invoke()
            val worstResult = { async(CommonPool) { models[1].solve(worstParameters) } }.invoke()
            val averageResult = { async(CommonPool) { models[2].solve(averageParameters) } }.invoke()

            runBlocking {
                view.print(bestResult.await(), resultPath + "${Constants.TARGET_BEST}-")
                view.print(worstResult.await(), resultPath + "${Constants.TARGET_WORST}-")
                view.print(averageResult.await(), resultPath + "${Constants.TARGET_AVERAGE}-")
            }

            logger.info("Data aggregation started.")
            view.print(aggregate(models), "${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_ALL}-Final.csv")
            logger.info("Aggregated data available at:")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_ALL}-Final.csv\"")
            logger.info("Execution information gathering started.")
            view.print(info(models), "${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_ALL}-Info.csv")
            logger.info("Execution information available at:")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_ALL}-Info.csv\"")


        } else {

            logger.info("\"${Constants.OUTPUT_PATH}${resultPath}Final.csv\" (Aggregated data)")
            logger.info("\"${Constants.OUTPUT_PATH}${resultPath}Fun.csv\" (Target function values)")
            logger.info("\"${Constants.OUTPUT_PATH}${resultPath}Var.csv\" (Variable values)")

            view.print(models[0].solve(parameters), resultPath)

            logger.info("Data aggregation started.")
            view.print(aggregate(models), "${Constants.OUTPUT_PATH}${resultPath}Final.csv")
            logger.info("Aggregated data available at:")
            logger.info("\"${Constants.OUTPUT_PATH}${resultPath}Final.csv\"")
            logger.info("Execution information gathering started.")
            view.print(info(models), "${Constants.OUTPUT_PATH}${resultPath}Info.csv")
            logger.info("Execution information available at:")
            logger.info("\"${Constants.OUTPUT_PATH}${resultPath}Info.csv\"")

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
        models.forEach {
            model ->
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

        (0..models[0].numberOfTopics - 1).forEach {
            index ->
            val currentCardinality = (index + 1).toDouble()
            val currentLine = LinkedList<String>()
            currentLine.add(currentCardinality.toString())
            models.forEach {
                model ->
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

        incompleteData.forEach {
            aLine ->
            var newDataEntry = aLine
            val currentCardinality = newDataEntry[0].toDouble()

            percentiles.entries.forEach {
                (_, percentileValues) ->
                newDataEntry = newDataEntry.plus(percentileValues[currentCardinality.toInt() - 1].toString())
            }

            topicLabels.forEach {
                currentLabel ->
                var topicPresence = ""
                models.forEach {
                    model ->
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
        models.forEach {
            model ->
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

    fun merge() {

        var bestFunctionValuesReaders = emptyArray<BufferedReader>()
        var bestFunctionValues = LinkedList<LinkedList<String>>()
        var bestVariableValuesReaders = emptyArray<BufferedReader>()
        var bestVariableValues = LinkedList<LinkedList<String>>()
        var worstFunctionValuesReaders = emptyArray<BufferedReader>()
        var worstFunctionValues = LinkedList<LinkedList<String>>()
        var worstVariableValuesReaders = emptyArray<BufferedReader>()
        var worstVariableValues = LinkedList<LinkedList<String>>()
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

        val aggregatedDataReaders = Array(resultPaths.size, {
            index ->
            if (targetToAchieve == Constants.TARGET_ALL)
                CSVReader(FileReader("${Constants.OUTPUT_PATH}${resultPaths[index]}${Constants.TARGET_ALL}-Final.csv"))
            else
                CSVReader(FileReader("${Constants.OUTPUT_PATH}${resultPaths[index]}Final.csv"))
        })
        val aggregatedCardinality = LinkedList<LinkedList<Array<String>>>()

        if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_BEST) {
            bestFunctionValuesReaders = Array(resultPaths.size, {
                index ->
                if (targetToAchieve == Constants.TARGET_ALL)
                    Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Best-Fun.csv"))
                else
                    Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Fun.csv"))
            })
            bestFunctionValues = LinkedList()
            bestVariableValuesReaders = Array(resultPaths.size, {
                index ->
                if (targetToAchieve == Constants.TARGET_ALL)
                    Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Best-Var.csv"))
                else
                    Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Var.csv"))
            })
            bestVariableValues = LinkedList()
        }

        if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_WORST) {
            worstFunctionValuesReaders = Array(resultPaths.size, {
                index ->
                if (targetToAchieve == Constants.TARGET_ALL)
                    Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Worst-Fun.csv"))
                else
                    Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Fun.csv"))
            })
            worstFunctionValues = LinkedList()
            worstVariableValuesReaders = Array(resultPaths.size, {
                index ->
                if (targetToAchieve == Constants.TARGET_ALL)
                    Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Worst-Var.csv"))
                else
                    Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Var.csv"))
            })
            worstVariableValues = LinkedList()
        }

        if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {
            averageFunctionValuesReaders = Array(resultPaths.size, {
                index ->
                if (targetToAchieve == Constants.TARGET_ALL)
                    Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Average-Fun.csv"))
                else
                    Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Fun.csv"))
            })
            averageFunctionValues = LinkedList()
            averageVariableValuesReaders = Array(resultPaths.size, {
                index ->
                if (targetToAchieve == Constants.TARGET_ALL)
                    Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Average-Var.csv"))
                else
                    Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Var.csv"))
            })
            averageVariableValues = LinkedList()
        }

        val infoReaders = Array(resultPaths.size, {
            index ->
            if (targetToAchieve == Constants.TARGET_ALL)
                Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}${Constants.TARGET_ALL}-Info.csv"))
            else
                Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Info.csv"))
        })
        val info = LinkedList<LinkedList<String>>()

        while (readCounter < models[0].numberOfTopics) {
            val currentAggregatedCardinality = LinkedList<Array<String>>()
            aggregatedDataReaders.forEach {
                anAggregatedDataReader ->
                currentAggregatedCardinality.plusAssign(anAggregatedDataReader.readNext())
            }
            aggregatedCardinality.add(currentAggregatedCardinality)
            readCounter++
        }
        aggregatedDataReaders.forEach(CSVReader::close)

        if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_BEST) {
            readCounter = 0
            while (readCounter < models[0].numberOfTopics) {
                val currentBestFunctionValue = LinkedList<String>()
                bestFunctionValuesReaders.forEach {
                    aFunctionValuesReader ->
                    currentBestFunctionValue.plusAssign(aFunctionValuesReader.readLine())
                }
                bestFunctionValues.add(currentBestFunctionValue)
                readCounter++
            }
            bestFunctionValuesReaders.forEach(BufferedReader::close)
            readCounter = 0
            while (readCounter < models[0].numberOfTopics) {
                val currentBestVariableValue = LinkedList<String>()
                bestVariableValuesReaders.forEach {
                    aVariableValuesReader ->
                    currentBestVariableValue.plusAssign(aVariableValuesReader.readLine())
                }
                bestVariableValues.add(currentBestVariableValue)
                readCounter++
            }
            bestVariableValuesReaders.forEach(BufferedReader::close)
        }

        if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_WORST) {
            readCounter = 0
            while (readCounter < models[0].numberOfTopics) {
                val currentWorstFunctionValue = LinkedList<String>()
                worstFunctionValuesReaders.forEach {
                    aFunctionValuesReader ->
                    currentWorstFunctionValue.plusAssign(aFunctionValuesReader.readLine())
                }
                worstFunctionValues.add(currentWorstFunctionValue)
                readCounter++
            }
            worstFunctionValuesReaders.forEach(BufferedReader::close)
            readCounter = 0
            while (readCounter < models[0].numberOfTopics) {
                val currentWorstVariableValue = LinkedList<String>()
                worstVariableValuesReaders.forEach {
                    aVariableValuesReader ->
                    currentWorstVariableValue.plusAssign(aVariableValuesReader.readLine())
                }
                worstVariableValues.add(currentWorstVariableValue)
                readCounter++
            }
            worstVariableValuesReaders.forEach(BufferedReader::close)
        }

        if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {
            readCounter = 0
            while (readCounter < models[0].numberOfTopics) {
                val currentAverageFunctionValue = LinkedList<String>()
                averageFunctionValuesReaders.forEach {
                    aFunctionValuesReader ->
                    currentAverageFunctionValue.plusAssign(aFunctionValuesReader.readLine())
                }
                averageFunctionValues.add(currentAverageFunctionValue)
                readCounter++
            }
            averageFunctionValuesReaders.forEach(BufferedReader::close)
            readCounter = 0
            while (readCounter < models[0].numberOfTopics) {
                val currentAverageVariableValue = LinkedList<String>()
                averageVariableValuesReaders.forEach {
                    aVariableValuesReader ->
                    currentAverageVariableValue.plusAssign(aVariableValuesReader.readLine())
                }
                averageVariableValues.add(currentAverageVariableValue)
                readCounter++
            }
            averageVariableValuesReaders.forEach(BufferedReader::close)
        }

        readCounter = 0
        while (readCounter <= resultPaths.size) {
            val currentInfo = LinkedList<String>()
            infoReaders.forEach {
                aInfoReader ->
                currentInfo.plusAssign(aInfoReader.readLine())
            }
            info.add(currentInfo)
            readCounter++
        }
        infoReaders.forEach(BufferedReader::close)

        val aggregatedDataHeader = aggregatedCardinality.pop().pop()
        val mergedAggregatedData = LinkedList<Array<String>>()
        mergedAggregatedData.add(aggregatedDataHeader)

        val infoHeader = info.pop().pop()
        val mergedInfo = LinkedList<String>()
        mergedInfo.add(infoHeader)

        aggregatedCardinality.forEachIndexed {
            index, currentAggregatedCardinality ->
            var bestAggregatedCorrelation = -10.0
            var bestAggregatedCorrelationIndex = -10
            var worstAggregatedCorrelation = 10.0
            var worstAggregatedCorrelationIndex = 10
            currentAggregatedCardinality.forEachIndexed {
                anotherIndex, aggregatedCardinalityForAnExecution ->
                val maximumValue = Math.max(bestAggregatedCorrelation, aggregatedCardinalityForAnExecution[1].toDouble())
                if (bestAggregatedCorrelation < maximumValue) {
                    bestAggregatedCorrelation = maximumValue
                    bestAggregatedCorrelationIndex = anotherIndex
                }
                val minimumValue: Double
                if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_WORST)
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
            when (targetToAchieve) {
                Constants.TARGET_ALL -> {
                    mergedDataAggregatedForCurrentAggregatedCardinality[1] = bestAggregatedCorrelation.toString()
                    mergedDataAggregatedForCurrentAggregatedCardinality[2] = worstAggregatedCorrelation.toString()
                    (3..currentAggregatedCardinality[0].size - 1).forEach {
                        anotherIndex ->
                        mergedDataAggregatedForCurrentAggregatedCardinality[anotherIndex] = currentAggregatedCardinality[0][anotherIndex]
                    }
                }
                else -> {
                    (1..currentAggregatedCardinality[0].size - 1).forEach {
                        anotherIndex ->
                        mergedDataAggregatedForCurrentAggregatedCardinality[anotherIndex] = currentAggregatedCardinality[0][anotherIndex]
                    }
                }
            }
            mergedAggregatedData.add(mergedDataAggregatedForCurrentAggregatedCardinality)
            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_BEST) {
                mergedBestFunctionValues.add(bestFunctionValues[index][bestAggregatedCorrelationIndex])
                mergedBestVariableValues.add(bestVariableValues[index][bestAggregatedCorrelationIndex])
            }
            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_WORST) {
                mergedWorstFunctionValues.add(worstFunctionValues[index][worstAggregatedCorrelationIndex])
                mergedWorstVariableValues.add(worstVariableValues[index][worstAggregatedCorrelationIndex])
            }
            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {
                mergedAverageFunctionValues.add(averageFunctionValues[index][0])
                mergedAverageVariableValues.add(averageVariableValues[index][0])
            }
        }

        var bestComputingTime = 0
        var worstComputingTime = 0
        var averageComputingTime = 0
        val mergedBestExecutionInfo: MutableList<String>
        val mergedWorstExecutionInfo: MutableList<String>
        val mergedAverageExecutionInfo: MutableList<String>

        when (targetToAchieve) {
            Constants.TARGET_ALL -> {
                info[0].forEach {
                    infoForABestExecution ->
                    bestComputingTime += infoForABestExecution.split(",").last().replace("\"", "").toInt()
                }
                mergedBestExecutionInfo = info[0][0].split(",").toMutableList()
                mergedBestExecutionInfo[mergedBestExecutionInfo.lastIndex] = bestComputingTime.toString()
                mergedInfo.add(StringUtils.join(mergedBestExecutionInfo, ","))
                info[1].forEach {
                    infoForAWorstExecution ->
                    worstComputingTime += infoForAWorstExecution.split(",").last().replace("\"", "").toInt()
                }
                mergedWorstExecutionInfo = info[1][0].split(",").toMutableList()
                mergedWorstExecutionInfo[mergedWorstExecutionInfo.lastIndex] = worstComputingTime.toString()
                mergedInfo.add(StringUtils.join(mergedWorstExecutionInfo, ","))
                info[2].forEach {
                    infoForAnAverageExecution ->
                    averageComputingTime += infoForAnAverageExecution.split(",").last().replace("\"", "").toInt()
                }
                mergedAverageExecutionInfo = info[2][0].split(",").toMutableList()
                mergedAverageExecutionInfo[mergedAverageExecutionInfo.lastIndex] = averageComputingTime.toString()
                mergedInfo.add(StringUtils.join(mergedAverageExecutionInfo, ","))
            }
            Constants.TARGET_BEST -> {
                info[0].forEach {
                    infoForABestExecution ->
                    bestComputingTime += infoForABestExecution.split(",").last().replace("\"", "").toInt()
                }
                mergedBestExecutionInfo = info[0][0].split(",").toMutableList()
                mergedBestExecutionInfo[mergedBestExecutionInfo.lastIndex] = bestComputingTime.toString()
                mergedInfo.add(StringUtils.join(mergedBestExecutionInfo, ","))
            }
            Constants.TARGET_WORST -> {
                info[0].forEach {
                    infoForAWorstExecution ->
                    worstComputingTime += infoForAWorstExecution.split(",").last().replace("\"", "").toInt()
                }
                mergedWorstExecutionInfo = info[1][0].split(",").toMutableList()
                mergedWorstExecutionInfo[mergedWorstExecutionInfo.lastIndex] = worstComputingTime.toString()
                mergedInfo.add(StringUtils.join(mergedWorstExecutionInfo, ","))
            }
            Constants.TARGET_AVERAGE -> {
                info[0].forEach {
                    infoForAnAverageExecution ->
                    averageComputingTime += infoForAnAverageExecution.split(",").last().replace("\"", "").toInt()
                }
                mergedAverageExecutionInfo = info[2][0].split(",").toMutableList()
                mergedAverageExecutionInfo[mergedAverageExecutionInfo.lastIndex] = averageComputingTime.toString()
                mergedInfo.add(StringUtils.join(mergedAverageExecutionInfo, ","))
            }
        }

        val mergedAggregatedDataWriter = CSVWriter(FileWriter("${Constants.OUTPUT_PATH}${resultPaths.last()}${Constants.TARGET_ALL}-Final-Merged.csv"))
        mergedAggregatedDataWriter.writeAll(mergedAggregatedData)
        mergedAggregatedDataWriter.close()

        if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_BEST) {
            val bestFunctionValuesDataWriter: BufferedWriter
            if (targetToAchieve == Constants.TARGET_ALL)
                bestFunctionValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Best-Fun-Merged.csv"))
            else
                bestFunctionValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Fun-Merged.csv"))
            mergedBestFunctionValues.forEach {
                aMergedBestFunctionValues ->
                bestFunctionValuesDataWriter.write(aMergedBestFunctionValues)
                bestFunctionValuesDataWriter.newLine()
            }
            bestFunctionValuesDataWriter.close()
            val bestVariableValuesDataWriter: BufferedWriter
            if (targetToAchieve == Constants.TARGET_ALL)
                bestVariableValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Best-Val-Merged.csv"))
            else
                bestVariableValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Val-Merged.csv"))
            mergedBestVariableValues.forEach {
                aMergedBestVariableValues ->
                bestVariableValuesDataWriter.write(aMergedBestVariableValues)
                bestVariableValuesDataWriter.newLine()
            }
            bestVariableValuesDataWriter.close()
        }

        if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_WORST) {
            val worstFunctionValuesDataWriter: BufferedWriter
            if (targetToAchieve == Constants.TARGET_ALL)
                worstFunctionValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Worst-Fun-Merged.csv"))
            else
                worstFunctionValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Fun-Merged.csv"))
            mergedWorstFunctionValues.forEach {
                aMergedWorstFunctionValues ->
                worstFunctionValuesDataWriter.write(aMergedWorstFunctionValues)
                worstFunctionValuesDataWriter.newLine()
            }
            worstFunctionValuesDataWriter.close()
            val worstVariableValuesDataWriter: BufferedWriter
            if (targetToAchieve == Constants.TARGET_ALL)
                worstVariableValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Worst-Val-Merged.csv"))
            else
                worstVariableValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Val-Merged.csv"))
            mergedWorstVariableValues.forEach {
                aMergedWorstVariableValues ->
                worstVariableValuesDataWriter.write(aMergedWorstVariableValues)
                worstVariableValuesDataWriter.newLine()
            }
            worstVariableValuesDataWriter.close()
        }

        if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {
            val averageFunctionValuesDataWriter: BufferedWriter
            if (targetToAchieve == Constants.TARGET_ALL)
                averageFunctionValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Average-Fun-Merged.csv"))
            else
                averageFunctionValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Fun-Merged.csv"))
            mergedAverageFunctionValues.forEach {
                aMergedAverageFunctionValues ->
                averageFunctionValuesDataWriter.write(aMergedAverageFunctionValues)
                averageFunctionValuesDataWriter.newLine()
            }
            averageFunctionValuesDataWriter.close()
            val averageVariableValuesDataWriter: BufferedWriter
            if (targetToAchieve == Constants.TARGET_ALL)
                averageVariableValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Average-Val-Merged.csv"))
            else
                averageVariableValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Val-Merged.csv"))
            mergedAverageVariableValues.forEach {
                aMergedAverageVariableValues ->
                averageVariableValuesDataWriter.write(aMergedAverageVariableValues)
                averageVariableValuesDataWriter.newLine()
            }
            averageVariableValuesDataWriter.close()
        }

        val infoValuesWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Info-Merged.csv"))
        mergedInfo.forEach {
            aMergedInfo ->
            infoValuesWriter.write(aMergedInfo)
            infoValuesWriter.newLine()
        }
        infoValuesWriter.close()

        resultPaths.forEach {
            aResultPath ->
            if (targetToAchieve == Constants.TARGET_ALL) {
                Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}$aResultPath${Constants.TARGET_ALL}-Final.csv"))
                Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Best-Fun.csv"))
                Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Best-Var.csv"))
                Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Worst-Fun.csv"))
                Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Worst-Var.csv"))
                Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Average-Fun.csv"))
                Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Average-Var.csv"))
                Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}$aResultPath${Constants.TARGET_ALL}-Info.csv"))
            } else {
                Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Final.csv"))
                Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Fun.csv"))
                Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Var.csv"))
                Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Info.csv"))
            }
        }

    }
}
