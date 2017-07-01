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
    private lateinit var resultPath: String
    private var resultPaths = mutableListOf<String>()
    private var logger = LogManager.getLogger()

    init {
        logger.info("Problem resolution started.")
    }

    fun load(datasetPath: String) {

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
            models[0].loadData(datasetPath)
            if (targetToAchieve == Constants.TARGET_ALL) {
                models.plusAssign(DatasetModel())
                models[1].loadData(datasetPath)
                models.plusAssign(DatasetModel())
                models[2].loadData(datasetPath)
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

    fun save(resultPath: String) {
        resultPaths.plusAssign(resultPath)
    }

    fun solve(parameters: Parameters, resultPath: String) {

        this.parameters = parameters
        this.resultPath = resultPath

        logger.info("Printing common execution parameters.")

        logger.info("Correlation: ${parameters.correlationMethod}.")
        logger.info("Target: ${parameters.targetToAchieve}.")
        logger.info("Number of iterations: ${parameters.numberOfIterations}.")
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

            val bestParameters = Parameters(parameters.correlationMethod, Constants.TARGET_BEST, parameters.numberOfIterations, parameters.numberOfRepetitions, parameters.populationSize, parameters.percentiles)
            val worstParameters = Parameters(parameters.correlationMethod, Constants.TARGET_WORST, parameters.numberOfIterations, parameters.numberOfRepetitions, parameters.populationSize, parameters.percentiles)
            val averageParameters = Parameters(parameters.correlationMethod, Constants.TARGET_AVERAGE, parameters.numberOfIterations, parameters.numberOfRepetitions, parameters.populationSize, parameters.percentiles)
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
            logger.info("Execution informations gathering started.")
            view.print(info(models), "${Constants.OUTPUT_PATH}${resultPath}Info.csv")
            logger.info("Execution informations available at:")
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
            logger.info("Execution informations gathering started.")
            view.print(info(models), "${Constants.OUTPUT_PATH}${resultPath}Info.csv")
            logger.info("Execution informations available at:")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_ALL}-Info.csv\"")

        }

        logger.info("Execution informations gathering completed.")
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
            executionParameters = mutableListOf<String>()
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

        val aggregatedDataReaders = Array(resultPaths.size, {
            index ->
            CSVReader(FileReader("${Constants.OUTPUT_PATH}${resultPaths[index]}${Constants.TARGET_ALL}-Final.csv"))
        })
        val aggregatedCardinality = LinkedList<LinkedList<Array<String>>>()
        var aggregatedCardinalityRead = 0

        val bestFunctionValuesReaders = Array(resultPaths.size, {
            index ->
            Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Best-Fun.csv"))
        })
        val bestFunctionValues = LinkedList<LinkedList<String>>()
        var bestFunctionValuesRead = 0

        val bestVariableValuesReaders = Array(resultPaths.size, {
            index ->
            Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Best-Var.csv"))
        })
        val bestVariableValues = LinkedList<LinkedList<String>>()
        var bestVariableValuesRead = 0

        val worstFunctionValuesReaders = Array(resultPaths.size, {
            index ->
            Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Worst-Fun.csv"))
        })
        val worstFunctionValues = LinkedList<LinkedList<String>>()
        var worstFunctionValuesRead = 0

        val worstVariableValuesReaders = Array(resultPaths.size, {
            index ->
            Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Worst-Var.csv"))
        })
        val worstVariableValues = LinkedList<LinkedList<String>>()
        var worstVariableValuesRead = 0

        val averageFunctionValuesReaders = Array(resultPaths.size, {
            index ->
            Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Average-Fun.csv"))
        })
        val averageFunctionValues = LinkedList<LinkedList<String>>()
        var averageFunctionValuesRead = 0

        val averageVariableValuesReaders = Array(resultPaths.size, {
            index ->
            Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Average-Var.csv"))
        })
        val averageVariableValues = LinkedList<LinkedList<String>>()
        var averageVariableValuesRead = 0

        val infoReaders = Array(resultPaths.size, {
            index ->
            Files.newBufferedReader(Paths.get("${Constants.OUTPUT_PATH}${resultPaths[index]}Info.csv"))
        })
        val infos = LinkedList<LinkedList<String>>()
        var infosRead = 0

        while (aggregatedCardinalityRead < models[0].numberOfTopics) {
            val currentAggregatedCardinality = LinkedList<Array<String>>()
            aggregatedDataReaders.forEach {
                anAggregatedDataReader ->
                currentAggregatedCardinality.plusAssign(anAggregatedDataReader.readNext())
            }
            aggregatedCardinality.add(currentAggregatedCardinality)
            aggregatedCardinalityRead++
        }
        aggregatedDataReaders.forEach(CSVReader::close)

        while (bestFunctionValuesRead < models[0].numberOfTopics) {
            val currentBestFunctionValue = LinkedList<String>()
            bestFunctionValuesReaders.forEach {
                aFunctionValuesReader ->
                currentBestFunctionValue.plusAssign(aFunctionValuesReader.readLine())
            }
            bestFunctionValues.add(currentBestFunctionValue)
            bestFunctionValuesRead++
        }
        bestFunctionValuesReaders.forEach(BufferedReader::close)

        while (bestVariableValuesRead < models[0].numberOfTopics) {
            val currentBestVariableValue = LinkedList<String>()
            bestVariableValuesReaders.forEach {
                aVariableValuesReader ->
                currentBestVariableValue.plusAssign(aVariableValuesReader.readLine())
            }
            bestVariableValues.add(currentBestVariableValue)
            bestVariableValuesRead++
        }
        bestVariableValuesReaders.forEach(BufferedReader::close)

        while (worstFunctionValuesRead < models[0].numberOfTopics) {
            val currentWorstFunctionValue = LinkedList<String>()
            worstFunctionValuesReaders.forEach {
                aFunctionValuesReader ->
                currentWorstFunctionValue.plusAssign(aFunctionValuesReader.readLine())
            }
            worstFunctionValues.add(currentWorstFunctionValue)
            worstFunctionValuesRead++
        }
        worstFunctionValuesReaders.forEach(BufferedReader::close)

        while (worstVariableValuesRead < models[0].numberOfTopics) {
            val currentWorstVariableValue = LinkedList<String>()
            worstVariableValuesReaders.forEach {
                aVariableValuesReader ->
                currentWorstVariableValue.plusAssign(aVariableValuesReader.readLine())
            }
            worstVariableValues.add(currentWorstVariableValue)
            worstVariableValuesRead++
        }
        worstVariableValuesReaders.forEach(BufferedReader::close)

        while (averageFunctionValuesRead < models[0].numberOfTopics) {
            val currentAverageFunctionValue = LinkedList<String>()
            averageFunctionValuesReaders.forEach {
                aFunctionValuesReader ->
                currentAverageFunctionValue.plusAssign(aFunctionValuesReader.readLine())
            }
            averageFunctionValues.add(currentAverageFunctionValue)
            averageFunctionValuesRead++
        }
        averageFunctionValuesReaders.forEach(BufferedReader::close)

        while (averageVariableValuesRead < models[0].numberOfTopics) {
            val currentAverageVariableValue = LinkedList<String>()
            averageVariableValuesReaders.forEach {
                aVariableValuesReader ->
                currentAverageVariableValue.plusAssign(aVariableValuesReader.readLine())
            }
            averageVariableValues.add(currentAverageVariableValue)
            averageVariableValuesRead++
        }
        averageVariableValuesReaders.forEach(BufferedReader::close)

        while (infosRead <= resultPaths.size) {
            val currentInfo = LinkedList<String>()
            infoReaders.forEach {
                aInfoReader ->
                currentInfo.plusAssign(aInfoReader.readLine())
            }
            infos.add(currentInfo)
            infosRead++
        }
        infoReaders.forEach(BufferedReader::close)

        val aggregatedDataHeader = aggregatedCardinality.pop().pop()
        val mergedAggregatedData = LinkedList<Array<String>>()
        mergedAggregatedData.add(aggregatedDataHeader)

        val infosHeader = infos.pop().pop()
        val mergedInfos = LinkedList<String>()
        mergedInfos.add(infosHeader)

        val mergedBestFunctionValues = LinkedList<String>()
        val mergedBestVariableValues = LinkedList<String>()
        val mergedWorstFunctionValues = LinkedList<String>()
        val mergedWorstVariableValues = LinkedList<String>()
        val mergedAverageFunctionValues = LinkedList<String>()
        val mergedAverageVariableValues = LinkedList<String>()

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
                val minimumValue = Math.min(worstAggregatedCorrelation, aggregatedCardinalityForAnExecution[2].toDouble())
                if (worstAggregatedCorrelation > minimumValue) {
                    worstAggregatedCorrelation = minimumValue
                    worstAggregatedCorrelationIndex = anotherIndex
                }
            }
            val mergedDataAggregatedForCurrentAggregatedCardinality = Array(currentAggregatedCardinality[0].size, { "" })
            mergedDataAggregatedForCurrentAggregatedCardinality[0] = currentAggregatedCardinality[0][0]
            mergedDataAggregatedForCurrentAggregatedCardinality[1] = bestAggregatedCorrelation.toString()
            mergedDataAggregatedForCurrentAggregatedCardinality[2] = worstAggregatedCorrelation.toString()
            (3..currentAggregatedCardinality[0].size - 1).forEach {
                anotherIndex ->
                mergedDataAggregatedForCurrentAggregatedCardinality[anotherIndex] = currentAggregatedCardinality[0][anotherIndex]
            }
            mergedAggregatedData.add(mergedDataAggregatedForCurrentAggregatedCardinality)
            mergedBestFunctionValues.add(bestFunctionValues[index][bestAggregatedCorrelationIndex])
            mergedBestVariableValues.add(bestVariableValues[index][bestAggregatedCorrelationIndex])
            mergedWorstFunctionValues.add(worstFunctionValues[index][worstAggregatedCorrelationIndex])
            mergedWorstVariableValues.add(worstVariableValues[index][worstAggregatedCorrelationIndex])
            mergedAverageFunctionValues.add(averageFunctionValues[index][0])
            mergedAverageVariableValues.add(averageVariableValues[index][0])
        }

        var bestComputingTime = 0
        var worstComputingTime = 0
        var averageComputingTime = 0
        infos[0].forEach {
            infoForABestExecution ->
            bestComputingTime += infoForABestExecution.split(",").last().replace("\"", "").toInt()
        }
        infos[1].forEach {
            infoForAWorstExecution ->
            worstComputingTime += infoForAWorstExecution.split(",").last().replace("\"", "").toInt()
        }
        infos[2].forEach {
            infoForAnAverageExecution ->
            averageComputingTime += infoForAnAverageExecution.split(",").last().replace("\"", "").toInt()
        }
        val mergedBestExecutionInfo = infos[0][0].split(",").toMutableList()
        mergedBestExecutionInfo[mergedBestExecutionInfo.lastIndex] = bestComputingTime.toString()
        val mergedWorstExecutionInfo = infos[1][0].split(",").toMutableList()
        mergedWorstExecutionInfo[mergedBestExecutionInfo.lastIndex] = worstComputingTime.toString()
        val mergedAverageExecutionInfo = infos[2][0].split(",").toMutableList()
        mergedAverageExecutionInfo[mergedAverageExecutionInfo.lastIndex] = averageComputingTime.toString()
        mergedInfos.add(StringUtils.join(mergedBestExecutionInfo, ","))
        mergedInfos.add(StringUtils.join(mergedWorstExecutionInfo, ","))
        mergedInfos.add(StringUtils.join(mergedAverageExecutionInfo, ","))

        val mergedAggregatedDataWriter = CSVWriter(FileWriter("${Constants.OUTPUT_PATH}${resultPaths.last()}${Constants.TARGET_ALL}-Final-Merged.csv"))
        mergedAggregatedDataWriter.writeAll(mergedAggregatedData)
        mergedAggregatedDataWriter.close()

        val bestFunctionValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Best-Fun-Merged.csv"))
        mergedBestFunctionValues.forEach {
            aMergedBestFunctionValues ->
            bestFunctionValuesDataWriter.write(aMergedBestFunctionValues)
            bestFunctionValuesDataWriter.newLine()
        }
        bestFunctionValuesDataWriter.close()

        val bestVariableValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Best-Var-Merged.csv"))
        mergedBestVariableValues.forEach {
            aMergedBestVariableValues ->
            bestVariableValuesDataWriter.write(aMergedBestVariableValues)
            bestVariableValuesDataWriter.newLine()
        }
        bestVariableValuesDataWriter.close()

        val worstFunctionValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Worst-Fun-Merged.csv"))
        mergedWorstFunctionValues.forEach {
            aMergedWorstFunctionValues ->
            worstFunctionValuesDataWriter.write(aMergedWorstFunctionValues)
            worstFunctionValuesDataWriter.newLine()
        }
        worstFunctionValuesDataWriter.close()

        val worstVariableValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Worst-Var-Merged.csv"))
        mergedWorstVariableValues.forEach {
            aMergedWorstVariableValues ->
            worstVariableValuesDataWriter.write(aMergedWorstVariableValues)
            worstVariableValuesDataWriter.newLine()
        }
        worstVariableValuesDataWriter.close()

        val averageFunctionValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Average-Fun-Merged.csv"))
        mergedAverageFunctionValues.forEach {
            aMergedAverageFunctionValues ->
            averageFunctionValuesDataWriter.write(aMergedAverageFunctionValues)
            averageFunctionValuesDataWriter.newLine()
        }
        averageFunctionValuesDataWriter.close()

        val infoValuesWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Info-Merged.csv"))
        mergedInfos.forEach {
            aMergedInfo ->
            infoValuesWriter.write(aMergedInfo)
            infoValuesWriter.newLine()
        }
        infoValuesWriter.close()

        val averageVariableValuesDataWriter = Files.newBufferedWriter(Paths.get("${Constants.OUTPUT_PATH}${resultPaths.last()}Average-Var-Merged.csv"))
        mergedAverageVariableValues.forEach {
            aMergedAverageVariableValues ->
            averageVariableValuesDataWriter.write(aMergedAverageVariableValues)
            averageVariableValuesDataWriter.newLine()
        }
        averageVariableValuesDataWriter.close()

        resultPaths.forEach {
            aResultPath ->
            Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}$aResultPath${Constants.TARGET_ALL}-Final.csv"))
            Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Best-Fun.csv"))
            Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Best-Var.csv"))
            Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Worst-Fun.csv"))
            Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Worst-Var.csv"))
            Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Average-Fun.csv"))
            Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Average-Var.csv"))
            Files.deleteIfExists(Paths.get("${Constants.OUTPUT_PATH}${aResultPath}Info.csv"))
        }

    }
}
