package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.utils.Constants
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.runBlocking
import org.apache.logging.log4j.LogManager
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.util.*
import kotlin.collections.LinkedHashMap

class DatasetController {

    var model = DatasetModel()
    var models = listOf<DatasetModel>()
    var aggregatedData = listOf<Array<String>>()
    private var modelBest = DatasetModel()
    private var modelWorst = DatasetModel()
    private var modelAverage = DatasetModel()
    private var view = DatasetView()
    private lateinit var parameters: Parameters
    private var logger = LogManager.getLogger()

    init {
        logger.info("Problem resolution started.")
    }

    fun loadData(datasetPath: String) {

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
            model.loadData(datasetPath)
            modelBest.loadData(datasetPath)
            modelWorst.loadData(datasetPath)
            modelAverage.loadData(datasetPath)
        } catch (exception: FileNotFoundException) {
            logger.warn("Data set not found. Is file inside a \"data\" dir.?")
        } catch (exception: IOException) {
            logger.warn(exception.message as String)
        }

        logger.info("Data set loading completed.")
    }

    fun expandData(expansionCoefficient: Int, target: String) {

        val random = Random()
        val systemLabels = model.systemLabels
        val topicLabels = Array(expansionCoefficient, { "${random.nextInt(998 + 1 - 100) + 100} (F)" })
        val randomizedAveragePrecisions = LinkedHashMap<String, DoubleArray>()

        systemLabels.forEach {
            systemLabel ->
            randomizedAveragePrecisions[systemLabel] = DoubleArray(expansionCoefficient, { Math.random() })
        }
        if (target == Constants.TARGET_ALL) models.forEach { model -> model.expandData(randomizedAveragePrecisions, topicLabels) }
        else model.expandData(randomizedAveragePrecisions, topicLabels)
    }

    fun solve(parameters: Parameters, resultPath: String): List<Array<String>> {

        this.parameters = parameters

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

            val bestParameters = Parameters(parameters.correlationMethod, Constants.TARGET_BEST, parameters.numberOfIterations, parameters.populationSize, parameters.percentiles)
            val worstParameters = Parameters(parameters.correlationMethod, Constants.TARGET_WORST, parameters.numberOfIterations, parameters.populationSize, parameters.percentiles)
            val averageParameters = Parameters(parameters.correlationMethod, Constants.TARGET_AVERAGE, parameters.numberOfIterations, parameters.populationSize, parameters.percentiles)
            val bestResult = { async(CommonPool) { modelBest.solve(bestParameters) } }.invoke()
            val worstResult = { async(CommonPool) { modelWorst.solve(worstParameters) } }.invoke()
            val averageResult = { async(CommonPool) { modelAverage.solve(averageParameters) } }.invoke()

            runBlocking {
                view.print(bestResult.await(), resultPath + Constants.TARGET_BEST)
                view.print(worstResult.await(), resultPath + Constants.TARGET_WORST)
                view.print(averageResult.await(), resultPath + Constants.TARGET_AVERAGE)
            }

            logger.info("Data aggregation started.")

            models = listOf(modelBest, modelWorst, modelAverage)
            aggregatedData = aggregate(models)
            view.print(aggregatedData, "${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_ALL}-Final.csv")

            logger.info("Aggregated data available at:")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath${Constants.TARGET_ALL}-Final.csv\"")

        } else {

            logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Final.csv\" (Aggregated data)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Fun.csv\" (Target function values)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Var.csv\" (Variable values)")

            view.print(model.solve(parameters), resultPath)

            logger.info("Data aggregation started.")

            models = listOf(model)
            aggregatedData = aggregate(models)
            view.print(aggregatedData, "${Constants.OUTPUT_PATH}$resultPath-Final.csv")

            logger.info("Aggregated data available at:")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Final.csv\"")

        }

        logger.info("Data aggregation completed.")
        logger.info("Problem resolution completed.")

        return aggregatedData
    }

    fun aggregate(models: List<DatasetModel>): List<Array<String>> {

        val incompleteData = mutableListOf<Array<String>>()
        val aggregatedData = mutableListOf<Array<String>>()
        val header = mutableListOf<String>()
        val topicLabels = models[0].topicLabels
        var percentiles = linkedMapOf<Int, List<Double>>()

        header.add("Cardinality")
        models.forEach {
            model ->
            header.add(model.targetToAchieve)
            if (model.targetToAchieve == Constants.TARGET_AVERAGE) percentiles = model.percentiles
        }

        topicLabels.forEach { topicLabel -> header.add(topicLabel) }
        percentiles.keys.forEach { percentile -> header.add(" Percentile: $percentile%") }

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

}
