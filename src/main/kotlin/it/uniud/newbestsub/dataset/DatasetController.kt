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

class DatasetController {

    var model: DatasetModel = DatasetModel()
    private var modelBest: DatasetModel = DatasetModel()
    private var modelWorst: DatasetModel = DatasetModel()
    private var modelAverage: DatasetModel = DatasetModel()
    private var models: List<DatasetModel> = LinkedList()
    private var view: DatasetView = DatasetView()
    private var logger = LogManager.getLogger()

    init {
        logger.info("Problem resolution started.")
    }

    fun loadData(datasetPath: String) {

        logger.info("Dataset loading started.")
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
            logger.warn("Dataset not found. Is file inside a \"data\" dir.?")
        } catch (exception: IOException) {
            logger.warn(exception.message as String)
        }

        logger.info("Dataset loading completed.")
    }

    fun solve(parameters: Parameters, resultPath: String) {

        logger.info("Printing common execution parameters.")

        logger.info("Correlation: ${parameters.correlationMethod}.")
        logger.info("Target: ${parameters.targetToAchieve}.")
        logger.info("Number of iterations: ${parameters.numberOfIterations}.")
        logger.info("Output path:")

        if (parameters.targetToAchieve == "All") {

            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "All-Final.csv\" (Aggregated data)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "Best-Fun.csv\" (Target function values)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "Best-Var.csv\" (Variable values)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "Worst-Fun.csv\" (Target function values)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "Worst-Var.csv\" (Variable values)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "Average-Fun.csv\" (Target function values)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "Average-Var.csv\" (Variable values)")

            val bestParameters = Parameters(parameters.correlationMethod, "Best", parameters.numberOfIterations, parameters.populationSize, parameters.percentiles)
            val worstParameters = Parameters(parameters.correlationMethod, "Worst", parameters.numberOfIterations, parameters.populationSize, parameters.percentiles)
            val averageParameters = Parameters(parameters.correlationMethod, "Average", parameters.numberOfIterations, parameters.populationSize, parameters.percentiles)
            val bestResult = { async(CommonPool) { modelBest.solve(bestParameters) } }.invoke()
            val worstResult = { async(CommonPool) { modelWorst.solve(worstParameters) } }.invoke()
            val averageResult = { async(CommonPool) { modelAverage.solve(averageParameters) } }.invoke()

            runBlocking {
                view.print(bestResult.await(), resultPath + "Best")
                view.print(worstResult.await(), resultPath + "Worst")
                view.print(averageResult.await(), resultPath + "Average")
            }

            logger.info("Model data aggregation started.")

            models = listOf(modelBest, modelWorst, modelAverage)
            view.print(aggregate(models), "${Constants.OUTPUT_PATH}${resultPath}All-Final.csv")

            logger.info("Aggregated data available at:")
            logger.info("\"${Constants.OUTPUT_PATH}${resultPath}All-Final.csv\"")

        } else {

            logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Final.csv\" (Aggregated data)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Fun.csv\" (Target function values)")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Var.csv\" (Variable values)")

            view.print(model.solve(parameters), resultPath)

            logger.info("Data aggregation started.")

            models = listOf(model)
            view.print(aggregate(models), "${Constants.OUTPUT_PATH}$resultPath-Final.csv")

            logger.info("Aggregated data available at:")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Final.csv\"")

        }

        logger.info("Data aggregation completed.")
        logger.info("Finished to solve the problem.")

    }

    fun aggregate(models: List<DatasetModel>) : List<Array<String>> {

        val incompleteData = LinkedList<Array<String>>()
        val aggregatedData = LinkedList<Array<String>>()
        val header = LinkedList<String>()
        val topicLabels = models[0].topicLabels
        var percentiles : MutableMap<Int,List<Double>> = LinkedHashMap()

        header.add("Cardinality")
        models.forEach {
            model ->
            header.add(model.targetToAchieve)
            if(model.targetToAchieve=="Average") percentiles = model.percentiles
        }
        topicLabels.forEach { topicLabel -> header.add(topicLabel) }
        percentiles.keys.forEach { percentile -> header.add(" Percentile: $percentile") }

        logger.info("Common data between models.")
        logger.info("Topics number: ${models[0].numberOfTopics}")
        logger.info("Systems number: ${models[0].numberOfSystems}")

        (0..models[0].numberOfTopics - 1).forEach {
            index ->
            val currentCardinality = (index + 1).toDouble()
            val currentLine = LinkedList<String>()
            currentLine.add(currentCardinality.toString())
            models.forEach {
                model ->
                val correlationValueForCurrentCardinality = model.findCorrelationForCardinality(currentCardinality)
                if (correlationValueForCurrentCardinality != null) currentLine.add(correlationValueForCurrentCardinality.toString()) else currentLine.add("UNAVAILABLE")
            }
            incompleteData.add(currentLine.toTypedArray())
        }

        incompleteData.forEach {
            aLine ->
            val currentCardinality = aLine[0].toDouble()
            var newDataEntry = aLine
            topicLabels.forEach {
                currentLabel ->
                var topicPresence = ""
                models.forEach {
                    model ->
                    val isTopicInASolutionOfCurrentCard = model.isTopicInASolutionOfCardinality(currentLabel, currentCardinality)
                    when (model.targetToAchieve) {
                        "Best" -> if (isTopicInASolutionOfCurrentCard) topicPresence += "b"
                        "Worst" -> if (isTopicInASolutionOfCurrentCard) topicPresence += "w"
                    }
                }
                if (topicPresence == "") topicPresence += "n"
                newDataEntry = newDataEntry.plus(topicPresence)
            }

            percentiles.entries.forEach {
                (_, percentileValues) ->
                newDataEntry = newDataEntry.plus(percentileValues[currentCardinality.toInt() - 1].toString())
            }
            aggregatedData.add(newDataEntry)
        }
        incompleteData.clear()
        aggregatedData.addFirst(header.toTypedArray())

        return aggregatedData
    }
}
