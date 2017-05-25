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

            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "All-Final.csv\"")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "Best-Fun.csv\"")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "Best-Var.csv\"")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "Worst-Fun.csv\"")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "Worst-Var.csv\"")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "Average-Fun.csv\"")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath" + "Average-Var.csv\"")

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
            view.aggregate(models, "${Constants.OUTPUT_PATH}${resultPath}All-Final.csv")

            logger.info("Aggregated data available at:")
            logger.info("\"${Constants.OUTPUT_PATH}${resultPath}All-Final.csv\"")

        } else {

            logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Final.csv\"")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Fun.csv\"")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Var.csv\"")

            view.print(model.solve(parameters), resultPath)

            logger.info("Data aggregation started.")

            models = listOf(model)
            view.aggregate(models, "${Constants.OUTPUT_PATH}$resultPath-Final.csv")

            logger.info("Aggregated data available at:")
            logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Final.csv\"")

        }

        logger.info("Data aggregation completed.")
        logger.info("Finished to solve the problem.")

    }
}
