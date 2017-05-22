package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.utils.Constants
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.runBlocking
import org.apache.logging.log4j.LogManager
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException

class DatasetController {

    private var model: DatasetModel = DatasetModel()
    private var modelBest: DatasetModel = DatasetModel()
    private var modelWorst: DatasetModel = DatasetModel()
    private var modelAverage: DatasetModel = DatasetModel()
    private var view: DatasetView = DatasetView()
    private var logger = LogManager.getLogger()

    fun loadData(datasetPath: String) {

        logger.info("Dataset loading started.")
        logger.info("Path: \"$datasetPath\".")

        val outputDirectory = File(Constants.OUTPUT_PATH)
        if (!outputDirectory.exists()) {
            logger.info("Checking if output dir. exists.")
            if (outputDirectory.mkdir()) {
                logger.info("Output dir. created.")
                logger.info("Path: \"${outputDirectory.name}\".")
            }
        } else {
            logger.warn("Output dir. already exists.")
            logger.warn("Path: \"${outputDirectory.name}\".")
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

        logger.info("Problem resolution started.")
        logger.info("Correlation: ${parameters.chosenCorrelationMethod}.")
        logger.info("Target: ${parameters.targetToAchieve}.")
        logger.info("Number of iterations: ${parameters.numberOfIterations}.")
        logger.info("Output path:")

        if (parameters.targetToAchieve == "All") {

            logger.info("${Constants.OUTPUT_PATH}$resultPath" + "Best-Fun.csv")
            logger.info("${Constants.OUTPUT_PATH}$resultPath" + "Best-Var.csv")
            logger.info("${Constants.OUTPUT_PATH}$resultPath" + "Worst-Fun.csv")
            logger.info("${Constants.OUTPUT_PATH}$resultPath" + "Worst-Var.csv")
            logger.info("${Constants.OUTPUT_PATH}$resultPath" + "Average-Fun.csv")
            logger.info("${Constants.OUTPUT_PATH}$resultPath" + "Average-Var.csv")

            val bestParameters = Parameters(parameters.chosenCorrelationMethod, "Best", parameters.numberOfIterations, parameters.populationSize)
            val worstParameters = Parameters(parameters.chosenCorrelationMethod, "Worst", parameters.numberOfIterations, parameters.populationSize)
            val averageParameters = Parameters(parameters.chosenCorrelationMethod, "Average", parameters.numberOfIterations, parameters.populationSize)
            val bestResult = { async(CommonPool) { modelBest.solve(bestParameters) } }.invoke()
            val worstResult = { async(CommonPool) { modelWorst.solve(worstParameters) } }.invoke()
            val averageResult = { async(CommonPool) { modelAverage.solve(averageParameters) } }.invoke()

            runBlocking {
                view.print(bestResult.await(), resultPath + "Best")
                view.print(worstResult.await(), resultPath + "Worst")
                view.print(averageResult.await(), resultPath + "Average")
            }

        } else {

            logger.info("${Constants.OUTPUT_PATH}$resultPath-Fun.csv")
            logger.info("${Constants.OUTPUT_PATH}$resultPath-Var.csv")
            view.print(model.solve(parameters), resultPath)

        }

        logger.info("Finished to solve the problem.")

    }
}
