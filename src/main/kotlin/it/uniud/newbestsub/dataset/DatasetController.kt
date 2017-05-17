package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.utils.BestSubsetLogger
import it.uniud.newbestsub.utils.Constants
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.runBlocking
import scala.util.parsing.combinator.testing.Str

import java.io.File
import java.io.FileNotFoundException
import java.io.IOException


class DatasetController {

    private var model: DatasetModel = DatasetModel()
    private var view: DatasetView = DatasetView()

    fun loadData(datasetPath: String) {

        BestSubsetLogger.log("CONTROLLER - Dataset loading has been started.")
        BestSubsetLogger.log("CONTROLLER - Path to dataset file is: \"$datasetPath\".")

        val outputDirectory = File(Constants.OUTPUT_PATH)
        if (!outputDirectory.exists()) {
            BestSubsetLogger.log("CONTROLLER - Starting to create output directory.")
            if (outputDirectory.mkdir()) BestSubsetLogger.log("CONTROLLER - Output directory created. Path is: \"${outputDirectory.name}\".")
        } else BestSubsetLogger.log("CONTROLLER - Output directory already exists. Path is: \"${outputDirectory.name}\".")
        try {
            model.loadData(datasetPath)
        } catch (exception: FileNotFoundException) {
            BestSubsetLogger.log("EXCEPTION (Controller) - Dataset file wasn't found. Be sure that your file is inside a \"data\" folder.")
        } catch (exception: IOException) {
            BestSubsetLogger.log(exception.message as String)
        }

        BestSubsetLogger.log("CONTROLLER - Dataset loading has been completed successfully.")
    }

    fun solve(chosenCorrelationMethod: String, targetToAchieve: String, numberOfIterations: Int, resultPath: String) {

        BestSubsetLogger.log("CONTROLLER - Starting to solve the problem")
        BestSubsetLogger.log("CONTROLLER - Chosen method to compute correlation is: $chosenCorrelationMethod.")
        BestSubsetLogger.log("CONTROLLER - Target to achieve is: $targetToAchieve.")
        BestSubsetLogger.log("CONTROLLER - Number of iterations to do is: " + numberOfIterations)
        BestSubsetLogger.log("CONTROLLER - Path to the result files are: \"${Constants.OUTPUT_PATH}$resultPath-Fun.csv\" and \"${Constants.OUTPUT_PATH}$resultPath-Var.csv\".")

        val result = model.solve(chosenCorrelationMethod, targetToAchieve, numberOfIterations)

        view.print(result, resultPath)

        BestSubsetLogger.log("CONTROLLER - Finished to solve the problem.")
    }

    fun solve(chosenCorrelationMethod: String, numberOfIterations: Int, resultPath: String) = runBlocking {

        BestSubsetLogger.log("CONTROLLER - Starting to solve the problem")
        BestSubsetLogger.log("CONTROLLER - Chosen method to compute correlation is: $chosenCorrelationMethod.")
        BestSubsetLogger.log("CONTROLLER - Target to achieve is: All.")
        BestSubsetLogger.log("CONTROLLER - Number of iterations to do is: " + numberOfIterations)
        BestSubsetLogger.log("CONTROLLER - Path to the result files are: \"${Constants.OUTPUT_PATH}$resultPath"+"Best-Fun.csv\" and \"${Constants.OUTPUT_PATH}$resultPath"+"Best-Var.csv\".")
        BestSubsetLogger.log("CONTROLLER - Path to the result files are: \"${Constants.OUTPUT_PATH}$resultPath"+"Worst-Fun.csv\" and \"${Constants.OUTPUT_PATH}$resultPath"+"Worst-Var.csv\".")
        BestSubsetLogger.log("CONTROLLER - Path to the result files are: \"${Constants.OUTPUT_PATH}$resultPath"+"Average-Fun.csv\" and \"${Constants.OUTPUT_PATH}$resultPath"+"Average-Var.csv\".")

        val bestResult = async(CommonPool) {
            model.solve(chosenCorrelationMethod, "Best", numberOfIterations)
        }.await()
        val worstResult = async(CommonPool) {
            model.solve(chosenCorrelationMethod, "Worst", numberOfIterations)
        }.await()
        val averageResult = async(CommonPool) {
            model.solve(chosenCorrelationMethod, "Worst", numberOfIterations)
        }.await()

        view.print(bestResult, resultPath + "Best")
        view.print(worstResult, resultPath + "Worst")
        view.print(averageResult, resultPath + "Average")

        BestSubsetLogger.log("CONTROLLER - Finished to solve the problem.")
    }

}
