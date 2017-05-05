package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.utils.BestSubsetLogger
import it.uniud.newbestsub.utils.Constants

import java.io.File
import java.io.FileNotFoundException
import java.io.IOException

class DatasetController {

    var model: DatasetModel
    var view: DatasetView

    init {
        model = DatasetModel()
        view = DatasetView()
    }

    fun loadData(datasetPath: String) {

        BestSubsetLogger.log("CONTROLLER - Datased loading has been started.")
        BestSubsetLogger.log("CONTROLLER - Path to dataset file is: \"$datasetPath\".")

        val outputDirectory = File(Constants.OUTPUT_PATH)
        if (!outputDirectory.exists()) {
            BestSubsetLogger.log("CONTROLLER - Starting to create output directory.")
            var result : Boolean
            outputDirectory.mkdir()
            result = true
            if (result) {
                BestSubsetLogger.log("CONTROLLER - Output directory created. Path is: \"" + outputDirectory.name + "/\".")
            }
        } else {
            BestSubsetLogger.log("CONTROLLER - Output directory already exists. Path is: \"" + outputDirectory.name + "/\".")
        }

        try {
            model.loadData(datasetPath)
        } catch (exception: FileNotFoundException) {
            BestSubsetLogger.log("EXCEPTION (Controller) - Dataset file wasn't found. Be sure that your file is inside a \"data\" folder.")
        } catch (exception: IOException) {
            BestSubsetLogger.log("FD")
        }

        BestSubsetLogger.log("CONTROLLER - Dataset loading has been completed successfully.")
    }

    fun solve(chosenCorrelationMethod: String, targetToAchieve: String, numberOfIterations: Int, resultPath: String) {
        BestSubsetLogger.log("CONTROLLER - Starting to solve the problem")
        BestSubsetLogger.log("CONTROLLER - Chosen method to compute correlation is: $chosenCorrelationMethod.")
        BestSubsetLogger.log("CONTROLLER - Target to achieve is: $targetToAchieve.")
        BestSubsetLogger.log("CONTROLLER - Number of iterations to do is: " + numberOfIterations)
        BestSubsetLogger.log("CONTROLLER - Path to the result files are: \"" + Constants.OUTPUT_PATH + resultPath + "-Fun.csv\" and \"" + Constants.OUTPUT_PATH + resultPath + "-Var.csv\"" + ".")
        view.print(model.solve(chosenCorrelationMethod, targetToAchieve, numberOfIterations), resultPath)
        BestSubsetLogger.log("CONTROLLER - Finished to solve the problem.")
    }

}
