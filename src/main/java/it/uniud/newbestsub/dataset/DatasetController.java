package it.uniud.newbestsub.dataset;

import it.uniud.newbestsub.utils.BestSubsetLogger;
import it.uniud.newbestsub.utils.Constants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class DatasetController {

    public DatasetModel model;
    public DatasetView view;
    private BestSubsetLogger logger;

    public DatasetController() {
        model = new DatasetModel();
        view = new DatasetView();
        logger = BestSubsetLogger.getInstance();
    }

    public void loadData(String datasetPath) {

        logger.log("CONTROLLER - Datased loading has been started.");
        logger.log("CONTROLLER - Path to dataset file is: \"" + datasetPath + "\".");

        File outputDirectory = new File(Constants.OUTPUT_PATH);
        if (!outputDirectory.exists()) {
            logger.log("CONTROLLER - Starting to create output directory.");
            boolean result = false;
                outputDirectory.mkdir();
                result = true;
            if(result) {
                logger.log("CONTROLLER - Output directory created. Path is: \"" + outputDirectory.getName() + "/\".");
            }
        } else {
            logger.log("CONTROLLER - Output directory already exists. Path is: \"" + outputDirectory.getName() + "/\".");
        }

        try {
            model.loadData(datasetPath);
        } catch (FileNotFoundException exception) {
            logger.log("EXCEPTION (Controller) - Dataset file wasn't found. Be sure that your file is inside a \"data\" folder.");
        } catch (IOException exception) {
            logger.log(exception.getMessage());
        }
        logger.log("CONTROLLER - Dataset loading has been completed successfully.");
    }

    public void solve(String chosenCorrelationMethod, String targetToAchieve, int numberOfIterations, String resultPath) {
        logger.log("CONTROLLER - Starting to solve the problem");
        logger.log("CONTROLLER - Chosen method to compute correlation is: " + chosenCorrelationMethod + ".");
        logger.log("CONTROLLER - Target to achieve is: " + targetToAchieve);
        logger.log("CONTROLLER - Number of iterations to do is: " + numberOfIterations);
        logger.log("CONTROLLER - Path to the result files are: \"" + Constants.OUTPUT_PATH +  resultPath + "_fun.csv\" and \""+ Constants.OUTPUT_PATH +  resultPath + "_var.csv\"" + ".");
        view.print(model.solve(chosenCorrelationMethod, targetToAchieve, numberOfIterations),resultPath);
        logger.log("CONTROLLER - Finished to solve the problem.");
    }

    public void solve(String chosenCorrelationMethod, String resultPath) {
        logger.log("CONTROLLER - Starting to solve the problem");
        logger.log("CONTROLLER - Chosen method to compute correlation is: " + chosenCorrelationMethod + ".");
        logger.log("CONTROLLER - Target to achieve is: Average");
        logger.log("CONTROLLER - Number of iterations to do is: not necessary to define.");
        logger.log("CONTROLLER - Path to the result files are: \"" + Constants.OUTPUT_PATH +  resultPath + "_fun.csv\" and \""+ Constants.OUTPUT_PATH +  resultPath + "_var.csv\"" + ".");
        view.print(model.solve(chosenCorrelationMethod),resultPath);
        logger.log("CONTROLLER - Finished to solve the problem.");
    }

}
