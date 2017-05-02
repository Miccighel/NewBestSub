package it.uniud.newbestsub.dataset;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class DatasetController {

    public DatasetModel model;
    public DatasetView view;

    public DatasetController() {
        model = new DatasetModel();
        view = new DatasetView();
    }

    public void loadData(String datasetPath) {
        System.out.println("CONTROLLER - Datased loading has been started.");
        System.out.println("CONTROLLER - Path to dataset file is: \"" + datasetPath + "\".");

        File outputDirectory = new File("res/");
        if (!outputDirectory.exists()) {
            System.out.println("CONTROLLER - Starting to create output directory.");
            boolean result = false;
                outputDirectory.mkdir();
                result = true;
            if(result) {
                System.out.println("CONTROLLER - Output directory created. Path is: \"" + outputDirectory.getName() + "/\".");
            }
        } else {
            System.out.println("CONTROLLER - Output directory already exists. Path is: \"" + outputDirectory.getName() + "/\".");
        }

        try {
            model.loadData(datasetPath);
        } catch (FileNotFoundException exception) {
            System.out.println("EXCEPTION (Controller) - Dataset file wasn't found. Be sure that your file is inside a \"data\" folder.");
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
        }
        System.out.println("CONTROLLER - Dataset loading has been completed successfully.");
    }

    public void solve(String chosenCorrelationMethod, String targetToAchieve, int numberOfIterations, String resultPath) {
        System.out.println("CONTROLLER - Starting to solve the problem");
        System.out.println("CONTROLLER - Chosen method to compute correlation is: " + chosenCorrelationMethod + ".");
        System.out.println("CONTROLLER - Target to achieve is: " + targetToAchieve);
        System.out.println("CONTROLLER - Number of iterations to do is: " + numberOfIterations);
        System.out.println("CONTROLLER - Path to the result file is: \"res/" + resultPath + ".csv\".");
        view.print(model.solve(chosenCorrelationMethod, targetToAchieve, numberOfIterations),resultPath);
        System.out.println("CONTROLLER - Finished to solve the problem.");
    }

    public void solve(String chosenCorrelationMethod, String resultPath) {
        System.out.println("CONTROLLER - Starting to solve the problem");
        System.out.println("CONTROLLER - Chosen method to compute correlation is: " + chosenCorrelationMethod + ".");
        System.out.println("CONTROLLER - Target to achieve is: Average");
        System.out.println("CONTROLLER - Number of iterations to do is: not necessary to define.");
        System.out.println("CONTROLLER - Path to the result file is: \"res/" + resultPath + ".csv\".");
        view.print(model.solve(chosenCorrelationMethod),resultPath);
        System.out.println("CONTROLLER - Finished to solve the problem.");
    }

}
