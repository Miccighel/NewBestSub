package it.uniud.newbestsub.dataset;

import java.io.FileNotFoundException;
import java.io.IOException;

public class DatasetController {

    public DatasetModel model;
    public DatasetView view;

    public DatasetController() {
        model = new DatasetModel();
        view = new DatasetView();
    }

    public void loadData(String datasetName) {
        System.out.println("CONTROLLER - Datased loading has been started");
        try {
            model.loadData(datasetName);
        } catch (FileNotFoundException exception) {
            System.out.println("EXCEPTION (Controller) - The path to your file isn't correct, fix it and try again.");
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
        }
        System.out.println("CONTROLLER - Dataset loading has been completed successfully.");
    }

    public void solve(String chosenCorrelationMethod, String outputFile) {
        System.out.println("CONTROLLER - Starting to solve the problem");
        System.out.println("CONTROLLER - The chosen method to compute correlation is " + chosenCorrelationMethod + ".");
        view.print(outputFile, model.solve(chosenCorrelationMethod));
        System.out.println("CONTROLLER - Finished to solve the problem.");
    }

}
