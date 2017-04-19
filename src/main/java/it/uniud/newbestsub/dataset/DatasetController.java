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
        System.out.println("INFO: Datased loading has been started");
        try {
            model.loadData(datasetName);
        } catch (FileNotFoundException exception) {
            System.out.println("EXCEPTION: The path to your file isn't correct, fix it and try again.");
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
        }
        System.out.println("SUCCESS: Dataset loading has been completed successfully.");
    }

    public void solve() {
        System.out.println("INFO: Starting to solve the problem");
        model.solve();
        System.out.println("SUCCESS: Problem has been solved successfully.");
    }

}
