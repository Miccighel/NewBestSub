package it.uniud.newbestsub.dataset;

public class DatasetController {

    public DatasetModel model;
    public DatasetView view;

    public DatasetController(){
        model = new DatasetModel();
        view = new DatasetView();
    }

    public void loadData(String datasetName) {
        System.out.println("INFO: Datased loading has been started");
        model.loadData(datasetName);
        System.out.println("SUCCESS: Dataset loading has been completed successfully.");
    }

}
