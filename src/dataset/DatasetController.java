package dataset;

public class DatasetController {

    public DatasetModel model;
    public DatasetView view;

    public DatasetController(){
        this.model = new DatasetModel();
        this.view = new DatasetView();
    }

    public void loadData(String datasetName) {
        model.loadData(datasetName);
    }

}
