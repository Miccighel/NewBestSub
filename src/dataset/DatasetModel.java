package dataset;

import com.opencsv.CSVReader;

import java.io.FileNotFoundException;
import java.io.FileReader;

public class DatasetModel {

    public DatasetModel () {}

    public DatasetModel loadData (String datasetName) {

        try {
            CSVReader reader = new CSVReader(new FileReader(datasetName));
            System.out.println("Ready to load the CSV file!");
        } catch (FileNotFoundException exception) {
            System.out.println(exception.getMessage());
        }

        return new DatasetModel();
    }

}
