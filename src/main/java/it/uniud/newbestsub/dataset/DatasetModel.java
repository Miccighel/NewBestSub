package it.uniud.newbestsub.dataset;

import com.opencsv.CSVReader;

import java.io.FileNotFoundException;
import java.io.FileReader;

import java.io.IOException;
import java.util.*;

public class DatasetModel {

    String[] topicLabels;
    Map<String,double[]>  averagePrecisions = new LinkedHashMap<String,double[]>();

    public DatasetModel() {}

    public DatasetModel getModel() {
        return this;
    }

    public void loadData(String datasetName) {

        try {

            // The parsing phase of the original .csv dataset file starts there

            CSVReader reader = new CSVReader(new FileReader(datasetName));
            topicLabels = ((String[])reader.readNext());
            String [] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                String systemLabel = nextLine[0];
                double[] averagePrecisions = new double[nextLine.length-1];
                for (int i=1;i<nextLine.length;i++){
                    averagePrecisions[i-1]= Double.parseDouble(nextLine[i]);
                }
                this.averagePrecisions.put(systemLabel,averagePrecisions);
            }

            /* averagePrecisions is a <String,double[]> dictionary where, for each entry, the key is the system label
            and the value is an array that contains the AP values of the system for each topic */

        } catch (FileNotFoundException exception) {
            System.out.println("EXCEPTION: The path for you file isn't correct, fix it and try again.");
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
        }

    }

}
