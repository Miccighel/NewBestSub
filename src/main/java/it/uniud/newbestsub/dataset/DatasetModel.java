package it.uniud.newbestsub.dataset;

import com.opencsv.CSVReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import it.uniud.newbestsub.problem.BestSubsetSolution;
import org.uma.jmetal.problem.BinaryProblem;
import org.uma.jmetal.algorithm.Algorithm;
import org.uma.jmetal.operator.Operator;
import org.uma.jmetal.solution.BinarySolution;

import it.uniud.newbestsub.problem.BestSubsetProblem;
import org.uma.jmetal.util.binarySet.BinarySet;

public class DatasetModel {

    public String[] systemLabels;
    public String[] topicLabels;
    public int topicSize;
    public Map<String, double[]> averagePrecisionsPerSystem = new LinkedHashMap<String, double[]>();
    public Map<String, double[]> averagePrecisionsPerTopic = new LinkedHashMap<String, double[]>();

    private BinaryProblem problem;
    private Algorithm<List<BinarySolution>> algorithm;
    private Operator crossover;
    private Operator mutation;
    private Operator selection;

    public DatasetModel() {}

    public DatasetModel getModel() {
        return this;
    }

    public void loadData(String datasetName) throws FileNotFoundException, IOException {

        // The parsing phase of the original .csv dataset file starts there.

        CSVReader reader = new CSVReader(new FileReader(datasetName));
        topicLabels = ((String[]) reader.readNext());

        int topicSize = topicLabels.length-1;

        String[] nextLine;
        while ((nextLine = reader.readNext()) != null) {
            String systemLabel = nextLine[0];
            double[] averagePrecisions = new double[nextLine.length - 1];
            for (int i = 1; i < nextLine.length; i++) {
                averagePrecisions[i - 1] = Double.parseDouble(nextLine[i]);
            }
            this.averagePrecisionsPerSystem.put(systemLabel, averagePrecisions);
        }

        systemLabels = new String[averagePrecisionsPerSystem.entrySet().size()];

        /* averagePrecisionsPerSystem is a <String,double[]> dictionary where, for each entry, the key is the system label
        and the value is an array that contains the AP values of a single system, for each topic. */

        double[][] averagePrecisionsPerSystemAsMatrix = new double[averagePrecisionsPerSystem.entrySet().size()][averagePrecisionsPerSystem.entrySet().iterator().next().getValue().length];

        Iterator iterator = averagePrecisionsPerSystem.entrySet().iterator();
        int counter = 0;
        while(iterator.hasNext()) {
            Map.Entry<String,double[]> singleSystem = (Map.Entry<String,double[]>)iterator.next();
            averagePrecisionsPerSystemAsMatrix[counter] = singleSystem.getValue();
            systemLabels[counter] = singleSystem.getKey();
            counter++;
        }

        double[][] transposedMatrix = new double[averagePrecisionsPerSystemAsMatrix[0].length][averagePrecisionsPerSystemAsMatrix.length];
        for (int i = 0; i < averagePrecisionsPerSystemAsMatrix.length; i++)
            for (int j = 0; j < averagePrecisionsPerSystemAsMatrix[0].length; j++)
                transposedMatrix[j][i] = averagePrecisionsPerSystemAsMatrix[i][j];

        /* The above code is needed to traspose the average precision values collected to "move"
         from a "average precision values for each topic" perspective to a "average precision values for each system"
         perspective. */

        for(int i=1; i<topicLabels.length;i++) {
            this.averagePrecisionsPerTopic.put(topicLabels[i],transposedMatrix[i-1]);
        }

        /* averagePrecisionsPerTopic is a <String,double[]> dictionary where, for each entry, the key is the topic label
        and the value is an array that contains the AP values for of a single topic, for each system. */

        /* In the loading phase there is an extensive use of the Map data structure. This has been done to do not lose
        the system and topic labels, which maybe will be useful in the future. */

        String[] labels = new String[topicLabels.length-1];
        for(int i=1;i<topicLabels.length;i++){
            labels[i-1] = topicLabels[i];
        }
        topicLabels = labels;

        /* The first label is stripped from the topic labels array because it's a fake label. */
    }

    public void solve() {

        problem = new BestSubsetProblem(averagePrecisionsPerTopic.entrySet().size(), averagePrecisionsPerTopic);
        System.out.println(problem.createSolution().getVariableValueString(0));

    }

}
