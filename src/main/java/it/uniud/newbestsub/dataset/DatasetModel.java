package it.uniud.newbestsub.dataset;

import com.opencsv.CSVReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import it.uniud.newbestsub.problem.BestSubsetSolution;
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder;
import org.uma.jmetal.operator.CrossoverOperator;
import org.uma.jmetal.operator.SelectionOperator;
import org.uma.jmetal.problem.BinaryProblem;
import org.uma.jmetal.problem.Problem;
import org.uma.jmetal.algorithm.Algorithm;
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAII;
import org.uma.jmetal.operator.Operator;
import org.uma.jmetal.operator.impl.crossover.SBXCrossover;
import org.uma.jmetal.operator.impl.mutation.PolynomialMutation;
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.util.AlgorithmRunner;
import org.uma.jmetal.util.JMetalLogger;
import org.uma.jmetal.util.ProblemUtils;
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator;


import it.uniud.newbestsub.problem.BestSubsetProblem;

import static org.uma.jmetal.runner.AbstractAlgorithmRunner.printFinalSolutionSet;
import static org.uma.jmetal.runner.AbstractAlgorithmRunner.printQualityIndicators;

public class DatasetModel {

    String[] topicLabels;
    Map<String, double[]> averagePrecisions = new LinkedHashMap<String, double[]>();

    BinaryProblem problem;
    Algorithm<List<BinarySolution>> algorithm;
    Operator crossover;
    Operator mutation;
    SelectionOperator selection;

    public DatasetModel() {}

    public DatasetModel getModel() {
        return this;
    }

    public void loadData(String datasetName) throws FileNotFoundException, IOException {

        // The parsing phase of the original .csv dataset file starts there

        CSVReader reader = new CSVReader(new FileReader(datasetName));
        topicLabels = ((String[]) reader.readNext());
        String[] nextLine;
        while ((nextLine = reader.readNext()) != null) {
            String systemLabel = nextLine[0];
            double[] averagePrecisions = new double[nextLine.length - 1];
            for (int i = 1; i < nextLine.length; i++) {
                averagePrecisions[i - 1] = Double.parseDouble(nextLine[i]);
            }
            this.averagePrecisions.put(systemLabel, averagePrecisions);
        }

        /* averagePrecisions is a <String,double[]> dictionary where, for each entry, the key is the system label
        and the value is an array that contains the AP values of the system for each topic */

    }

    public void solve() {

        problem = new BestSubsetProblem(averagePrecisions.entrySet().size(), averagePrecisions);

    }

}
