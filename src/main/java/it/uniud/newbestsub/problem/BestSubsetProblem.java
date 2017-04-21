package it.uniud.newbestsub.problem;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import org.uma.jmetal.problem.impl.AbstractBinaryProblem;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.util.binarySet.BinarySet;

public class BestSubsetProblem extends AbstractBinaryProblem {

    protected Map<String,double[]> averagePrecisions;
    public double[] meanAveragePrecisions;
    protected int numberOfTopics;
    protected int systemSize;
    CorrelationStrategy<double[],double[],Double> correlationStrategy;
    BestSubsetSolution solution;

    public BestSubsetProblem(int numberOfTopics, Map<String,double[]> averagePrecisions, CorrelationStrategy<double[],double[],Double> correlationStrategy) {

        this.numberOfTopics = numberOfTopics;
        this.averagePrecisions = averagePrecisions;
        this.correlationStrategy = correlationStrategy;

        setNumberOfVariables(1);
        setNumberOfObjectives(2);
        setName("BestSubsetProblem");

        meanAveragePrecisions = new double[averagePrecisions.entrySet().size()];

        boolean[] useColumns = new boolean[numberOfTopics];
        Arrays.fill(useColumns, Boolean.TRUE);

        Iterator iterator = averagePrecisions.entrySet().iterator();
        int counter = 0;
        while(iterator.hasNext()) {
            Map.Entry<String,double[]> singleSystem = (Map.Entry<String,double[]>)iterator.next();
            meanAveragePrecisions[counter] = getMean(singleSystem.getValue(), useColumns);
            this.systemSize =  (this.systemSize==0) ? singleSystem.getValue().length : this.systemSize;
            counter++;
        }
    }

    public int getBitsPerVariable(int index) {
        return solution.getVariableValue(index).getBinarySetLength();
    }

    public BestSubsetSolution createSolution() {
        solution = new BestSubsetSolution(this, numberOfTopics)  ;
        return solution;
    }

    public void evaluate (BinarySolution solution) {

        BinarySet topicsStatus = solution.getVariableValue(0);
        boolean[] topicStatusValues = new boolean [topicsStatus.getBinarySetLength()];
        for (int i=0;i<topicsStatus.getBinarySetLength();i++){
            topicStatusValues[i] = topicsStatus.get(i);
        }

        System.out.println("PROBLEM - Evaluating gene: "+ Arrays.toString(topicStatusValues).replaceAll("true", "1").replaceAll("false", "0").replaceAll(", ", ""));
        System.out.println("PROBLEM - Number of selected topics: "+ ((BestSubsetSolution) solution).getNumberOfSelectedTopics());

        double[] meanAveragePrecisionsReduced = new double[averagePrecisions.entrySet().size()];

        Iterator iterator = averagePrecisions.entrySet().iterator();
        int counter = 0;
        while(iterator.hasNext()) {
            Map.Entry<String,double[]> singleSystem = (Map.Entry<String,double[]>)iterator.next();
            meanAveragePrecisionsReduced[counter] = getMean(singleSystem.getValue(), topicStatusValues);
            counter++;
        }

        System.out.println("PROBLEM - Mean Average Precisions: " + Arrays.toString(meanAveragePrecisions));
        System.out.println("PROBLEM - Mean Average Precisions reduced: " + Arrays.toString(meanAveragePrecisionsReduced));

        double correlation = correlationStrategy.computeCorrelation(meanAveragePrecisionsReduced,meanAveragePrecisions);

        System.out.println("PROBLEM - Correlation: " + correlation);

        //MULTIPLY BY -1 IF YOU WANT TO MAXIMIZE; LEAVE AS IT IS IF YOU WANT TO MINIMIZE

        // Obiettivo 0: correlazione
        solution.setObjective(0,correlation*-1);
        // Obiettivo 1: minimizzare il numero di colonne selezionate
        solution.setObjective(1, ((BestSubsetSolution) solution).getNumberOfSelectedTopics());

    }

    protected double getMean(double[] run, boolean[] useColumns){

        double mean = 0.0;

        int numberOfUsedCols = 0;
        for(int i=0; i<useColumns.length; i++){
            if(useColumns[i]){
                numberOfUsedCols++;
            }
        }

        for(int i=0; i<run.length;i++){
            if(useColumns[i]){
                mean = mean + run[i];
            }
        }
        mean = mean / ((double) numberOfUsedCols);

        return mean;
    }

}
