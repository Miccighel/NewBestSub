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
    protected CorrelationStrategy<double[],double[],Double> correlationStrategy;
    protected TargetStrategy<BestSubsetSolution,Double> targetToAchieve;
    BestSubsetSolution solution;

    public BestSubsetProblem(int numberOfTopics, Map<String,double[]> averagePrecisions, CorrelationStrategy<double[],double[],Double> correlationMethod, TargetStrategy<BestSubsetSolution,Double> targetToAchieve) {

        this.numberOfTopics = numberOfTopics;
        this.averagePrecisions = averagePrecisions;
        this.correlationStrategy = correlationMethod;
        this.targetToAchieve = targetToAchieve;

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
        return solution.getNumberOfBits(0);
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

        System.out.println("PROBLEM - Evaluating gene: "+ solution.getVariableValueString(0));
        System.out.println("PROBLEM - Number of selected topics: "+ ((BestSubsetSolution) solution).getNumberOfSelectedTopics());

        double[] meanAveragePrecisionsReduced = new double[averagePrecisions.entrySet().size()];

        Iterator iterator = averagePrecisions.entrySet().iterator();
        int counter = 0;
        while(iterator.hasNext()) {
            Map.Entry<String,double[]> singleSystem = (Map.Entry<String,double[]>)iterator.next();
            meanAveragePrecisionsReduced[counter] = getMean(singleSystem.getValue(), topicStatusValues);
            counter++;
        }

        // System.out.println("PROBLEM - Mean Average Precisions: " + Arrays.toString(meanAveragePrecisions));
        // System.out.println("PROBLEM - Mean Average Precisions reduced: " + Arrays.toString(meanAveragePrecisionsReduced));

        double correlation = correlationStrategy.computeCorrelation(meanAveragePrecisionsReduced,meanAveragePrecisions);

        System.out.println("PROBLEM - Correlation: " + correlation);

        targetToAchieve.setTarget((BestSubsetSolution)solution,correlation);

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
