package it.uniud.newbestsub.problem;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import it.uniud.newbestsub.utils.BestSubsetLogger;
import it.uniud.newbestsub.utils.Formula;

import org.uma.jmetal.problem.impl.AbstractBinaryProblem;

import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.solution.Solution;

import org.uma.jmetal.util.binarySet.BinarySet;

public class BestSubsetProblem extends AbstractBinaryProblem {

    protected Map<String,double[]> averagePrecisions;
    public double[] meanAveragePrecisions;
    protected int numberOfTopics;
    protected int systemSize;
    protected CorrelationStrategy<double[],double[],Double> correlationStrategy;
    protected TargetStrategy<Solution,Double> targetToAchieve;
    protected BestSubsetSolution solution;
    
    private BestSubsetLogger logger;

    public BestSubsetProblem(int numberOfTopics, Map<String,double[]> averagePrecisions, double[] meanAveragePrecisions, CorrelationStrategy<double[],double[],Double> correlationMethod, TargetStrategy<Solution,Double> targetToAchieve) {

        this.numberOfTopics = numberOfTopics;
        this.averagePrecisions = averagePrecisions;
        this.meanAveragePrecisions = meanAveragePrecisions;
        this.correlationStrategy = correlationMethod;
        this.targetToAchieve = targetToAchieve;

        setNumberOfVariables(1);
        setNumberOfObjectives(2);
        setName("BestSubsetProblem");

        logger = BestSubsetLogger.getInstance();
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

        logger.log("PROBLEM - Evaluating gene: "+ solution.getVariableValueString(0));
        logger.log("PROBLEM - Number of selected topics: "+ ((BestSubsetSolution) solution).getNumberOfSelectedTopics());

        double[] meanAveragePrecisionsReduced = new double[averagePrecisions.entrySet().size()];

        Iterator iterator = averagePrecisions.entrySet().iterator();
        int counter = 0;
        while(iterator.hasNext()) {
            Map.Entry<String,double[]> singleSystem = (Map.Entry<String,double[]>)iterator.next();
            meanAveragePrecisionsReduced[counter] = Formula.getMean(singleSystem.getValue(), topicStatusValues);
            counter++;
        }

        // logger.log("PROBLEM - Mean Average Precisions: " + Arrays.toString(meanAveragePrecisions));
        // logger.log("PROBLEM - Mean Average Precisions reduced: " + Arrays.toString(meanAveragePrecisionsReduced));

        double correlation = correlationStrategy.computeCorrelation(meanAveragePrecisionsReduced,meanAveragePrecisions);

        logger.log("PROBLEM - Correlation: " + correlation);

        targetToAchieve.setTarget((BestSubsetSolution)solution,correlation);

    }

}
