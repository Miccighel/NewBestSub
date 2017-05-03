package it.uniud.newbestsub.dataset;

import it.uniud.newbestsub.problem.BestSubsetSolution;
import it.uniud.newbestsub.utils.BestSubsetLogger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import org.uma.jmetal.util.fileoutput.SolutionListOutput;
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext;

import java.util.Comparator;
import java.util.List;

public class DatasetView {
    
    public BestSubsetLogger logger;

    public DatasetView() {
        logger = BestSubsetLogger.getInstance();
    }

    public void print(ImmutablePair<List<BestSubsetSolution>,Long> runResult, String outputPath) {

        logger.log("VIEW - Starting to print the result");

        List<BestSubsetSolution> population = runResult.left;
        long computingTime = runResult.right;

        population.sort((BestSubsetSolution sol1, BestSubsetSolution sol2) -> sol1.compareTo(sol2));

        new SolutionListOutput(population)
                .setSeparator(",")
                .setVarFileOutputContext(new DefaultFileOutputContext("res/" + outputPath + "_var.csv"))
                .setFunFileOutputContext(new DefaultFileOutputContext("res/" + outputPath + "_fun.csv"))
                .print();

        logger.log("VIEW - Algorithm computing time: " + computingTime/1000 + " seconds.");
        logger.log("VIEW - Finished to print the result");

    }

    public void print(ImmutableTriple<List<int[]>, int[], double[]> runResult, String outputPath) {

        logger.log("VIEW - Starting to print the result");

        List<int[]> population = runResult.left;
        int[] cardinalities = runResult.middle;
        double[] correlations = runResult.right;

        logger.log("VIEW - Finished to print the result");

    }

}
