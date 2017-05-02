package it.uniud.newbestsub.dataset;

import it.uniud.newbestsub.problem.BestSubsetSolution;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.uma.jmetal.util.fileoutput.SolutionListOutput;
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext;

import java.util.Comparator;
import java.util.List;

public class DatasetView {

    public DatasetView() {}

    public void print(ImmutablePair<List<BestSubsetSolution>,Long> runResult, String outputPath) {

        System.out.println("VIEW - Starting to print the result");

        List<BestSubsetSolution> population = runResult.left;
        long computingTime = runResult.right;

        population.sort((BestSubsetSolution sol1, BestSubsetSolution sol2) -> sol1.compareTo(sol2));

        new SolutionListOutput(population)
                .setSeparator(",")
                .setVarFileOutputContext(new DefaultFileOutputContext("res/" + outputPath + "_var.csv"))
                .setFunFileOutputContext(new DefaultFileOutputContext("res/" + outputPath + "_fun.csv"))
                .print();

        System.out.println("VIEW - Algorithm computing time: " + computingTime/1000 + " seconds.");
        System.out.println("VIEW - Finished to print the result");

    }

    public void print(int test, String outputPath) {
        System.out.println(test);
    }

}
