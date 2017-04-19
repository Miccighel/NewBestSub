package it.uniud.newbestsub.problem;

import java.util.List;
import java.util.Map;

import org.uma.jmetal.problem.impl.AbstractBinaryProblem;
import org.uma.jmetal.solution.BinarySolution;

import it.uniud.newbestsub.problem.BestSubsetSolution;

public class BestSubsetProblem extends AbstractBinaryProblem {

    protected int totalNumberOfTopics;
    protected Map<String,double[]> averagePrecisions;

    public BestSubsetProblem(int totalNumberOfTopics, Map<String,double[]> averagePrecisions) {
        this.totalNumberOfTopics = totalNumberOfTopics;
        this.averagePrecisions = averagePrecisions;
    }

    public void evaluate (BinarySolution solution) {

    }

    public int getBitsPerVariable(int index) {
        return totalNumberOfTopics;
    }
}
