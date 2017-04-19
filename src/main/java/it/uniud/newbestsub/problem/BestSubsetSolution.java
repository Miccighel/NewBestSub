package it.uniud.newbestsub.problem;

import org.uma.jmetal.problem.BinaryProblem;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.solution.impl.AbstractGenericSolution;
import org.uma.jmetal.util.binarySet.BinarySet;
import org.uma.jmetal.util.pseudorandom.JMetalRandom;
import org.uma.jmetal.util.pseudorandom.PseudoRandomGenerator;
import org.uma.jmetal.util.pseudorandom.impl.ExtendedPseudoRandomGenerator;

import java.util.Arrays;

public class BestSubsetSolution extends AbstractGenericSolution<BinarySet, BinaryProblem> implements BinarySolution {

    protected boolean topicStatus[];
    protected int numberOfSelectedTopics;

    public BestSubsetSolution(BinaryProblem problem, int totalNumberOfTopics) {
        super(problem) ;

        topicStatus = new boolean[totalNumberOfTopics];

        double columnKeepProbability = JMetalRandom.getInstance().nextDouble();
        numberOfSelectedTopics = 0;

        for(int i=0; i<totalNumberOfTopics; i++){
            double pointProbability = JMetalRandom.getInstance().nextDouble();
            if(pointProbability > columnKeepProbability){
                topicStatus[i] = true;
                numberOfSelectedTopics++;
            } else {
                topicStatus[i] = false;
            }
        }

        if(numberOfSelectedTopics == 0){
            int flipIndex = (int) Math.floor(JMetalRandom.getInstance().nextDouble()*topicStatus.length);
            if(flipIndex == topicStatus.length){
                flipIndex = flipIndex - 1;
            }
            topicStatus[flipIndex] = true;
            numberOfSelectedTopics++;
        }

        System.out.println("SOLUTION - Current Gene: " + Arrays.toString(topicStatus).replaceAll("true", "1").replaceAll("false","0").replaceAll(", ",""));
        System.out.println("SOLUTION - Number of selected topics: " + numberOfSelectedTopics);
    }

    public BestSubsetSolution(BestSubsetSolution solution) {
        super(solution.problem);
    }

    public BestSubsetSolution copy() {
        return new BestSubsetSolution(this);
    }

    public int getNumberOfBits(int index) {
        return topicStatus.length ;
    }

    public int getTotalNumberOfBits() {
        return topicStatus.length;
    }

    public String getVariableValueString(int index) {
        String toReturn = "";
        for (boolean status : topicStatus) {
            toReturn += status;
        }
        return toReturn;
    }

}
