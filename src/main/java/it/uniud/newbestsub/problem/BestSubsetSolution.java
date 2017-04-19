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
        initializeObjectiveValues();

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

        setVariableValue(0, createNewBitSet(topicStatus.length,topicStatus));

        System.out.println("SOLUTION - Current Gene: " + Arrays.toString(topicStatus).replaceAll("true", "1").replaceAll("false","0").replaceAll(", ",""));
        System.out.println("SOLUTION - Number of selected topics: " + numberOfSelectedTopics);
    }

    public BestSubsetSolution(BestSubsetSolution solution) {
        super(solution.problem);
        for (int i = 0; i < problem.getNumberOfVariables(); i++) {
            setVariableValue(i, (BinarySet) solution.getVariableValue(i).clone());
        }

        for (int i = 0; i < problem.getNumberOfObjectives(); i++) {
            setObjective(i, solution.getObjective(i)) ;
        }
    }

    public BestSubsetSolution copy() {
        return new BestSubsetSolution(this);
    }

    public int getNumberOfBits(int index) {
        return getVariableValue(index).getBinarySetLength() ;
    }

    public int getTotalNumberOfBits() {
        int sum = 0 ;
        for (int i = 0; i < getNumberOfVariables(); i++) {
            sum += getVariableValue(i).getBinarySetLength() ;
        }
        return sum ;
    }

    public String getVariableValueString(int index) {
        String toReturn = "";
        for (boolean status : topicStatus) {
            if(status) {
                toReturn += "1";
            } else {
                toReturn += "0";
            }
        }
        return toReturn;
    }

    private BinarySet createNewBitSet(int numberOfBits, boolean[] values) {
        BinarySet bitSet = new BinarySet(numberOfBits) ;
        for (int i = 0; i < numberOfBits; i++) {
            if(values[i]){
                bitSet.set(i);
            } else {
                bitSet.clear(i);
            }
        }
        return bitSet ;
    }
}
