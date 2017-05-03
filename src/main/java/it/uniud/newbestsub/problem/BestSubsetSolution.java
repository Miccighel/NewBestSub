package it.uniud.newbestsub.problem;

import it.uniud.newbestsub.utils.BestSubsetLogger;
import org.uma.jmetal.problem.BinaryProblem;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.solution.impl.AbstractGenericSolution;
import org.uma.jmetal.util.binarySet.BinarySet;
import org.uma.jmetal.util.pseudorandom.JMetalRandom;
import org.uma.jmetal.util.pseudorandom.PseudoRandomGenerator;
import org.uma.jmetal.util.pseudorandom.impl.ExtendedPseudoRandomGenerator;

import java.util.Arrays;

public class BestSubsetSolution extends AbstractGenericSolution<BinarySet, BinaryProblem> implements BinarySolution, Comparable<BestSubsetSolution> {

    protected boolean topicStatus[];
    protected int numberOfSelectedTopics;
    private BestSubsetLogger logger;

    public BestSubsetSolution(BinaryProblem problem, int numberOfTopics) {
        super(problem);
        initializeObjectiveValues();

        logger = BestSubsetLogger.getInstance();
        
        topicStatus = new boolean[numberOfTopics];
        numberOfSelectedTopics = 0;

        double columnKeepProbability = JMetalRandom.getInstance().nextDouble();

        for (int i = 0; i < numberOfTopics; i++) {
            double pointProbability = JMetalRandom.getInstance().nextDouble();
            if (pointProbability > columnKeepProbability) {
                topicStatus[i] = true;
                numberOfSelectedTopics++;
            } else {
                topicStatus[i] = false;
            }
        }

        if (numberOfSelectedTopics == 0) {
            int flipIndex = (int) Math.floor(JMetalRandom.getInstance().nextDouble() * topicStatus.length);
            if (flipIndex == topicStatus.length) {
                flipIndex = flipIndex - 1;
            }
            topicStatus[flipIndex] = true;
            numberOfSelectedTopics++;
        }

        setVariableValue(0, createNewBitSet(topicStatus.length, topicStatus));

        logger.log("SOLUTION - (New) Gene: " + getVariableValueString(0));
        logger.log("SOLUTION - (New) Number of selected topics: " + numberOfSelectedTopics);
    }

    public BestSubsetSolution(BestSubsetSolution solution) {
        super(solution.problem);

        topicStatus = solution.topicStatus;
        numberOfSelectedTopics = solution.numberOfSelectedTopics;

        for (int i = 0; i < problem.getNumberOfVariables(); i++) {
            setVariableValue(i, (BinarySet) solution.getVariableValue(i).clone());
        }

        for (int i = 0; i < problem.getNumberOfObjectives(); i++) {
            setObjective(i, solution.getObjective(i));
        }
    }

    public BestSubsetSolution copy() {
        return new BestSubsetSolution(this);
    }

    private BinarySet createNewBitSet(int numberOfBits, boolean[] values) {
        BinarySet bitSet = new BinarySet(numberOfBits);
        for (int i = 0; i < numberOfBits; i++) {
            if (values[i]) {
                bitSet.set(i);
            } else {
                bitSet.clear(i);
            }
        }
        return bitSet;
    }

    public boolean[] getTopicStatus() {
        boolean[] topicStatusValues = new boolean[getVariableValue(0).getBinarySetLength()];
        for (int i=0;i<topicStatusValues.length;i++){
            topicStatusValues[i] = getVariableValue(0).get(i);
        }
        return topicStatusValues;
    }

    public int getNumberOfSelectedTopics() {
        return numberOfSelectedTopics;
    }

    public void setBitValue(int index, boolean value) {
        BinarySet topicStatusValues = getVariableValue(0);
        if(topicStatusValues.get(index) != value) {
            topicStatusValues.set(index, value);
            if(value){
                numberOfSelectedTopics++;
            } else {
                numberOfSelectedTopics--;
            }
        }
        setVariableValue(0, topicStatusValues);
    }

    public int getNumberOfBits(int index) {
        return getVariableValue(index).getBinarySetLength();
    }

    public int getTotalNumberOfBits() {
        int sum = 0;
        for (int i = 0; i < getNumberOfVariables(); i++) {
            sum += getVariableValue(i).getBinarySetLength();
        }
        return sum;
    }

    public String getVariableValueString(int index) {
        String toReturn = "";
        BinarySet topicStatusValues = getVariableValue(0);
        for (int i=0;i<topicStatusValues.getBinarySetLength();i++){
            if(topicStatusValues.get(i)){
                toReturn += "1";
            } else {
                toReturn += "0";
            }
        }
        return toReturn;
    }

    public int compareTo(BestSubsetSolution anotherSolution) {
        if (this.getNumberOfSelectedTopics() > anotherSolution.getNumberOfSelectedTopics()){
            return 1;
        } else {
            if(this.getNumberOfSelectedTopics() == anotherSolution.getNumberOfSelectedTopics()){
                return 0;
            } else {
                return -1;
            }
        }
    }

}
