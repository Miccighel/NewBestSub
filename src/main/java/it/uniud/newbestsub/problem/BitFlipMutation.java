package it.uniud.newbestsub.problem;

import it.uniud.newbestsub.utils.BestSubsetLogger;
import it.uniud.newbestsub.utils.Formula;
import org.uma.jmetal.operator.MutationOperator;
import org.uma.jmetal.util.pseudorandom.JMetalRandom;
import org.uma.jmetal.util.binarySet.BinarySet;

public class BitFlipMutation implements MutationOperator<BestSubsetSolution> {

    public double probability;
    private BestSubsetLogger logger;

    public BitFlipMutation(double probability) {
        this.probability = probability;
        this.logger = BestSubsetLogger.getInstance();
    }

    public BestSubsetSolution execute (BestSubsetSolution solution){

        BinarySet topicStatus = solution.getVariableValue(0);
        int totalNumberOfTopics = solution.getNumberOfBits(0);
        String oldGene = solution.getVariableValueString(0);

        logger.log("MUTATION - (Pre) Gene: " + solution.getVariableValueString(0));
        logger.log("MUTATION - (Pre) Number of selected topics: " + solution.getNumberOfSelectedTopics());

        if(JMetalRandom.getInstance().nextDouble() < probability) {

            int flipIndex = (int) Math.floor(JMetalRandom.getInstance().nextDouble()*totalNumberOfTopics);
            if(flipIndex == totalNumberOfTopics){
                flipIndex = flipIndex - 1;
            }

            solution.setBitValue(flipIndex,!topicStatus.get(flipIndex));

            if(solution.getNumberOfSelectedTopics() == 0){
                flipIndex = (int) Math.floor(JMetalRandom.getInstance().nextDouble()*totalNumberOfTopics);
                if(flipIndex == totalNumberOfTopics){
                    flipIndex = flipIndex - 1;
                }
                solution.setBitValue(flipIndex,!topicStatus.get(flipIndex));
            }

        }

        String newGene = solution.getVariableValueString(0);

        logger.log("MUTATION - (Post) Gene: " + solution.getVariableValueString(0));
        logger.log("MUTATION - (Post) Number of selected topics: " + solution.getNumberOfSelectedTopics());
        logger.log("MUTATION - Hamming distance: " + Formula.stringComparison(oldGene,newGene));

        return solution;

    }

}
