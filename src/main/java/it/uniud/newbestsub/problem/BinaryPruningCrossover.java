package it.uniud.newbestsub.problem;

import it.uniud.newbestsub.utils.BestSubsetLogger;
import org.uma.jmetal.operator.CrossoverOperator;
import org.uma.jmetal.util.pseudorandom.JMetalRandom;

import java.util.LinkedList;
import java.util.List;

public class BinaryPruningCrossover implements CrossoverOperator<BestSubsetSolution> {

    public double probability;
    private BestSubsetLogger logger;

    public BinaryPruningCrossover(double probability) {
        this.probability = probability;
        this.logger = BestSubsetLogger.getInstance();
    }

    public int getNumberOfParents(){
        return 2;
    }

    public List<BestSubsetSolution> execute (List<BestSubsetSolution> solutionList){

        BestSubsetSolution firstSolution = solutionList.get(0);
        BestSubsetSolution secondSolution = solutionList.get(1);

        boolean[] firstTopicStatus = firstSolution.getTopicStatus();
        boolean[] secondTopicStatus = secondSolution.getTopicStatus();

        List<BestSubsetSolution> childrenSolution = new LinkedList<BestSubsetSolution>();

        childrenSolution.add(new BestSubsetSolution(firstSolution));
        childrenSolution.add(new BestSubsetSolution(secondSolution));

        if(JMetalRandom.getInstance().nextDouble() < probability){

            logger.log("CROSSOVER: Starting to apply it");

            for(int i=0;i<firstTopicStatus.length;i++){
                childrenSolution.get(0).setBitValue(i,firstTopicStatus[i] && secondTopicStatus[i]);
                childrenSolution.get(1).setBitValue(i, firstTopicStatus[i] || secondTopicStatus[i]);
            }

            if(childrenSolution.get(0).getNumberOfSelectedTopics()==0) {
                int flipIndex = (int) Math.floor(JMetalRandom.getInstance().nextDouble()*childrenSolution.get(0).getNumberOfBits(0));
                if(flipIndex == childrenSolution.get(0).getNumberOfBits(0)){
                    flipIndex = flipIndex - 1;
                }
                childrenSolution.get(0).setBitValue(flipIndex, true);
            }

        }

        logger.log("CROSSOVER - Parent 1: " + firstSolution.getVariableValueString(0));
        logger.log("CROSSOVER - Number of selected topics: " + firstSolution.getNumberOfSelectedTopics());
        logger.log("CROSSOVER - Parent 2: " + secondSolution.getVariableValueString(0));
        logger.log("CROSSOVER - Number of selected topics: " + secondSolution.getNumberOfSelectedTopics());
        logger.log("CROSSOVER - Child 1: " + childrenSolution.get(0).getVariableValueString(0));
        logger.log("CROSSOVER - Number of selected topics: " + childrenSolution.get(0).getNumberOfSelectedTopics());
        logger.log("CROSSOVER - Child 2: " + childrenSolution.get(1).getVariableValueString(0));
        logger.log("CROSSOVER - Number of selected topics: " + childrenSolution.get(1).getNumberOfSelectedTopics());

        return childrenSolution;
    }
}
