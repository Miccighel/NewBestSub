package it.uniud.newbestsub.problem;

import it.uniud.newbestsub.utils.BestSubsetLogger;
import org.uma.jmetal.operator.CrossoverOperator;
import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.util.pseudorandom.JMetalRandom;

import java.util.LinkedList;
import java.util.List;

public class BinaryPruningCrossover implements CrossoverOperator<BinarySolution> {

    public double probability;
    private BestSubsetLogger logger;

    public BinaryPruningCrossover(double probability) {
        this.probability = probability;
        this.logger = BestSubsetLogger.getInstance();
    }

    public int getNumberOfParents(){
        return 2;
    }

    public List<BinarySolution> execute (List<BinarySolution> solutionList){

        BestSubsetSolution firstSolution = (BestSubsetSolution)solutionList.get(0);
        BestSubsetSolution secondSolution =  (BestSubsetSolution)solutionList.get(1);

        boolean[] firstTopicStatus = firstSolution.getTopicStatus();
        boolean[] secondTopicStatus = secondSolution.getTopicStatus();

        List<BinarySolution> childrenSolution = new LinkedList<BinarySolution>();

        childrenSolution.add(new BestSubsetSolution(firstSolution));
        childrenSolution.add(new BestSubsetSolution(secondSolution));

        BestSubsetSolution firstChildren =  ((BestSubsetSolution)childrenSolution.get(0));
        BestSubsetSolution secondChildren =  ((BestSubsetSolution)childrenSolution.get(1));

        if(JMetalRandom.getInstance().nextDouble() < probability){

            logger.log("CROSSOVER: Starting to apply it");

            for(int i=0;i<firstTopicStatus.length;i++){
               firstChildren.setBitValue(i,firstTopicStatus[i] && secondTopicStatus[i]);
               secondChildren.setBitValue(i, firstTopicStatus[i] || secondTopicStatus[i]);
            }

            if(firstChildren.getNumberOfSelectedTopics()==0) {
                int flipIndex = (int) Math.floor(JMetalRandom.getInstance().nextDouble()*firstChildren.getNumberOfBits(0));
                if(flipIndex == firstChildren.getNumberOfBits(0)){
                    flipIndex = flipIndex - 1;
                }
                firstChildren.setBitValue(flipIndex, true);
            }

        }

        childrenSolution.set(0, firstChildren);
        childrenSolution.set(1, secondChildren);

        logger.log("CROSSOVER - Parent 1: " + firstSolution.getVariableValueString(0));
        logger.log("CROSSOVER - Number of selected topics: " + firstSolution.getNumberOfSelectedTopics());
        logger.log("CROSSOVER - Parent 2: " + secondSolution.getVariableValueString(0));
        logger.log("CROSSOVER - Number of selected topics: " + secondSolution.getNumberOfSelectedTopics());
        logger.log("CROSSOVER - Child 1: " + firstChildren.getVariableValueString(0));
        logger.log("CROSSOVER - Number of selected topics: " + firstChildren.getNumberOfSelectedTopics());
        logger.log("CROSSOVER - Child 2: " + secondChildren.getVariableValueString(0));
        logger.log("CROSSOVER - Number of selected topics: " + secondChildren.getNumberOfSelectedTopics());

        return childrenSolution;
    }
}
