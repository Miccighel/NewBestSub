package it.uniud.newbestsub.problem;

import org.uma.jmetal.operator.CrossoverOperator;
import org.uma.jmetal.util.pseudorandom.JMetalRandom;

import java.util.LinkedList;
import java.util.List;

public class BinaryPruningCrossover implements CrossoverOperator<BestSubsetSolution> {

    public double probability;

    public BinaryPruningCrossover(double probability) {
        this.probability = probability;
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

            System.out.println("CROSSOVER: Starting to apply it");

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

        System.out.println("CROSSOVER - Parent 1: " + firstSolution.getVariableValueString(0));
        System.out.println("CROSSOVER - Number of selected topics: " + firstSolution.getNumberOfSelectedTopics());
        System.out.println("CROSSOVER - Parent 2: " + secondSolution.getVariableValueString(0));
        System.out.println("CROSSOVER - Number of selected topics: " + secondSolution.getNumberOfSelectedTopics());
        System.out.println("CROSSOVER - Child 1: " + childrenSolution.get(0).getVariableValueString(0));
        System.out.println("CROSSOVER - Number of selected topics: " + childrenSolution.get(0).getNumberOfSelectedTopics());
        System.out.println("CROSSOVER - Child 2: " + childrenSolution.get(1).getVariableValueString(0));
        System.out.println("CROSSOVER - Number of selected topics: " + childrenSolution.get(1).getNumberOfSelectedTopics());

        return childrenSolution;
    }
}
