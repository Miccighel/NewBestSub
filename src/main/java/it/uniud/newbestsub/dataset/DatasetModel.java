package it.uniud.newbestsub.dataset;

import com.opencsv.CSVReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.util.*;

import it.uniud.newbestsub.utils.BestSubsetLogger;
import it.uniud.newbestsub.utils.Formula;
import it.uniud.newbestsub.problem.*;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.math3.stat.correlation.KendallsCorrelation;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import org.uma.jmetal.problem.BinaryProblem;
import org.uma.jmetal.problem.Problem;

import org.uma.jmetal.operator.CrossoverOperator;
import org.uma.jmetal.operator.MutationOperator;
import org.uma.jmetal.operator.SelectionOperator;
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection;

import org.uma.jmetal.algorithm.Algorithm;
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder;

import org.uma.jmetal.solution.BinarySolution;
import org.uma.jmetal.solution.Solution;
import org.uma.jmetal.util.AlgorithmRunner;
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator;

public class DatasetModel {

    public String[] systemLabels;
    public String[] topicLabels;
    public int numberOfSystems;
    public int systemSize;
    public int numberOfTopics;
    public int topicSize;
    public Map<String, double[]> averagePrecisions = new LinkedHashMap<String, double[]>();
    public double[] meanAveragePrecisions;
    public Map<String, Map<Double, Integer>> topicsSetsDistribution = new LinkedHashMap<String, Map<Double, Integer>>();

    private Problem<BinarySolution> problem;
    private CrossoverOperator<BinarySolution> crossover;
    private MutationOperator<BinarySolution> mutation;
    private SelectionOperator<List<BinarySolution>, BinarySolution> selection;
    private List<Solution> population;
    private NSGAIIBuilder builder;
    private Algorithm<List<Solution>> algorithm;
    private AlgorithmRunner algorithmRunner;
    private long computingTime;

    private BestSubsetLogger logger;

    public DatasetModel() {
        logger = BestSubsetLogger.getInstance();
    }

    public DatasetModel retrieveModel() {
        return this;
    }

    public void loadData(String datasetPath) throws FileNotFoundException, IOException {

        // The parsing phase of the original .csv dataset file starts there.

        CSVReader reader = new CSVReader(new FileReader(datasetPath));
        topicLabels = ((String[]) reader.readNext());

        numberOfTopics = topicLabels.length - 1;

        logger.log("MODEL - Total number of topics: " + numberOfTopics);

        String[] nextLine;
        double[] averagePrecisions = new double[0];
        while ((nextLine = reader.readNext()) != null) {
            String systemLabel = nextLine[0];
            averagePrecisions = new double[nextLine.length - 1];
            for (int i = 1; i < nextLine.length; i++) {
                averagePrecisions[i - 1] = Double.parseDouble(nextLine[i]);
            }
            this.averagePrecisions.put(systemLabel, averagePrecisions);
        }

        systemLabels = new String[this.averagePrecisions.entrySet().size()];
        numberOfSystems = this.averagePrecisions.entrySet().size();
        systemSize = averagePrecisions.length;

        /* averagePrecisions is a <String,double[]> dictionary where, for each entry, the key is the system label
        and the value is an array that contains the AP values of a single system, for each topic. */

        double[][] averagePrecisionsPerSystemAsMatrix = new double[this.averagePrecisions.entrySet().size()][this.averagePrecisions.entrySet().iterator().next().getValue().length];

        Iterator iterator = this.averagePrecisions.entrySet().iterator();
        int counter = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, double[]> singleSystem = (Map.Entry<String, double[]>) iterator.next();
            averagePrecisionsPerSystemAsMatrix[counter] = singleSystem.getValue();
            systemLabels[counter] = singleSystem.getKey();
            counter++;
        }

        logger.log("MODEL - Total number of systems: " + this.averagePrecisions.entrySet().size());

        /* In the loading phase there is an extensive use of the Map data structure. This has been done to do not lose
        the system and topic labels, which maybe will be useful in the future. */

        String[] labels = new String[topicLabels.length - 1];
        for (int i = 1; i < topicLabels.length; i++) {
            labels[i - 1] = topicLabels[i];
        }
        topicLabels = labels;

        /* The first label is stripped from the topic labels array because it's a fake label. */

        meanAveragePrecisions = new double[this.averagePrecisions.entrySet().size()];

        boolean[] useColumns = new boolean[numberOfTopics];
        Arrays.fill(useColumns, Boolean.TRUE);

        iterator = this.averagePrecisions.entrySet().iterator();
        counter = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, double[]> singleSystem = (Map.Entry<String, double[]>) iterator.next();
            meanAveragePrecisions[counter] = Formula.getMean(singleSystem.getValue(), useColumns);
            this.systemSize = (this.systemSize == 0) ? singleSystem.getValue().length : this.systemSize;
            counter++;
        }

    }

    private CorrelationStrategy<double[], double[], Double> loadCorrelationStrategy(String chosenCorrelationMethod) {

        CorrelationStrategy<double[], double[], Double> correlationMethod;

        switch (chosenCorrelationMethod) {
            case "Pearson":
                correlationMethod = (firstArray, secondArray) -> {
                    PearsonsCorrelation pcorr = new PearsonsCorrelation();
                    return pcorr.correlation(firstArray, secondArray);
                };
                break;
            case "Kendall":
                correlationMethod = (firstArray, secondArray) -> {
                    KendallsCorrelation pcorr = new KendallsCorrelation();
                    return pcorr.correlation(firstArray, secondArray);
                };
                break;
            default:
                correlationMethod = (firstArray, secondArray) -> {
                    PearsonsCorrelation pcorr = new PearsonsCorrelation();
                    return pcorr.correlation(firstArray, secondArray);

                };
                break;
        }

        return correlationMethod;
    }

    private TargetStrategy<Solution, Double> loadTargetStrategy(String targetToAchieve) {

        TargetStrategy<Solution, Double> targetStrategy;

        switch (targetToAchieve) {
            case "Best":
                targetStrategy = (solution, correlation) -> {
                    solution.setObjective(0, correlation * -1);
                    solution.setObjective(1, ((BestSubsetSolution) solution).getNumberOfSelectedTopics());
                };
                break;
            case "Worst":
                targetStrategy = (solution, correlation) -> {
                    solution.setObjective(0, correlation);
                    solution.setObjective(1, ((BestSubsetSolution) solution).getNumberOfSelectedTopics() * -1);
                };
                break;
            default:
                targetStrategy = (solution, correlation) -> {
                    solution.setObjective(0, correlation * -1);
                    solution.setObjective(1, ((BestSubsetSolution) solution).getNumberOfSelectedTopics());
                };
                break;
        }

        return targetStrategy;
    }

    public ImmutablePair<List<Solution>, Long> solve(String chosenCorrelationMethod, String targetToAchieve, int numberOfIterations) {

        CorrelationStrategy<double[], double[], Double> correlationStrategy = this.loadCorrelationStrategy(chosenCorrelationMethod);
        TargetStrategy<Solution, Double> targetStrategy = this.loadTargetStrategy(targetToAchieve);

        if (targetToAchieve.equals("Average")) {

            List<boolean[]> variableValues = new LinkedList<boolean[]>();
            int[] cardinalities = new int[numberOfTopics];
            double[] correlations = new double[numberOfTopics];

            Random generator = new Random();

            for (int currentCardinality = 0; currentCardinality < numberOfTopics; currentCardinality++) {

                Set<Integer> topicToChoose = new HashSet<Integer>();
                while (topicToChoose.size() < currentCardinality + 1) {
                    Integer next = generator.nextInt(numberOfTopics) + 1;
                    topicToChoose.add(next);
                }

                boolean[] topicStatus = new boolean[numberOfTopics];

                Iterator iterator = topicToChoose.iterator();
                while (iterator.hasNext()) {
                    int chosenTopic = (int) iterator.next();
                    topicStatus[chosenTopic - 1] = true;
                }

                String toString = "";
                for (int j = 0; j < topicStatus.length; j++) {
                    if (topicStatus[j]) {
                        toString += 1;
                    } else {
                        toString += 0;
                    }
                }

                logger.log("PROBLEM - Evaluating gene: " + toString);
                logger.log("PROBLEM - Number of selected topics: " + currentCardinality);

                double[] meanAveragePrecisionsReduced = new double[averagePrecisions.entrySet().size()];

                iterator = averagePrecisions.entrySet().iterator();
                int counter = 0;
                while (iterator.hasNext()) {
                    Map.Entry<String, double[]> singleSystem = (Map.Entry<String, double[]>) iterator.next();
                    meanAveragePrecisionsReduced[counter] = Formula.getMean(singleSystem.getValue(), topicStatus);
                    counter++;
                }

                double correlation = correlationStrategy.computeCorrelation(meanAveragePrecisionsReduced, meanAveragePrecisions);

                logger.log("PROBLEM - Correlation: " + correlation);

                cardinalities[currentCardinality] = currentCardinality + 1;
                correlations[currentCardinality] = correlation;
                variableValues.add(topicStatus);

            }

            problem = new BestSubsetProblem(numberOfTopics, averagePrecisions, meanAveragePrecisions, correlationStrategy, targetStrategy);
            population = new LinkedList<Solution>();

            for (int i = 0; i < numberOfTopics; i++) {
                BestSubsetSolution solution = new BestSubsetSolution((BinaryProblem) problem, numberOfTopics);
                solution.setVariableValue(0, solution.createNewBitSet(numberOfTopics, variableValues.get(i)));
                solution.setObjective(0, correlations[i]);
                solution.setObjective(1, cardinalities[i]);
                population.add(solution);
            }

            population.sort((Solution sol1, Solution sol2) -> ((BestSubsetSolution) sol1).compareTo((BestSubsetSolution) sol2));

        } else {

            problem = new BestSubsetProblem(numberOfTopics, averagePrecisions, meanAveragePrecisions, correlationStrategy, targetStrategy);
            crossover = new BinaryPruningCrossover(0.9);
            mutation = new BitFlipMutation(1);
            selection = new BinaryTournamentSelection<BinarySolution>(new RankingAndCrowdingDistanceComparator<BinarySolution>());
            builder = new NSGAIIBuilder<BinarySolution>(problem, crossover, mutation);
            algorithm = builder.setSelectionOperator(selection)
                    .setMaxEvaluations(numberOfIterations)
                    .setPopulationSize(averagePrecisions.size())
                    .build();
            algorithmRunner = new AlgorithmRunner.Executor(algorithm).execute();
            computingTime = algorithmRunner.getComputingTime();
            population = algorithm.getResult();

            population.sort((Solution sol1, Solution sol2) -> ((BestSubsetSolution) sol1).compareTo((BestSubsetSolution) sol2));

            switch (targetToAchieve) {
                case "Best":
                    for (int i = 0; i < population.size(); i++) {
                        Solution solutionToFix = population.get(i);
                        double correlationToFix = solutionToFix.getObjective(0) * -1;
                        solutionToFix.setObjective(0, correlationToFix);
                        population.set(i, solutionToFix);
                    }
                    break;
                case "Worst":
                    for (int i = 0; i < population.size(); i++) {
                        Solution solutionToFix = population.get(i);
                        double cardinalityToFix = solutionToFix.getObjective(1) * -1;
                        solutionToFix.setObjective(1, cardinalityToFix);
                        population.set(i, solutionToFix);
                    }
                    break;
            }
        }

        for (int i = 0; i < population.size(); i++) {
            Solution solutionToAnalyze = population.get(i);
            boolean[] topicStatus = ((BestSubsetSolution) solutionToAnalyze).getTopicStatus();
            double cardinality = solutionToAnalyze.getObjective(1);
            for (int j = 0; j < topicStatus.length; j++) {
                Map<Double, Integer> distributionsPerCardinalities = topicsSetsDistribution.get(topicLabels[j]);
                if (distributionsPerCardinalities == null) {
                    distributionsPerCardinalities = new TreeMap<Double, Integer>();
                }
                int distributionPerCardinality;
                try {
                    distributionPerCardinality = distributionsPerCardinalities.get(cardinality);
                } catch (NullPointerException e) {
                    distributionPerCardinality = 0;
                }
                if (topicStatus[j]) {
                    distributionPerCardinality++;
                }
                distributionsPerCardinalities.put(cardinality, distributionPerCardinality);
                topicsSetsDistribution.put(topicLabels[j], distributionsPerCardinalities);
            }
        }

        return new ImmutablePair<List<Solution>, Long>(population, computingTime);

    }

}
