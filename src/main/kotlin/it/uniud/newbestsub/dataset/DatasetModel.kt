package it.uniud.newbestsub.dataset

import com.opencsv.CSVReader
import it.uniud.newbestsub.problem.BestSubsetProblem
import it.uniud.newbestsub.problem.BestSubsetSolution
import it.uniud.newbestsub.problem.BinaryPruningCrossover
import it.uniud.newbestsub.problem.BitFlipMutation
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools
import org.apache.commons.math3.stat.correlation.KendallsCorrelation
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.algorithm.Algorithm
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder
import org.uma.jmetal.operator.CrossoverOperator
import org.uma.jmetal.operator.MutationOperator
import org.uma.jmetal.operator.SelectionOperator
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection
import org.uma.jmetal.problem.BinaryProblem
import org.uma.jmetal.problem.Problem
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.util.AlgorithmRunner
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator
import java.io.FileReader
import java.util.*
import kotlin.collections.LinkedHashMap

class DatasetModel {

    var systemLabels = Array(0, { "" })
    var topicLabels = Array(0, { "" })
    var numberOfSystems = 0
    var numberOfTopics = 0
    var targetToAchieve = ""
    var correlationMethod = ""
    var averagePrecisions: MutableMap<String, DoubleArray> = LinkedHashMap()
    var meanAveragePrecisions = DoubleArray(0)
    var topicDistribution: MutableMap<String, MutableMap<Double, Boolean>> = LinkedHashMap()
    var computingTime: Long = 0

    private val logger = LogManager.getLogger()

    private lateinit var problem: Problem<BinarySolution>
    private lateinit var crossover: CrossoverOperator<BinarySolution>
    private lateinit var mutation: MutationOperator<BinarySolution>
    private lateinit var selection: SelectionOperator<List<BinarySolution>, BinarySolution>
    lateinit var population: MutableList<BinarySolution>
    private lateinit var builder: NSGAIIBuilder<BinarySolution>
    private lateinit var algorithm: Algorithm<List<BinarySolution>>
    private lateinit var algorithmRunner: AlgorithmRunner

    fun loadData(datasetPath: String) {

        // The parsing phase of the original .csv dataset file starts there.

        val reader = CSVReader(FileReader(datasetPath))
        topicLabels = reader.readNext()
        numberOfTopics = topicLabels.size - 1

        //BestSubsetLogger.log("MODEL - Total number of topics: $numberOfTopics")

        reader.readAll().forEach {
            nextLine ->
            val averagePrecisions = DoubleArray(nextLine.size - 1)
            (1..nextLine.size - 1).forEach { i -> averagePrecisions[i - 1] = java.lang.Double.parseDouble(nextLine[i]) }
            this.averagePrecisions.put(nextLine[0], averagePrecisions)
        }

        numberOfSystems = this.averagePrecisions.entries.size

        /* averagePrecisions is a <String,double[]> dictionary where, for each entry, the key is the system label
        and the value is an array that contains the AP values of a single system, for each topic. */

        val iterator = this.averagePrecisions.entries.iterator()
        systemLabels = Array(numberOfSystems, { _ -> iterator.next().key })

        //BestSubsetLogger.log("MODEL - Total number of systems: ${this.averagePrecisions.entries.size}")

        /* In the loading phase there is an extensive use of the Map data structure. This has been done to do not lose
        the system and topic labels, which maybe will be useful in the future. */

        topicLabels = Array(topicLabels.size, { i -> topicLabels[i] }).sliceArray(1..topicLabels.size - 1)

        /* The first label is stripped from the topic labels array because it's a fake label. */

        meanAveragePrecisions = DoubleArray(this.averagePrecisions.entries.size)

        val useColumns = BooleanArray(numberOfTopics)
        Arrays.fill(useColumns, true)

        this.averagePrecisions.entries.forEachIndexed {
            index, singleSystem ->
            meanAveragePrecisions[index] = Tools.getMean(singleSystem.value, useColumns)
        }

    }

    private fun loadCorrelationStrategy(correlationMethod: String): (DoubleArray, DoubleArray) -> Double {

        val pearsonCorrelation: (DoubleArray, DoubleArray) -> Double = {
            firstArray, secondArray ->
            val pcorr = PearsonsCorrelation()
            pcorr.correlation(firstArray, secondArray)
        }
        val kendallCorrelation: (DoubleArray, DoubleArray) -> Double = {
            firstArray, secondArray ->
            val pcorr = KendallsCorrelation()
            pcorr.correlation(firstArray, secondArray)
        }
        this.correlationMethod = correlationMethod
        when (correlationMethod) {
            "Pearson" -> return pearsonCorrelation
            "Kendall" -> return kendallCorrelation
            else -> return pearsonCorrelation
        }
    }

    private fun loadTargetStrategy(targetToAchieve: String): (BinarySolution, Double) -> BinarySolution {

        val bestStrategy: (BinarySolution, Double) -> BinarySolution = {
            solution, correlation ->
            solution.setObjective(0, correlation * -1)
            solution.setObjective(1, (solution as BestSubsetSolution).numberOfSelectedTopics.toDouble())
            solution
        }
        val worstStrategy: (BinarySolution, Double) -> BinarySolution = {
            solution, correlation ->
            solution.setObjective(0, correlation)
            solution.setObjective(1, ((solution as BestSubsetSolution).numberOfSelectedTopics * -1).toDouble())
            solution
        }
        this.targetToAchieve = targetToAchieve
        when (targetToAchieve) {
            "Best" -> return bestStrategy
            "Worst" -> return worstStrategy
            else -> return bestStrategy
        }
    }

    fun findCorrelationForCardinality(cardinality: Double): Double? {
        population.forEach { aSolution -> if (aSolution.getObjective(1) == cardinality) return aSolution.getObjective(0) }
        return null
    }

    fun isTopicInASolutionOfCardinality(topicLabel: String, cardinality: Double): Boolean {
        val answer = topicDistribution[topicLabel]?.get(cardinality)
        if (answer != null) return answer else return false
    }

    fun solve(parameters: Parameters): Pair<List<BinarySolution>, Triple<String, String, Long>> {

        val correlationStrategy = this.loadCorrelationStrategy(parameters.correlationMethod)
        val targetStrategy = this.loadTargetStrategy(parameters.targetToAchieve)

        logger.info("Computation started on \"${Thread.currentThread().name}\" with target \"${parameters.targetToAchieve}\". Wait please...")

        if (targetToAchieve == "Average") {

            val variableValues = LinkedList<BooleanArray>()
            val cardinalities = IntArray(numberOfTopics)
            val correlations = DoubleArray(numberOfTopics)

            for (currentCardinality in 0..numberOfTopics - 1) {

                val correlationsToSum = DoubleArray(Constants.AVERAGE_EXP_REPETITIONS)
                var correlationsSum = 0.0
                var meanCorrelation: Double
                var topicStatusToString = ""
                var topicStatus = BooleanArray(0)
                val generator = Random()

                (0..Constants.AVERAGE_EXP_REPETITIONS - 1).forEach {
                    currentIteration ->

                    val topicToChoose = HashSet<Int>()
                    while (topicToChoose.size < currentCardinality + 1) {
                        val next = generator.nextInt(numberOfTopics) + 1
                        topicToChoose.add(next)
                    }

                    topicStatus = BooleanArray(numberOfTopics)
                    for (chosenTopic in topicToChoose) topicStatus[chosenTopic - 1] = true

                    topicStatusToString = ""
                    for (j in topicStatus.indices) if (topicStatus[j]) topicStatusToString += 1 else topicStatusToString += 0

                    val meanAveragePrecisionsReduced = DoubleArray(averagePrecisions.entries.size)

                    for ((index, singleSystem) in this.averagePrecisions.entries.withIndex())
                        meanAveragePrecisionsReduced[index] = Tools.getMean(singleSystem.value, topicStatus)

                    correlationsToSum[currentIteration] = correlationStrategy.invoke(meanAveragePrecisionsReduced, meanAveragePrecisions)
                }

                correlationsToSum.forEach { value -> correlationsSum += value }
                meanCorrelation = correlationsSum / Constants.AVERAGE_EXP_REPETITIONS

                logger.debug("<Correlation: $meanCorrelation, Number of selected topics: $currentCardinality, Last gene evaluated: $topicStatusToString>")

                cardinalities[currentCardinality] = currentCardinality + 1
                correlations[currentCardinality] = meanCorrelation
                variableValues.add(topicStatus)

            }

            problem = BestSubsetProblem(numberOfTopics, averagePrecisions, meanAveragePrecisions, correlationStrategy, targetStrategy)
            population = LinkedList()

            (0..numberOfTopics - 1).forEach {
                i ->
                val solution = BestSubsetSolution(problem as BinaryProblem, numberOfTopics)
                solution.setVariableValue(0, solution.createNewBitSet(numberOfTopics, variableValues[i]))
                solution.setObjective(0, correlations[i])
                solution.setObjective(1, cardinalities[i].toDouble())
                @Suppress("UNCHECKED_CAST")
                population.add(solution as BinarySolution)
            }

        } else {

            problem = BestSubsetProblem(numberOfTopics, averagePrecisions, meanAveragePrecisions, correlationStrategy, targetStrategy)
            crossover = BinaryPruningCrossover(0.9)
            mutation = BitFlipMutation(1.0)
            selection = BinaryTournamentSelection(RankingAndCrowdingDistanceComparator<BinarySolution>())

            builder = NSGAIIBuilder(problem, crossover, mutation)
            builder.selectionOperator = selection
            builder.populationSize = parameters.populationSize
            builder.setMaxEvaluations(parameters.numberOfIterations)

            algorithm = builder.build()
            algorithmRunner = AlgorithmRunner.Executor(algorithm).execute()
            computingTime = algorithmRunner.computingTime

            @Suppress("UNCHECKED_CAST")
            population = algorithm.result as MutableList<BinarySolution>
            val populationWithNoDups = LinkedHashSet<BestSubsetSolution>()
            population.forEach { aSolution -> (populationWithNoDups::add)(aSolution as BestSubsetSolution) }
            @Suppress("UNCHECKED_CAST")
            population = populationWithNoDups.toList() as MutableList<BinarySolution>

            when (parameters.targetToAchieve) {
                "Best" -> for (i in population.indices) {
                    val solutionToFix = population[i]
                    val correlationToFix = solutionToFix.getObjective(0) * -1
                    solutionToFix.setObjective(0, correlationToFix)
                    population[i] = solutionToFix
                }
                "Worst" -> for (i in population.indices) {
                    val solutionToFix = population[i]
                    val cardinalityToFix = solutionToFix.getObjective(1) * -1
                    solutionToFix.setObjective(1, cardinalityToFix)
                    population[i] = solutionToFix
                }
            }
        }

        for (topicLabel in topicLabels) topicDistribution.put(topicLabel, TreeMap<Double, Boolean>())
        for (solutionToAnalyze in population) {
            val topicStatus = (solutionToAnalyze as BestSubsetSolution).retrieveTopicStatus()
            val cardinality = solutionToAnalyze.getObjective(1)
            for (j in topicStatus.indices) {
                val isInSolutionForCard = topicDistribution[topicLabels[j]] as MutableMap
                var status: Boolean = false
                if (topicStatus[j]) status = true
                isInSolutionForCard[cardinality] = status
                topicDistribution[topicLabels[j]] = isInSolutionForCard
            }
        }

        population.sortWith(kotlin.Comparator {
            sol1: BinarySolution, sol2: BinarySolution ->
            (sol1 as BestSubsetSolution).compareTo(sol2 as BestSubsetSolution)
        })

        return Pair<List<BinarySolution>, Triple<String, String, Long>>(population, Triple(parameters.targetToAchieve, Thread.currentThread().name, computingTime))

    }

}
