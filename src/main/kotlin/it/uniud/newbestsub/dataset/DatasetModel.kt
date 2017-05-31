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
    var percentiles: MutableMap<Int, List<Double>> = LinkedHashMap()

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

        val reader = CSVReader(FileReader(datasetPath))
        topicLabels = reader.readNext()
        numberOfTopics = topicLabels.size - 1

        reader.readAll().forEach {
            nextLine ->
            val averagePrecisions = DoubleArray(nextLine.size - 1)
            (1..nextLine.size - 1).forEach { i -> averagePrecisions[i - 1] = java.lang.Double.parseDouble(nextLine[i]) }
            this.averagePrecisions.put(nextLine[0], averagePrecisions)
        }

        numberOfSystems = this.averagePrecisions.entries.size

        val iterator = this.averagePrecisions.entries.iterator()
        systemLabels = Array(numberOfSystems, { _ -> iterator.next().key })

        topicLabels = topicLabels.sliceArray(1..topicLabels.size - 1)

        meanAveragePrecisions = DoubleArray(this.averagePrecisions.entries.size)

        val useColumns = BooleanArray(numberOfTopics)
        Arrays.fill(useColumns, true)

        this.averagePrecisions.entries.forEachIndexed {
            index, singleSystem ->
            meanAveragePrecisions[index] = Tools.getMean(singleSystem.value, useColumns)
        }

        topicLabels.forEach { topicLabel -> topicDistribution.put(topicLabel, TreeMap<Double, Boolean>()) }
    }

    fun expandData(averagePrecisions: Map<String, DoubleArray>, topicLabels: Array<String>) {

        this.numberOfTopics += topicLabels.size
        this.topicLabels = (this.topicLabels.toList() + topicLabels.toList()).toTypedArray()

        this.averagePrecisions.entries.forEach {
            (systemLabel, averagePrecisionValues) ->
            this.averagePrecisions[systemLabel] = (averagePrecisionValues.toList() + (averagePrecisions[systemLabel] ?: DoubleArray(0, { 0.0 })).toList()).toDoubleArray()
        }

        numberOfSystems = this.averagePrecisions.entries.size

        val iterator = this.averagePrecisions.entries.iterator()
        systemLabels = Array(numberOfSystems, { _ -> iterator.next().key })

        meanAveragePrecisions = DoubleArray(this.averagePrecisions.entries.size)

        val useColumns = BooleanArray(numberOfTopics)
        Arrays.fill(useColumns, true)

        this.averagePrecisions.entries.forEachIndexed {
            index, singleSystem ->
            meanAveragePrecisions[index] = Tools.getMean(singleSystem.value, useColumns)
        }

        this.topicLabels.forEach { topicLabel -> topicDistribution.put(topicLabel, TreeMap<Double, Boolean>()) }
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
            Constants.CORRELATION_PEARSON -> return pearsonCorrelation
            Constants.CORRELATION_KENDALL -> return kendallCorrelation
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
            Constants.TARGET_BEST -> return bestStrategy
            Constants.TARGET_WORST -> return worstStrategy
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

        if (targetToAchieve == Constants.TARGET_AVERAGE) {

            val variableValues = LinkedList<BooleanArray>()
            val cardinalities = LinkedList<Int>()
            val correlations = LinkedList<Double>()

            parameters.percentiles.forEach { percentileToFind -> percentiles[percentileToFind] = LinkedList<Double>() }

            for (currentCardinality in 0..numberOfTopics - 1) {

                var correlationsSum = 0.0
                var meanCorrelation: Double
                var topicStatusToString = ""
                var topicStatus = BooleanArray(0)
                val generator = Random()

                val correlationsToSum = Array(Constants.AVG_EXPERIMENT_REPETITIONS, {
                    val topicToChoose = HashSet<Int>()
                    while (topicToChoose.size < currentCardinality + 1) topicToChoose.add(generator.nextInt(numberOfTopics) + 1)

                    topicStatus = BooleanArray(numberOfTopics); topicStatusToString = ""

                    topicToChoose.forEach { chosenTopic -> topicStatus[chosenTopic - 1] = true }
                    topicStatus.forEach { singleTopic -> if (singleTopic) topicStatusToString += 1 else topicStatusToString += 0 }

                    val meanAveragePrecisionsReduced = DoubleArray(averagePrecisions.entries.size)

                    for ((index, singleSystem) in this.averagePrecisions.entries.withIndex())
                        meanAveragePrecisionsReduced[index] = Tools.getMean(singleSystem.value, topicStatus)

                    correlationStrategy.invoke(meanAveragePrecisionsReduced, meanAveragePrecisions)
                })

                correlationsToSum.forEach { singleCorrelation -> correlationsSum += singleCorrelation }
                correlationsToSum.sort()
                meanCorrelation = correlationsSum / Constants.AVG_EXPERIMENT_REPETITIONS

                percentiles.entries.forEach {
                    (percentileToFind, foundPercentiles) ->
                    val percentilePosition = Math.ceil((percentileToFind / 100.0) * correlationsToSum.size).toInt()
                    val percentileValue = correlationsToSum[percentilePosition - 1]
                    percentiles[percentileToFind] = foundPercentiles.plus(percentileValue)
                    logger.debug("<Cardinality: $currentCardinality, Percentile: $percentileToFind, Value: $percentileValue>")
                }

                logger.debug("<Correlation: $meanCorrelation, Number of selected topics: $currentCardinality, Last gene evaluated: $topicStatusToString>")

                cardinalities.add(currentCardinality + 1)
                correlations.add(meanCorrelation)
                variableValues.add(topicStatus)
            }

            problem = BestSubsetProblem(numberOfTopics, averagePrecisions, meanAveragePrecisions, correlationStrategy, targetStrategy)
            population = LinkedList()

            (0..numberOfTopics - 1).forEach {
                index ->
                val solution = BestSubsetSolution(problem as BinaryProblem, numberOfTopics)
                solution.setVariableValue(0, solution.createNewBitSet(numberOfTopics, variableValues[index]))
                solution.setObjective(0, correlations[index])
                solution.setObjective(1, cardinalities[index].toDouble())
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

            population = algorithm.result.toMutableList()
            population = population.distinct().toMutableList()

            when (parameters.targetToAchieve) {
                Constants.TARGET_BEST -> population.forEach { solutionToFix -> solutionToFix.setObjective(0, solutionToFix.getObjective(0) * -1) }
                Constants.TARGET_WORST -> population.forEach { solutionToFix -> solutionToFix.setObjective(1, solutionToFix.getObjective(1) * -1) }
            }
        }

        for (solutionToAnalyze in population) {
            val topicStatus = (solutionToAnalyze as BestSubsetSolution).retrieveTopicStatus()
            val cardinality = solutionToAnalyze.getObjective(1)
            for (index in topicStatus.indices) {
                topicDistribution[topicLabels[index]]?.let {
                    var status: Boolean = false; if (topicStatus[index]) status = true
                    it[cardinality] = status
                }
            }
        }

        population.sortWith(kotlin.Comparator {
            sol1: BinarySolution, sol2: BinarySolution ->
            (sol1 as BestSubsetSolution).compareTo(sol2 as BestSubsetSolution)
        })

        return Pair<List<BinarySolution>, Triple<String, String, Long>>(population, Triple(parameters.targetToAchieve, Thread.currentThread().name, computingTime))

    }

}
