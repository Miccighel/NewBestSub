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
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.util.AlgorithmRunner
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator
import org.uma.jmetal.util.evaluator.impl.MultithreadedSolutionListEvaluator
import java.io.FileReader
import java.util.*

class DatasetModel {

    var systemLabels = emptyArray<String>()
    var topicLabels = emptyArray<String>()
    var numberOfSystems = 0
    var numberOfTopics = 0
    var targetToAchieve = ""
    var correlationMethod = ""

    var averagePrecisions = linkedMapOf<String, Array<Double>>()
    var meanAveragePrecisions = emptyArray<Double>()
    var topicDistribution = linkedMapOf<String, MutableMap<Double, Boolean>>()
    var percentiles = linkedMapOf<Int, List<Double>>()
    var computingTime: Long = 0

    private val logger = LogManager.getLogger()

    private lateinit var problem: BestSubsetProblem
    private lateinit var crossover: CrossoverOperator<BinarySolution>
    private lateinit var mutation: MutationOperator<BinarySolution>
    private lateinit var selection: SelectionOperator<List<BinarySolution>, BinarySolution>
    private lateinit var evaluator: MultithreadedSolutionListEvaluator<BinarySolution>
    private lateinit var builder: NSGAIIBuilder<BinarySolution>
    private lateinit var algorithm: Algorithm<List<BinarySolution>>
    private lateinit var algorithmRunner: AlgorithmRunner
    var notDominatedSolutions = mutableListOf<BinarySolution>()
    var dominatedSolutions = linkedMapOf<Double, BinarySolution>()
    var allSolutions = mutableListOf<BinarySolution>()

    fun loadData(datasetPath: String) {

        val reader = CSVReader(FileReader(datasetPath))
        topicLabels = reader.readNext()
        numberOfTopics = topicLabels.size - 1

        reader.readAll().forEach {
            nextLine ->
            val averagePrecisions = DoubleArray(nextLine.size - 1)
            (1..nextLine.size - 1).forEach { i -> averagePrecisions[i - 1] = java.lang.Double.parseDouble(nextLine[i]) }
            this.averagePrecisions.put(nextLine[0], averagePrecisions.toTypedArray())
        }

        topicLabels = topicLabels.sliceArray(1..topicLabels.size - 1)
        updateData()
    }

    fun expandData(randomizedAveragePrecisions: Map<String, DoubleArray>, randomizedTopicLabels: Array<String>) {

        numberOfTopics += randomizedTopicLabels.size

        averagePrecisions.entries.forEach {
            (systemLabel, averagePrecisionValues) ->
            averagePrecisions[systemLabel] = (averagePrecisionValues.toList() + (randomizedAveragePrecisions[systemLabel]?.toList() ?: emptyList())).toTypedArray()
        }

        topicLabels = (topicLabels.toList() + randomizedTopicLabels.toList()).toTypedArray()
        updateData()
    }

    private fun updateData() {

        topicLabels.forEach { topicLabel -> topicDistribution.put(topicLabel, TreeMap<Double, Boolean>()) }

        numberOfSystems = averagePrecisions.entries.size

        var iterator = averagePrecisions.entries.iterator()
        systemLabels = Array(numberOfSystems, { iterator.next().key })

        val useColumns = BooleanArray(numberOfTopics)
        Arrays.fill(useColumns, true)

        iterator = averagePrecisions.entries.iterator()
        meanAveragePrecisions = Array(averagePrecisions.entries.size, { Tools.getMean(iterator.next().value.toDoubleArray(), useColumns) })

    }

    private fun loadCorrelationStrategy(correlationMethod: String): (Array<Double>, Array<Double>) -> Double {

        val pearsonCorrelation: (Array<Double>, Array<Double>) -> Double = {
            firstArray, secondArray ->
            val pcorr = PearsonsCorrelation()
            pcorr.correlation(firstArray.toDoubleArray(), secondArray.toDoubleArray())
        }
        val kendallCorrelation: (Array<Double>, Array<Double>) -> Double = {
            firstArray, secondArray ->
            val pcorr = KendallsCorrelation()
            pcorr.correlation(firstArray.toDoubleArray(), secondArray.toDoubleArray())
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
        allSolutions.forEach { aSolution -> if (aSolution.getObjective(1) == cardinality) return aSolution.getObjective(0) }
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

            val variableValues = mutableListOf<Array<Boolean>>()
            val cardinality = mutableListOf<Int>()
            val correlations = mutableListOf<Double>()

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

                    topicStatus = BooleanArray(numberOfTopics)
                    topicStatusToString = ""

                    topicToChoose.forEach { chosenTopic -> topicStatus[chosenTopic - 1] = true }
                    topicStatus.forEach { singleTopic -> if (singleTopic) topicStatusToString += 1 else topicStatusToString += 0 }

                    val iterator = this.averagePrecisions.entries.iterator()
                    val meanAveragePrecisionsReduced = Array(averagePrecisions.entries.size, { Tools.getMean(iterator.next().value.toDoubleArray(), topicStatus) })

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

                cardinality.add(currentCardinality + 1)
                correlations.add(meanCorrelation)
                variableValues.add(topicStatus.toTypedArray())
            }

            problem = BestSubsetProblem(parameters, numberOfTopics, averagePrecisions, meanAveragePrecisions, correlationStrategy, targetStrategy)
            notDominatedSolutions = LinkedList()

            (0..numberOfTopics - 1).forEach {
                index ->
                val solution = BestSubsetSolution(problem as BinaryProblem, numberOfTopics)
                solution.setVariableValue(0, solution.createNewBitSet(numberOfTopics, variableValues[index]))
                solution.setObjective(0, correlations[index])
                solution.setObjective(1, cardinality[index].toDouble())
                notDominatedSolutions.add(solution as BinarySolution)
            }

        } else {

            problem = BestSubsetProblem(parameters, numberOfTopics, averagePrecisions, meanAveragePrecisions, correlationStrategy, targetStrategy)
            crossover = BinaryPruningCrossover(0.9)
            mutation = BitFlipMutation(1.0)
            selection = BinaryTournamentSelection(RankingAndCrowdingDistanceComparator<BinarySolution>())
            evaluator = MultithreadedSolutionListEvaluator<BinarySolution>(0, problem)

            builder = NSGAIIBuilder(problem, crossover, mutation)
            builder.selectionOperator = selection
            builder.populationSize = parameters.populationSize
            builder.setMaxEvaluations(parameters.numberOfIterations)
            builder.solutionListEvaluator = evaluator

            algorithm = builder.build()
            algorithmRunner = AlgorithmRunner.Executor(algorithm).execute()
            computingTime = algorithmRunner.computingTime
            builder.solutionListEvaluator.shutdown()

            notDominatedSolutions = algorithm.result.toMutableList().distinct().toMutableList()
            dominatedSolutions = problem.dominatedSolutions

            var nonDominatedSolutionCardinality = listOf<Double>()
            notDominatedSolutions.forEach {
                aNonDominatedSolution ->
                nonDominatedSolutionCardinality = nonDominatedSolutionCardinality.plus(aNonDominatedSolution.getObjective(1))
                allSolutions.plusAssign(aNonDominatedSolution)
            }
            dominatedSolutions.forEach {
                (_, aDominatedSolution) ->
                if (!nonDominatedSolutionCardinality.contains(aDominatedSolution.getObjective(1))) allSolutions.plusAssign(aDominatedSolution)
            }

            logger.info("Not dominated solutions generated for the execution with target \"${parameters.targetToAchieve}\": ${notDominatedSolutions.size}/$numberOfTopics.")
            logger.info("Dominated solutions generated for the execution with target \"${parameters.targetToAchieve}\": ${numberOfTopics - notDominatedSolutions.size}/$numberOfTopics.")

            when (parameters.targetToAchieve) {
                Constants.TARGET_BEST -> notDominatedSolutions.forEach { solutionToFix -> solutionToFix.setObjective(0, solutionToFix.getObjective(0) * -1) }
                Constants.TARGET_WORST -> notDominatedSolutions.forEach { solutionToFix -> solutionToFix.setObjective(1, solutionToFix.getObjective(1) * -1) }
            }

        }

        for (solutionToAnalyze in allSolutions) {
            val topicStatus = (solutionToAnalyze as BestSubsetSolution).retrieveTopicStatus()
            val cardinality = solutionToAnalyze.getObjective(1)
            for (index in topicStatus.indices) {
                topicDistribution[topicLabels[index]]?.let {
                    var status: Boolean = false
                    if (topicStatus[index]) status = true
                    it[cardinality] = status
                }
            }
        }

        allSolutions.sortWith(kotlin.Comparator {
            sol1: BinarySolution, sol2: BinarySolution ->
            (sol1 as BestSubsetSolution).compareTo(sol2 as BestSubsetSolution)
        })

        return Pair<List<BinarySolution>, Triple<String, String, Long>>(allSolutions, Triple(parameters.targetToAchieve, Thread.currentThread().name, computingTime))
    }
}
