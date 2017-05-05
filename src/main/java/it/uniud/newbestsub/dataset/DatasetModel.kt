package it.uniud.newbestsub.dataset

import com.opencsv.CSVReader

import java.io.FileNotFoundException
import java.io.FileReader
import java.io.IOException

import java.util.*

import it.uniud.newbestsub.utils.BestSubsetLogger
import it.uniud.newbestsub.utils.Formula
import it.uniud.newbestsub.problem.*

import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.commons.math3.stat.correlation.KendallsCorrelation
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation

import org.uma.jmetal.problem.Problem

import org.uma.jmetal.operator.CrossoverOperator
import org.uma.jmetal.operator.MutationOperator
import org.uma.jmetal.operator.SelectionOperator
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection

import org.uma.jmetal.algorithm.Algorithm
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder

import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.solution.Solution
import org.uma.jmetal.util.AlgorithmRunner
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator

import kotlin.collections.LinkedHashMap

class DatasetModel {

    lateinit var systemLabels: Array<String>
    lateinit var topicLabels: Array<String>
    var numberOfSystems: Int
    var systemSize: Int
    var numberOfTopics: Int
    var topicSize: Int
    var averagePrecisions: MutableMap<String, DoubleArray>
    var meanAveragePrecisions: DoubleArray
    var topicDistribution: MutableMap<String, MutableMap<Double, Boolean>>

    private lateinit var problem: Problem<BinarySolution>
    private lateinit var crossover: CrossoverOperator<BinarySolution>
    private lateinit var mutation: MutationOperator<BinarySolution>
    private lateinit var selection: SelectionOperator<List<BinarySolution>, BinarySolution>
    private lateinit var population: MutableList<Solution<BinarySolution>>
    private lateinit var builder: NSGAIIBuilder<BinarySolution>
    private lateinit var algorithm: Algorithm<List<BinarySolution>>
    private lateinit var algorithmRunner: AlgorithmRunner
    private var computingTime: Long

    init {
        numberOfSystems = 0
        systemSize = 0
        numberOfTopics = 0
        topicSize = 0
        averagePrecisions = LinkedHashMap()
        meanAveragePrecisions = DoubleArray(0)
        topicDistribution = LinkedHashMap()
        computingTime = 0
    }

    @Throws(FileNotFoundException::class, IOException::class)
    fun loadData(datasetPath: String) {

        // The parsing phase of the original .csv dataset file starts there.

        val reader = CSVReader(FileReader(datasetPath))
        topicLabels = reader.readNext() as Array<String>
        numberOfTopics = topicLabels.size - 1

        BestSubsetLogger.Companion.log("MODEL - Total number of topics: " + numberOfTopics)

        var nextLine = reader.readNext();
        var averagePrecisions = DoubleArray(0)
        while (nextLine  != null) {
            val systemLabel = nextLine[0]
            averagePrecisions = DoubleArray(nextLine.size - 1)
            for (i in 1..nextLine.size - 1) {
                averagePrecisions[i - 1] = java.lang.Double.parseDouble(nextLine[i])
            }
            this.averagePrecisions.put(systemLabel, averagePrecisions)
            nextLine = reader.readNext()
        }

        numberOfSystems = this.averagePrecisions.entries.size
        systemSize = averagePrecisions.size

        /* averagePrecisions is a <String,double[]> dictionary where, for each entry, the key is the system label
        and the value is an array that contains the AP values of a single system, for each topic. */

        var iterator: Iterator<Map.Entry<String,DoubleArray>> = this.averagePrecisions.entries.iterator()
        systemLabels = Array<String>(numberOfSystems,{ _ ->
            val singleSystem = iterator.next()
            singleSystem.key
        })

        BestSubsetLogger.Companion.log("MODEL - Total number of systems: " + this.averagePrecisions.entries.size)

        /* In the loading phase there is an extensive use of the Map data structure. This has been done to do not lose
        the system and topic labels, which maybe will be useful in the future. */

        topicLabels = Array<String>(topicLabels.size, { i -> topicLabels[i] }).sliceArray(1..topicLabels.size-1)

        /* The first label is stripped from the topic labels array because it's a fake label. */

        meanAveragePrecisions = DoubleArray(this.averagePrecisions.entries.size)

        val useColumns = BooleanArray(numberOfTopics)
        Arrays.fill(useColumns, java.lang.Boolean.TRUE)

        var counter = 0
        for(singleSystem in this.averagePrecisions.entries) {
            meanAveragePrecisions[counter] = Formula.getMean(singleSystem.value, useColumns)
            this.systemSize = if (this.systemSize == 0) singleSystem.value.size else this.systemSize
            counter++
        }

    }

    class Correlation(val correlationStrategy: (DoubleArray, DoubleArray) -> Double) {
        fun computeCorrelation(firstArray : DoubleArray, secondArray : DoubleArray) : Double = (correlationStrategy.invoke(firstArray,secondArray))
    }

    private fun loadCorrelationStrategy(chosenCorrelationMethod: String): (DoubleArray, DoubleArray) -> Double {

        val pearsonCorrelation : (DoubleArray, DoubleArray) -> Double = {
            firstArray, secondArray ->
            val pcorr = PearsonsCorrelation()
            pcorr.correlation(firstArray, secondArray)
        }

        val kendallCorrelation : (DoubleArray, DoubleArray) -> Double = {
            firstArray, secondArray ->
            val pcorr = KendallsCorrelation()
            pcorr.correlation(firstArray, secondArray)
        }

        when (chosenCorrelationMethod) {
            "Pearson" -> return pearsonCorrelation
            "Kendall" -> return kendallCorrelation
            else -> return pearsonCorrelation
        }
    }

    class Target(val targetStrategy: (Solution<BestSubsetSolution>, Double) -> BestSubsetSolution) {
        fun adjustTargets(solution : Solution<BestSubsetSolution>, correlation: Double) : Solution<BestSubsetSolution> = (adjustTargets(solution,correlation))
    }

    private fun loadTargetStrategy(targetToAchieve: String): (Solution<BinarySolution>, Double) -> Solution<BinarySolution> {

        val bestStrategy : (Solution<BinarySolution>, Double) -> Solution<BinarySolution> = {
            solution, correlation ->
            solution.setObjective(0, correlation * -1)
            solution.setObjective(1, (solution as BestSubsetSolution).numberOfSelectedTopics.toDouble())
            solution
        }

        val worstStrategy : (Solution<BinarySolution>, Double) -> Solution<BinarySolution> = {
            solution, correlation ->
            solution.setObjective(0, correlation)
            solution.setObjective(1, ((solution as BestSubsetSolution).numberOfSelectedTopics * -1).toDouble())
            solution
        }

        when (targetToAchieve) {
            "Best" -> return bestStrategy
            "Worst" -> return worstStrategy
            else -> return bestStrategy
        }

    }

    fun solve(chosenCorrelationMethod: String, targetToAchieve: String, numberOfIterations: Int): ImmutablePair<List<Solution<BinarySolution>>, Long> {

        val correlationStrategy = this.loadCorrelationStrategy(chosenCorrelationMethod)
        val targetStrategy = this.loadTargetStrategy(targetToAchieve)

        if (targetToAchieve == "Average") {

            val variableValues = LinkedList<BooleanArray>()
            val cardinalities = IntArray(numberOfTopics)
            val correlations = DoubleArray(numberOfTopics)

            val generator = Random()

            for (currentCardinality in 0..numberOfTopics - 1) {

                val topicToChoose = HashSet<Int>()
                while (topicToChoose.size < currentCardinality + 1) {
                    val next = generator.nextInt(numberOfTopics) + 1
                    topicToChoose.add(next)
                }

                val topicStatus = BooleanArray(numberOfTopics)

                var iterator: Iterator<*> = topicToChoose.iterator()
                while (iterator.hasNext()) {
                    val chosenTopic = iterator.next() as Int
                    topicStatus[chosenTopic - 1] = true
                }

                var toString = ""
                for (j in topicStatus.indices) {
                    if (topicStatus[j]) {
                        toString += 1
                    } else {
                        toString += 0
                    }
                }

                BestSubsetLogger.Companion.log("PROBLEM - Evaluating gene: " + toString)
                BestSubsetLogger.Companion.log("PROBLEM - Number of selected topics: " + currentCardinality)

                val meanAveragePrecisionsReduced = DoubleArray(averagePrecisions.entries.size)

                var counter = 0
                for(singleSystem in this.averagePrecisions.entries) {
                    meanAveragePrecisionsReduced[counter] = Formula.getMean(singleSystem.value, topicStatus)
                    counter++
                }

                val correlation = correlationStrategy.invoke(meanAveragePrecisionsReduced,meanAveragePrecisions)

                BestSubsetLogger.Companion.log("PROBLEM - Correlation: " + correlation)

                cardinalities[currentCardinality] = currentCardinality + 1
                correlations[currentCardinality] = correlation
                variableValues.add(topicStatus)

            }

            problem = BestSubsetProblem(numberOfTopics, averagePrecisions, meanAveragePrecisions, correlationStrategy, targetStrategy)
            for (i in 0..numberOfTopics - 1) {
                val solution = BestSubsetSolution(problem as BestSubsetProblem?, numberOfTopics)
                solution.setVariableValue(0, solution.createNewBitSet(numberOfTopics, variableValues[i]))
                solution.setObjective(0, correlations[i])
                solution.setObjective(1, cardinalities[i].toDouble())
                population.add(solution as Solution<BinarySolution>)
            }

            population.sortWith(kotlin.Comparator({sol1: Solution<BinarySolution>, sol2: Solution<BinarySolution> -> (sol1 as BestSubsetSolution).compareTo(sol2 as BestSubsetSolution)}))

        } else {

            problem = BestSubsetProblem(numberOfTopics, averagePrecisions, meanAveragePrecisions, correlationStrategy, targetStrategy)
            crossover = BinaryPruningCrossover(0.9)
            mutation = BitFlipMutation(1.0)
            selection = BinaryTournamentSelection(RankingAndCrowdingDistanceComparator<BinarySolution>())
            builder = NSGAIIBuilder(problem, crossover, mutation)
            builder.selectionOperator = selection
            builder.populationSize = averagePrecisions.size
            builder.setMaxEvaluations(numberOfIterations)
            algorithm = builder.build()
            algorithmRunner = AlgorithmRunner.Executor(algorithm).execute()
            computingTime = algorithmRunner.computingTime
            population = algorithm.getResult() as MutableList<Solution<BinarySolution>>

            population.sortWith(kotlin.Comparator({sol1: Solution<BinarySolution>, sol2: Solution<BinarySolution> -> (sol1 as BestSubsetSolution).compareTo(sol2 as BestSubsetSolution)}))

            when (targetToAchieve) {
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

        for(topicLabel in topicLabels) {
            topicDistribution.put(topicLabel,TreeMap<Double,Boolean>())
        }

        for (solutionToAnalyze in population) {
            val topicStatus = (solutionToAnalyze as BestSubsetSolution).topicStatus
            val cardinality = solutionToAnalyze.getObjective(1)
            for (j in topicStatus.indices) {
                var isInSolutionForCard = topicDistribution[topicLabels[j]] as MutableMap
                var status : Boolean = false
                if(topicStatus[j]) {
                    status = true
                }
                isInSolutionForCard[cardinality] = status
                topicDistribution[topicLabels[j]] = isInSolutionForCard
            }
        }

        return ImmutablePair<List<Solution<BinarySolution>>, Long>(population, computingTime)

    }

}
