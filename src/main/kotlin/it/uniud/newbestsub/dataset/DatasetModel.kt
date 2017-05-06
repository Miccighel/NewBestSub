package it.uniud.newbestsub.dataset

import com.opencsv.CSVReader

import java.io.FileNotFoundException
import java.io.FileReader
import java.io.IOException

import java.util.*

import it.uniud.newbestsub.utils.BestSubsetLogger
import it.uniud.newbestsub.utils.Formula
import it.uniud.newbestsub.problem.*

import org.apache.commons.math3.stat.correlation.KendallsCorrelation
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation

import org.uma.jmetal.problem.Problem

import org.uma.jmetal.operator.CrossoverOperator
import org.uma.jmetal.operator.MutationOperator
import org.uma.jmetal.operator.SelectionOperator
import org.uma.jmetal.operator.impl.selection.BinaryTournamentSelection

import org.uma.jmetal.algorithm.Algorithm
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder
import org.uma.jmetal.problem.BinaryProblem

import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.solution.Solution
import org.uma.jmetal.util.AlgorithmRunner
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator

import kotlin.collections.LinkedHashMap


class DatasetModel {

    var systemLabels = Array(0,{""})
    var topicLabels = Array(0,{""})
    var numberOfSystems = 0
    var systemSize = 0
    var numberOfTopics = 0
    var averagePrecisions: MutableMap<String, DoubleArray> = LinkedHashMap()
    var meanAveragePrecisions = DoubleArray(0)
    val topicDistribution: MutableMap<String, MutableMap<Double, Boolean>> by lazy {
        var map : MutableMap<String, MutableMap<Double, Boolean>> = LinkedHashMap()
        for(topicLabel in topicLabels) map.put(topicLabel,TreeMap<Double,Boolean>())
        for (solutionToAnalyze in population) {
            val topicStatus = (solutionToAnalyze as BestSubsetSolution).retrieveTopicStatus()
            val cardinality = solutionToAnalyze.getObjective(1)
            for (j in topicStatus.indices) {
                var isInSolutionForCard = map[topicLabels[j]] as MutableMap
                var status : Boolean = false
                if(topicStatus[j]) status = true
                isInSolutionForCard[cardinality] = status
                map[topicLabels[j]] = isInSolutionForCard
            }
        }
        map
    }
    var computingTime : Long = 0

    private lateinit var problem: Problem<BinarySolution>
    private lateinit var crossover: CrossoverOperator<BinarySolution>
    private lateinit var mutation: MutationOperator<BinarySolution>
    private lateinit var selection: SelectionOperator<List<BinarySolution>, BinarySolution>
    private lateinit var population: MutableList<Solution<BinarySolution>>
    private lateinit var builder: NSGAIIBuilder<BinarySolution>
    private lateinit var algorithm: Algorithm<List<BinarySolution>>
    private lateinit var algorithmRunner: AlgorithmRunner

    @Throws(FileNotFoundException::class, IOException::class)
    fun loadData(datasetPath: String) {

        // The parsing phase of the original .csv dataset file starts there.

        val reader = CSVReader(FileReader(datasetPath))
        topicLabels = reader.readNext()
        numberOfTopics = topicLabels.size - 1

        BestSubsetLogger.log("MODEL - Total number of topics: $numberOfTopics")

        var nextLine = reader.readNext()
        var averagePrecisions = DoubleArray(0)
        while (nextLine  != null) {
            val systemLabel = nextLine[0]
            averagePrecisions = DoubleArray(nextLine.size - 1)
            for (i in 1..nextLine.size - 1) averagePrecisions[i - 1] = java.lang.Double.parseDouble(nextLine[i])
            this.averagePrecisions.put(systemLabel, averagePrecisions)
            nextLine = reader.readNext()
        }

        numberOfSystems = this.averagePrecisions.entries.size
        systemSize = averagePrecisions.size

        /* averagePrecisions is a <String,double[]> dictionary where, for each entry, the key is the system label
        and the value is an array that contains the AP values of a single system, for each topic. */

        val iterator = this.averagePrecisions.entries.iterator()
        systemLabels = Array(numberOfSystems,{ _ -> iterator.next().key })

        BestSubsetLogger.log("MODEL - Total number of systems: ${this.averagePrecisions.entries.size}")

        /* In the loading phase there is an extensive use of the Map data structure. This has been done to do not lose
        the system and topic labels, which maybe will be useful in the future. */

        topicLabels = Array(topicLabels.size, { i -> topicLabels[i] }).sliceArray(1..topicLabels.size-1)

        /* The first label is stripped from the topic labels array because it's a fake label. */

        meanAveragePrecisions = DoubleArray(this.averagePrecisions.entries.size)

        val useColumns = BooleanArray(numberOfTopics)
        Arrays.fill(useColumns, true)

        for((index, singleSystem) in this.averagePrecisions.entries.withIndex()) {
            meanAveragePrecisions[index] = Formula.getMean(singleSystem.value, useColumns)
            this.systemSize = if (this.systemSize == 0) singleSystem.value.size else this.systemSize
        }

    }

    class Correlation(val correlationStrategy: (DoubleArray, DoubleArray) -> Double) {
        fun computeCorrelation(firstArray : DoubleArray, secondArray : DoubleArray) : Double = (correlationStrategy.invoke(firstArray,secondArray))
    }

    private fun loadCorrelationStrategy(chosenCorrelationMethod: String) : (DoubleArray, DoubleArray) -> Double {

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

    class Target(val targetStrategy: (BinarySolution, Double) -> BinarySolution) {
        fun adjustTargets(solution : Solution<BestSubsetSolution>, correlation: Double) : Solution<BestSubsetSolution> = (adjustTargets(solution,correlation))
    }

    private fun loadTargetStrategy(targetToAchieve: String): (BinarySolution, Double) -> BinarySolution {

        val bestStrategy : (BinarySolution, Double) -> BinarySolution = {
            solution, correlation ->
            solution.setObjective(0, correlation * -1)
            solution.setObjective(1, (solution as BestSubsetSolution).numberOfSelectedTopics.toDouble())
            solution
        }

        val worstStrategy : (BinarySolution, Double) -> BinarySolution = {
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

    fun solve(chosenCorrelationMethod: String, targetToAchieve: String, numberOfIterations: Int): Pair<List<Solution<BinarySolution>>, Long> {

        val correlationStrategy = this.loadCorrelationStrategy(chosenCorrelationMethod)
        val targetStrategy = this.loadTargetStrategy(targetToAchieve)

        if (targetToAchieve == "Average") {

            val variableValues = LinkedList<BooleanArray>()
            val cardinalities = IntArray(numberOfTopics)
            val correlations = DoubleArray(numberOfTopics)
            val generator = Random()

            for (currentCardinality in 0..numberOfTopics-1) {

                val topicToChoose = HashSet<Int>()
                while (topicToChoose.size < currentCardinality + 1) {
                    val next = generator.nextInt(numberOfTopics) + 1
                    topicToChoose.add(next)
                }

                val topicStatus = BooleanArray(numberOfTopics)
                for(chosenTopic in topicToChoose) topicStatus[chosenTopic - 1] = true


                var toString = ""
                for (j in topicStatus.indices) if (topicStatus[j]) toString += 1 else toString += 0

                BestSubsetLogger.log("PROBLEM - Evaluating gene: $toString")
                BestSubsetLogger.log("PROBLEM - Number of selected topics: $currentCardinality")

                val meanAveragePrecisionsReduced = DoubleArray(averagePrecisions.entries.size)

                for((index, singleSystem) in this.averagePrecisions.entries.withIndex())
                    meanAveragePrecisionsReduced[index] = Formula.getMean(singleSystem.value, topicStatus)

                val correlation = correlationStrategy.invoke(meanAveragePrecisionsReduced,meanAveragePrecisions)

                BestSubsetLogger.log("PROBLEM - Correlation: $correlation")

                cardinalities[currentCardinality] = currentCardinality + 1
                correlations[currentCardinality] = correlation
                variableValues.add(topicStatus)

            }

            problem = BestSubsetProblem(numberOfTopics, averagePrecisions, meanAveragePrecisions, correlationStrategy, targetStrategy)
            population = LinkedList()

            for (i in 0..numberOfTopics - 1) {
                val solution = BestSubsetSolution(problem as BinaryProblem, numberOfTopics)
                solution.setVariableValue(0, solution.createNewBitSet(numberOfTopics, variableValues[i]))
                solution.setObjective(0, correlations[i])
                solution.setObjective(1, cardinalities[i].toDouble())
                @Suppress("UNCHECKED_CAST")
                population.add(solution as Solution<BinarySolution>)
            }

            population.sortWith(kotlin.Comparator({
                sol1: Solution<BinarySolution>, sol2: Solution<BinarySolution> ->
                (sol1 as BestSubsetSolution).compareTo(sol2 as BestSubsetSolution)
            }))

        } else {

            problem = BestSubsetProblem(numberOfTopics, averagePrecisions, meanAveragePrecisions, correlationStrategy, targetStrategy)
            crossover = BinaryPruningCrossover(0.9)
            mutation = BitFlipMutation(1.0)
            selection = BinaryTournamentSelection(RankingAndCrowdingDistanceComparator<BinarySolution>())

            builder = NSGAIIBuilder(problem, crossover, mutation)
            builder.selectionOperator = selection
            builder.populationSize = 1000
            builder.setMaxEvaluations(numberOfIterations)

            algorithm = builder.build()
            algorithmRunner = AlgorithmRunner.Executor(algorithm).execute()
            computingTime = algorithmRunner.computingTime

            @Suppress("UNCHECKED_CAST")
            population = algorithm.result as MutableList<Solution<BinarySolution>>
            population.sortWith(kotlin.Comparator({
                sol1: Solution<BinarySolution>, sol2: Solution<BinarySolution> ->
                (sol1 as BestSubsetSolution).compareTo(sol2 as BestSubsetSolution)
            }))

            when (targetToAchieve) {
                "Best" -> for (i in population.indices) {
                    val solutionToFix = population[i]; val correlationToFix = solutionToFix.getObjective(0) * -1
                    solutionToFix.setObjective(0, correlationToFix)
                    population[i] = solutionToFix
                }
                "Worst" -> for (i in population.indices) {
                    val solutionToFix = population[i]; val cardinalityToFix = solutionToFix.getObjective(1) * -1
                    solutionToFix.setObjective(1, cardinalityToFix)
                    population[i] = solutionToFix
                }
            }

        }

        return Pair<List<Solution<BinarySolution>>, Long>(population, computingTime)

    }

}
