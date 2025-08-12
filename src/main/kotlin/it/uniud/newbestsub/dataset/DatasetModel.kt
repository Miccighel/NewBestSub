package it.uniud.newbestsub.dataset

import com.opencsv.CSVReader
import it.uniud.newbestsub.problem.*
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools
import org.apache.commons.cli.ParseException
import org.apache.commons.math3.stat.correlation.KendallsCorrelation
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.algorithm.Algorithm
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder
import org.uma.jmetal.operator.crossover.CrossoverOperator
import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.operator.selection.SelectionOperator
import org.uma.jmetal.operator.selection.impl.BinaryTournamentSelection
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator
import java.io.File
import java.io.FileReader
import java.util.*

class DatasetModel {

    var datasetName = ""
    var numberOfSystems = 0
    var numberOfTopics = 0
    var targetToAchieve = ""
    var correlationMethod = ""
    var numberOfIterations = 0
    var numberOfRepetitions = 0
    var populationSize = 0
    var currentExecution = 0
    var expansionCoefficient = 0
    var systemLabels = emptyArray<String>()
    var topicLabels = emptyArray<String>()
    var averagePrecisions = linkedMapOf<String, Array<Double>>()
    var meanAveragePrecisions = emptyArray<Double>()
    var percentiles = linkedMapOf<Int, List<Double>>()
    var topicDistribution = linkedMapOf<String, MutableMap<Double, Boolean>>()
    var computingTime: Long = 0

    private var originalAveragePrecisions = linkedMapOf<String, Array<Double>>()

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    private lateinit var problem: BestSubsetProblem
    private lateinit var crossover: CrossoverOperator<BinarySolution>
    private lateinit var mutation: MutationOperator<BinarySolution>
    private lateinit var selection: SelectionOperator<List<BinarySolution>, BinarySolution>
    private lateinit var algorithm: Algorithm<List<BinarySolution>>

    var notDominatedSolutions = mutableListOf<BinarySolution>()
    var dominatedSolutions = mutableListOf<BinarySolution>()
    var topSolutions = mutableListOf<BinarySolution>()
    var allSolutions = mutableListOf<BinarySolution>()

    fun loadData(datasetPath: String) {

        val reader = CSVReader(FileReader(datasetPath))
        topicLabels = reader.readNext()
        numberOfTopics = topicLabels.size - 1
        datasetName = File(datasetPath).nameWithoutExtension

        reader.readAll().forEach { nextLine ->
            val averagePrecisions = DoubleArray(nextLine.size - 1)
            (1..nextLine.size - 1).forEach { i -> averagePrecisions[i - 1] = nextLine[i].toDouble() }
            this.averagePrecisions.put(nextLine[0], averagePrecisions.toTypedArray())
        }

        originalAveragePrecisions = averagePrecisions
        numberOfSystems = averagePrecisions.entries.size

        topicLabels = topicLabels.sliceArray(1..topicLabels.size - 1)
        updateData()
    }

    fun expandTopics(expansionCoefficient: Int, randomizedAveragePrecisions: Map<String, DoubleArray>, randomizedTopicLabels: Array<String>) {

        this.expansionCoefficient = expansionCoefficient
        numberOfTopics += randomizedTopicLabels.size

        averagePrecisions.entries.forEach { (aSystemLabel, averagePrecisionValues) ->
            averagePrecisions[aSystemLabel] = (averagePrecisionValues.toList() + (randomizedAveragePrecisions[aSystemLabel]?.toList() ?: emptyList())).toTypedArray()
        }

        topicLabels = (topicLabels.toList() + randomizedTopicLabels.toList()).toTypedArray()

        updateData()
    }

    fun expandSystems(expansionCoefficient: Int, trueNumberOfSystems: Int, randomizedAveragePrecisions: Map<String, DoubleArray>, randomizedSystemLabels: Array<String>) {

        this.expansionCoefficient = expansionCoefficient
        val newNumberOfSystems = numberOfSystems + expansionCoefficient

        if (newNumberOfSystems < trueNumberOfSystems) {
            averagePrecisions = originalAveragePrecisions
            val systemsToKeep = averagePrecisions.entries.take(newNumberOfSystems)
            averagePrecisions = linkedMapOf()
            systemsToKeep.forEach { aSystemToKeep ->
                averagePrecisions.put(aSystemToKeep.key, aSystemToKeep.value)
            }
        } else {
            randomizedSystemLabels.forEach { randomizedSystemLabel ->
                averagePrecisions.put(randomizedSystemLabel, (randomizedAveragePrecisions[randomizedSystemLabel]?.toList() ?: emptyList()).toTypedArray())
            }
            systemLabels = (systemLabels.toList() + randomizedSystemLabels.toList()).toTypedArray()
        }

        updateData()
    }

    private fun updateData() {

        topicLabels.forEach { topicLabel -> topicDistribution.put(topicLabel, TreeMap()) }

        numberOfSystems = averagePrecisions.entries.size

        var iterator = averagePrecisions.entries.iterator()
        systemLabels = Array(numberOfSystems, { iterator.next().key })

        val useColumns = BooleanArray(numberOfTopics)
        Arrays.fill(useColumns, true)

        iterator = averagePrecisions.entries.iterator()
        meanAveragePrecisions = Array(averagePrecisions.entries.size, { Tools.getMean(iterator.next().value.toDoubleArray(), useColumns) })

    }

    fun solve(parameters: Parameters): Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>> {

        datasetName = parameters.datasetName
        currentExecution = parameters.currentExecution
        populationSize = parameters.populationSize
        numberOfRepetitions = parameters.numberOfRepetitions

        val correlationStrategy = this.loadCorrelationMethod(parameters.correlationMethod)
        val targetStrategy = this.loadTargetToAchieve(parameters.targetToAchieve)

        logger.info("Execution started on \"${Thread.currentThread().name}\" with target \"${parameters.targetToAchieve}\". Wait please...")

        // The following four data structures need to be reinitialized during a data expansion session. During other experiments this set of instructions won't cause any trouble.

        notDominatedSolutions = mutableListOf()
        dominatedSolutions = mutableListOf()
        topSolutions = mutableListOf()
        allSolutions = mutableListOf()

        if (targetToAchieve == Constants.TARGET_AVERAGE) {

            // This branch gets executed when an "Average" experiment needs to be computed

            val startTime = System.nanoTime()

            val variableValues = mutableListOf<Array<Boolean>>()
            val cardinality = mutableListOf<Int>()
            val correlations = mutableListOf<Double>()

            val loggingFactor = (numberOfTopics * Constants.LOGGING_FACTOR) / 100
            var progressCounter = 0

            parameters.percentiles.forEach { percentileToFind -> percentiles[percentileToFind] = LinkedList() }

            for ((iterationCounter, currentCardinality) in (0..numberOfTopics - 1).withIndex()) {

                if ((iterationCounter % loggingFactor) == 0 && numberOfTopics - 1 > loggingFactor) {
                    logger.info("Completed iterations: $currentCardinality/$numberOfTopics ($progressCounter%) for evaluations being computed on \"${Thread.currentThread().name}\" with target ${parameters.targetToAchieve}.")
                    progressCounter += Constants.LOGGING_FACTOR
                }

                var correlationsSum = 0.0
                var topicStatusToString = ""
                var topicStatus = BooleanArray(0)
                val generator = Random()

                val correlationsToSum = Array(numberOfRepetitions, {
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
                val meanCorrelation: Double = correlationsSum / numberOfRepetitions

                percentiles.entries.forEach { (percentileToFind, foundPercentiles) ->
                    val percentilePosition = Math.ceil((percentileToFind / 100.0) * correlationsToSum.size).toInt()
                    val percentileValue = correlationsToSum[percentilePosition - 1]
                    percentiles[percentileToFind] = foundPercentiles.plus(percentileValue)
                    logger.debug("<Cardinality: $currentCardinality, Percentile: $percentileToFind, Value: $percentileValue>")
                }

                logger.debug("<Correlation: $meanCorrelation, Number of selected topics: $currentCardinality, Last gene evaluated: $topicStatusToString>")

                cardinality.add(currentCardinality + 1)
                correlations.add(meanCorrelation)
                variableValues.add(topicStatus.toTypedArray())

                computingTime = (System.nanoTime() - startTime) / 1000000

            }

            logger.info("Completed iterations: $numberOfTopics/$numberOfTopics (100%) for evaluations being computed on \"${Thread.currentThread().name}\" with target ${parameters.targetToAchieve}.")

            problem = BestSubsetProblem(parameters, numberOfTopics, averagePrecisions, meanAveragePrecisions, topicLabels, correlationStrategy, targetStrategy)

            /* Build solutions from precomputed gene vectors */
            for (index in 0 until numberOfTopics) {

                /* Create an empty solution (objectives/attributes will be filled below) */
                val solution = BestSubsetSolution(
                    numberOfVariables = 1,
                    numberOfObjectives = 2,
                    numberOfTopics = numberOfTopics,
                    topicLabels = problem.topicLabels,
                    forcedCardinality = null /* random init not used; we overwrite the bitset */
                )

                /* Accept either Array<Boolean> or BooleanArray */
                val genes: Array<Boolean> = when (val v = variableValues[index]) {
                    is Array<*> -> @Suppress("UNCHECKED_CAST") v
                }

                /* Encode genes into the binary variable 0 */
                solution.setVariable(0, solution.createNewBitSet(numberOfTopics, genes))

                /* objective[0] = cardinality, objective[1] = correlation */
                solution.setObjective(0, cardinality[index].toDouble())
                solution.setObjective(1, correlations[index])

                /* Collect */
                notDominatedSolutions.add(solution)
                allSolutions.add(solution)
            }

        } else {

            // This branch gets executed when a "Best" or a "Worst" experiment need to be computed

            if (populationSize < numberOfTopics) throw ParseException("Value for the option <<p>> or <<po>> must be greater or equal than/to $numberOfTopics. Current value is $populationSize. Check the usage section below.")
            numberOfIterations = parameters.numberOfIterations

            /* Build NSGA-II */
            problem = BestSubsetProblem(parameters, numberOfTopics, averagePrecisions, meanAveragePrecisions, topicLabels, correlationStrategy, targetStrategy)
            crossover = BinaryPruningCrossover(0.7)
            mutation = BitFlipMutation(0.3)
            selection = BinaryTournamentSelection(RankingAndCrowdingDistanceComparator())

            /* jMetal 5.10: use the 3-arg builder and set the population size explicitly */
            val builder = NSGAIIBuilder(
                problem,
                crossover,
                mutation,
                populationSize
            ).setSelectionOperator(selection).setMaxEvaluations(numberOfIterations)

            algorithm = builder.build()

            /* --- Option A: simplest/robust — run + time manually (recommended) --- */
            val startNs = System.nanoTime()
            algorithm.run()
            computingTime = (System.nanoTime() - startNs) / 1_000_000

            notDominatedSolutions = algorithm.result.toMutableList().distinct().toMutableList()
            notDominatedSolutions = fixObjectiveFunctionValues(notDominatedSolutions)
            var nonDominatedSolutionCardinality = listOf<Double>()
            notDominatedSolutions.forEach { aNonDominatedSolution ->
                nonDominatedSolutionCardinality = nonDominatedSolutionCardinality.plus(aNonDominatedSolution.getCardinality())
                allSolutions.plusAssign(aNonDominatedSolution)
            }

            dominatedSolutions = problem.dominatedSolutions.values.toMutableList()
            dominatedSolutions = fixObjectiveFunctionValues(dominatedSolutions)
            val dominatedSolutionsToRemove = mutableListOf<BinarySolution>()
            dominatedSolutions.forEach { aDominatedSolution ->
                if (!nonDominatedSolutionCardinality.contains(aDominatedSolution.getCardinality())) allSolutions.plusAssign(aDominatedSolution)
                else dominatedSolutionsToRemove.plusAssign(aDominatedSolution)
            }
            dominatedSolutionsToRemove.forEach { aDominatedSolutionToRemove -> dominatedSolutions.remove(aDominatedSolutionToRemove) }

            problem.topSolutions.values.forEach { aTopSolutionsList ->
                val topSolutionsList = fixObjectiveFunctionValues(aTopSolutionsList)
                topSolutionsList.forEach { aTopSolution -> topSolutions.plusAssign(aTopSolution) }
            }
        }

        for (solutionToAnalyze in allSolutions) {
            val topicStatus = (solutionToAnalyze as BestSubsetSolution).retrieveTopicStatus()
            val cardinality = solutionToAnalyze.getCardinality()
            for (index in topicStatus.indices) {
                topicDistribution[topicLabels[index]]?.let {
                    var status: Boolean = false
                    if (topicStatus[index]) status = true
                    it[cardinality] = status
                }
            }
        }

        allSolutions = sortByCardinality(allSolutions)

        logger.info("Not dominated solutions generated by execution with target \"$targetToAchieve\": ${notDominatedSolutions.size}/$numberOfTopics.")
        logger.info("Dominated solutions generated by execution with target \"$targetToAchieve\": ${dominatedSolutions.size}/$numberOfTopics.")
        if (targetToAchieve != Constants.TARGET_AVERAGE)
            logger.info("Total solutions generated by execution with target \"$targetToAchieve\": ${allSolutions.size}/$numberOfTopics.")

        return Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>>(allSolutions, topSolutions, Triple(targetToAchieve, Thread.currentThread().name, computingTime))
    }

    private fun loadCorrelationMethod(correlationMethod: String): (Array<Double>, Array<Double>) -> Double {

        val pearsonCorrelation: (Array<Double>, Array<Double>) -> Double = { firstArray, secondArray ->
            val pcorr = PearsonsCorrelation()
            pcorr.correlation(firstArray.toDoubleArray(), secondArray.toDoubleArray())
        }
        val kendallCorrelation: (Array<Double>, Array<Double>) -> Double = { firstArray, secondArray ->
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

    private fun loadTargetToAchieve(targetToAchieve: String): (BinarySolution, Double) -> BinarySolution {

        val bestStrategy: (BinarySolution, Double) -> BinarySolution = { solution, correlation ->
            solution.setObjective(0, (solution as BestSubsetSolution).numberOfSelectedTopics.toDouble())
            solution.setObjective(1, correlation * -1)
            solution
        }
        val worstStrategy: (BinarySolution, Double) -> BinarySolution = { solution, correlation ->
            solution.setObjective(0, ((solution as BestSubsetSolution).numberOfSelectedTopics * -1).toDouble())
            solution.setObjective(1, correlation)
            solution
        }

        this.targetToAchieve = targetToAchieve

        when (targetToAchieve) {
            Constants.TARGET_BEST -> return bestStrategy
            Constants.TARGET_WORST -> return worstStrategy
            else -> return bestStrategy
        }
    }

    private fun sortByCardinality(solutionsToSort: MutableList<BinarySolution>): MutableList<BinarySolution> {
        solutionsToSort.sortWith(kotlin.Comparator { sol1: BinarySolution, sol2: BinarySolution ->
            (sol1 as BestSubsetSolution).compareTo(sol2 as BestSubsetSolution)
        })
        return solutionsToSort
    }

    private fun fixObjectiveFunctionValues(solutionsToFix: MutableList<BinarySolution>): MutableList<BinarySolution> {
        when (targetToAchieve) {
            Constants.TARGET_BEST -> solutionsToFix.forEach { aSolutionToFix -> aSolutionToFix.setObjective(1, aSolutionToFix.getCorrelation() * -1) }
            Constants.TARGET_WORST -> solutionsToFix.forEach { aSolutionToFix -> aSolutionToFix.setObjective(0, aSolutionToFix.getCardinality() * -1) }
        }
        return solutionsToFix
    }

    fun findCorrelationForCardinality(cardinality: Double): Double? {
        allSolutions.forEach { aSolution -> if (aSolution.getCardinality() == cardinality) return aSolution.getCorrelation() }
        return null
    }

    fun isTopicInASolutionOfCardinality(topicLabel: String, cardinality: Double): Boolean {
        val answer = topicDistribution[topicLabel]?.get(cardinality)
        if (answer != null) return answer else return false
    }

    private fun getBaseFilePath(isTargetAll: Boolean): String {
        var baseResultPath =
            "${Constants.NEWBESTSUB_OUTPUT_PATH}$datasetName${Constants.FILE_NAME_SEPARATOR}$correlationMethod${Constants.FILE_NAME_SEPARATOR}$numberOfTopics${Constants.FILE_NAME_SEPARATOR}$numberOfSystems${Constants.FILE_NAME_SEPARATOR}"
        if (targetToAchieve != Constants.TARGET_AVERAGE && targetToAchieve != Constants.TARGET_ALL)
            baseResultPath += "$numberOfIterations${Constants.FILE_NAME_SEPARATOR}$populationSize${Constants.FILE_NAME_SEPARATOR}"
        if (targetToAchieve == Constants.TARGET_AVERAGE || isTargetAll)
            baseResultPath += "$numberOfRepetitions${Constants.FILE_NAME_SEPARATOR}"
        if (currentExecution > 0)
            baseResultPath += "$currentExecution${Constants.FILE_NAME_SEPARATOR}"
        if (isTargetAll)
            baseResultPath += "${Constants.TARGET_ALL}${Constants.FILE_NAME_SEPARATOR}"
        else
            baseResultPath += "$targetToAchieve${Constants.FILE_NAME_SEPARATOR}"
        return baseResultPath
    }

    fun getAggregatedDataFilePath(isTargetAll: Boolean): String {
        return "${getBaseFilePath(isTargetAll)}${Constants.AGGREGATED_DATA_FILE_SUFFIX}${Constants.CSV_FILE_EXTENSION}"
    }

    fun getAggregatedDataMergedFilePath(isTargetAll: Boolean): String {
        return "${getBaseFilePath(isTargetAll)}${Constants.AGGREGATED_DATA_FILE_SUFFIX}${Constants.FILE_NAME_SEPARATOR}${Constants.MERGED_RESULT_FILE_SUFFIX}${Constants.CSV_FILE_EXTENSION}"
    }

    fun getFunctionValuesFilePath(): String {
        return "${getBaseFilePath(false)}${Constants.FUNCTION_VALUES_FILE_SUFFIX}${Constants.CSV_FILE_EXTENSION}"
    }

    fun getFunctionValuesMergedFilePath(): String {
        return "${getBaseFilePath(false)}${Constants.FUNCTION_VALUES_FILE_SUFFIX}${Constants.FILE_NAME_SEPARATOR}${Constants.MERGED_RESULT_FILE_SUFFIX}${Constants.CSV_FILE_EXTENSION}"
    }

    fun getVariableValuesFilePath(): String {
        return "${getBaseFilePath(false)}${Constants.VARIABLE_VALUES_FILE_SUFFIX}${Constants.CSV_FILE_EXTENSION}"
    }

    fun getVariableValuesMergedFilePath(): String {
        return "${getBaseFilePath(false)}${Constants.VARIABLE_VALUES_FILE_SUFFIX}${Constants.FILE_NAME_SEPARATOR}${Constants.MERGED_RESULT_FILE_SUFFIX}${Constants.CSV_FILE_EXTENSION}"
    }

    fun getTopSolutionsFilePath(): String {
        return "${getBaseFilePath(false)}${Constants.TOP_SOLUTIONS_FILE_SUFFIX}${Constants.CSV_FILE_EXTENSION}"
    }

    fun getTopSolutionsMergedFilePath(): String {
        return "${getBaseFilePath(false)}${Constants.TOP_SOLUTIONS_FILE_SUFFIX}${Constants.FILE_NAME_SEPARATOR}${Constants.MERGED_RESULT_FILE_SUFFIX}${Constants.CSV_FILE_EXTENSION}"
    }

    fun getInfoFilePath(isTargetAll: Boolean): String {
        return "${getBaseFilePath(isTargetAll)}${Constants.INFO_FILE_SUFFIX}${Constants.CSV_FILE_EXTENSION}"
    }

    fun getInfoMergedFilePath(isTargetAll: Boolean): String {
        return "${getBaseFilePath(isTargetAll)}${Constants.INFO_FILE_SUFFIX}${Constants.FILE_NAME_SEPARATOR}${Constants.MERGED_RESULT_FILE_SUFFIX}${Constants.CSV_FILE_EXTENSION}"
    }
}
