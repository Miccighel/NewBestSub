package it.uniud.newbestsub.dataset

import com.opencsv.CSVReader
import it.uniud.newbestsub.problem.*
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools
import org.apache.commons.cli.ParseException
import org.apache.commons.math3.stat.correlation.KendallsCorrelation
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import org.apache.logging.log4j.LogManager

import org.uma.jmetal.operator.crossover.CrossoverOperator
import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.operator.selection.SelectionOperator
import org.uma.jmetal.operator.selection.impl.BinaryTournamentSelection
import org.uma.jmetal.problem.Problem
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator
import org.uma.jmetal.util.evaluator.SolutionListEvaluator
import org.uma.jmetal.util.evaluator.impl.SequentialSolutionListEvaluator
import org.uma.jmetal.util.pseudorandom.JMetalRandom
import org.uma.jmetal.util.ranking.Ranking
import org.uma.jmetal.util.ranking.impl.MergeNonDominatedSortRanking

import java.io.File
import java.io.FileReader
import java.util.*
import kotlinx.coroutines.channels.SendChannel

/**
 * Streaming behavior overview
 * ===========================
 *
 * Files produced:
 *  - **-Fun**: objective rows (K and correlation)
 *  - **-Var**: variable rows (gene bitstrings)
 *  - **-Top**: final top-10 solutions per cardinality K (exactly 10 lines per K, CSV)
 *
 * Targets:
 *  - **AVERAGE**: single-pass, one row per cardinality K.
 *  - **BEST/WORST**:
 *      • -Fun/-Var: after each generation, append rows only for K values whose representative
 *        solution improves vs the best-so-far (multiple rows per K are expected). Each appended
 *        batch is sorted by **(K asc, corr asc|desc)** to keep files ordered while growing.
 *        Final global sort is applied by the view.
 *      • -Top: replace (not append) the 10-row block for each K whenever contents change.
 *        Lines are sorted by **correlation ASC**; always exactly 10 lines per K. Blocks are sent
 *        via a single **TopKReplaceBatch** per generation.
 *
 * Determinism:
 *  - All randomized choices in AVERAGE use JMetalRandom.getInstance().randomGenerator,
 *    which can be redirected by an external seed bridge. NSGA-II also uses the same singleton RNG.
 */
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

    var notDominatedSolutions = mutableListOf<BinarySolution>()
    var dominatedSolutions = mutableListOf<BinarySolution>()
    var topSolutions = mutableListOf<BinarySolution>()
    var allSolutions = mutableListOf<BinarySolution>()

    /* ------------------------------ Data loading/expansion ------------------------------ */

    fun loadData(datasetPath: String) {
        val csvReader = CSVReader(FileReader(datasetPath))
        topicLabels = csvReader.readNext()
        numberOfTopics = topicLabels.size - 1
        datasetName = File(datasetPath).nameWithoutExtension

        csvReader.readAll().forEach { row ->
            val apValues = DoubleArray(row.size - 1)
            for (colIndex in 1 until row.size) {
                apValues[colIndex - 1] = row[colIndex].toDouble()
            }
            this.averagePrecisions[row[0]] = apValues.toTypedArray()
        }

        /* Deep copy to safely revert during system expansions. */
        originalAveragePrecisions = linkedMapOf<String, Array<Double>>().also { dst ->
            averagePrecisions.forEach { (system, values) -> dst[system] = values.copyOf() }
        }

        numberOfSystems = averagePrecisions.size

        topicLabels = topicLabels.sliceArray(1 until topicLabels.size)
        refreshDerivedData()
    }

    fun expandTopics(
        expansionCoefficient: Int,
        randomizedAveragePrecisions: Map<String, DoubleArray>,
        randomizedTopicLabels: Array<String>
    ) {
        this.expansionCoefficient = expansionCoefficient
        numberOfTopics += randomizedTopicLabels.size

        averagePrecisions.entries.forEach { (systemLabel, apValues) ->
            val extraValues = randomizedAveragePrecisions[systemLabel]?.toList() ?: emptyList()
            averagePrecisions[systemLabel] = (apValues.toList() + extraValues).toTypedArray()
        }

        topicLabels = (topicLabels.toList() + randomizedTopicLabels.toList()).toTypedArray()
        refreshDerivedData()
    }

    fun expandSystems(
        expansionCoefficient: Int,
        trueNumberOfSystems: Int,
        randomizedAveragePrecisions: Map<String, DoubleArray>,
        randomizedSystemLabels: Array<String>
    ) {
        this.expansionCoefficient = expansionCoefficient
        val newNumberOfSystems = numberOfSystems + expansionCoefficient

        if (newNumberOfSystems < trueNumberOfSystems) {
            /* Revert to a prefix of the original APs (stable order). */
            averagePrecisions = linkedMapOf<String, Array<Double>>().also { dst ->
                originalAveragePrecisions.entries.take(newNumberOfSystems).forEach { (system, values) ->
                    dst[system] = values.copyOf()
                }
            }
        } else {
            randomizedSystemLabels.forEach { system ->
                averagePrecisions[system] = (randomizedAveragePrecisions[system]?.toList() ?: emptyList()).toTypedArray()
            }
            systemLabels = (systemLabels.toList() + randomizedSystemLabels.toList()).toTypedArray()
        }

        refreshDerivedData()
    }

    private fun refreshDerivedData() {
        topicLabels.forEach { label -> topicDistribution[label] = TreeMap() }

        numberOfSystems = averagePrecisions.size

        val apEntryIterator = averagePrecisions.entries.iterator()
        systemLabels = Array(numberOfSystems) { apEntryIterator.next().key }

        val useAllTopicsMask = BooleanArray(numberOfTopics).apply { Arrays.fill(this, true) }

        val apIteratorForMean = averagePrecisions.entries.iterator()
        meanAveragePrecisions = Array(averagePrecisions.size) {
            Tools.getMean(apIteratorForMean.next().value.toDoubleArray(), useAllTopicsMask)
        }
    }

    /* ------------------------------ Solve + streaming ------------------------------ */

    /**
     * Single run with streaming:
     *  - AVERAGE: per-K aggregation; exactly one streamed row per K.
     *  - BEST/WORST: stream improvements for -Fun/-Var; replace changed K blocks for -Top.
     */
    fun solve(
        parameters: Parameters,
        out: SendChannel<ProgressEvent>? = null
    ): Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>> {

        datasetName = parameters.datasetName
        currentExecution = parameters.currentExecution
        populationSize = parameters.populationSize
        numberOfRepetitions = parameters.numberOfRepetitions

        val correlationStrategy = loadCorrelationMethod(parameters.correlationMethod)
        val objectiveEncodingStrategy = loadTargetToAchieve(parameters.targetToAchieve)

        logger.info(
            "Execution started on \"${Thread.currentThread().name}\" with target \"${parameters.targetToAchieve}\"."
        )

        /* Reset state for a clean run. */
        notDominatedSolutions = mutableListOf()
        dominatedSolutions = mutableListOf()
        topSolutions = mutableListOf()
        allSolutions = mutableListOf()

        val workerThreadName = Thread.currentThread().name
        val runStartNs = System.nanoTime()

        if (targetToAchieve == Constants.TARGET_AVERAGE) {

            /* -----------------------------
             * AVERAGE experiment (unchanged)
             * ----------------------------- */

            val randomGenerator = JMetalRandom.getInstance().randomGenerator

            val streamedBitsetsByK = mutableListOf<Array<Boolean>>()
            val streamedCardinalities = mutableListOf<Int>()
            val streamedCorrelations = mutableListOf<Double>()

            val loggingStep = (numberOfTopics * Constants.LOGGING_FACTOR) / 100
            var progressPercentage = 0

            parameters.percentiles.forEach { p -> percentiles[p] = LinkedList() }

            for ((iterationIndex, cardinalityIndex) in (0 until numberOfTopics).withIndex()) {

                if (loggingStep > 0 &&
                    (iterationIndex % loggingStep) == 0 &&
                    numberOfTopics > loggingStep
                ) {
                    logger.info(
                        "Completed iterations: $cardinalityIndex/$numberOfTopics ($progressPercentage%) on \"${Thread.currentThread().name}\" with target ${parameters.targetToAchieve}."
                    )
                    progressPercentage += Constants.LOGGING_FACTOR
                }

                var correlationsSum = 0.0
                var lastBitString = ""
                var selectedTopicMask = BooleanArray(numberOfTopics)

                val repetitionCorrelations = Array(numberOfRepetitions) {
                    val desiredCardinality = cardinalityIndex + 1

                    // Build 0..N-1 pool of topic indices
                    val topicIndexPool = IntArray(numberOfTopics) { it }

                    // Partial Fisher–Yates: fix the first `desiredCardinality` positions with unique picks
                    for (leftmostFixedIndex in 0 until desiredCardinality) {
                        val remainingSpan = numberOfTopics - leftmostFixedIndex
                        val randomOffsetWithinSpan = (randomGenerator.nextDouble() * remainingSpan).toInt()
                        val swapWithIndex = leftmostFixedIndex + randomOffsetWithinSpan

                        val tmp = topicIndexPool[leftmostFixedIndex]
                        topicIndexPool[leftmostFixedIndex] = topicIndexPool[swapWithIndex]
                        topicIndexPool[swapWithIndex] = tmp
                    }

                    // Mark selected topics
                    selectedTopicMask = BooleanArray(numberOfTopics)
                    for (selectedSlot in 0 until desiredCardinality) {
                        val selectedTopicIndex = topicIndexPool[selectedSlot]
                        selectedTopicMask[selectedTopicIndex] = true
                    }

                    // String form of the bitset (for logs/streaming)
                    lastBitString = buildString(numberOfTopics) {
                        selectedTopicMask.forEach { append(if (it) '1' else '0') }
                    }

                    // Compute correlation on the reduced mean vector
                    val apIterator = this@DatasetModel.averagePrecisions.entries.iterator()
                    val reducedMeanVector = Array(averagePrecisions.size) {
                        Tools.getMean(apIterator.next().value.toDoubleArray(), selectedTopicMask)
                    }

                    correlationStrategy.invoke(reducedMeanVector, meanAveragePrecisions)
                }

                repetitionCorrelations.forEach { c -> correlationsSum += c }
                repetitionCorrelations.sort()
                val meanCorrelation = correlationsSum / numberOfRepetitions

                percentiles.entries.forEach { (percentile, collected) ->
                    val position = kotlin.math.ceil((percentile / 100.0) * repetitionCorrelations.size).toInt()
                    val percentileValue = repetitionCorrelations[position - 1]
                    percentiles[percentile] = collected.plus(percentileValue)
                    logger.debug("<Cardinality: $cardinalityIndex, Percentile: $percentile, Value: $percentileValue>")
                }

                logger.debug(
                    "<Correlation: $meanCorrelation, Number of selected topics: $cardinalityIndex, Last gene evaluated: $lastBitString>"
                )

                streamedCardinalities.add(cardinalityIndex + 1)
                streamedCorrelations.add(meanCorrelation)
                streamedBitsetsByK.add(selectedTopicMask.toTypedArray())

                // STREAM NOW (AVERAGE)
                sendProgress(
                    out, CardinalityResult(
                        target = targetToAchieve,
                        threadName = workerThreadName,
                        cardinality = cardinalityIndex + 1,
                        correlation = meanCorrelation,
                        functionValuesCsvLine = "${cardinalityIndex + 1} $meanCorrelation",
                        variableValuesCsvLine = booleanArrayToVarLine(selectedTopicMask)
                    )
                )
            }

            problem = BestSubsetProblem(
                parameters, numberOfTopics, averagePrecisions, meanAveragePrecisions, topicLabels,
                correlationStrategy, objectiveEncodingStrategy
            )

            /* Build solutions from precomputed gene vectors to keep topicDistribution consistent. */
            for (index in 0 until numberOfTopics) {
                val solution = BestSubsetSolution(
                    numberOfVariables = 1,
                    numberOfObjectives = 2,
                    numberOfTopics = numberOfTopics,
                    topicLabels = problem.topicLabels,
                    forcedCardinality = null
                )
                val genes = streamedBitsetsByK[index]
                solution.setVariableValue(0, solution.createNewBitSet(numberOfTopics, genes))
                solution.objectives()[0] = streamedCardinalities[index].toDouble()
                solution.objectives()[1] = streamedCorrelations[index]

                notDominatedSolutions.add(solution)
                allSolutions.add(solution)
            }

        } else {

            /* ---------------------------------
             * BEST / WORST experiments (streamed)
             * ---------------------------------
             *
             * - -Fun/-Var: append only improvements per K (sorted by K and correlation).
             * - -Top: build exact 10-line blocks per K (corr ASC) and replace only on changes.
             *
             * Classic NSGA-II constructor is used; MNDS (MergeNonDominatedSortRanking) is plugged
             * into the environmental selection by overriding `replacement(...)` below.
             */
            if (populationSize < numberOfTopics) throw ParseException(
                "Value for the option <<p>> or <<po>> must be greater or equal than/to $numberOfTopics. Current value is $populationSize. Check the usage section below."
            )
            numberOfIterations = parameters.numberOfIterations

            problem = BestSubsetProblem(
                parameters, numberOfTopics, averagePrecisions, meanAveragePrecisions, topicLabels,
                correlationStrategy, objectiveEncodingStrategy
            )
            crossover = BinaryPruningCrossover(0.7)
            mutation = BitFlipMutation(0.3)

            /* Classic API: tournament selection + sequential evaluator. */
            val selectionOperator: SelectionOperator<List<BinarySolution>, BinarySolution> =
                BinaryTournamentSelection(RankingAndCrowdingDistanceComparator())
            val listEvaluator: SolutionListEvaluator<BinarySolution> =
                SequentialSolutionListEvaluator()

            /* (1) Track monotone best-so-far correlation per K for streaming decisions. */
            val bestSeenCorrelationByK = java.util.TreeMap<Int, Double>()

            /* (2) Track last sent 10-row signature for -Top replacement semantics. */
            val lastSentTopSignatureByK = mutableMapOf<Int, Int>()

            /* Per-generation hook:
             *  (A) -Fun/-Var: stream improved Ks (sorted), one row per K for this generation.
             *  (B) -Top: replace K blocks whose top-10 changed.
             */
            val onGeneration: (Int, List<BinarySolution>) -> Unit = { _, _ ->
                /* ------------------- (A) -Fun / -Var ------------------- */
                val topSolutionByK: Map<Int, BinarySolution> =
                    problem.topSolutions.mapNotNull { (kAsDouble, list) ->
                        val top = list.firstOrNull() ?: return@mapNotNull null
                        kAsDouble.toInt() to top
                    }.toMap()

                val improvedRows = mutableListOf<Pair<Int, BinarySolution>>()

                for ((k, topSol) in topSolutionByK.entries) {
                    val corr = (topSol as BestSubsetSolution).getCorrelation()
                    val previousBest = bestSeenCorrelationByK[k]
                    val improved = when (targetToAchieve) {
                        Constants.TARGET_WORST -> (previousBest == null) || corr < previousBest - 1e-12
                        else -> (previousBest == null) || corr > previousBest + 1e-12
                    }
                    if (improved) {
                        bestSeenCorrelationByK[k] = corr
                        improvedRows += k to topSol
                    }
                }

                improvedRows.sortWith(funVarComparatorFor(targetToAchieve))

                for ((k, sol) in improvedRows) {
                    sendProgress(
                        out, CardinalityResult(
                            target = targetToAchieve,
                            threadName = Thread.currentThread().name,
                            cardinality = k,
                            correlation = (sol as BestSubsetSolution).getCorrelation(),
                            functionValuesCsvLine = sol.toFunLineFor(targetToAchieve),
                            variableValuesCsvLine = sol.toVarLine()
                        )
                    )
                }

                /* ---------------------- (B) -Top (batched) -------------- */
                val changedBlocks = mutableMapOf<Int, List<String>>()
                for ((kAsDouble, list) in problem.topSolutions) {
                    val k = kAsDouble.toInt()
                    val lines = top10CsvLinesForK(list)
                    if (lines.isEmpty()) continue  // skip partial blocks

                    val signature = topBlockSig(lines)
                    val previousSignature = lastSentTopSignatureByK[k]
                    if (previousSignature == null || previousSignature != signature) {
                        lastSentTopSignatureByK[k] = signature
                        changedBlocks[k] = lines
                    }
                }
                if (changedBlocks.isNotEmpty()) {
                    sendProgress(
                        out, TopKReplaceBatch(
                            target = targetToAchieve,
                            threadName = Thread.currentThread().name,
                            blocks = changedBlocks
                        )
                    )
                }
            }

            /* Run NSGA-II with classic constructor and per-generation hook.
             * MNDS is injected by overriding `replacement(...)` in StreamingNSGAII. */
            val algorithm = StreamingNSGAII(
                problem = problem,
                maxEvaluations = numberOfIterations,
                populationSize = populationSize,
                crossover = crossover,
                mutation = mutation,
                selection = selectionOperator,
                evaluator = listEvaluator,
                onGen = onGeneration,
                logger = logger
            )

            logger.info("Starting NSGA-II run with Streaming hook: {}", algorithm.javaClass.name)

            val startNsLocal = System.nanoTime()
            algorithm.run()
            computingTime = (System.nanoTime() - startNsLocal) / 1_000_000

            /* ---------------- Collect final solution sets ---------------- */

            // Non-dominated set (deduplicated, order preserved); fix objectives to external view.
            notDominatedSolutions = java.util.LinkedHashSet(algorithm.result()).toMutableList()
            fixObjectiveFunctionValues(notDominatedSolutions)

            // Keep dominated only for cardinalities not present in non-dominated set.
            val nonDominatedCardinalities = notDominatedSolutions
                .mapTo(LinkedHashSet<Double>(notDominatedSolutions.size)) { it.getCardinality() }

            allSolutions.addAll(notDominatedSolutions)

            dominatedSolutions = problem.dominatedSolutions.values
                .asSequence()
                .filter { s -> !nonDominatedCardinalities.contains(s.getCardinality()) }
                .toMutableList()
            fixObjectiveFunctionValues(dominatedSolutions)

            allSolutions.addAll(dominatedSolutions)

            // Gather topSolutions (objectives fixed; streaming handled separately).
            topSolutions.clear()
            problem.topSolutions.values.forEach { perKList ->
                val fixed = fixObjectiveFunctionValues(perKList.toMutableList())
                topSolutions.addAll(fixed)
            }

            /* Final -Top replace to ensure a complete snapshot. */
            if (out != null) {
                val finalBlocks = mutableMapOf<Int, List<String>>()
                for ((kAsDouble, list) in problem.topSolutions) {
                    val k = kAsDouble.toInt()
                    val lines = top10CsvLinesForK(list)
                    if (lines.isNotEmpty()) finalBlocks[k] = lines
                }
                if (finalBlocks.isNotEmpty()) {
                    sendProgress(
                        out, TopKReplaceBatch(
                            target = targetToAchieve,
                            threadName = Thread.currentThread().name,
                            blocks = finalBlocks
                        )
                    )
                }
            }
        }

        /* ---------------- Topic presence matrix (unchanged) ---------------- */
        for (solution in allSolutions) {
            val topicMask = (solution as BestSubsetSolution).retrieveTopicStatus()
            val k = solution.getCardinality()
            for (topicIndex in topicMask.indices) {
                topicDistribution[topicLabels[topicIndex]]?.let { presenceByK ->
                    presenceByK[k] = topicMask[topicIndex]
                }
            }
        }

        allSolutions = sortByCardinality(allSolutions)

        val totalRuntimeMs = (System.nanoTime() - runStartNs) / 1_000_000
        this.computingTime = totalRuntimeMs
        sendProgress(out, RunCompleted(targetToAchieve, workerThreadName, totalRuntimeMs))

        logger.info("Not dominated solutions generated by execution with target \"$targetToAchieve\": ${notDominatedSolutions.size}/$numberOfTopics.")
        logger.info("Dominated solutions generated by execution with target \"$targetToAchieve\": ${dominatedSolutions.size}/$numberOfTopics.")
        if (targetToAchieve != Constants.TARGET_AVERAGE)
            logger.info("Total solutions generated by execution with target \"$targetToAchieve\": ${allSolutions.size}/$numberOfTopics.")

        return Triple(allSolutions, topSolutions, Triple(targetToAchieve, Thread.currentThread().name, totalRuntimeMs))
    }

    private fun sendProgress(out: SendChannel<ProgressEvent>?, event: ProgressEvent) {
        if (out == null) return
        kotlinx.coroutines.runBlocking { out.send(event) } // preserve ordering / back-pressure
    }

    /* ------------------------- Helpers ------------------------- */

    private fun loadCorrelationMethod(correlationMethod: String): (Array<Double>, Array<Double>) -> Double {

        val pearson: (Array<Double>, Array<Double>) -> Double = { first, second ->
            PearsonsCorrelation().correlation(first.toDoubleArray(), second.toDoubleArray())
        }
        val kendall: (Array<Double>, Array<Double>) -> Double = { first, second ->
            KendallsCorrelation().correlation(first.toDoubleArray(), second.toDoubleArray())
        }

        this.correlationMethod = correlationMethod

        return when (correlationMethod) {
            Constants.CORRELATION_PEARSON -> pearson
            Constants.CORRELATION_KENDALL -> kendall
            else -> pearson
        }
    }

    /**
     * Internal objective encoding for NSGA-II depending on the target:
     *  BEST:  objective[0] = +K, objective[1] = -corr (max corr).
     *  WORST: objective[0] = -K, objective[1] = +corr (min corr).
     */
    private fun loadTargetToAchieve(targetToAchieve: String): (BinarySolution, Double) -> BinarySolution {

        val encodeForBest: (BinarySolution, Double) -> BinarySolution = { solution, correlation ->
            solution.objectives()[0] = (solution as BestSubsetSolution).numberOfSelectedTopics.toDouble()
            solution.objectives()[1] = -correlation
            solution
        }
        val encodeForWorst: (BinarySolution, Double) -> BinarySolution = { solution, correlation ->
            solution.objectives()[0] = -(solution as BestSubsetSolution).numberOfSelectedTopics.toDouble()
            solution.objectives()[1] = correlation
            solution
        }

        this.targetToAchieve = targetToAchieve

        return when (targetToAchieve) {
            Constants.TARGET_BEST -> encodeForBest
            Constants.TARGET_WORST -> encodeForWorst
            else -> encodeForBest
        }
    }

    private fun sortByCardinality(solutionsToSort: MutableList<BinarySolution>): MutableList<BinarySolution> {
        solutionsToSort.sortWith { left: BinarySolution, right: BinarySolution ->
            (left as BestSubsetSolution).compareTo(right as BestSubsetSolution)
        }
        return solutionsToSort
    }

    /** Adjust objective values from internal encoding to the external reporting view. */
    private fun fixObjectiveFunctionValues(solutionsToFix: MutableList<BinarySolution>): MutableList<BinarySolution> {
        when (targetToAchieve) {
            Constants.TARGET_BEST -> solutionsToFix.forEach { s ->
                s.objectives()[1] = -s.getCorrelation()  // correlation stored as negative internally
            }
            Constants.TARGET_WORST -> solutionsToFix.forEach { s ->
                s.objectives()[0] = -s.getCardinality()  // K stored as negative internally
            }
        }
        return solutionsToFix
    }

    fun findCorrelationForCardinality(cardinality: Double): Double? {
        allSolutions.forEach { s -> if (s.getCardinality() == cardinality) return s.getCorrelation() }
        return null
    }

    fun isTopicInASolutionOfCardinality(topicLabel: String, cardinality: Double): Boolean {
        val answer = topicDistribution[topicLabel]?.get(cardinality)
        return answer ?: false
    }

    private fun booleanArrayToVarLine(bits: BooleanArray): String =
        bits.joinToString("") { if (it) "1" else "0" }

    /** Format for -Fun: (K, corr) using the external view required by the chosen target. */
    private fun BinarySolution.toFunLineFor(target: String): String {
        val k = this.getCardinality()
        val corr = this.getCorrelation()
        return when (target) {
            Constants.TARGET_BEST -> "${k} ${-corr}"   // corr negated internally → flip back
            Constants.TARGET_WORST -> "${-k} ${corr}"  // K negated internally → flip back
            else -> "$k $corr"
        }
    }

    /** Format for -Var: contiguous bitstring matching the existing writer. */
    private fun BinarySolution.toVarLine(): String {
        val bits = (this as BestSubsetSolution).retrieveTopicStatus()
        val sb = StringBuilder(bits.size)
        bits.forEach { sb.append(if (it) '1' else '0') }
        return sb.toString()
    }

    /** CSV row for -Top: "Cardinality,Correlation,Topics". */
    private fun BestSubsetSolution.toTopCsv(): String =
        "${getCardinality()},${getCorrelation()},${getTopicLabelsFromTopicStatus()}"

    /* ---------- Helpers for streaming order & -Top blocks ---------- */

    /**
     * Comparator to keep -Fun/-Var orderly as files grow by appending.
     * Primary key: K ascending. Secondary key: correlation ordered so that improvements
     * are appended after older rows (BEST: corr asc; WORST: corr desc).
     */
    private fun funVarComparatorFor(target: String): Comparator<Pair<Int, BinarySolution>> =
        if (target == Constants.TARGET_WORST) {
            compareBy<Pair<Int, BinarySolution>> { it.first }
                .thenByDescending { (it.second as BestSubsetSolution).getCorrelation() }
        } else {
            compareBy<Pair<Int, BinarySolution>> { it.first }
                .thenBy { (it.second as BestSubsetSolution).getCorrelation() }
        }

    /**
     * Build the exact 10 CSV lines for a -Top block (for a given K list).
     * Lines are sorted by correlation ascending. If fewer than 10 entries are available,
     * return an empty list and skip emitting a partial block.
     */
    private fun top10CsvLinesForK(topList: List<BinarySolution>): List<String> {
        val ascByCorr = topList.sortedBy { (it as BestSubsetSolution).getCorrelation() }
        if (ascByCorr.size < 10) return emptyList()
        val ten = ascByCorr.take(10)
        return ten.map { (it as BestSubsetSolution).toTopCsv() }
    }

    /** Small, stable signature of a 10-line block to detect content/order changes. */
    private fun topBlockSig(lines: List<String>): Int =
        lines.joinToString("\n").hashCode()

    /* ---------------------- Streaming NSGA-II wrapper (classic API + MNDS) ---------------------- */

    /**
     * Subclass of classic NSGA-II with:
     *  - A per-generation callback (invoked at init and after each replacement).
     *  - Environmental selection overridden to use **MNDS** (MergeNonDominatedSortRanking)
     *    instead of the default fast non-dominated sorting.
     *  - Local crowding-distance computation (no dependency on CrowdingDistance classes).
     *  - Does not mutate objective values; only reads solutions and delegates streaming to `onGen`.
     */
    private class StreamingNSGAII(
        problem: Problem<BinarySolution>,
        maxEvaluations: Int,
        populationSize: Int,
        crossover: CrossoverOperator<BinarySolution>,
        mutation: MutationOperator<BinarySolution>,
        selection: SelectionOperator<List<BinarySolution>, BinarySolution>,
        evaluator: SolutionListEvaluator<BinarySolution>,
        private val onGen: (Int, List<BinarySolution>) -> Unit,
        private val logger: org.apache.logging.log4j.Logger
    ) : org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAII<BinarySolution>(
        problem,
        maxEvaluations,
        populationSize,
        populationSize, /* matingPoolSize */
        populationSize, /* offspringPopulationSize */
        crossover,
        mutation,
        selection,
        evaluator
    ) {
        private var generationIndex = 0

        override fun initProgress() {
            super.initProgress()
            generationIndex = 0
            logger.debug("gen={} initProgress -> initial population ready (size={})", generationIndex, population.size)
            onGen.invoke(generationIndex, population)
        }

        override fun updateProgress() {
            super.updateProgress()
            generationIndex += 1
            logger.debug("gen={} updateProgress -> population updated (size={})", generationIndex, population.size)
            onGen.invoke(generationIndex, population)
        }

        /**
         * Environmental selection using **MNDS** instead of the default FNS:
         *   1) Join parent + offspring populations.
         *   2) Rank with MergeNonDominatedSortRanking (MNDS).
         *   3) Fill next population front-by-front; when a front overflows, apply *local*
         *      crowding distance and keep the most diverse.
         */
        override fun replacement(
            parentPopulation: List<BinarySolution>,
            offspringPopulation: List<BinarySolution>
        ): List<BinarySolution> {
            val targetPopulationSize = parentPopulation.size

            // 1) Merge parent + offspring
            val mergedPopulation = ArrayList<BinarySolution>(targetPopulationSize + offspringPopulation.size).apply {
                addAll(parentPopulation)
                addAll(offspringPopulation)
            }

            // 2) MNDS ranking — in 6.9.x: use no-arg ctor + compute(list)
            val mndsRanking: Ranking<BinarySolution> = MergeNonDominatedSortRanking()
            mndsRanking.compute(mergedPopulation)

            // 3) Build next gen using fronts + crowding on the last partial front
            val nextGeneration = ArrayList<BinarySolution>(targetPopulationSize)
            var frontIndex = 0
            while (nextGeneration.size < targetPopulationSize && frontIndex < mndsRanking.numberOfSubFronts) {
                val currentFront: MutableList<BinarySolution> = ArrayList(mndsRanking.getSubFront(frontIndex)) // copy to sort by crowding
                if (nextGeneration.size + currentFront.size <= targetPopulationSize) {
                    nextGeneration.addAll(currentFront)
                } else {
                    val slotsRemaining = targetPopulationSize - nextGeneration.size
                    val crowdingDistanceBySolution = computeCrowdingDistances(currentFront)

                    // Highest crowding distance first (desc), using explicit comparator
                    currentFront.sortWith(java.util.Comparator { a, b ->
                        val da = crowdingDistanceBySolution[a] ?: Double.NEGATIVE_INFINITY
                        val db = crowdingDistanceBySolution[b] ?: Double.NEGATIVE_INFINITY
                        java.lang.Double.compare(db, da)
                    })

                    nextGeneration.addAll(currentFront.subList(0, slotsRemaining))
                }
                frontIndex++
            }
            return nextGeneration
        }

        /**
         * Minimal, local **crowding distance** computation for a single front.
         * Matches NSGA-II’s definition:
         *   - For each objective, normalize [f(i+1) - f(i-1)] by (f_max - f_min).
         *   - Endpoints get +∞ to preserve boundary points.
         */
        private fun computeCrowdingDistances(frontSolutions: List<BinarySolution>): Map<BinarySolution, Double> {
            val frontSize = frontSolutions.size
            if (frontSize == 0) return emptyMap()
            if (frontSize <= 2) {
                // both (or single) endpoints: infinite crowding to keep them
                return frontSolutions.associateWith { Double.POSITIVE_INFINITY }
            }

            val objectiveCount = frontSolutions[0].objectives().size
            val crowdingDistanceBySolution = HashMap<BinarySolution, Double>(frontSize)
            frontSolutions.forEach { crowdingDistanceBySolution[it] = 0.0 }

            // Indices used to sort the front by each objective
            val sortIndex: MutableList<Int> = MutableList(frontSize) { it }

            for (objective in 0 until objectiveCount) {
                // Sort indices by this objective's value
                sortIndex.sortBy { idx -> frontSolutions[idx].objectives()[objective] }

                val minObjective = frontSolutions[sortIndex.first()].objectives()[objective]
                val maxObjective = frontSolutions[sortIndex.last()].objectives()[objective]
                val objectiveRange = maxObjective - minObjective

                // Endpoints always infinite (preserve extreme trade-offs)
                crowdingDistanceBySolution[frontSolutions[sortIndex.first()]] = Double.POSITIVE_INFINITY
                crowdingDistanceBySolution[frontSolutions[sortIndex.last()]] = Double.POSITIVE_INFINITY

                if (objectiveRange == 0.0) {
                    // All same value on this objective → contributes nothing this round
                    continue
                }

                // Interior points
                for (position in 1 until frontSize - 1) {
                    val leftNeighbor = frontSolutions[sortIndex[position - 1]].objectives()[objective]
                    val rightNeighbor = frontSolutions[sortIndex[position + 1]].objectives()[objective]
                    val increment = (rightNeighbor - leftNeighbor) / objectiveRange

                    val currentSolution = frontSolutions[sortIndex[position]]
                    if (crowdingDistanceBySolution[currentSolution] != Double.POSITIVE_INFINITY) {
                        crowdingDistanceBySolution[currentSolution] =
                            crowdingDistanceBySolution[currentSolution]!! + increment
                    }
                }
            }
            return crowdingDistanceBySolution
        }
    }
}
