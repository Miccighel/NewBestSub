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

/*
 * Model + streaming semantics
 * ===========================
 *
 * Artifacts produced (CSV + Parquet):
 *  - -Fun : objective rows (K, correlation), streamed during the run.
 *  - -Var : genotype rows, packed as Base64 (compact) and streamed in lockstep with FUN:
 *           • BEST/WORST → only when the per-K representative improves (monotone).
 *           • AVERAGE    → exactly one row per K (the last sampled genotype).
 *  - -Top : top-10 solutions per cardinality K (exactly 10 lines per K),
 *           maintained by block replacement (not by append).
 *
 * Branches:
 *  - AVERAGE
 *      • For each K = 1..N, sample `numberOfRepetitions` random subsets of size K,
 *        compute correlations vs the full-set mean vector, and stream exactly one -Fun/-Var row.
 *      • Percentiles (1%..99%) are computed from the same repetition samples.
 *      • -Top is not used in AVERAGE.
 *
 *  - BEST / WORST
 *      • Internal objective encoding for NSGA-II:
 *          BEST : obj[0] = +K,  obj[1] = -corr   (maximize corr)
 *          WORST: obj[0] = -K,  obj[1] = +corr   (minimize corr)
 *        The encoding is local to the algorithm; external printing always uses natural signs.
 *      • Per generation:
 *          (A) Choose one representative per K from the entire generation on the natural scale.
 *          (B) Append -Fun/-Var only if the representative improves the best-so-far for that K.
 *              Appends are ordered to keep files stably growing:
 *              – Primary key: K ascending
 *              – Secondary : correlation ordered so that newer improvements appear after older ones
 *                (BEST: corr ASC; WORST: corr DESC).
 *              -Var payload is Base64-packed to minimize I/O.
 *          (C) Maintain -Top as 10-line blocks per K (sorted on the natural scale), replace on change.
 *
 * Sign handling (internal vs external view):
 *  - Internal (algorithm state) encodes signs as above to steer selection.
 *  - External (all outputs and comparisons):
 *      • BEST  outputs use the natural correlation (flip the internal sign).
 *      • WORST outputs use the correlation as-is.
 *  - Prior to final snapshot printing, objectives are converted internal → external view.
 *
 * Determinism and seeds:
 *  - AVERAGE uses `JMetalRandom.getInstance().randomGenerator`; NSGA-II uses the same RNG.
 *  - An external seed bridge can provide determinism across runs.
 *
 * Tolerances and ordering:
 *  - Floating comparisons use epsilon = 1e-12 when checking improvements.
 *  - Where stable growth order matters, sorting is explicitly defined as above.
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

    /* Shared, read-only numeric bundle (dense primitive matrices). Built in refreshDerivedData(). */
    private var precomputedData: PrecomputedData? = null

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

        /* Deep copy to allow reversion during system expansions. */
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
        /* Reset per-topic presence matrix. */
        topicLabels.forEach { label -> topicDistribution[label] = TreeMap() }

        /* Systems count & labels (preserve iteration order used by external reporting). */
        numberOfSystems = averagePrecisions.size
        val apEntryIterator = averagePrecisions.entries.iterator()
        systemLabels = Array(numberOfSystems) { apEntryIterator.next().key }

        /* Full-set mean AP per system (boxed path kept for AVERAGE branch). */
        val useAllTopicsMask = BooleanArray(numberOfTopics).apply { Arrays.fill(this, true) }
        val apIteratorForMean = averagePrecisions.entries.iterator()
        meanAveragePrecisions = Array(averagePrecisions.size) {
            Tools.getMean(apIteratorForMean.next().value.toDoubleArray(), useAllTopicsMask)
        }

        /* Build the shared primitive bundle once per refresh (fast path for Best/Worst). */
        val primitiveAP: Map<String, DoubleArray> = toPrimitiveAPRows(averagePrecisions)
        precomputedData = buildPrecomputedData(primitiveAP)
    }

    /* ------------------------------ Solve + streaming ------------------------------ */

    /*
     * Execute a single run and stream progress.
     *
     * AVERAGE:
     *  - For each K = 1..N: sample subsets, compute mean correlation, append exactly one -Fun/-Var row (VAR packed).
     *  - Percentiles are derived from the same samples and written to the aggregated table.
     *
     * BEST/WORST:
     *  - NSGA-II runs with internal objectives (sign-encoded).
     *  - On each generation:
     *      • One representative per K is selected from the whole generation on the natural scale.
     *      • -Fun/-Var append only when the per-K best-so-far improves (monotone). VAR is Base64-packed.
     *      • -Top blocks are replaced (not appended) when the sorted top-10 content changes.
     *
     * Finalization:
     *  - Non-dominated and dominated sets are collected, then adjusted to the external view
     *    for printing (signs fixed for BEST correlation or WORST cardinality).
     *  - Topic presence matrix is built for all solutions that were generated.
     */
    fun solve(
        parameters: Parameters,
        out: SendChannel<ProgressEvent>? = null
    ): Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>> {

        datasetName = parameters.datasetName
        currentExecution = parameters.currentExecution
        populationSize = parameters.populationSize
        numberOfRepetitions = parameters.numberOfRepetitions

        val correlationStrategy = loadCorrelationMethod(parameters.correlationMethod)  /* boxed (AVERAGE) */
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
             * AVERAGE experiment (stream once per K)
             * ----------------------------- */

            /*
             * AVERAGE branch notes:
             *  - For each K, a single streaming row is produced: the mean correlation over
             *    `numberOfRepetitions` random subsets of size K (Fisher–Yates partial selection).
             *  - Percentiles are computed from the same repetition correlations.
             *  - -Fun and -Var are streamed exactly once per K; -Var is Base64-packed.
             */
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

                    /* Build 0..N-1 pool of topic indices. */
                    val topicIndexPool = IntArray(numberOfTopics) { it }

                    /* Partial Fisher–Yates: fix the first `desiredCardinality` positions with unique picks. */
                    for (leftmostFixedIndex in 0 until desiredCardinality) {
                        val remainingSpan = numberOfTopics - leftmostFixedIndex
                        val randomOffsetWithinSpan = (randomGenerator.nextDouble() * remainingSpan).toInt()
                        val swapWithIndex = leftmostFixedIndex + randomOffsetWithinSpan

                        val tmp = topicIndexPool[leftmostFixedIndex]
                        topicIndexPool[leftmostFixedIndex] = topicIndexPool[swapWithIndex]
                        topicIndexPool[swapWithIndex] = tmp
                    }

                    /* Mark selected topics. */
                    selectedTopicMask = BooleanArray(numberOfTopics)
                    for (selectedSlot in 0 until desiredCardinality) {
                        val selectedTopicIndex = topicIndexPool[selectedSlot]
                        selectedTopicMask[selectedTopicIndex] = true
                    }

                    /* String form of the bitset for logs only (not streamed raw). */
                    lastBitString = buildString(numberOfTopics) {
                        selectedTopicMask.forEach { append(if (it) '1' else '0') }
                    }

                    /* Compute correlation on the reduced mean vector (boxed path by design). */
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

                /* Stream AVERAGE row now — FUN and VAR (packed), exactly once per K. */
                sendProgress(
                    out, CardinalityResult(
                        target = targetToAchieve,
                        threadName = workerThreadName,
                        cardinality = cardinalityIndex + 1,
                        correlation = meanCorrelation,
                        functionValuesCsvLine = "${cardinalityIndex + 1} $meanCorrelation",
                        variableValuesCsvLine = packTopicMaskToVarLineBase64(selectedTopicMask)
                    )
                )
            }

            /* Construct the problem with the primitive path (keeps types uniform). */
            problem = BestSubsetProblem(
                parameters = parameters,
                numberOfTopics = numberOfTopics,
                precomputedData = precomputedData
                    ?: buildPrecomputedData(toPrimitiveAPRows(averagePrecisions)),
                topicLabels = topicLabels,
                correlationFunction = loadPrimitiveCorrelationMethod(parameters.correlationMethod),
                targetStrategy = objectiveEncodingStrategy
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
                solution.variables()[0] = solution.createNewBitSet(numberOfTopics, genes)
                solution.objectives()[0] = streamedCardinalities[index].toDouble()
                solution.objectives()[1] = streamedCorrelations[index]

                notDominatedSolutions.add(solution)
                allSolutions.add(solution)
            }

        } else {

            /* ---------------------------------
             * BEST / WORST experiments (streamed)
             * --------------------------------- */
            if (populationSize < numberOfTopics) throw ParseException(
                "Value for the option <<p>> or <<po>> must be greater or equal than/to $numberOfTopics. Current value is $populationSize. Check the usage section below."
            )
            numberOfIterations = parameters.numberOfIterations

            /* Construct the problem via the primitive precomputed bundle. */
            problem = BestSubsetProblem(
                parameters = parameters,
                numberOfTopics = numberOfTopics,
                precomputedData = precomputedData
                    ?: buildPrecomputedData(toPrimitiveAPRows(averagePrecisions)),
                topicLabels = topicLabels,
                correlationFunction = loadPrimitiveCorrelationMethod(parameters.correlationMethod),
                targetStrategy = objectiveEncodingStrategy
            )

            crossover = BinaryPruningCrossover(0.7)
            mutation = FixedKSwapMutation(0.3)

            /* Classic API: tournament selection + sequential evaluator. */
            val selectionOperator: SelectionOperator<List<BinarySolution>, BinarySolution> =
                BinaryTournamentSelection(RankingAndCrowdingDistanceComparator())
            val listEvaluator: SolutionListEvaluator<BinarySolution> =
                SequentialSolutionListEvaluator()

            /* (1) Track monotone best-so-far correlation per K for streaming decisions. */
            val bestSeenCorrelationByK = java.util.TreeMap<Int, Double>()

            /* (2) Track last sent 10-row signature for -Top replacement semantics. */
            val lastSentTopSignatureByK = mutableMapOf<Int, Int>()

            /*
             * Per-generation streaming policy:
             *
             * (A) Representative selection per K:
             *   - Scan the whole generation and keep, for each K, the candidate with the best
             *     natural correlation for BEST (max) or WORST (min).
             *
             * (B) Improvement check vs best-so-far per K (monotone):
             *   - Append to -Fun/-Var only when the representative's natural correlation improves
             *     beyond the stored best for that K (epsilon = 1e-12).
             *   - Appends are ordered by (K asc, corr asc|desc) to keep files ordered as they grow.
             *
             * (C) -Top batch replace on content change only.
             */
            val onGeneration: (Int, List<BinarySolution>) -> Unit = { _, generationPopulation ->

                /* Natural correlation reader:
                 *  - BEST is stored negated internally → flip to compare/print.
                 *  - WORST is already natural. */
                val naturalCorr: (BinarySolution) -> Double = { s ->
                    val c = (s as BestSubsetSolution).getCorrelation()
                    if (targetToAchieve == Constants.TARGET_BEST) -c else c
                }

                /* (A) Choose one representative per K from the whole generation. */
                val representativeByK = HashMap<Int, BinarySolution>()
                for (sol in generationPopulation) {
                    val k = sol.getCardinality().toInt()
                    val candidate = representativeByK[k]
                    val chooseCandidate = if (candidate == null) {
                        true
                    } else {
                        val cand = naturalCorr(sol)
                        val rep = naturalCorr(candidate)
                        if (targetToAchieve == Constants.TARGET_WORST) cand < rep - 1e-12 else cand > rep + 1e-12
                    }
                    if (chooseCandidate) representativeByK[k] = sol
                }

                /* (B) Stream only Ks whose representative improved vs best-so-far. */
                val improvedRows = mutableListOf<Pair<Int, BinarySolution>>()
                for ((k, sol) in representativeByK) {
                    val candidate = naturalCorr(sol)
                    val previous = bestSeenCorrelationByK[k]
                    val improved =
                        (previous == null) || (
                                if (targetToAchieve == Constants.TARGET_WORST) candidate < previous - 1e-12
                                else candidate > previous + 1e-12
                                )
                    if (improved) {
                        bestSeenCorrelationByK[k] = candidate
                        improvedRows += k to sol
                    }
                }

                /* Keep file append order stable while growing. */
                val appendOrder =
                    if (targetToAchieve == Constants.TARGET_WORST) {
                        compareBy<Pair<Int, BinarySolution>> { it.first }
                            .thenByDescending { naturalCorr(it.second) } /* WORST: newer (lower) go later */
                    } else {
                        compareBy<Pair<Int, BinarySolution>> { it.first }
                            .thenBy { naturalCorr(it.second) }           /* BEST: newer (higher) go later */
                    }
                improvedRows.sortWith(appendOrder)

                /* Emit -Fun and -Var (packed Base64) only on per-K improvement. */
                for ((k, sol) in improvedRows) {
                    sendProgress(
                        out,
                        CardinalityResult(
                            target = targetToAchieve,
                            threadName = Thread.currentThread().name,
                            cardinality = k,
                            correlation = naturalCorr(sol),                /* natural value for Parquet too */
                            functionValuesCsvLine = sol.toFunLineFor(targetToAchieve),
                            variableValuesCsvLine = (sol as BestSubsetSolution).toVarLinePackedBase64()
                        )
                    )
                }

                /* (C) -Top batch replace on content change only. */
                val changedBlocks = mutableMapOf<Int, List<String>>()
                for ((kAsDouble, list) in problem.topSolutions) {
                    val k = kAsDouble.toInt()
                    val lines = top10CsvLinesForK(list)
                    if (lines.isEmpty()) continue
                    val sig = topBlockSig(lines)
                    if (lastSentTopSignatureByK[k] != sig) {
                        lastSentTopSignatureByK[k] = sig
                        changedBlocks[k] = lines
                    }
                }
                if (changedBlocks.isNotEmpty()) {
                    sendProgress(out, TopKReplaceBatch(targetToAchieve, Thread.currentThread().name, changedBlocks))
                }
            }

            /* Run NSGA-II with classic constructor and per-generation hook (MNDS replacement). */
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

            /* Non-dominated set (deduplicated, order preserved); fix objectives to external view. */
            notDominatedSolutions = java.util.LinkedHashSet(algorithm.result()).toMutableList()
            fixObjectiveFunctionValues(notDominatedSolutions)

            /* Keep dominated only for cardinalities not present in non-dominated set. */
            val nonDominatedCardinalities = notDominatedSolutions
                .mapTo(LinkedHashSet<Double>(notDominatedSolutions.size)) { it.getCardinality() }

            allSolutions.addAll(notDominatedSolutions)

            dominatedSolutions = problem.dominatedSolutions.values
                .asSequence()
                .filter { s -> !nonDominatedCardinalities.contains(s.getCardinality()) }
                .toMutableList()
            fixObjectiveFunctionValues(dominatedSolutions)

            allSolutions.addAll(dominatedSolutions)

            /* Gather topSolutions (objectives fixed; streaming handled separately). */
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
        kotlinx.coroutines.runBlocking { out.send(event) } /* preserve ordering / back-pressure */
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

    /* Primitive correlation loader for Best/Worst (DoubleArray-based, no boxing). */
    private fun loadPrimitiveCorrelationMethod(
        correlationMethod: String
    ): (DoubleArray, DoubleArray) -> Double {
        val pearson: (DoubleArray, DoubleArray) -> Double = { left, right ->
            PearsonsCorrelation().correlation(left, right)
        }
        val kendall: (DoubleArray, DoubleArray) -> Double = { left, right ->
            KendallsCorrelation().correlation(left, right)
        }
        return when (correlationMethod) {
            Constants.CORRELATION_PEARSON -> pearson
            Constants.CORRELATION_KENDALL -> kendall
            else -> pearson
        }
    }

    /*
     * Objective encoding used internally by NSGA-II.
     * BEST  stores correlation as negative to turn maximization into minimization.
     * WORST stores cardinality as negative to prioritize smaller K when correlations tie.
     * External writers must present the natural signs; see toFunLineFor(), top10CsvLinesForK(),
     * and fixObjectiveFunctionValues().
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

    /*
     * Canonical ordering for final tables: primary key = K ascending,
     * secondary key = correlation ascending on the stored objective scale.
     * (Final snapshot lists have already been converted to the external view.)
     */
    private fun sortByCardinality(solutionsToSort: MutableList<BinarySolution>): MutableList<BinarySolution> {
        solutionsToSort.sortWith(
            compareBy<BinarySolution> { it.getCardinality() }
                .thenBy { (it as BestSubsetSolution).getCorrelation() }
        )
        return solutionsToSort
    }

    /*
     * Convert a list of solutions from internal objective encoding to the external view
     * in-place, prior to final snapshot printing:
     *  - BEST  : obj[1] becomes +corr (flips sign).
     *  - WORST : obj[0] becomes +K     (flips sign).
     * Streaming paths remain unaffected; they compute/print directly in the natural view.
     */
    private fun fixObjectiveFunctionValues(solutionsToFix: MutableList<BinarySolution>): MutableList<BinarySolution> {
        when (targetToAchieve) {
            Constants.TARGET_BEST -> solutionsToFix.forEach { s ->
                s.objectives()[1] = -s.getCorrelation()  /* correlation stored as negative internally */
            }

            Constants.TARGET_WORST -> solutionsToFix.forEach { s ->
                s.objectives()[0] = -s.getCardinality()  /* K stored as negative internally */
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

    /* Serialize a topic mask as a contiguous 0/1 bitstring (legacy). Not used for streaming anymore. */
    private fun booleanArrayToVarLine(bits: BooleanArray): String =
        bits.joinToString("") { if (it) "1" else "0" }

    /*
     * Format a -Fun row in the external (natural) view.
     *  - BEST : print K, +corr (flip internal sign before printing)
     *  - WORST: print K, +corr (K is stored negative internally; corr already natural)
     * The same natural correlation value is also carried to Parquet streaming.
     */
    private fun BinarySolution.toFunLineFor(target: String): String {
        val kInt = this.getCardinality().toInt()
        val internalCorr = this.getCorrelation()
        val naturalCorr = if (target == Constants.TARGET_BEST) -internalCorr else internalCorr
        return "$kInt $naturalCorr"
    }

    /* --------------------- VAR streaming: compact Base64 payload --------------------- */

    /*
     * VAR streaming (compact form)
     * ----------------------------
     * We avoid writing a giant 0/1 string (one char per topic). Instead we:
     *   1) Pack the Boolean topic mask into 64-bit words (LSB-first within each word).
     *   2) Serialize those words to a little-endian ByteArray (8 bytes per word).
     *   3) Encode the bytes as Base64 (without padding) to keep the VAR line textual.
     *
     * Size intuition:
     *   - Raw bitstring: N characters (one per topic).
     *   - Packed bytes:  ceil(N/8) bytes.
     *   - Base64 text:   ~ceil( (ceil(N/8) * 4) / 3 ) chars  ≈  N/6  (≈ 6× smaller than raw).
     */

    /** Pack a BooleanArray into LongArray words (LSB-first within each 64-bit word). */
    private fun packTopicMaskToLongWords(topicPresenceMask: BooleanArray): LongArray {
        val totalBits = topicPresenceMask.size
        the@ run { } // keep Kotlin imports tidy (no-op)
        val numberOfWords = (totalBits + 63) ushr 6  /* ceil(totalBits / 64) */
        val packedWords = LongArray(numberOfWords)

        var currentWordIndex = 0
        var bitIndexWithinWord = 0
        var accumulator = 0L

        for (absoluteBitIndex in 0 until totalBits) {
            if (topicPresenceMask[absoluteBitIndex]) {
                accumulator = accumulator or (1L shl bitIndexWithinWord)
            }
            bitIndexWithinWord += 1
            if (bitIndexWithinWord == 64) {
                packedWords[currentWordIndex] = accumulator
                currentWordIndex += 1
                bitIndexWithinWord = 0
                accumulator = 0L
            }
        }
        if (bitIndexWithinWord != 0) {
            packedWords[currentWordIndex] = accumulator
        }
        return packedWords
    }

    /** Serialize packed Long words to a little-endian ByteArray (8 bytes per word). */
    private fun longWordsToLittleEndianBytes(packedWords: LongArray): ByteArray {
        val byteBuffer = ByteArray(packedWords.size * java.lang.Long.BYTES)
        var writeOffset = 0
        for (word in packedWords) {
            var w = word
            /* Write 8 bytes, least-significant first, to match the bit-ordering choice. */
            for (i in 0 until java.lang.Long.BYTES) {
                byteBuffer[writeOffset + i] = (w and 0xFF).toByte()
                w = w ushr 8
            }
            writeOffset += java.lang.Long.BYTES
        }
        return byteBuffer
    }

    /** BooleanArray → Base64 (unpadded) textual form for the VAR line. */
    private fun booleanMaskToBase64(topicPresenceMask: BooleanArray): String {
        val packedWords = packTopicMaskToLongWords(topicPresenceMask)
        val packedBytes = longWordsToLittleEndianBytes(packedWords)
        return java.util.Base64.getEncoder().withoutPadding().encodeToString(packedBytes)
    }

    /** BinarySolution → VAR line (packed Base64, prefixed). */
    private fun BinarySolution.toVarLinePackedBase64(): String {
        val topicPresenceMask = (this as BestSubsetSolution).retrieveTopicStatus()
        return "B64:" + booleanMaskToBase64(topicPresenceMask)
    }

    /** AVERAGE branch helper: BooleanArray → VAR line (packed Base64, prefixed). */
    private fun packTopicMaskToVarLineBase64(topicPresenceMask: BooleanArray): String =
        "B64:" + booleanMaskToBase64(topicPresenceMask)

    /*
     * CSV row for -Top: "Cardinality,Correlation,Topics".
     * Emit Topics as Base64-packed bitmask ("B64:<base64>") to avoid any ambiguity.
     * Correlation is always on the natural scale (BEST flips internal sign earlier).
     */
    private fun BestSubsetSolution.toTopCsv(): String {
        val natural = getCorrelation()
        val mask = retrieveTopicStatus()
        val b64 = booleanMaskToBase64(mask)           /* already defined below (unpadded Base64) */
        return "${getCardinality()},$natural,B64:$b64"
    }

    /*
     * Build the 10 CSV lines for a -Top block at a given K using the natural correlation scale.
     * Sorting policy (by natural correlation):
     *  - BEST  : descending natural correlation (highest first).
     *  - WORST : ascending natural correlation (lowest first).
     * Partial blocks are skipped: fewer than 10 entries → no emission for that K in this generation.
     * Blocks are later compared by hash to decide whether to replace the previously printed block.
     */
    private fun top10CsvLinesForK(candidatesForK: List<BinarySolution>): List<String> {
        val naturalCorr: (BinarySolution) -> Double = { s -> (s as BestSubsetSolution).getCorrelation() }

        val orderedTop =
            if (targetToAchieve == Constants.TARGET_BEST)
                candidatesForK.sortedByDescending(naturalCorr)
            else
                candidatesForK.sortedBy(naturalCorr)

        if (orderedTop.size < 10) return emptyList()
        return orderedTop.take(10).map { (it as BestSubsetSolution).toTopCsv() }
    }

    /* Small, stable signature of a 10-line block to detect content/order changes. */
    private fun topBlockSig(lines: List<String>): Int =
        lines.joinToString("\n").hashCode()

    /* ---------------------- Streaming NSGA-II wrapper (classic API + MNDS) ---------------------- */

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

        override fun result(): List<BinarySolution> {
            logger.debug("gen={} final onGen before result() (size={})", generationIndex, population.size)
            onGen.invoke(generationIndex, population)
            return super.result()
        }

        override fun replacement(
            parentPopulation: List<BinarySolution>,
            offspringPopulation: List<BinarySolution>
        ): List<BinarySolution> {
            val targetPopulationSize = parentPopulation.size

            val mergedPopulation = ArrayList<BinarySolution>(targetPopulationSize + offspringPopulation.size).apply {
                addAll(parentPopulation)
                addAll(offspringPopulation)
            }

            val mndsRanking: Ranking<BinarySolution> = MergeNonDominatedSortRanking()
            mndsRanking.compute(mergedPopulation)

            val nextGeneration = ArrayList<BinarySolution>(targetPopulationSize)
            var frontIndex = 0
            while (nextGeneration.size < targetPopulationSize && frontIndex < mndsRanking.numberOfSubFronts) {
                val currentFront: MutableList<BinarySolution> = ArrayList(mndsRanking.getSubFront(frontIndex))
                if (nextGeneration.size + currentFront.size <= targetPopulationSize) {
                    nextGeneration.addAll(currentFront)
                } else {
                    val slotsRemaining = targetPopulationSize - nextGeneration.size
                    val crowdingDistanceBySolution = computeCrowdingDistances(currentFront)
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

        private fun computeCrowdingDistances(frontSolutions: List<BinarySolution>): Map<BinarySolution, Double> {
            val frontSize = frontSolutions.size
            if (frontSize == 0) return emptyMap()
            if (frontSize <= 2) return frontSolutions.associateWith { Double.POSITIVE_INFINITY }

            val objectiveCount = frontSolutions[0].objectives().size
            val crowdingDistanceBySolution = HashMap<BinarySolution, Double>(frontSize)
            frontSolutions.forEach { crowdingDistanceBySolution[it] = 0.0 }

            val sortIndex: MutableList<Int> = MutableList(frontSize) { it }

            for (objective in 0 until objectiveCount) {
                sortIndex.sortBy { idx -> frontSolutions[idx].objectives()[objective] }

                val minObjective = frontSolutions[sortIndex.first()].objectives()[objective]
                val maxObjective = frontSolutions[sortIndex.last()].objectives()[objective]
                val objectiveRange = maxObjective - minObjective

                crowdingDistanceBySolution[frontSolutions[sortIndex.first()]] = Double.POSITIVE_INFINITY
                crowdingDistanceBySolution[frontSolutions[sortIndex.last()]] = Double.POSITIVE_INFINITY

                if (objectiveRange == 0.0) continue

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
