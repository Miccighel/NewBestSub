package it.uniud.newbestsub.dataset

import com.opencsv.CSVReader
import it.uniud.newbestsub.dataset.model.CardinalityResult
import it.uniud.newbestsub.dataset.model.PrecomputedData
import it.uniud.newbestsub.dataset.model.ProgressEvent
import it.uniud.newbestsub.dataset.model.RunCompleted
import it.uniud.newbestsub.dataset.model.TopKReplaceBatch
import it.uniud.newbestsub.dataset.model.buildPrecomputedData
import it.uniud.newbestsub.dataset.model.toPrimitiveAPRows
import it.uniud.newbestsub.math.Correlations
import it.uniud.newbestsub.math.Statistics
import it.uniud.newbestsub.problem.*
import it.uniud.newbestsub.problem.operators.BinaryPruningCrossover
import it.uniud.newbestsub.problem.operators.FixedKSwapMutation
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Constants.GENERATION_STRIDE
import it.uniud.newbestsub.utils.Constants.GENERATION_WARMUP
import org.apache.commons.cli.ParseException
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
 *          (C) Maintain -Top as 10-line blocks per K (sorted on the natural scale), replace on change.
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
    var topicDistribution = linkedMapOf<String, MutableMap<Double, Boolean>>() /* kept for API compat */
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

    /* ---- Presence optimization: one representative mask per K (no post-run sweep) ---- */
    private val representativeMaskByK = mutableMapOf<Int, BooleanArray>()
    private var topicIndexByLabel: Map<String, Int> = emptyMap()

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
        /* Reset per-topic presence matrix (kept for API compat, though not filled post-run anymore). */
        topicLabels.forEach { label -> topicDistribution[label] = TreeMap() }

        /* Index for O(1) topic lookups by label */
        topicIndexByLabel = topicLabels.withIndex().associate { (i, lbl) -> lbl to i }

        /* Systems count & labels (preserve iteration order used by external reporting). */
        numberOfSystems = averagePrecisions.size
        val apEntryIterator = averagePrecisions.entries.iterator()
        systemLabels = Array(numberOfSystems) { apEntryIterator.next().key }

        /* Full-set mean AP per system (boxed path kept for AVERAGE branch). */
        val useAllTopicsMask = BooleanArray(numberOfTopics).apply { Arrays.fill(this, true) }
        val apIteratorForMean = averagePrecisions.entries.iterator()
        meanAveragePrecisions = Array(averagePrecisions.size) {
            Statistics.getMean(apIteratorForMean.next().value.toDoubleArray(), useAllTopicsMask)
        }

        /* Build the shared primitive bundle once per refresh (fast path for Best/Worst). */
        val primitiveAP: Map<String, DoubleArray> = toPrimitiveAPRows(averagePrecisions)
        precomputedData = buildPrecomputedData(primitiveAP)
    }

    /* ------------------------------ Solve + streaming ------------------------------ */

    fun solve(
        parameters: Parameters,
        out: SendChannel<ProgressEvent>? = null
    ): Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>> {

        datasetName = parameters.datasetName
        currentExecution = parameters.currentExecution
        populationSize = parameters.populationSize
        numberOfRepetitions = parameters.numberOfRepetitions
        correlationMethod = parameters.correlationMethod

        val objectiveEncodingStrategy = loadTargetToAchieve(parameters.targetToAchieve)

        val workerThreadName = Thread.currentThread().name
        val runStartNs = System.nanoTime()

        logger.info(
            "Execution started on \"${Thread.currentThread().name}\" with target \"${parameters.targetToAchieve}\"."
        )

        /* Reset state for a clean run. */
        notDominatedSolutions = mutableListOf()
        dominatedSolutions = mutableListOf()
        topSolutions = mutableListOf()
        allSolutions = mutableListOf()
        representativeMaskByK.clear()

        if (targetToAchieve == Constants.TARGET_AVERAGE) {

            /* ----------------------------------------------------------------------------------------------------------------
             * AVERAGE experiment (stream once per K) — Pearson/Kendall controlled by parameter
             * ----------------------------------------------------------------------------------------------------------------
             *  • Correlations delegated to it.uniud.newbestsub.math.Correlations (single source of truth).
             *  • We compute subset means into a reusable buffer, then call the primitive correlation.
             *  • Reuse buffers: topicIndexPool, subsetSumsBySystem, subsetMeansBySystem, correlationSamples.
             *  • Build Boolean mask once per K (for streaming VAR).
             * ---------------------------------------------------------------------------------------------------------------- */

            val randomGenerator = JMetalRandom.getInstance().randomGenerator

            /* --- Shared numeric bundle (primitive, read-only) --- */
            val bundle = precomputedData ?: buildPrecomputedData(toPrimitiveAPRows(averagePrecisions))
            val systemsCount = bundle.numberOfSystems
            val fullSetMeans = bundle.fullSetMeanAPBySystem

            /* --- Reusable work buffers --- */
            val topicIndexPool = IntArray(numberOfTopics) { it }      /* 0..N-1, partially shuffled per K */
            val subsetSumsBySystem = DoubleArray(systemsCount)        /* Σ_t AP[s][t] over selected topics  */
            val subsetMeansBySystem = DoubleArray(systemsCount)       /* subset means for Pearson/Kendall   */
            val correlationSamples = DoubleArray(maxOf(1, numberOfRepetitions))

            val streamedBitsetsByK = ArrayList<Array<Boolean>>(numberOfTopics)
            val streamedCardinalities = ArrayList<Int>(numberOfTopics)
            val streamedCorrelations = ArrayList<Double>(numberOfTopics)

            parameters.percentiles.forEach { p -> percentiles[p] = LinkedList() }

            val corrFuncPrimitive = Correlations.makePrimitiveCorrelation(parameters.correlationMethod)

            /* ---- adaptive logging (AVERAGE): percent milestones + wall interval ---- */
            val minWallIntervalMs = 1500L
            var nextWallLogAtMs = System.currentTimeMillis() + minWallIntervalMs
            var nextPercentMilestone = Constants.LOGGING_FACTOR

            /* ----------------------------------------------------------------------------------------------------------------
             * Iterate cardinalities K = 1..N
             * ---------------------------------------------------------------------------------------------------------------- */
            for (cardinalityIndex in 0 until numberOfTopics) {

                /* ---- progress logging (AVERAGE) ----
                 * Conditions for emitting a log message:
                 *  - percent milestone reached (every LOGGING_FACTOR%)
                 *  - minimum wall-clock interval elapsed (default ~1.5s)
                 *  - edge cases: very first or very last iteration
                 */
                val percentCompleted = ((cardinalityIndex.toLong() * 100) / numberOfTopics).toInt()
                val reachedPercentMilestone = percentCompleted >= nextPercentMilestone
                val reachedWallInterval = System.currentTimeMillis() >= nextWallLogAtMs
                val isEdgeCase = (cardinalityIndex == 0 || cardinalityIndex + 1 == numberOfTopics)

                if (reachedPercentMilestone || reachedWallInterval || isEdgeCase) {
                    logger.info(
                        "Completed iterations: $cardinalityIndex/$numberOfTopics ($percentCompleted%) on \"${Thread.currentThread().name}\" with target ${parameters.targetToAchieve}."
                    )
                    while (nextPercentMilestone <= percentCompleted) nextPercentMilestone += Constants.LOGGING_FACTOR
                    nextWallLogAtMs = System.currentTimeMillis() + minWallIntervalMs
                }

                val currentCardinality = cardinalityIndex + 1
                val repetitions = maxOf(1, numberOfRepetitions)

                /* --- Repetition loop for this K --- */
                var rep = 0
                while (rep < repetitions) {

                    /* Partial Fisher–Yates: fix first `currentCardinality` slots with unique picks */
                    var fixed = 0
                    while (fixed < currentCardinality) {
                        val remaining = numberOfTopics - fixed
                        val offset = (randomGenerator.nextDouble() * remaining).toInt()
                        val swapWith = fixed + offset
                        val tmp = topicIndexPool[fixed]
                        topicIndexPool[fixed] = topicIndexPool[swapWith]
                        topicIndexPool[swapWith] = tmp
                        fixed++
                    }

                    /* Build per-system sums for selected topics */
                    java.util.Arrays.fill(subsetSumsBySystem, 0.0)
                    var sel = 0
                    while (sel < currentCardinality) {
                        val t = topicIndexPool[sel]
                        val col = bundle.topicColumnViewByTopic[t]
                        var s = 0
                        while (s < systemsCount) {
                            subsetSumsBySystem[s] += col[s]
                            s++
                        }
                        sel++
                    }

                    /* Subset means (primitive buffer), then centralized correlation */
                    val invK = 1.0 / currentCardinality
                    var s = 0
                    while (s < systemsCount) {
                        subsetMeansBySystem[s] = subsetSumsBySystem[s] * invK
                        s++
                    }
                    val corr = corrFuncPrimitive(subsetMeansBySystem, fullSetMeans)

                    correlationSamples[rep] = corr
                    rep++
                }

                /* Aggregate stats for this K */
                var sumCorr = 0.0
                var i = 0
                while (i < repetitions) {
                    sumCorr += correlationSamples[i]; i++
                }
                val meanCorrelation = sumCorr / repetitions

                java.util.Arrays.sort(correlationSamples, 0, repetitions)
                for ((percentile, collected) in percentiles.entries) {
                    val pos = kotlin.math.ceil((percentile / 100.0) * repetitions).toInt().coerceIn(1, repetitions)
                    val pVal = correlationSamples[pos - 1]
                    percentiles[percentile] = collected.plus(pVal)
                }

                /* Build Boolean mask once (for streaming VAR row) */
                val maskForStreaming = BooleanArray(numberOfTopics)
                i = 0
                while (i < currentCardinality) {
                    maskForStreaming[topicIndexPool[i]] = true
                    i++
                }

                /* Cache a representative mask for this K (clone to decouple future edits) */
                representativeMaskByK[currentCardinality] = maskForStreaming.copyOf()

                streamedCardinalities.add(currentCardinality)
                streamedCorrelations.add(meanCorrelation)
                streamedBitsetsByK.add(maskForStreaming.toTypedArray())

                /* Stream AVERAGE row (FUN + VAR) */
                sendProgress(
                    out, CardinalityResult(
                        target = targetToAchieve,
                        threadName = workerThreadName,
                        cardinality = currentCardinality,
                        correlation = meanCorrelation,
                        functionValuesCsvLine = "$currentCardinality $meanCorrelation",
                        variableValuesCsvLine = packTopicMaskToVarLineBase64(maskForStreaming)
                    )
                )
            }

            /* ----------------------------------------------------------------------------------------------------------------
             * Construct problem + build solutions (unchanged; correlationFunction kept for ctor compat)
             * ---------------------------------------------------------------------------------------------------------------- */
            problem = BestSubsetProblem(
                parameters = parameters,
                numberOfTopics = numberOfTopics,
                precomputedData = bundle,
                topicLabels = topicLabels,
                legacyBoxedCorrelation = Correlations.makePrimitiveCorrelation(parameters.correlationMethod),
                targetStrategy = objectiveEncodingStrategy
            )

            for (index in 0 until numberOfTopics) {
                val s = BestSubsetSolution(
                    numberOfVariables = 1,
                    numberOfObjectives = 2,
                    numberOfTopics = numberOfTopics,
                    topicLabels = problem.topicLabels,
                    forcedCardinality = null
                )
                val genes = streamedBitsetsByK[index]
                s.variables()[0] = s.createNewBitSet(numberOfTopics, genes)
                s.objectives()[0] = streamedCardinalities[index].toDouble()
                s.objectives()[1] = streamedCorrelations[index]
                notDominatedSolutions.add(s)
                allSolutions.add(s)
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
                legacyBoxedCorrelation = Correlations.makePrimitiveCorrelation(parameters.correlationMethod),
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

            val onGeneration: (Int, List<BinarySolution>) -> Unit = fun(gen, generationPopulation) {
                /* Natural correlation reader */
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

                /* (B) Detect improvements vs best-so-far. */
                val improvedRows = mutableListOf<Pair<Int, BinarySolution>>()
                for ((k, sol) in representativeByK) {
                    val cand = naturalCorr(sol)
                    val prev = bestSeenCorrelationByK[k]
                    val improved = (prev == null) || (
                            if (targetToAchieve == Constants.TARGET_WORST) cand < prev - 1e-12 else cand > prev + 1e-12
                            )
                    if (improved) {
                        bestSeenCorrelationByK[k] = cand
                        improvedRows += k to sol
                    }
                }

                /* ---- Throttling AFTER we know if there were improvements ---- */
                val isWarmup = gen < GENERATION_WARMUP
                val onStride = ((gen - GENERATION_WARMUP) % GENERATION_STRIDE == 0)
                val shouldProcess = (!isWarmup && onStride) || gen == 0 || improvedRows.isNotEmpty()
                if (!shouldProcess) return

                /* Keep file append order stable while growing. */
                val appendOrder =
                    if (targetToAchieve == Constants.TARGET_WORST) {
                        compareBy<Pair<Int, BinarySolution>> { it.first }
                            .thenByDescending { naturalCorr(it.second) }  // WORST: newer (lower) later
                    } else {
                        compareBy<Pair<Int, BinarySolution>> { it.first }
                            .thenBy { naturalCorr(it.second) }            // BEST: newer (higher) later
                    }
                improvedRows.sortWith(appendOrder)

                /* Emit -Fun and -Var (packed Base64) only on per-K improvement. Also cache mask. */
                for ((k, sol) in improvedRows) {
                    representativeMaskByK[k] = (sol as BestSubsetSolution).retrieveTopicStatus().copyOf()
                    sendProgress(
                        out,
                        CardinalityResult(
                            target = targetToAchieve,
                            threadName = Thread.currentThread().name,
                            cardinality = k,
                            correlation = naturalCorr(sol),
                            functionValuesCsvLine = sol.toFunLineFor(targetToAchieve),   // emits "K,corr" after our CSV fix
                            variableValuesCsvLine = sol.toVarLinePackedBase64()
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

        /* No post-run topic presence sweep — presence answered from representativeMaskByK */

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

    /* Target encoding loader (internal sign scheme) */
    private fun loadTargetToAchieve(targetToAchieve: String): (BinarySolution, Double) -> BinarySolution {
        val encodeForBest: (BinarySolution, Double) -> BinarySolution = { solution, correlation ->
            /* BEST: obj[0] = +K, obj[1] = -corr (maximize corr) */
            solution.objectives()[0] = (solution as BestSubsetSolution).numberOfSelectedTopics.toDouble()
            solution.objectives()[1] = -correlation
            solution
        }
        val encodeForWorst: (BinarySolution, Double) -> BinarySolution = { solution, correlation ->
            /* WORST: obj[0] = -K, obj[1] = +corr (minimize corr) */
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

    fun findCorrelationForCardinality(cardinality: Double): Double? {
        allSolutions.forEach { s -> if (s.getCardinality() == cardinality) return s.getCorrelation() }
        return null
    }

    /* O(1) presence lookup using the cached representative mask per K */
    fun isTopicInASolutionOfCardinality(topicLabel: String, cardinality: Double): Boolean {
        val k = cardinality.toInt()
        val mask = representativeMaskByK[k] ?: return false
        val idx = topicIndexByLabel[topicLabel] ?: return false
        return mask.getOrNull(idx) ?: false
    }

    /* --------------------- VAR streaming: compact Base64 payload --------------------- */

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

    /** Pack a BooleanArray into LongArray words (LSB-first within each 64-bit word). */
    private fun packTopicMaskToLongWords(topicPresenceMask: BooleanArray): LongArray {
        val totalBits = topicPresenceMask.size
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

    /**
     * Return a COPY of the presence mask for the given cardinality K,
     * or null if no representative was cached for that K.
     */
    fun retrieveMaskForCardinality(cardinality: Double): BooleanArray? {
        val k = cardinality.toInt()
        val mask = representativeMaskByK[k] ?: return null
        return mask.copyOf()
    }

    /**
     * Return a presence mask sized exactly to `expectedSize`.
     *  - If we have a cached mask for K, copy and pad/truncate to fit.
     *  - If not, return an all‑false mask of the requested size.
     */
    fun retrieveMaskForCardinalitySized(cardinality: Double, expectedSize: Int): BooleanArray {
        val k = cardinality.toInt()
        val cached = representativeMaskByK[k] ?: return BooleanArray(expectedSize)

        /* Fast path: exact size match */
        if (cached.size == expectedSize) return cached.copyOf()

        /* Pad or truncate to match expectedSize */
        val out = BooleanArray(expectedSize)
        val n = kotlin.math.min(expectedSize, cached.size)
        java.lang.System.arraycopy(cached, 0, out, 0, n)
        return out
    }

    /**
     * Return the presence mask for K encoded as "B64:<base64>".
     * Always sized to the model's current numberOfTopics.
     */
    fun retrieveMaskB64ForCardinality(cardinality: Double): String {
        val mask = retrieveMaskForCardinalitySized(cardinality, numberOfTopics)
        return "B64:" + booleanMaskToBase64(mask)
    }

    /*
     * CSV row for -Top: "Cardinality,Correlation,Topics".
     * Emit Topics as Base64-packed bitmask ("B64:<base64>") to avoid any ambiguity.
     * Correlation is always on the natural scale (BEST flips internal sign earlier).
     */
    private fun BestSubsetSolution.toTopCsv(): String {
        val natural = getCorrelation()
        val mask = retrieveTopicStatus()
        val b64 = booleanMaskToBase64(mask)
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

        if (orderedTop.size < Constants.TOP_SOLUTIONS_NUMBER) return emptyList()
        return orderedTop.take(Constants.TOP_SOLUTIONS_NUMBER).map { (it as BestSubsetSolution).toTopCsv() }
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