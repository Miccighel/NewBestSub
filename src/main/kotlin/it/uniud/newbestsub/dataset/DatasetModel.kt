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
import java.io.File
import java.io.FileReader
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
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
 *  - **AVERAGE**: single-pass, one row per cardinality K (unchanged from before).
 *  - **BEST/WORST**:
 *      • -Fun/-Var: after each generation we **append** rows for any K whose representative
 *        solution **improved** vs the best-so-far we have streamed. (Multiple rows per K are expected.)
 *        Before streaming a batch, we sort those rows by **(K asc, corr asc|desc)** to
 *        keep the growing file ordered. At the end of the run, the view **globally** sorts
 *        the whole file by **(K asc, corr asc)** and rewrites both -Fun and -Var.
 *      • -Top: we **replace** (not append) the 10-row block for each K whenever its content changes.
 *        Lines are sorted by **correlation ASC**, and we always send **exactly 10 rows** per K.
 *        We send these as a **TopKReplaceBatch** once per generation.
 *
 * Notes:
 *  - -Fun/-Var contain **checkpoint** rows: multiple lines for the same K reflect improvements over time.
 *  - Final sorting is handled by `DatasetView.closeStreams`, ensuring definitive global order.
 * Determinism note:
 *  - All randomized choices in AVERAGE now use JMetalRandom.getInstance().randomGenerator,
 *    which is redirected by RngBridge when deterministic mode is enabled.
 *  - NSGA-II already uses the same singleton RNG; no extra changes needed for BEST/WORST here.
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
    private lateinit var selection: SelectionOperator<List<BinarySolution>, BinarySolution>

    var notDominatedSolutions = mutableListOf<BinarySolution>()
    var dominatedSolutions = mutableListOf<BinarySolution>()
    var topSolutions = mutableListOf<BinarySolution>()
    var allSolutions = mutableListOf<BinarySolution>()

    /* ------------------------------ Data loading/expansion ------------------------------ */

    fun loadData(datasetPath: String) {
        val reader = CSVReader(FileReader(datasetPath))
        topicLabels = reader.readNext()
        numberOfTopics = topicLabels.size - 1
        datasetName = File(datasetPath).nameWithoutExtension

        reader.readAll().forEach { nextLine ->
            val aps = DoubleArray(nextLine.size - 1)
            (1..nextLine.size - 1).forEach { i -> aps[i - 1] = nextLine[i].toDouble() }
            this.averagePrecisions[nextLine[0]] = aps.toTypedArray()
        }

        /* Deep-ish copy to safely revert during system expansions */
        originalAveragePrecisions = linkedMapOf<String, Array<Double>>().also { dst ->
            averagePrecisions.forEach { (k, v) -> dst[k] = v.copyOf() }
        }

        numberOfSystems = averagePrecisions.entries.size

        topicLabels = topicLabels.sliceArray(1..topicLabels.size - 1)
        updateData()
    }

    fun expandTopics(expansionCoefficient: Int, randomizedAveragePrecisions: Map<String, DoubleArray>, randomizedTopicLabels: Array<String>) {
        this.expansionCoefficient = expansionCoefficient
        numberOfTopics += randomizedTopicLabels.size

        averagePrecisions.entries.forEach { (systemLabel, apValues) ->
            averagePrecisions[systemLabel] = (apValues.toList() + (randomizedAveragePrecisions[systemLabel]?.toList()
                ?: emptyList())).toTypedArray()
        }

        topicLabels = (topicLabels.toList() + randomizedTopicLabels.toList()).toTypedArray()
        updateData()
    }

    fun expandSystems(expansionCoefficient: Int, trueNumberOfSystems: Int, randomizedAveragePrecisions: Map<String, DoubleArray>, randomizedSystemLabels: Array<String>) {
        this.expansionCoefficient = expansionCoefficient
        val newNumberOfSystems = numberOfSystems + expansionCoefficient

        if (newNumberOfSystems < trueNumberOfSystems) {
            /* Revert to a prefix of the original APs (stable order) */
            averagePrecisions = linkedMapOf<String, Array<Double>>().also { dst ->
                originalAveragePrecisions.entries.take(newNumberOfSystems).forEach { (k, v) ->
                    dst[k] = v.copyOf()
                }
            }
        } else {
            randomizedSystemLabels.forEach { sys ->
                averagePrecisions[sys] = (randomizedAveragePrecisions[sys]?.toList() ?: emptyList()).toTypedArray()
            }
            systemLabels = (systemLabels.toList() + randomizedSystemLabels.toList()).toTypedArray()
        }

        updateData()
    }

    private fun updateData() {
        topicLabels.forEach { t -> topicDistribution[t] = TreeMap() }

        numberOfSystems = averagePrecisions.entries.size

        var iterator = averagePrecisions.entries.iterator()
        systemLabels = Array(numberOfSystems) { iterator.next().key }

        val useColumns = BooleanArray(numberOfTopics)
        Arrays.fill(useColumns, true)

        iterator = averagePrecisions.entries.iterator()
        meanAveragePrecisions = Array(averagePrecisions.entries.size) {
            Tools.getMean(iterator.next().value.toDoubleArray(), useColumns)
        }
    }

    /* ------------------------------ Solve + streaming ------------------------------ */

    /**
     * Solve once:
     *  - AVERAGE streams per-K during the loop (unchanged).
     *  - BEST/WORST streams:
     *      * -Fun/-Var: only when the per-K representative improves (append).
     *      * -Top:       batch K-block replacements once per generation.
     */
    fun solve(
        parameters: Parameters,
        out: SendChannel<ProgressEvent>? = null
    ): Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>> {

        datasetName = parameters.datasetName
        currentExecution = parameters.currentExecution
        populationSize = parameters.populationSize
        numberOfRepetitions = parameters.numberOfRepetitions

        val correlationStrategy = this.loadCorrelationMethod(parameters.correlationMethod)
        val targetStrategy = this.loadTargetToAchieve(parameters.targetToAchieve)

        logger.info("Execution started on \"${Thread.currentThread().name}\" with target \"${parameters.targetToAchieve}\".")

        /* Reset state for a clean run */
        notDominatedSolutions = mutableListOf()
        dominatedSolutions = mutableListOf()
        topSolutions = mutableListOf()
        allSolutions = mutableListOf()

        val threadName = Thread.currentThread().name
        val startNs = System.nanoTime()

        if (targetToAchieve == Constants.TARGET_AVERAGE) {

            /* -----------------------------
             * AVERAGE experiment (unchanged)
             * -----------------------------
             * We compute percentiles and stream exactly one row per cardinality K during the loop.
             */

            /* Deterministic RNG source:
             * Use jMetal's singleton RNG so RngBridge can pin the seed. */
            val rng = JMetalRandom.getInstance().randomGenerator

            val variableValues = mutableListOf<Array<Boolean>>()
            val cardinalities = mutableListOf<Int>()
            val correlations = mutableListOf<Double>()

            val loggingFactor = (numberOfTopics * Constants.LOGGING_FACTOR) / 100
            var progressCounter = 0

            parameters.percentiles.forEach { p -> percentiles[p] = LinkedList() }

            for ((iterationCounter, currentCardinalityIdx) in (0 until numberOfTopics).withIndex()) {

                if (loggingFactor > 0 &&
                    (iterationCounter % loggingFactor) == 0 &&
                    numberOfTopics > loggingFactor
                ) {
                    logger.info(
                        "Completed iterations: $currentCardinalityIdx/$numberOfTopics ($progressCounter%) on \"${Thread.currentThread().name}\" with target ${parameters.targetToAchieve}."
                    )
                    progressCounter += Constants.LOGGING_FACTOR
                }

                var sumCorrelations = 0.0
                var topicStatusString = ""
                var topicStatus = BooleanArray(numberOfTopics)

                val correlationsToAggregate = Array(numberOfRepetitions) {
                    val desiredCardinality = currentCardinalityIdx + 1

                    // Build 0..N-1 pool of topic indices
                    val topicIndexPool = IntArray(numberOfTopics) { it }

                    // Partial Fisher–Yates: fix the first `desiredCardinality` positions with unique picks
                    for (leftmostFixedIndex in 0 until desiredCardinality) {
                        val remainingSpan = numberOfTopics - leftmostFixedIndex
                        val randomOffsetWithinSpan = (rng.nextDouble() * remainingSpan).toInt()
                        val swapWithIndex = leftmostFixedIndex + randomOffsetWithinSpan

                        val tempIndex = topicIndexPool[leftmostFixedIndex]
                        topicIndexPool[leftmostFixedIndex] = topicIndexPool[swapWithIndex]
                        topicIndexPool[swapWithIndex] = tempIndex
                    }

                    // Mark selected topics
                    topicStatus = BooleanArray(numberOfTopics)
                    for (selectedSlot in 0 until desiredCardinality) {
                        val selectedTopicIndex = topicIndexPool[selectedSlot]
                        topicStatus[selectedTopicIndex] = true
                    }

                    // String form of the bitset (for logs/streaming)
                    topicStatusString = buildString(numberOfTopics) {
                        topicStatus.forEach { append(if (it) '1' else '0') }
                    }

                    // Compute correlation on the reduced mean vector
                    val apEntryIterator = this.averagePrecisions.entries.iterator()
                    val reducedMeanVector = Array(averagePrecisions.entries.size) {
                        Tools.getMean(apEntryIterator.next().value.toDoubleArray(), topicStatus)
                    }

                    correlationStrategy.invoke(reducedMeanVector, meanAveragePrecisions)
                }

                correlationsToAggregate.forEach { c -> sumCorrelations += c }
                correlationsToAggregate.sort()
                val meanCorrelation = sumCorrelations / numberOfRepetitions

                percentiles.entries.forEach { (percentile, collected) ->
                    val position = kotlin.math.ceil((percentile / 100.0) * correlationsToAggregate.size).toInt()
                    val percentileValue = correlationsToAggregate[position - 1]
                    percentiles[percentile] = collected.plus(percentileValue)
                    logger.debug("<Cardinality: $currentCardinalityIdx, Percentile: $percentile, Value: $percentileValue>")
                }

                logger.debug("<Correlation: $meanCorrelation, Number of selected topics: $currentCardinalityIdx, Last gene evaluated: $topicStatusString>")

                cardinalities.add(currentCardinalityIdx + 1)
                correlations.add(meanCorrelation)
                variableValues.add(topicStatus.toTypedArray())

                // STREAM NOW (AVERAGE)
                sendProgress(
                    out, CardinalityResult(
                        target = targetToAchieve,
                        threadName = threadName,
                        cardinality = currentCardinalityIdx + 1,
                        correlation = meanCorrelation,
                        functionValuesCsvLine = "${currentCardinalityIdx + 1} $meanCorrelation",
                        variableValuesCsvLine = booleanArrayToVarLine(topicStatus)
                    )
                )
            }

            problem = BestSubsetProblem(
                parameters, numberOfTopics, averagePrecisions, meanAveragePrecisions, topicLabels, correlationStrategy, targetStrategy
            )

            /* Build solutions from precomputed gene vectors to keep topicDistribution/etc. consistent */
            for (index in 0 until numberOfTopics) {
                val solution = BestSubsetSolution(
                    numberOfVariables = 1,
                    numberOfObjectives = 2,
                    numberOfTopics = numberOfTopics,
                    topicLabels = problem.topicLabels,
                    forcedCardinality = null
                )
                val genes = variableValues[index]
                solution.setVariable(0, solution.createNewBitSet(numberOfTopics, genes))
                solution.setObjective(0, cardinalities[index].toDouble())
                solution.setObjective(1, correlations[index])

                notDominatedSolutions.add(solution)
                allSolutions.add(solution)
            }

        } else {

            /* ---------------------------------
             * BEST / WORST experiments (streamed)
             * ---------------------------------
             *
             * - -Fun/-Var: Append a row **only** when the per-K representative improves (multiple rows per K allowed).
             *              We sort each increment batch by (K asc, corr asc|desc) before streaming.
             *              At the very end, `DatasetView` globally re-sorts by (K asc, corr asc).
             * - -Top:      After each generation, compute the **exact 10 lines** for each K from problem.topSolutions;
             *              if that 10-line block changed (content or order), send a **TopKReplaceBatch** for the
             *              changed Ks only. On `RunCompleted`, also send a final batch to ensure completeness.
             */

            if (populationSize < numberOfTopics) throw ParseException(
                "Value for the option <<p>> or <<po>> must be greater or equal than/to $numberOfTopics. Current value is $populationSize. Check the usage section below."
            )
            numberOfIterations = parameters.numberOfIterations

            problem = BestSubsetProblem(
                parameters, numberOfTopics, averagePrecisions, meanAveragePrecisions, topicLabels, correlationStrategy, targetStrategy
            )
            crossover = BinaryPruningCrossover(0.7)
            mutation = BitFlipMutation(0.3)
            selection = BinaryTournamentSelection(RankingAndCrowdingDistanceComparator())

            val evaluator = SequentialSolutionListEvaluator<BinarySolution>()

            /* (1) Track monotone best-so-far correlation per K (for deciding whether to stream). */
            val bestSeenCorrByK = java.util.TreeMap<Int, Double>()

            /* (2) Track the last sent 10-row signature for -Top per K (for replace semantics). */
            val lastSentTopSigByK = mutableMapOf<Int, Int>()

            val lastGenerationIndex = AtomicInteger(0)

            /* Per-generation hook:
             *  (A) -Fun/-Var: stream improved Ks (sorted), one row per K for this generation.
             *  (B) -Top:      replace the 10-row block for any K whose top-10 changed (batched).
             */
            val onGeneration: (Int, List<BinarySolution>) -> Unit = { generationIndex, _ ->
                lastGenerationIndex.set(generationIndex)

                /* ------------------- (A) -Fun / -Var ------------------- */
                // Use the top solution per K from BestSubsetProblem.topSolutions.
                val currentTopByK: Map<Int, BinarySolution> =
                    problem.topSolutions.mapNotNull { (kAsDouble, list) ->
                        val top = list.firstOrNull() ?: return@mapNotNull null
                        kAsDouble.toInt() to top
                    }.toMap()

                val improvedRows = mutableListOf<Pair<Int, BinarySolution>>()

                for ((k, topSol) in currentTopByK.entries) {
                    val corr = (topSol as BestSubsetSolution).getCorrelation()
                    val prev = bestSeenCorrByK[k]

                    val improved = when (targetToAchieve) {
                        Constants.TARGET_WORST -> (prev == null) || corr < prev - 1e-12
                        else -> (prev == null) || corr > prev + 1e-12
                    }

                    if (improved) {
                        bestSeenCorrByK[k] = corr
                        improvedRows += k to topSol
                    }
                }

                // Sort improved rows to keep the files ordered as they grow by appending.
                //  BEST  -> (K asc, corr asc)
                //  WORST -> (K asc, corr desc)
                improvedRows.sortWith(funVarComparatorFor(targetToAchieve))

                // Stream the improved rows (append). Multiple rows per K over time is expected.
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
                // For each K, build the exact 10-line block (corr ASC).
                // Replace only if the content or order actually changed.
                val changedBlocks = mutableMapOf<Int, List<String>>()
                for ((kAsDouble, list) in problem.topSolutions) {
                    val k = kAsDouble.toInt()
                    val lines = top10CsvLinesForK(list)
                    if (lines.isEmpty()) continue  // do not send partial blocks

                    val sig = topBlockSig(lines)
                    val prevSig = lastSentTopSigByK[k]
                    if (prevSig == null || prevSig != sig) {
                        lastSentTopSigByK[k] = sig
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

            /* Run NSGA-II with the per-generation streaming hook. */
            val algo = StreamingNSGAII(
                problem = problem,
                maxEvaluations = numberOfIterations,
                populationSize = populationSize,
                crossover = crossover,
                mutation = mutation,
                selection = selection,
                evaluator = evaluator,
                onGen = onGeneration,
                logger = logger
            )

            logger.info("Starting NSGA-II run with Streaming hook: {}", algo.javaClass.name)

            val startNsLocal = System.nanoTime()
            algo.run()
            computingTime = (System.nanoTime() - startNsLocal) / 1_000_000

            /* ---------------- Collect final solution sets ---------------- */

            // Non-dominated set (deduplicated, order preserved), fix objectives to "external view".
            notDominatedSolutions = java.util.LinkedHashSet(algo.result).toMutableList()
            fixObjectiveFunctionValues(notDominatedSolutions)

            // Track Ks already covered by non-dominated; we keep dominated only for Ks not present above.
            val nonDominatedK = notDominatedSolutions
                .mapTo(LinkedHashSet<Double>(notDominatedSolutions.size)) { it.getCardinality() }

            allSolutions.addAll(notDominatedSolutions)

            dominatedSolutions = problem.dominatedSolutions.values
                .asSequence()
                .filter { s -> !nonDominatedK.contains(s.getCardinality()) }
                .toMutableList()
            fixObjectiveFunctionValues(dominatedSolutions)

            allSolutions.addAll(dominatedSolutions)

            // Gather topSolutions (fixed objectives, but we do not stream them line-by-line here).
            topSolutions.clear()
            problem.topSolutions.values.forEach { aList ->
                val fixed = fixObjectiveFunctionValues(aList.toMutableList())
                topSolutions.addAll(fixed)
            }

            /* -------- Final -Top replace to ensure the snapshot is complete -------- */
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
        for (solutionToAnalyze in allSolutions) {
            val topicStatus = (solutionToAnalyze as BestSubsetSolution).retrieveTopicStatus()
            val k = solutionToAnalyze.getCardinality()
            for (i in topicStatus.indices) {
                topicDistribution[topicLabels[i]]?.let {
                    var status = false
                    if (topicStatus[i]) status = true
                    it[k] = status
                }
            }
        }

        allSolutions = sortByCardinality(allSolutions)

        val computingTime = (System.nanoTime() - startNs) / 1_000_000
        this.computingTime = computingTime
        sendProgress(out, RunCompleted(targetToAchieve, threadName, computingTime))

        logger.info("Not dominated solutions generated by execution with target \"$targetToAchieve\": ${notDominatedSolutions.size}/$numberOfTopics.")
        logger.info("Dominated solutions generated by execution with target \"$targetToAchieve\": ${dominatedSolutions.size}/$numberOfTopics.")
        if (targetToAchieve != Constants.TARGET_AVERAGE)
            logger.info("Total solutions generated by execution with target \"$targetToAchieve\": ${allSolutions.size}/$numberOfTopics.")

        return Triple(allSolutions, topSolutions, Triple(targetToAchieve, Thread.currentThread().name, computingTime))
    }

    private fun sendProgress(out: SendChannel<ProgressEvent>?, ev: ProgressEvent) {
        if (out == null) return
        /* Block to preserve ordering / back-pressure semantics */
        kotlinx.coroutines.runBlocking { out.send(ev) }
    }

    /* ------------------------- Helpers ------------------------- */

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

        return when (correlationMethod) {
            Constants.CORRELATION_PEARSON -> pearsonCorrelation
            Constants.CORRELATION_KENDALL -> kendallCorrelation
            else -> pearsonCorrelation
        }
    }

    /**
     * Internal objective encoding for NSGA-II depending on the **target**.
     * BEST:  keep K as-is, store correlation negated (so "smaller" is better internally).
     * WORST: negate K, keep correlation as-is.
     */
    private fun loadTargetToAchieve(targetToAchieve: String): (BinarySolution, Double) -> BinarySolution {

        val bestStrategy: (BinarySolution, Double) -> BinarySolution = { solution, correlation ->
            solution.setObjective(0, (solution as BestSubsetSolution).numberOfSelectedTopics.toDouble())
            solution.setObjective(1, correlation * -1)   // NOTE: negate corr for BEST for internal Pareto sense
            solution
        }
        val worstStrategy: (BinarySolution, Double) -> BinarySolution = { solution, correlation ->
            solution.setObjective(0, ((solution as BestSubsetSolution).numberOfSelectedTopics * -1).toDouble())
            solution.setObjective(1, correlation)         // NOTE: keep corr as-is for WORST internally
            solution
        }

        this.targetToAchieve = targetToAchieve

        return when (targetToAchieve) {
            Constants.TARGET_BEST -> bestStrategy
            Constants.TARGET_WORST -> worstStrategy
            else -> bestStrategy
        }
    }

    private fun sortByCardinality(solutionsToSort: MutableList<BinarySolution>): MutableList<BinarySolution> {
        solutionsToSort.sortWith(kotlin.Comparator { sol1: BinarySolution, sol2: BinarySolution ->
            (sol1 as BestSubsetSolution).compareTo(sol2 as BestSubsetSolution)
        })
        return solutionsToSort
    }

    /** Adjust objective values from the internal NSGA-II encoding to the external reporting view. */
    private fun fixObjectiveFunctionValues(solutionsToFix: MutableList<BinarySolution>): MutableList<BinarySolution> {
        when (targetToAchieve) {
            Constants.TARGET_BEST -> solutionsToFix.forEach { s ->
                s.setObjective(1, s.getCorrelation() * -1)  // restore reported correlation
            }

            Constants.TARGET_WORST -> solutionsToFix.forEach { s ->
                s.setObjective(0, s.getCardinality() * -1)  // restore reported K
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
            Constants.TARGET_BEST -> "${k} ${-corr}"  // corr negated internally, flip back here
            Constants.TARGET_WORST -> "${-k} ${corr}"  // K negated internally, flip back here
            else -> "$k $corr"
        }
    }

    /** Format for -Var: contiguous bitstring (no spaces) to match existing writer. */
    private fun BinarySolution.toVarLine(): String {
        val bits = (this as BestSubsetSolution).retrieveTopicStatus()
        val sb = StringBuilder(bits.size)
        bits.forEach { sb.append(if (it) '1' else '0') }
        return sb.toString()
    }

    /** CSV row for -Top: "Cardinality,Correlation,Topics" */
    private fun BestSubsetSolution.toTopCsv(): String =
        "${getCardinality()},${getCorrelation()},${getTopicLabelsFromTopicStatus()}"

    /* ---------- Additional helpers for streaming order & -Top blocks ---------- */

    /**
     * Comparator used to keep -Fun/-Var **orderly as the file grows by appending**.
     *
     * Rule:
     *  • Primary: K ascending (actual cardinality, external view).
     *  • Secondary: correlation ordering chosen to keep *append order* sensible:
     *      - BEST:  corr ascending (improvements increase corr → newer lines go after older).
     *      - WORST: corr **descending** (improvements decrease corr → newer lines go after older).
     *
     * Final rewrite by the view is always (K asc, corr asc) for a single stable order.
     */
    private fun funVarComparatorFor(target: String): Comparator<Pair<Int, BinarySolution>> {
        return if (target == Constants.TARGET_WORST) {
            compareBy<Pair<Int, BinarySolution>> { it.first }
                .thenByDescending { (it.second as BestSubsetSolution).getCorrelation() }
        } else {
            compareBy<Pair<Int, BinarySolution>> { it.first }
                .thenBy { (it.second as BestSubsetSolution).getCorrelation() }
        }
    }

    /**
     * Build the **exact 10 CSV lines** for a -Top block (for a given K's list).
     * Lines are sorted by correlation **ascending** to match the requested format.
     * If there are fewer than 10 entries available, we return an empty list and
     * skip emitting a partial block (keeps the -Top file shape stable).
     */
    private fun top10CsvLinesForK(
        topList: List<BinarySolution>
    ): List<String> {
        val ascByCorr = topList.sortedBy { (it as BestSubsetSolution).getCorrelation() }
        if (ascByCorr.size < 10) return emptyList()
        val ten = ascByCorr.take(10)
        return ten.map { (it as BestSubsetSolution).toTopCsv() }
    }

    /** Cheap and stable signature of a 10-line block to detect content/order changes. */
    private fun topBlockSig(lines: List<String>): Int =
        lines.joinToString("\n").hashCode()

    /* ---------------------- Streaming NSGA-II wrapper ---------------------- */

    /**
     * Minimal subclass of NSGA-II with a per-generation callback:
     *  - We invoke `onGen` at init (gen = 0) and once per generation after replacement.
     *  - The model never mutates objective values here; it only reads solutions
     *    and delegates streaming decisions to the `onGen` lambda.
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
        private var genIndex = 0

        override fun initProgress() {
            super.initProgress()
            genIndex = 0
            logger.debug("gen={} initProgress -> initial population ready (size={})", genIndex, population.size)
            onGen.invoke(genIndex, population)
        }

        override fun updateProgress() {
            super.updateProgress()
            genIndex += 1
            logger.debug("gen={} updateProgress -> population updated (size={})", genIndex, population.size)
            onGen.invoke(genIndex, population)
        }
    }
}
