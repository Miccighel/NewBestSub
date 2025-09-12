package it.uniud.newbestsub.dataset

import com.opencsv.CSVReader
import it.uniud.newbestsub.dataset.model.*
import it.uniud.newbestsub.dataset.model.buildPrecomputedData
import it.uniud.newbestsub.dataset.model.toPrimitiveAPRows
import it.uniud.newbestsub.math.Correlations
import it.uniud.newbestsub.problem.*
import it.uniud.newbestsub.problem.operators.BinaryPruningCrossover
import it.uniud.newbestsub.problem.operators.BitFlipMutation
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Constants.GENERATION_STRIDE
import it.uniud.newbestsub.utils.Constants.GENERATION_WARMUP
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.launch
import org.apache.commons.cli.ParseException
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.crossover.CrossoverOperator
import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.operator.selection.SelectionOperator
import org.uma.jmetal.operator.selection.impl.BinaryTournamentSelection
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator
import org.uma.jmetal.util.evaluator.SolutionListEvaluator
import org.uma.jmetal.util.evaluator.impl.MultiThreadedSolutionListEvaluator
import org.uma.jmetal.util.evaluator.impl.SequentialSolutionListEvaluator
import org.uma.jmetal.util.pseudorandom.JMetalRandom
import java.io.File
import java.io.FileReader
import java.util.*
import kotlin.collections.set

/**
 * DatasetModel
 *
 * In-memory orchestrator for:
 * - Dataset AP matrix (systems × topics).
 * - Streaming NSGA-II execution for BEST/WORST and random sampling for AVERAGE.
 * - Compact caches so aggregations do not retain large populations.
 *
 * Output artifacts:
 * - -Fun: best-so-far improvements per K (monotone in natural objective; AVERAGE writes one row/K).
 * - -Var: genotypes aligned with -Fun, encoded as "B64:<...>" (AVERAGE: one/K).
 * - -Top: presentation snapshots per K from a small persistent pool (ordered, capped).
 *
 * Branches:
 * - AVERAGE samples [numberOfRepetitions] subsets per K, streams one FUN/VAR row, computes percentiles.
 * - BEST/WORST stream only on strict improvement per K in natural scale and maintain the Top pool.
 *
 * Notes on negative correlations:
 * - Correlation ∈ [-1,1]; WORST minimizes it (negatives expected at small K).
 * - AVERAGE can be negative for small K due to rank inversions in samples.
 *
 * Invariants:
 * - -Fun (BEST/WORST) strictly improves per K in natural scale, and aligns 1:1 with -Var rows.
 * - -Top is a snapshot; consumers should pick extrema per K (BEST=max, WORST=min) or use the first line post-ordering.
 *
 * RAM & CPU efficiency:
 * - Column-major hot path: per-topic column views allow tight loops over systems.
 * - After load/expansion, [sealData] keeps dense [PrecomputedData] and drops boxed maps.
 * - During runs we cache only per-K representative correlation/mask and a small Top pool.
 *
 * Evaluator policy:
 * - Controlled by the controller; this class respects `parameters.evaluatorThreads` and `parameters.deterministic`.
 *   MultiThreaded evaluator is used only when non-deterministic and threads > 1; otherwise Sequential.
 */
class DatasetModel {

    /* Basic dataset fields */

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

    /* Boxed AP rows are used only during load/expansion. Cleared by [sealData] to reduce RAM. */
    var averagePrecisions = linkedMapOf<String, Array<Double>>()
    private var originalAveragePrecisions = linkedMapOf<String, Array<Double>>()

    /* Full-set mean AP per system (boxed for API compatibility with callers). */
    var meanAveragePrecisions = emptyArray<Double>()

    /* AVERAGE branch percentiles (materialized at the end). */
    var percentiles = linkedMapOf<Int, List<Double>>()

    /* Kept for API compatibility; not populated to avoid per-topic maps during runs. */
    var topicDistribution = linkedMapOf<String, MutableMap<Double, Boolean>>()

    /* Wall-clock computing time in milliseconds for the last run. */
    var computingTime: Long = 0

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    private lateinit var problem: BestSubsetProblem
    private lateinit var crossover: CrossoverOperator<BinarySolution>
    private lateinit var mutation: MutationOperator<BinarySolution>

    /* Memory-friendly run-time state */

    private var precomputedData: PrecomputedData? = null
    private val repMaskB64ByK = mutableMapOf<Int, String>()   /* representative mask per K, Base64 (no prefix) */
    private val corrByK = TreeMap<Int, Double>()              /* natural correlation per K */
    private var topicIndexByLabel: Map<String, Int> = emptyMap()

    /* Persistent Top pool */

    private data class TopEntry(val natural: Double, val maskB64: String)

    private val topPoolByK: MutableMap<Int, MutableMap<String, TopEntry>> = mutableMapOf()

    /* Multicore metrics (filled in solve) */
    var evaluatorThreadsUsed: Int = 1
    var processCpuTimeMs: Long = 0L
    var avgParallelismX: Double = 0.0

    /* Helpers */

    /**
     * Convert internal minimization value to natural correlation.
     * BEST stores negative objective internally; WORST stores positive.
     * Returning the natural value keeps external reporting uniform.
     */
    fun naturalCorrOf(solution: BestSubsetSolution): Double {
        val internal = solution.getCorrelation()
        return if (targetToAchieve == Constants.TARGET_BEST) -internal else internal
    }

    /**
     * Build ordered lines for the Top file from the pool of unique masks per K.
     * Sorting depends on the target:
     * - BEST: descending by natural correlation, tie-break by mask to keep stable output.
     * - WORST: ascending by natural correlation, tie-break by mask.
     */
    private fun buildTopLinesFromPool(k: Int): List<String> {
        val pool = topPoolByK[k] ?: return emptyList()
        fun rankBest(v: Double) = if (v.isNaN()) Double.NEGATIVE_INFINITY else v
        fun rankWorst(v: Double) = if (v.isNaN()) Double.POSITIVE_INFINITY else v
        val ordered = if (targetToAchieve == Constants.TARGET_BEST) {
            pool.values.sortedWith(compareByDescending<TopEntry> { rankBest(it.natural) }.thenBy { it.maskB64 })
        } else {
            pool.values.sortedWith(compareBy<TopEntry> { rankWorst(it.natural) }.thenBy { it.maskB64 })
        }
        /* Emit "K,natural,TopicsB64" with the "B64:" prefix restored */
        return ordered.take(Constants.TOP_SOLUTIONS_NUMBER).map { e -> "$k,${e.natural},B64:${e.maskB64}" }
    }

    /* Data loading and expansion */

    /**
     * Load dataset AP matrix from a CSV file.
     *
     * Expected layout:
     * - Row 0: header. First cell is a label, remaining cells are topic labels [T1..TN].
     * - Rows 1..M: first cell = system label; remaining cells = AP for each topic.
     *
     * Implementation notes:
     * - Uses OpenCSV for parsing.
     * - Stores AP rows as boxed arrays initially; later sealed into primitives and column views.
     */
    fun loadData(datasetPath: String) {
        CSVReader(FileReader(datasetPath)).use { csv ->
            val header = csv.readNext() ?: throw IllegalArgumentException("Empty CSV: $datasetPath")
            topicLabels = header
            numberOfTopics = topicLabels.size - 1
            datasetName = File(datasetPath).nameWithoutExtension

            var row = csv.readNext()
            while (row != null) {
                if (row.isNotEmpty()) {
                    /* Parse all AP values for this system; skip column 0 which is the system label. */
                    val apValues = DoubleArray(row.size - 1)
                    var colIndex = 1
                    while (colIndex < row.size) {
                        apValues[colIndex - 1] = row[colIndex].toDouble()
                        colIndex++
                    }
                    averagePrecisions[row[0]] = apValues.toTypedArray()
                }
                row = csv.readNext()
            }
        }

        /* Keep a copy of original boxed rows for expandSystems reverts. */
        originalAveragePrecisions = linkedMapOf<String, Array<Double>>().also { dst ->
            averagePrecisions.forEach { (system, values) -> dst[system] = values.copyOf() }
        }

        numberOfSystems = averagePrecisions.size
        topicLabels = topicLabels.sliceArray(1 until topicLabels.size)
        refreshDerivedData()
    }

    /**
     * Append fake topics to all systems with given AP values and labels.
     * The caller provides already randomized APs and labels for determinism.
     */
    fun expandTopics(
        expansionCoefficient: Int,
        randomizedAveragePrecisions: Map<String, DoubleArray>,
        randomizedTopicLabels: Array<String>
    ) {
        this.expansionCoefficient = expansionCoefficient
        numberOfTopics += randomizedTopicLabels.size

        /* Append the extra APs to each existing system row. */
        averagePrecisions.entries.forEach { (systemLabel, apValues) ->
            val extraValues = randomizedAveragePrecisions[systemLabel]?.toList() ?: emptyList()
            averagePrecisions[systemLabel] = (apValues.toList() + extraValues).toTypedArray()
        }

        topicLabels = (topicLabels.toList() + randomizedTopicLabels.toList()).toTypedArray()
        refreshDerivedData()
    }

    /**
     * Append fake systems, or revert to a prefix of the originals when requested size
     * is smaller than the true number of systems.
     *
     * Implementation detail:
     * - New systems are given AP rows sized exactly to the current numberOfTopics.
     */
    fun expandSystems(
        expansionCoefficient: Int,
        trueNumberOfSystems: Int,
        randomizedAveragePrecisions: Map<String, DoubleArray>,
        randomizedSystemLabels: Array<String>
    ) {
        this.expansionCoefficient = expansionCoefficient
        val newNumberOfSystems = numberOfSystems + expansionCoefficient

        if (newNumberOfSystems < trueNumberOfSystems) {
            /* Revert to a prefix of the original AP map rather than appending fakes. */
            averagePrecisions = linkedMapOf<String, Array<Double>>().also { dst ->
                originalAveragePrecisions.entries.take(newNumberOfSystems).forEach { (system, values) ->
                    dst[system] = values.copyOf()
                }
            }
        } else {
            /* Append provided randomized systems. */
            randomizedSystemLabels.forEach { system ->
                averagePrecisions[system] = (randomizedAveragePrecisions[system]?.toList() ?: emptyList()).toTypedArray()
            }
            systemLabels = (systemLabels.toList() + randomizedSystemLabels.toList()).toTypedArray()
        }

        refreshDerivedData()
    }

    /**
     * Refresh derived fields after load/expansion:
     * - topicDistribution is reset for API compatibility.
     * - topicIndexByLabel is rebuilt for quick lookups.
     * - averagePrecisions (boxed) are sealed into [PrecomputedData] primitives.
     * - meanAveragePrecisions is updated from the sealed bundle.
     */
    private fun refreshDerivedData() {
        topicDistribution.clear()
        topicLabels.forEach { label -> topicDistribution[label] = TreeMap() }
        topicIndexByLabel = topicLabels.withIndex().associate { (i, lbl) -> lbl to i }

        numberOfSystems = averagePrecisions.size
        val apEntryIterator = averagePrecisions.entries.iterator()
        systemLabels = Array(numberOfSystems) { apEntryIterator.next().key }

        /* Seal boxed to primitive and build column-major views for hot loops. */
        val primitiveAP: Map<String, DoubleArray> = toPrimitiveAPRows(averagePrecisions)
        precomputedData = buildPrecomputedData(primitiveAP)

        val means = precomputedData!!.fullSetMeanAPBySystem
        meanAveragePrecisions = Array(means.size) { idx -> means[idx] }
    }

    /**
     * Finalize the numeric bundle and drop large boxed structures to free memory.
     * Idempotent; safe to call multiple times.
     */
    fun sealData() {
        if (precomputedData == null) {
            val primitiveAP = toPrimitiveAPRows(averagePrecisions)
            precomputedData = buildPrecomputedData(primitiveAP)
        }
        val means = precomputedData!!.fullSetMeanAPBySystem
        meanAveragePrecisions = Array(means.size) { i -> means[i] }
        systemLabels = precomputedData!!.systemIdsInOrder.toTypedArray()
        numberOfSystems = precomputedData!!.numberOfSystems
        numberOfTopics = precomputedData!!.numberOfTopics

        /* After sealing, drop boxed maps to keep the memory profile tight. */
        averagePrecisions.clear()
        originalAveragePrecisions.clear()
    }

    /* Solve and streaming */

    /**
     * Run the chosen experiment branch (AVERAGE vs BEST/WORST).
     *
     * Streaming:
     * - AVERAGE: one row per K.
     * - BEST/WORST: only on strict improvement per K (natural scale). Also emits Top batches.
     *
     * Back-pressure:
     * - Events are pushed via [sendProgress], which tries non-blocking first, then suspends on a safe scope.
     *
     * Returns an empty triple for compatibility with older callers; results are streamed and cached internally.
     */
    fun solve(
        parameters: Parameters,
        out: SendChannel<ProgressEvent>? = null,
        eventScope: CoroutineScope? = null
    ): Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>> {

        datasetName = parameters.datasetName
        currentExecution = parameters.currentExecution
        populationSize = parameters.populationSize
        numberOfIterations = parameters.numberOfIterations
        numberOfRepetitions = parameters.numberOfRepetitions
        correlationMethod = parameters.correlationMethod

        val encodeObjectives = loadTargetToAchieve(parameters.targetToAchieve)
        val runStartNs = System.nanoTime()
        val workerThreadName = Thread.currentThread().name

        val osMxGeneric = java.lang.management.ManagementFactory.getOperatingSystemMXBean()
        val osBean = osMxGeneric as? com.sun.management.OperatingSystemMXBean
        val cpuStartNs: Long = osBean?.processCpuTime ?: -1L

        logger.info("Execution started on \"{}\" with target \"{}\".", workerThreadName, parameters.targetToAchieve)

        /* Clear per-run caches */
        repMaskB64ByK.clear()
        corrByK.clear()
        topPoolByK.clear()

        if (targetToAchieve == Constants.TARGET_AVERAGE) {

            /* AVERAGE branch: sampling-based estimate per K with optional Pearson fast path. */

            evaluatorThreadsUsed = 1
            logger.info("Evaluator: Sequential (threads={}) for target {}.", evaluatorThreadsUsed, parameters.targetToAchieve)

            val numericBundle = precomputedData ?: buildPrecomputedData(toPrimitiveAPRows(averagePrecisions))
            val systemsCount = numericBundle.numberOfSystems
            val fullSetMeans = numericBundle.fullSetMeanAPBySystem

            val usePearson = parameters.correlationMethod == Constants.CORRELATION_PEARSON
            val yStats = if (usePearson) Correlations.precomputeYStats(fullSetMeans) else null

            /* Pool of topic indices used by partial Fisher–Yates to pick first K without allocations. */
            val topicIndexPool = IntArray(numberOfTopics) { it }

            /* Accumulators reused across repetitions. */
            val subsetSumsBySystem = DoubleArray(systemsCount)
            /* For non-Pearson correlations we compute subset means first to use the generic correlator. */
            val subsetMeansBySystem = if (usePearson) DoubleArray(0) else DoubleArray(systemsCount)

            val corrSamples = DoubleArray(maxOf(1, numberOfRepetitions))
            val requestedPercentiles: List<Int> = parameters.percentiles
            val tmpPercentiles = requestedPercentiles.associateWith { mutableListOf<Double>() }.toMutableMap()

            val primitiveCorrFn = Correlations.makePrimitiveCorrelation(parameters.correlationMethod)

            val totalAvgIterations = numberOfTopics
            val logEveryAvg = ((totalAvgIterations * Constants.LOGGING_FACTOR) / 100).coerceAtLeast(1)
            var progressPct = 0

            var kIdx = 0
            while (kIdx < numberOfTopics) {

                /* Lightweight progress logging with throttle based on dataset size. */
                if ((kIdx % logEveryAvg) == 0 && kIdx <= totalAvgIterations) {
                    logger.info(
                        "Completed iterations: {}/{} ({}%) on \"{}\" with target {}.",
                        kIdx, totalAvgIterations, progressPct,
                        Thread.currentThread().name, parameters.targetToAchieve
                    )
                    logger.debug("Log messages throttled every {} iterations ({}%) (debounced).", logEveryAvg, Constants.LOGGING_FACTOR)
                    progressPct += Constants.LOGGING_FACTOR
                }

                val currentCardinality = kIdx + 1
                val repsForK = maxOf(1, numberOfRepetitions)

                var rep = 0
                while (rep < repsForK) {
                    /* Partial Fisher–Yates: move a random index into position [fixed]. */
                    var fixed = 0
                    while (fixed < currentCardinality) {
                        val remaining = numberOfTopics - fixed
                        val offset = (JMetalRandom.getInstance().randomGenerator.nextDouble() * remaining).toInt()
                        val swapWith = fixed + offset
                        val tmp = topicIndexPool[fixed]
                        topicIndexPool[fixed] = topicIndexPool[swapWith]
                        topicIndexPool[swapWith] = tmp
                        fixed++
                    }

                    /* Accumulate per-system sums across the K selected topics. */
                    Arrays.fill(subsetSumsBySystem, 0.0)
                    var sel = 0
                    while (sel < currentCardinality) {
                        val t = topicIndexPool[sel]
                        val col = numericBundle.topicColumnViewByTopic[t]
                        var s = 0
                        while (s < systemsCount) {
                            subsetSumsBySystem[s] += col[s]
                            s++
                        }
                        sel++
                    }

                    /* Pearson fast path: correlation from sums and K using precomputed Y stats. */
                    corrSamples[rep] =
                        if (usePearson) {
                            Correlations.fastPearsonFromSumsWithKAuto(subsetSumsBySystem, currentCardinality, yStats!!)
                        } else {
                            /* Generic path: compute subset means then call the selected correlator. */
                            val invK = 1.0 / currentCardinality
                            var s = 0
                            while (s < systemsCount) {
                                subsetMeansBySystem[s] = subsetSumsBySystem[s] * invK
                                s++
                            }
                            primitiveCorrFn(subsetMeansBySystem, fullSetMeans)
                        }
                    rep++
                }

                /* Mean across repetitions. */
                var sum = 0.0
                var i = 0
                while (i < repsForK) {
                    sum += corrSamples[i]; i++
                }
                val meanCorrForK = sum / repsForK

                /* Percentiles (nearest-rank). corrSamples are sorted in place to reuse the buffer. */
                Arrays.sort(corrSamples, 0, repsForK)
                for (p in requestedPercentiles) {
                    val acc = tmpPercentiles.getValue(p)
                    val pos = kotlin.math.ceil((p / 100.0) * repsForK).toInt().coerceIn(1, repsForK)
                    acc += corrSamples[pos - 1]
                }

                /* Build the representative mask from the K chosen indices of this outer loop. */
                val maskForStreaming = BooleanArray(numberOfTopics)
                var j = 0
                while (j < currentCardinality) {
                    maskForStreaming[topicIndexPool[j]] = true
                    j++
                }

                repMaskB64ByK[currentCardinality] = booleanMaskToBase64(maskForStreaming)
                corrByK[currentCardinality] = meanCorrForK

                /* Stream one AVERAGE row per K. */
                sendProgress(
                    out,
                    CardinalityResult(
                        target = targetToAchieve,
                        threadName = workerThreadName,
                        cardinality = currentCardinality,
                        correlation = meanCorrForK,
                        functionValuesCsvLine = "$currentCardinality,$meanCorrForK",
                        variableValuesCsvLine = packTopicMaskToVarLineBase64(maskForStreaming)
                    ),
                    eventScope
                )

                kIdx++
            }

            logger.info(
                "Completed iterations: {}/{} (100%) on \"{}\" with target {}.",
                totalAvgIterations, totalAvgIterations,
                Thread.currentThread().name, parameters.targetToAchieve
            )

            /* Materialize percentile columns in model.state for CSV/Parquet writers. */
            percentiles.clear()
            for (p in requestedPercentiles) {
                percentiles[p] = tmpPercentiles.getValue(p).toList()
            }
        } else {

            /* BEST / WORST branch: NSGA-II streaming with improvement-only FUN/VAR and Top blocks. */

            var effectivePopulationSize = populationSize
            if (effectivePopulationSize < 0) {
                logger.warn("Population size <= 0 provided; auto-setting to numberOfTopics ({}) for {}.", numberOfTopics, parameters.targetToAchieve)
                effectivePopulationSize = numberOfTopics
            }
            if (effectivePopulationSize < numberOfTopics) {
                throw ParseException(
                    "Value for the option <<p>> or <<po>> must be >= $numberOfTopics. " +
                            "Current value is $populationSize. Check the usage section below."
                )
            }

            val numericBundle = precomputedData ?: buildPrecomputedData(toPrimitiveAPRows(averagePrecisions))

            problem = BestSubsetProblem(
                parameters = parameters,
                numberOfTopics = numberOfTopics,
                precomputedData = numericBundle,
                topicLabels = topicLabels,
                legacyBoxedCorrelation = Correlations.makePrimitiveCorrelation(parameters.correlationMethod),
                targetStrategy = encodeObjectives
            )

            /* Operators: BinaryPruningCrossover reduces growth by pruning, mutation flips bits with given prob. */
            crossover = BinaryPruningCrossover(0.7)
            mutation = BitFlipMutation(0.3)

            val selectionOperator: SelectionOperator<List<BinarySolution>, BinarySolution> =
                BinaryTournamentSelection(RankingAndCrowdingDistanceComparator())

            /* Evaluator selection matches controller policy; here we only honor the requested threads. */
            val requestedThreads = (if (parameters.evaluatorThreads > 0) parameters.evaluatorThreads else 1).coerceAtLeast(1)
            val listEvaluator: SolutionListEvaluator<BinarySolution> =
                if (!parameters.deterministic && requestedThreads > 1)
                    MultiThreadedSolutionListEvaluator(requestedThreads)
                else
                    SequentialSolutionListEvaluator()

            evaluatorThreadsUsed = if (listEvaluator is MultiThreadedSolutionListEvaluator<*>) requestedThreads else 1
            logger.info(
                "Evaluator: {} (threads={}) for target {}. [deterministic={}, requestedThreads={}]",
                if (listEvaluator is MultiThreadedSolutionListEvaluator<*>) "MultiThreaded" else "Sequential",
                evaluatorThreadsUsed,
                parameters.targetToAchieve,
                parameters.deterministic,
                requestedThreads
            )

            /* Track best natural correlation per K to gate improvement-only streaming. */
            val bestSeenNaturalCorrByK = TreeMap<Int, Double>()
            /* Track block signatures sent to avoid redundant Top rewrites. */
            val lastSentTopSignatureByK = mutableMapOf<Int, Int>()

            val onGeneration: (Int, List<BinarySolution>) -> Unit = fun(generationIndex: Int, populationAtGeneration: List<BinarySolution>) {

                val naturalCorrOfLocal = { s: BestSubsetSolution -> naturalCorrOf(s) }

                /* One representative per K in natural scale. We scan the population and keep the best (or worst). */
                val representativeByK = HashMap<Int, BestSubsetSolution>()
                for (individual in populationAtGeneration) {
                    val bs = individual as BestSubsetSolution
                    val k = bs.getCardinality().toInt()
                    val currentRep = representativeByK[k]
                    val chooseThis = if (currentRep == null) {
                        true
                    } else {
                        val cand = naturalCorrOfLocal(bs)
                        val rep = naturalCorrOfLocal(currentRep)
                        if (targetToAchieve == Constants.TARGET_WORST) cand < rep - 1e-12 else cand > rep + 1e-12
                    }
                    if (chooseThis) representativeByK[k] = bs
                }

                /* Improvement-only streaming to FUN/VAR; also update the per-K Top pool. */
                val improvedRows = mutableListOf<Pair<Int, BestSubsetSolution>>()
                for ((k, bs) in representativeByK) {
                    val cand = naturalCorrOfLocal(bs)
                    val prev = bestSeenNaturalCorrByK[k]
                    val improved =
                        (prev == null) ||
                                (if (targetToAchieve == Constants.TARGET_WORST) cand < prev - 1e-12 else cand > prev + 1e-12)
                    if (improved) {
                        bestSeenNaturalCorrByK[k] = cand
                        improvedRows += k to bs
                    }
                }

                /* Maintain a ranked Top pool keyed by Base64 mask to preserve unique solutions. */
                for (ind in populationAtGeneration) {
                    val bs = ind as BestSubsetSolution
                    val k = bs.getCardinality().toInt()
                    val maskB64 = bs.packMaskToBase64()
                    val natural = naturalCorrOfLocal(bs)
                    val pool = topPoolByK.getOrPut(k) { mutableMapOf() }
                    val prev = pool[maskB64]
                    val better = when (targetToAchieve) {
                        Constants.TARGET_BEST -> (prev == null) || (natural > prev.natural + 1e-12)
                        Constants.TARGET_WORST -> (prev == null) || (natural < prev.natural - 1e-12)
                        else -> (prev == null)
                    }
                    if (better) pool[maskB64] = TopEntry(natural, maskB64)
                }

                /* Throttle streaming to reduce I/O churn. Emit on warmup boundary, on stride, or when improved. */
                val warmup = generationIndex < GENERATION_WARMUP
                val onStride = ((generationIndex - GENERATION_WARMUP) % GENERATION_STRIDE == 0)
                val shouldEmit = (!warmup && onStride) || generationIndex == 0 || improvedRows.isNotEmpty()
                if (!shouldEmit) return

                /* Stabilize chart lines by choosing an order consistent with the target. */
                val appendOrder = if (targetToAchieve == Constants.TARGET_WORST) {
                    compareBy<Pair<Int, BestSubsetSolution>> { it.first }
                        .thenByDescending { naturalCorrOfLocal(it.second) }
                } else {
                    compareBy<Pair<Int, BestSubsetSolution>> { it.first }
                        .thenBy { naturalCorrOfLocal(it.second) }
                }
                improvedRows.sortWith(appendOrder)

                for ((k, bs) in improvedRows) {
                    repMaskB64ByK[k] = bs.packMaskToBase64()
                    corrByK[k] = naturalCorrOfLocal(bs)
                    sendProgress(
                        out,
                        CardinalityResult(
                            target = targetToAchieve,
                            threadName = Thread.currentThread().name,
                            cardinality = k,
                            correlation = naturalCorrOfLocal(bs),
                            functionValuesCsvLine = bs.toFunLineFor(targetToAchieve),
                            variableValuesCsvLine = bs.toVarLinePackedBase64()
                        ),
                        eventScope
                    )
                }

                /* Emit TopK replacement blocks only when content changed to avoid redundant disk work. */
                val changedBlocks = mutableMapOf<Int, List<String>>()
                val seenKs = populationAtGeneration.asSequence().map { it.getCardinality().toInt() }.toSet()
                for (k in seenKs) {
                    val lines = buildTopLinesFromPool(k)
                    if (lines.isEmpty()) continue
                    val sig = topBlockSig(lines)
                    if (lastSentTopSignatureByK[k] != sig) {
                        lastSentTopSignatureByK[k] = sig
                        changedBlocks[k] = lines
                    }
                }
                if (changedBlocks.isNotEmpty()) {
                    sendProgress(out, TopKReplaceBatch(targetToAchieve, Thread.currentThread().name, changedBlocks), eventScope)
                }
            }

            val algorithm = StreamingNSGAII(
                problem = problem,
                maxEvaluations = numberOfIterations,
                populationSize = effectivePopulationSize,
                crossover = crossover,
                mutation = mutation,
                selection = selectionOperator,
                evaluator = listEvaluator,
                onGeneration = onGeneration,
                logger = logger
            )

            logger.info("Starting NSGA-II run with Streaming hook: {}", algorithm.javaClass.name)
            algorithm.run()

            if (listEvaluator is MultiThreadedSolutionListEvaluator<*>) {
                listEvaluator.shutdown()
            }

            /* Final Top blocks flush for all K we have in the pool. */
            if (out != null) {
                val finalBlocks = mutableMapOf<Int, List<String>>()
                for (k in topPoolByK.keys) {
                    val lines = buildTopLinesFromPool(k)
                    if (lines.isNotEmpty()) finalBlocks[k] = lines
                }
                if (finalBlocks.isNotEmpty()) {
                    sendProgress(out, TopKReplaceBatch(target = targetToAchieve, threadName = Thread.currentThread().name, blocks = finalBlocks), eventScope)
                }
            }
        }

        /* Timing end and summary */

        val totalWallMs = (System.nanoTime() - runStartNs) / 1_000_000
        val cpuEndNs: Long = osBean?.processCpuTime ?: -1L
        processCpuTimeMs = if (cpuStartNs >= 0 && cpuEndNs >= 0) (cpuEndNs - cpuStartNs) / 1_000_000 else 0L
        avgParallelismX = if (totalWallMs > 0) processCpuTimeMs.toDouble() / totalWallMs.toDouble() else 0.0
        computingTime = totalWallMs

        logger.info(
            "Timing summary [{}]: wall={} ms, cpu={} ms, avg parallelism ≈ {}×, evaluatorThreads={}",
            targetToAchieve,
            totalWallMs,
            processCpuTimeMs,
            String.format(Locale.ROOT, "%.2f", avgParallelismX),
            evaluatorThreadsUsed
        )

        sendProgress(out, RunCompleted(targetToAchieve, workerThreadName, totalWallMs), eventScope)
        logger.info("Per-K representatives cached: {}/{} for target \"{}\".", corrByK.size, numberOfTopics, targetToAchieve)

        /* Compatibility triple; data is streamed and cached internally for writers. */
        return Triple(emptyList(), emptyList(), Triple(targetToAchieve, workerThreadName, totalWallMs))
    }

    /* Progress and event helpers */

    /**
     * Non-blocking send with structured fallback:
     * - Fast path: trySend never suspends. If it succeeds or channel is closed, stop.
     * - Back-pressure path: if buffer is full, suspend send on a safe scope.
     *   Prefer the caller's [eventScope]; else use a short-lived IO scope to avoid blocking compute threads.
     */
    private fun sendProgress(
        out: SendChannel<ProgressEvent>?,
        event: ProgressEvent,
        eventScope: CoroutineScope? = null
    ) {
        if (out == null) return
        val result = out.trySend(event)
        if (result.isSuccess || result.isClosed) return
        val scope = eventScope ?: CoroutineScope(Dispatchers.IO)
        scope.launch {
            runCatching { out.send(event) }.onFailure { /* Channel closed while suspended → ignore */ }
        }
    }

    /* Objective encoding helpers */

    /**
     * Configure objective encoding based on the target.
     * We use two objectives:
     * - Obj0 encodes K (maximize for BEST via positive value, minimize for WORST via negative).
     * - Obj1 encodes correlation with the correct sign for jMetal's minimization convention.
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

    /* Aggregation-facing helpers */

    fun findCorrelationForCardinality(cardinality: Double): Double? = corrByK[cardinality.toInt()]

    /**
     * Decode the cached Base64 mask for K into a BooleanArray sized to current topics.
     */
    fun retrieveMaskForCardinality(cardinality: Double): BooleanArray? {
        val k = cardinality.toInt()
        val b64 = repMaskB64ByK[k] ?: return null
        return base64ToBooleanMask(b64, numberOfTopics)
    }

    /**
     * Same as above but ensures the result has the requested length,
     * padding or truncating as needed. Useful when schemas differ.
     */
    fun retrieveMaskForCardinalitySized(cardinality: Double, expectedSize: Int): BooleanArray {
        val k = cardinality.toInt()
        val b64 = repMaskB64ByK[k] ?: return BooleanArray(expectedSize)
        val decoded = base64ToBooleanMask(b64, numberOfTopics)
        if (decoded.size == expectedSize) return decoded
        val out = BooleanArray(expectedSize)
        val n = kotlin.math.min(expectedSize, decoded.size)
        System.arraycopy(decoded, 0, out, 0, n)
        return out
    }

    /**
     * Return the cached Base64 mask for K with the "B64:" prefix expected by views.
     */
    fun retrieveMaskB64ForCardinality(cardinality: Double): String = "B64:" + (repMaskB64ByK[cardinality.toInt()] ?: "")

    /* Encoding/decoding of masks */

    /**
     * Small signature to detect changes in a block of lines.
     * Used by the Top writer to avoid redundant rewrites.
     */
    private fun topBlockSig(lines: List<String>): Int = lines.joinToString("\n").hashCode()

    /**
     * Build a FUN line in "K,natural" form, leaving formatting to the view.
     */
    private fun BinarySolution.toFunLineFor(@Suppress("UNUSED_PARAMETER") target: String): String {
        val s = this as BestSubsetSolution
        val kInt = s.getCardinality().toInt()
        val naturalCorr = naturalCorrOf(s)
        return "$kInt,$naturalCorr"
    }

    /**
     * Pack a boolean mask into little-endian 64-bit words.
     * The consumer expects little-endian byte order, so we build words then emit bytes LE.
     */
    private fun packTopicMaskToLongWords(topicPresenceMask: BooleanArray): LongArray {
        val totalBits = topicPresenceMask.size
        val numberOfWords = (totalBits + 63) ushr 6
        val packedWords = LongArray(numberOfWords)

        var currentWordIndex = 0
        var bitIndexWithinWord = 0
        var accumulator = 0L
        var absoluteBitIndex = 0
        while (absoluteBitIndex < totalBits) {
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
            absoluteBitIndex++
        }
        if (bitIndexWithinWord != 0) {
            packedWords[currentWordIndex] = accumulator
        }
        return packedWords
    }

    /**
     * Convert packed 64-bit words into little-endian bytes as required by the CSV/Parquet views.
     */
    private fun longWordsToLittleEndianBytes(packedWords: LongArray): ByteArray {
        val byteBuffer = ByteArray(packedWords.size * java.lang.Long.BYTES)
        var writeOffset = 0
        for (word in packedWords) {
            var w = word
            var i = 0
            while (i < java.lang.Long.BYTES) {
                byteBuffer[writeOffset + i] = (w and 0xFF).toByte()
                w = w ushr 8
                i++
            }
            writeOffset += java.lang.Long.BYTES
        }
        return byteBuffer
    }

    /**
     * Encode a boolean mask to unpadded Base64 string (without the "B64:" prefix).
     */
    private fun booleanMaskToBase64(topicPresenceMask: BooleanArray): String {
        val packedWords = packTopicMaskToLongWords(topicPresenceMask)
        val packedBytes = longWordsToLittleEndianBytes(packedWords)
        return Base64.getEncoder().withoutPadding().encodeToString(packedBytes)
    }

    /**
     * Encode a boolean mask to the exact VAR-line payload expected by views ("B64:<base64>").
     */
    private fun packTopicMaskToVarLineBase64(topicPresenceMask: BooleanArray): String =
        "B64:" + booleanMaskToBase64(topicPresenceMask)

    /**
     * Decode an unprefixed Base64 mask into a BooleanArray of expected length.
     * Bytes are read little-endian and expanded bit-by-bit.
     */
    private fun base64ToBooleanMask(b64: String, expectedSize: Int): BooleanArray {
        if (b64.isEmpty()) return BooleanArray(expectedSize)
        val raw = Base64.getDecoder().decode(b64)
        val out = BooleanArray(expectedSize)

        var bitAbsolute = 0
        var offset = 0
        while (offset < raw.size && bitAbsolute < expectedSize) {
            var w = 0L
            var i = 0
            while (i < 8) {
                val b = (raw[offset + i].toLong() and 0xFF)
                w = w or (b shl (8 * i))
                i++
            }
            var bitInWord = 0
            while (bitInWord < 64 && bitAbsolute < expectedSize) {
                out[bitAbsolute] = ((w ushr bitInWord) and 1L) != 0L
                bitInWord++
                bitAbsolute++
            }
            offset += 8
        }
        return out
    }

    /* Post-write cleanup helpers */

    fun clearPercentiles() {
        percentiles.clear()
    }

    fun clearAfterSerialization() {
        repMaskB64ByK.clear()
        corrByK.clear()
        topPoolByK.clear()
    }
}
