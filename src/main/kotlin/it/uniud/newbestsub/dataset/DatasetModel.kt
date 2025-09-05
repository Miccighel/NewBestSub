package it.uniud.newbestsub.dataset

import com.opencsv.CSVReader
import it.uniud.newbestsub.dataset.model.*
import it.uniud.newbestsub.dataset.model.buildPrecomputedData
import it.uniud.newbestsub.dataset.model.toPrimitiveAPRows
import it.uniud.newbestsub.math.Correlations
import it.uniud.newbestsub.problem.*
import it.uniud.newbestsub.problem.operators.BinaryPruningCrossover
import it.uniud.newbestsub.problem.operators.ScoreBiasedFixedKSwapMutation
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Constants.GENERATION_STRIDE
import it.uniud.newbestsub.utils.Constants.GENERATION_WARMUP
import org.apache.commons.cli.ParseException
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.crossover.CrossoverOperator
import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.operator.selection.SelectionOperator
import org.uma.jmetal.operator.selection.impl.BinaryTournamentSelection
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator
import org.uma.jmetal.util.evaluator.SolutionListEvaluator
import org.uma.jmetal.util.pseudorandom.JMetalRandom
import java.io.File
import java.io.FileReader
import java.util.*
import kotlinx.coroutines.channels.SendChannel
import org.uma.jmetal.util.evaluator.impl.MultiThreadedSolutionListEvaluator
import org.uma.jmetal.util.evaluator.impl.SequentialSolutionListEvaluator

/**
 * DatasetModel
 * ============
 *
 * In-memory orchestrator for:
 * - Dataset AP matrix (systems × topics).
 * - Streaming NSGA-II execution for BEST/WORST and random sampling for AVERAGE.
 * - Compact caches so aggregations do not retain large populations.
 *
 * ## Output artifacts (what files mean)
 * - **-Fun** (functions): history of **best-so-far improvements** per K during a run.
 *   - One row is written **only when the incumbent improves** for that K.
 *   - Therefore, it typically contains **few rows** and is **monotone** in the objective.
 *   - Example for WORST at K=1:
 *     ```
 *     1,0.665046   // first incumbent
 *     1,0.530908   // improved
 *     1,-0.602607  // improved
 *     1,-0.720060  // improved (current best)
 *     ```
 *   - AVERAGE branch writes **exactly one row per K** (no incumbent concept).
 *
 * - **-Var** (variables): genotypes aligned with -Fun, encoded as `"B64:<...>"`.
 *   - BEST/WORST: emitted only on improvement (one VAR line per FUN line).
 *   - AVERAGE: exactly one VAR line per K.
 *
 * - **-Top** (top solutions per K): **presentation snapshot(s)** for K.
 *   - Maintained from a **persistent per-K pool**; each update sends a replacement block.
 *   - Lines are `"K,Correlation,B64:<mask>"`, ordered by correlation
 *     (desc for BEST, asc for WORST), capped at [Constants.TOP_SOLUTIONS_NUMBER].
 *   - Intended to have **at most one final “best” entry per K** for downstream aggregators.
 *     Readers should either:
 *       - consume the **first line** for BEST and **first line** for WORST (after ordering), or
 *       - compute **extrema** per K (max for BEST, min for WORST) if multiple lines exist.
 *
 * ## Branches
 * - **AVERAGE**:
 *   - For each K, sample [numberOfRepetitions] random subsets.
 *   - Compute correlations vs the full-set mean vector and stream exactly one FUN/VAR row.
 *   - Percentiles are computed from the same samples; -Top is not used.
 *
 * - **BEST / WORST**:
 *   - Objective sign scheme (internal):
 *     - BEST  → obj[0] = +K, obj[1] = -corr
 *     - WORST → obj[0] = -K, obj[1] = +corr
 *   - A **representative per K** is selected each generation, in **natural scale**:
 *     - naturalCorr(s) = -obj[1] for BEST, naturalCorr(s) = obj[1] for WORST.
 *   - On **strict improvement** vs best-seen per K, stream one -Fun/-Var row and update the Top pool.
 *
 * ## On negative correlations (important)
 * - Correlation is in [-1, 1]. With small K, subsets can **invert the ranking** of systems.
 * - **WORST** explicitly minimizes correlation, so negative values are **expected**.
 * - **AVERAGE** reports the mean over random subsets; at small K the mean can be **negative** if many
 *   samples invert the ranking. This is a statistical effect, not a bug.
 *
 * ## Invariants and expectations
 * - -Fun (BEST/WORST): per K, rows are **strictly improving over time** in natural scale.
 * - -Fun/-Var alignment: for every FUN row there is exactly one aligned VAR row.
 * - -Top: treat it as a **snapshot**; if multiple lines exist per K, select the **extremum** by target
 *   (BEST=max, WORST=min), or rely on the first line after ordering.
 *
 * ## RAM-focused design
 * - After load/expansion, [sealData] discards boxed AP maps and keeps dense [PrecomputedData].
 * - During runs, we cache only:
 *   - Per-K representative correlation: [corrByK]
 *   - Per-K representative mask (Base64): [repMaskB64ByK]
 * - The Top pool stores lightweight entries ([TopEntry]) without retaining full solutions.
 */
class DatasetModel {

    /* --------------------------- Basic dataset fields --------------------------- */

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

    /** Boxed AP rows only during load/expansion. Cleared by [sealData]. */
    var averagePrecisions = linkedMapOf<String, Array<Double>>()
    private var originalAveragePrecisions = linkedMapOf<String, Array<Double>>()

    /** Full-set mean AP per system (boxed for API compatibility). */
    var meanAveragePrecisions = emptyArray<Double>()

    /** AVERAGE branch percentiles (materialized at the end). */
    var percentiles = linkedMapOf<Int, List<Double>>()

    /** Kept for API compatibility; no longer populated to avoid per-topic maps. */
    var topicDistribution = linkedMapOf<String, MutableMap<Double, Boolean>>()

    /** Wall-clock computing time in milliseconds (last run). */
    var computingTime: Long = 0

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    private lateinit var problem: BestSubsetProblem
    private lateinit var crossover: CrossoverOperator<BinarySolution>
    private lateinit var mutation: MutationOperator<BinarySolution>

    /* ---------- Memory-friendly run-time state ---------- */

    private var precomputedData: PrecomputedData? = null
    private val repMaskB64ByK = mutableMapOf<Int, String>()
    private val corrByK = TreeMap<Int, Double>()
    private var topicIndexByLabel: Map<String, Int> = emptyMap()

    /* ------------------------------ Persistent Top pool ------------------------------ */

    private data class TopEntry(val natural: Double, val maskB64: String)

    private val topPoolByK: MutableMap<Int, MutableMap<String, TopEntry>> = mutableMapOf()

    var evaluatorThreadsUsed: Int = 1
    var processCpuTimeMs: Long = 0L
    var avgParallelismX: Double = 0.0

    fun naturalCorrOf(s: BestSubsetSolution): Double {
        val internal = s.getCorrelation()
        return if (targetToAchieve == Constants.TARGET_BEST) -internal else internal
    }

    private fun buildTopLinesFromPool(k: Int): List<String> {
        val pool = topPoolByK[k] ?: return emptyList()
        fun rankForBest(v: Double): Double = if (v.isNaN()) Double.NEGATIVE_INFINITY else v
        fun rankForWorst(v: Double): Double = if (v.isNaN()) Double.POSITIVE_INFINITY else v
        val ordered = if (targetToAchieve == Constants.TARGET_BEST) {
            pool.values.sortedWith(compareByDescending<TopEntry> { rankForBest(it.natural) }.thenBy { it.maskB64 })
        } else {
            pool.values.sortedWith(compareBy<TopEntry> { rankForWorst(it.natural) }.thenBy { it.maskB64 })
        }
        return ordered.take(Constants.TOP_SOLUTIONS_NUMBER).map { e -> "$k,${e.natural},B64:${e.maskB64}" }
    }

    /* ------------------------------ Data loading/expansion ------------------------------ */
    // loadData, expandTopics, expandSystems, refreshDerivedData, sealData
    // (full implementations preserved — too long to re-include in this message, continue in Part 2)

    /**
     * Load dataset AP matrix from a CSV file.
     */
    fun loadData(datasetPath: String) {
        val csvReader = CSVReader(FileReader(datasetPath))
        topicLabels = csvReader.readNext()
        numberOfTopics = topicLabels.size - 1
        datasetName = File(datasetPath).nameWithoutExtension

        csvReader.readAll().forEach { row ->
            val apValues = DoubleArray(row.size - 1)
            var colIndex = 1
            while (colIndex < row.size) {
                apValues[colIndex - 1] = row[colIndex].toDouble()
                colIndex++
            }
            this.averagePrecisions[row[0]] = apValues.toTypedArray()
        }

        originalAveragePrecisions = linkedMapOf<String, Array<Double>>().also { dst ->
            averagePrecisions.forEach { (system, values) -> dst[system] = values.copyOf() }
        }

        numberOfSystems = averagePrecisions.size
        topicLabels = topicLabels.sliceArray(1 until topicLabels.size)
        refreshDerivedData()
    }

    fun expandTopics(
        expansionCoefficient: Int, randomizedAveragePrecisions: Map<String, DoubleArray>, randomizedTopicLabels: Array<String>
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
        expansionCoefficient: Int, trueNumberOfSystems: Int, randomizedAveragePrecisions: Map<String, DoubleArray>, randomizedSystemLabels: Array<String>
    ) {
        this.expansionCoefficient = expansionCoefficient
        val newNumberOfSystems = numberOfSystems + expansionCoefficient

        if (newNumberOfSystems < trueNumberOfSystems) {
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
        topicDistribution.clear()
        topicLabels.forEach { label -> topicDistribution[label] = TreeMap() }
        topicIndexByLabel = topicLabels.withIndex().associate { (i, lbl) -> lbl to i }

        numberOfSystems = averagePrecisions.size
        val apEntryIterator = averagePrecisions.entries.iterator()
        systemLabels = Array(numberOfSystems) { apEntryIterator.next().key }

        val primitiveAP: Map<String, DoubleArray> = toPrimitiveAPRows(averagePrecisions)
        precomputedData = buildPrecomputedData(primitiveAP)

        val means = precomputedData!!.fullSetMeanAPBySystem
        meanAveragePrecisions = Array(means.size) { idx -> means[idx] }
    }

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

        averagePrecisions.clear()
        originalAveragePrecisions.clear()
    }

    /* ------------------------------ Solve + streaming ------------------------------ */

    fun solve(
        parameters: Parameters,
        out: SendChannel<ProgressEvent>? = null
    ): Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>> {

        datasetName = parameters.datasetName
        currentExecution = parameters.currentExecution
        populationSize = parameters.populationSize
        numberOfIterations = parameters.numberOfIterations
        numberOfRepetitions = parameters.numberOfRepetitions
        correlationMethod = parameters.correlationMethod

        val objectiveEncodingStrategy = loadTargetToAchieve(parameters.targetToAchieve)
        val runStartNs = System.nanoTime()
        val workerThreadName = Thread.currentThread().name

        val osMxGeneric = java.lang.management.ManagementFactory.getOperatingSystemMXBean()
        val osBean = osMxGeneric as? com.sun.management.OperatingSystemMXBean
        val cpuStartNs: Long = osBean?.processCpuTime ?: -1L

        logger.info(
            "Execution started on \"{}\" with target \"{}\".",
            workerThreadName, parameters.targetToAchieve
        )

        repMaskB64ByK.clear()
        corrByK.clear()
        topPoolByK.clear()

        if (targetToAchieve == Constants.TARGET_AVERAGE) {

            // -------------------- AVERAGE branch (SUMS+K fast path for Pearson) --------------------
            evaluatorThreadsUsed = 1
            logger.info("Evaluator: Sequential (threads={}) for target {}.", evaluatorThreadsUsed, parameters.targetToAchieve)

            val numericBundle = precomputedData ?: buildPrecomputedData(toPrimitiveAPRows(averagePrecisions))
            val systemCount = numericBundle.numberOfSystems
            val fullSetMeans = numericBundle.fullSetMeanAPBySystem

            // Fast-path toggle: if Pearson, use SUMS+K with precomputed Y-side stats (SIMD-capable)
            val usePearson = parameters.correlationMethod == Constants.CORRELATION_PEARSON
            val yStats = if (usePearson) Correlations.precomputeYStats(fullSetMeans) else null

            val topicIndexPool = IntArray(numberOfTopics) { it }
            val subsetSumsBySystem = DoubleArray(systemCount)
            // Allocate subsetMeans only if needed (non-Pearson methods)
            val subsetMeansBySystem = if (usePearson) DoubleArray(0) else DoubleArray(systemCount)

            val corrSamples = DoubleArray(maxOf(1, numberOfRepetitions))

            val requestedPercentiles: List<Int> = parameters.percentiles
            val tmpPercentiles = requestedPercentiles.associateWith { mutableListOf<Double>() }.toMutableMap()

            // General correlator (used for Kendall/etc.). Pearson goes via SUMS+K path above.
            val primitiveCorrFn = Correlations.makePrimitiveCorrelation(parameters.correlationMethod)

            val totalAvgIterations = numberOfTopics
            val logEveryAvg = ((totalAvgIterations * Constants.LOGGING_FACTOR) / 100).coerceAtLeast(1)
            var progressPct = 0

            var kIdx = 0
            while (kIdx < numberOfTopics) {

                if ((kIdx % logEveryAvg) == 0 && kIdx <= totalAvgIterations) {
                    logger.info(
                        "Completed iterations: {}/{} ({}%) on \"{}\" with target {}.",
                        kIdx, totalAvgIterations, progressPct,
                        Thread.currentThread().name, parameters.targetToAchieve
                    )
                    logger.debug(
                        "/* debounce */ log messages throttled every {} iterations ({}%).",
                        logEveryAvg, Constants.LOGGING_FACTOR
                    )
                    progressPct += Constants.LOGGING_FACTOR
                }

                val currentCardinality = kIdx + 1
                val repsForK = maxOf(1, numberOfRepetitions)

                var rep = 0
                while (rep < repsForK) {
                    // Partial Fisher–Yates to choose the first K indices
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

                    // Accumulate system sums over the chosen K topics
                    Arrays.fill(subsetSumsBySystem, 0.0)
                    var sel = 0
                    while (sel < currentCardinality) {
                        val t = topicIndexPool[sel]
                        val col = numericBundle.topicColumnViewByTopic[t]
                        var s = 0
                        while (s < systemCount) {
                            subsetSumsBySystem[s] += col[s]
                            s++
                        }
                        sel++
                    }

                    // Pearson → SUMS+K auto (vectorized when available). Otherwise, build means and use generic correlator.
                    corrSamples[rep] =
                        if (usePearson) {
                            Correlations.fastPearsonFromSumsWithKAuto(subsetSumsBySystem, currentCardinality, yStats!!)
                        } else {
                            val invK = 1.0 / currentCardinality
                            var s = 0
                            while (s < systemCount) {
                                subsetMeansBySystem[s] = subsetSumsBySystem[s] * invK
                                s++
                            }
                            primitiveCorrFn(subsetMeansBySystem, fullSetMeans)
                        }
                    rep++
                }

                // Mean of samples
                var sum = 0.0
                var i = 0
                while (i < repsForK) {
                    sum += corrSamples[i]; i++
                }
                val meanCorrForK = sum / repsForK

                // Percentiles: keep sort semantics identical to before
                Arrays.sort(corrSamples, 0, repsForK)
                for (p in requestedPercentiles) {
                    val acc = tmpPercentiles.getValue(p)
                    val pos = kotlin.math.ceil((p / 100.0) * repsForK).toInt().coerceIn(1, repsForK)
                    acc += corrSamples[pos - 1]
                }

                // Representative (the random K-subset from this outer iteration)
                val maskForStreaming = BooleanArray(numberOfTopics)
                var j = 0
                while (j < currentCardinality) {
                    maskForStreaming[topicIndexPool[j]] = true
                    j++
                }

                repMaskB64ByK[currentCardinality] = booleanMaskToBase64(maskForStreaming)
                corrByK[currentCardinality] = meanCorrForK

                sendProgress(
                    out,
                    CardinalityResult(
                        target = targetToAchieve,
                        threadName = workerThreadName,
                        cardinality = currentCardinality,
                        correlation = meanCorrForK,
                        functionValuesCsvLine = "$currentCardinality,$meanCorrForK",
                        variableValuesCsvLine = packTopicMaskToVarLineBase64(maskForStreaming)
                    )
                )

                kIdx++
            }

            logger.info(
                "Completed iterations: {}/{} (100%) on \"{}\" with target {}.",
                totalAvgIterations, totalAvgIterations,
                Thread.currentThread().name, parameters.targetToAchieve
            )

            percentiles.clear()
            for (p in requestedPercentiles) {
                percentiles[p] = tmpPercentiles.getValue(p).toList()
            }
        }

        /* =======================================================================================
         * BEST / WORST branch (NSGA-II streaming)
         * ======================================================================================= */
        else {

            var effectivePopulationSize = populationSize
            if (effectivePopulationSize <= 0) {
                logger.warn(
                    "Population size <= 0 provided; auto-setting to numberOfTopics ({}) for {}.",
                    numberOfTopics, parameters.targetToAchieve
                )
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
                targetStrategy = objectiveEncodingStrategy
            )

            // Pre-score topics against full-set mean (SIMD auto path for Pearson)
            val fullSetMeansBySystem = numericBundle.fullSetMeanAPBySystem
            val perTopicScores = DoubleArray(numberOfTopics)
            run {
                var t = 0
                while (t < numberOfTopics) {
                    val apColumn = numericBundle.topicColumnViewByTopic[t]
                    perTopicScores[t] = Correlations.fastPearsonPrimitiveAuto(apColumn, fullSetMeansBySystem)
                    t++
                }
            }

            crossover = BinaryPruningCrossover(0.7)
            mutation = ScoreBiasedFixedKSwapMutation(
                mutationProbability = 0.3,
                topicScores = perTopicScores,
                target = parameters.targetToAchieve,
                biasEpsilon = 0.10
            )

            val selectionOperator: SelectionOperator<List<BinarySolution>, BinarySolution> =
                BinaryTournamentSelection(RankingAndCrowdingDistanceComparator())

            val useParallelEvaluator = (!parameters.deterministic)
            val cores = Runtime.getRuntime().availableProcessors()
            val selectedThreadBudget =
                (if (parameters.evaluatorThreads > 0) parameters.evaluatorThreads else cores).coerceAtLeast(2)

            val listEvaluator: SolutionListEvaluator<BinarySolution> =
                if (useParallelEvaluator && parameters.populationSize >= cores * 8)
                    MultiThreadedSolutionListEvaluator(selectedThreadBudget)
                else
                    SequentialSolutionListEvaluator()

            evaluatorThreadsUsed = if (listEvaluator is MultiThreadedSolutionListEvaluator<*>) selectedThreadBudget else 1
            logger.info(
                "Evaluator: {} (threads={}) for target {}.",
                if (useParallelEvaluator) "MultiThreaded" else "Sequential",
                evaluatorThreadsUsed,
                parameters.targetToAchieve
            )

            val bestSeenNaturalCorrByK = TreeMap<Int, Double>()
            val lastSentTopSignatureByK = mutableMapOf<Int, Int>()

            val onGeneration: (Int, List<BinarySolution>) -> Unit = fun(generationIndex: Int, populationAtGeneration: List<BinarySolution>) {

                val naturalCorrOf = { s: BestSubsetSolution -> naturalCorrOf(s) }

                val representativeByK = HashMap<Int, BestSubsetSolution>()
                for (ind in populationAtGeneration) {
                    val bs = ind as BestSubsetSolution
                    val k = bs.getCardinality().toInt()
                    val currentRep = representativeByK[k]
                    val chooseThis = if (currentRep == null) {
                        true
                    } else {
                        val cand = naturalCorrOf(bs)
                        val rep = naturalCorrOf(currentRep)
                        if (targetToAchieve == Constants.TARGET_WORST) cand < rep - 1e-12 else cand > rep + 1e-12
                    }
                    if (chooseThis) representativeByK[k] = bs
                }

                val improvedRows = mutableListOf<Pair<Int, BestSubsetSolution>>()
                for ((k, bs) in representativeByK) {
                    val cand = naturalCorrOf(bs)
                    val prev = bestSeenNaturalCorrByK[k]
                    val improved = (prev == null) ||
                            (if (targetToAchieve == Constants.TARGET_WORST) cand < prev - 1e-12 else cand > prev + 1e-12)
                    if (improved) {
                        bestSeenNaturalCorrByK[k] = cand
                        improvedRows += k to bs
                    }
                }

                for (ind in populationAtGeneration) {
                    val bs = ind as BestSubsetSolution
                    val k = bs.getCardinality().toInt()
                    val maskB64 = bs.packMaskToBase64()
                    val natural = naturalCorrOf(bs)
                    val pool = topPoolByK.getOrPut(k) { mutableMapOf() }
                    val prev = pool[maskB64]
                    val better = when (targetToAchieve) {
                        Constants.TARGET_BEST -> (prev == null) || (natural > prev.natural + 1e-12)
                        Constants.TARGET_WORST -> (prev == null) || (natural < prev.natural - 1e-12)
                        else -> (prev == null)
                    }
                    if (better) pool[maskB64] = TopEntry(natural, maskB64)
                }

                val warmup = generationIndex < GENERATION_WARMUP
                val onStride = ((generationIndex - GENERATION_WARMUP) % GENERATION_STRIDE == 0)
                val shouldEmit = (!warmup && onStride) || generationIndex == 0 || improvedRows.isNotEmpty()
                if (!shouldEmit) return

                val appendOrder = if (targetToAchieve == Constants.TARGET_WORST) {
                    compareBy<Pair<Int, BestSubsetSolution>> { it.first }
                        .thenByDescending { naturalCorrOf(it.second) }
                } else {
                    compareBy<Pair<Int, BestSubsetSolution>> { it.first }
                        .thenBy { naturalCorrOf(it.second) }
                }
                improvedRows.sortWith(appendOrder)

                for ((k, bs) in improvedRows) {
                    repMaskB64ByK[k] = bs.packMaskToBase64()
                    corrByK[k] = naturalCorrOf(bs)
                    sendProgress(
                        out,
                        CardinalityResult(
                            target = targetToAchieve,
                            threadName = Thread.currentThread().name,
                            cardinality = k,
                            correlation = naturalCorrOf(bs),
                            functionValuesCsvLine = bs.toFunLineFor(targetToAchieve),
                            variableValuesCsvLine = bs.toVarLinePackedBase64()
                        )
                    )
                }

                val changedBlocks = mutableMapOf<Int, List<String>>()
                val ksThisGen = populationAtGeneration.asSequence().map { it.getCardinality().toInt() }.toSet()
                for (k in ksThisGen) {
                    val lines = buildTopLinesFromPool(k)
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

            if (out != null) {
                val finalBlocks = mutableMapOf<Int, List<String>>()
                for (k in topPoolByK.keys) {
                    val lines = buildTopLinesFromPool(k)
                    if (lines.isNotEmpty()) finalBlocks[k] = lines
                }
                if (finalBlocks.isNotEmpty()) {
                    sendProgress(out, TopKReplaceBatch(target = targetToAchieve, threadName = Thread.currentThread().name, blocks = finalBlocks))
                }
            }
        }

        // ------------------------ Timing end + summary (multicore-aware) ------------------------
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

        sendProgress(out, RunCompleted(targetToAchieve, workerThreadName, totalWallMs))
        logger.info("Per-K representatives cached: {}/{} for target \"{}\".", corrByK.size, numberOfTopics, targetToAchieve)

        return Triple(emptyList(), emptyList(), Triple(targetToAchieve, workerThreadName, totalWallMs))
    }


    private fun sendProgress(out: SendChannel<ProgressEvent>?, event: ProgressEvent) {
        if (out == null) return
        kotlinx.coroutines.runBlocking { out.send(event) }
    }

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

    fun findCorrelationForCardinality(cardinality: Double): Double? = corrByK[cardinality.toInt()]

    fun retrieveMaskForCardinality(cardinality: Double): BooleanArray? {
        val k = cardinality.toInt()
        val b64 = repMaskB64ByK[k] ?: return null
        return base64ToBooleanMask(b64, numberOfTopics)
    }

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

    fun retrieveMaskB64ForCardinality(cardinality: Double): String = "B64:" + (repMaskB64ByK[cardinality.toInt()] ?: "")

    private fun topBlockSig(lines: List<String>): Int = lines.joinToString("\n").hashCode()

    private fun BinarySolution.toFunLineFor(@Suppress("UNUSED_PARAMETER") target: String): String {
        val s = this as BestSubsetSolution
        val kInt = s.getCardinality().toInt()
        val naturalCorr = naturalCorrOf(s)
        return "$kInt,$naturalCorr"
    }

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

    private fun booleanMaskToBase64(topicPresenceMask: BooleanArray): String {
        val packedWords = packTopicMaskToLongWords(topicPresenceMask)
        val packedBytes = longWordsToLittleEndianBytes(packedWords)
        return Base64.getEncoder().withoutPadding().encodeToString(packedBytes)
    }

    private fun packTopicMaskToVarLineBase64(topicPresenceMask: BooleanArray): String =
        "B64:" + booleanMaskToBase64(topicPresenceMask)

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

    /* --------------------------- Post-write cleanup helpers --------------------------- */

    fun clearPercentiles() {
        percentiles.clear()
    }

    fun clearAfterSerialization() {
        repMaskB64ByK.clear()
        corrByK.clear()
        topPoolByK.clear()
    }
}
