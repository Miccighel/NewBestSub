package it.uniud.newbestsub.dataset

import com.opencsv.CSVReader
import it.uniud.newbestsub.dataset.model.CardinalityResult
import it.uniud.newbestsub.dataset.model.ProgressEvent
import it.uniud.newbestsub.dataset.model.RunCompleted
import it.uniud.newbestsub.dataset.model.TopKReplaceBatch
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.RandomBridge
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.apache.logging.log4j.LogManager
import java.io.*
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.util.*
import java.util.SplittableRandom
import kotlin.collections.LinkedHashMap
import kotlin.math.max
import kotlin.math.min

/**
 * DatasetController.
 *
 * Orchestrates:
 * - [DatasetModel] lifecycle (load → optional expand → seal → solve).
 * - Streaming consumption (printer coroutine reading [ProgressEvent] instances).
 * - View writing for final snapshots and merged artifacts (CSV and Parquet).
 *
 * Determinism
 * - When [Parameters.deterministic] is true:
 *   - Runs execute sequentially in the order BEST, WORST, AVERAGE.
 *   - All randomness flows through jMetal's singleton via [RandomBridge.withSeed].
 *   - Expansion helpers ([expandTopics], [expandSystems]) use a [SplittableRandom]
 *     seeded via [RandomBridge.childSeed] for reproducible synthetic data.
 *
 * Efficiency
 * - A single printer coroutine consumes a back-pressured [Channel] of events.
 * - Uses batched [TopKReplaceBatch] to avoid rewriting the TOP file multiple times per generation.
 *
 * Parallelism and thread budgeting
 * - BEST and WORST evaluators inside the model are selected by a workload-aware policy:
 *   - Use a multi-threaded evaluator only when:
 *     - Heavy work: (#systems ≥ 256) or (Kendall τ-b and #systems ≥ 128), and
 *     - Big population: populationSize ≥ (CPU cores × 8), and
 *     - deterministic == false.
 *   - Otherwise, use the sequential evaluator (threads = 1).
 * - AVERAGE is always single-threaded.
 * - When `targetToAchieve == ALL` and `deterministic == false`:
 *   - CPU cores are split between BEST and WORST if both qualify for parallel evaluators.
 *   - The printer and OS/GC keep approximately one core.
 *   - Each NSGA job is launched on one coroutine worker
 *     (`Dispatchers.Default.limitedParallelism(1)`); the jMetal pool executes inside the job.
 *
 * @property targetToAchieve "BEST" | "WORST" | "AVERAGE" | "ALL" top-level routing for solve/merge/copy flows.
 */
class DatasetController(
    private var targetToAchieve: String
) {

    /** Per-target models (when TARGET_ALL: indices 0=BEST, 1=WORST, 2=AVERAGE; else a single entry). */
    var models = mutableListOf<DatasetModel>()

    /** Unified view wrapper writing CSV and Parquet artifacts. */
    private var view = DatasetView()

    /** Parameters of the last or ongoing run. Set by [solve]. */
    private lateinit var parameters: Parameters

    /** Path passed to [load]; kept to support error diagnostics. */
    private lateinit var datasetPath: String

    /** Collected output paths for later merge/copy steps (CSV lists; Parquet derived when needed). */
    var aggregatedDataResultPaths = mutableListOf<String>()
    var variableValuesResultPaths = mutableListOf<String>()
    var functionValuesResultPaths = mutableListOf<String>()
    var topSolutionsResultPaths = mutableListOf<String>()
    var infoResultPaths = mutableListOf<String>()

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /** Monotone counter to diversify expansion child seeds in deterministic mode. */
    private var expansionNonce: Int = 0

    init {
        logger.info("Problem resolution started.")
    }

    /**
     * Load a dataset from a CSV path into one or three [DatasetModel] instances depending on [targetToAchieve].
     *
     * - Always loads one model at index 0.
     * - If [targetToAchieve] == ALL, also loads models at indices 1 (WORST) and 2 (AVERAGE).
     *
     * Models are immediately tagged with their target so downstream code (printer, tuners)
     * can safely look them up by target without races:
     * - TARGET_ALL → [0]=BEST, [1]=WORST, [2]=AVERAGE.
     * - Single target → [0] = that target.
     *
     * @param datasetPath Path to the CSV dataset file.
     */
    fun load(datasetPath: String) {

        this.datasetPath = datasetPath

        logger.info("Dataset loading started.")
        logger.info("Path: \"$datasetPath\".")

        try {
            /* Always allocate and load the first model. Any failure turns into a warning. */
            models.plusAssign(DatasetModel())
            models[0].loadData(this.datasetPath)

            if (targetToAchieve == Constants.TARGET_ALL) {
                /* For ALL, clone the loading to independent model instances so their caches/stats do not interfere. */
                models.plusAssign(DatasetModel())
                models[1].loadData(this.datasetPath)
                models.plusAssign(DatasetModel())
                models[2].loadData(this.datasetPath)

                /* Pre-tag targets so any lookup by token is stable from the start (printer relies on this). */
                models[0].targetToAchieve = Constants.TARGET_BEST
                models[1].targetToAchieve = Constants.TARGET_WORST
                models[2].targetToAchieve = Constants.TARGET_AVERAGE
            } else {
                models[0].targetToAchieve = targetToAchieve
            }
        } catch (_: FileNotFoundException) {
            /* Retain a friendly message for common setup issues. */
            logger.warn("Dataset not found. Is file inside a \"data\" dir.?")
        } catch (exception: IOException) {
            /* Avoid noisy stack traces at this level; message is enough for operator logs. */
            logger.warn(exception.message as String)
        }

        logger.info("Dataset loading for input file \"${models[0].datasetName}\" completed.")
    }

    /**
     * Expand the dataset by appending [expansionCoefficient] synthetic topics to all loaded models.
     *
     * - Topic labels follow the pattern "NNN (F)" where NNN ∈ [800..998].
     * - AP values for the new topics are generated per system.
     * - In deterministic mode, randomness derives from [RandomBridge.childSeed].
     *
     * @param expansionCoefficient Number of random topics to append.
     */
    fun expandTopics(expansionCoefficient: Int) {
        /* Use child seeds when RandomBridge is installed to keep determinism across runs. */
        val seededRng: SplittableRandom? =
            if (RandomBridge.isInstalled()) SplittableRandom(RandomBridge.childSeed("EXPAND_TOPICS", expansionNonce++))
            else null

        /* Synthesize labels once, reuse across models to keep correspondence. */
        val systemLabels = models[0].systemLabels
        val random = if (seededRng == null) Random() else null
        val generatedTopicLabels = Array(expansionCoefficient) {
            /* Prefer SplittableRandom for deterministic runs; fall back to java.util.Random for ad-hoc experiments. */
            val suffix = (seededRng?.nextInt(800, 999) ?: (random!!.nextInt(999 - 800) + 800))
            "$suffix (F)"
        }

        /* Allocate a randomized AP column block for each system. */
        val randomizedAveragePrecisions = LinkedHashMap<String, DoubleArray>()
        systemLabels.forEach { systemLabel ->
            randomizedAveragePrecisions[systemLabel] =
                DoubleArray(expansionCoefficient) { seededRng?.nextDouble() ?: Math.random() }
        }

        /* Expand every loaded model in lockstep so BEST/WORST/AVERAGE stay aligned. */
        models.forEach { model ->
            model.expandTopics(expansionCoefficient, randomizedAveragePrecisions, generatedTopicLabels)
        }
    }

    /**
     * Expand the dataset by appending [expansionCoefficient] synthetic systems (or revert to a prefix),
     * across all loaded models.
     *
     * - System labels follow the pattern "Sys{index}{NNN} (F)" with NNN ∈ [800..998].
     * - AP rows for the new systems are random; in deterministic mode they come from a [SplittableRandom]
     *   seeded via [RandomBridge.childSeed].
     * - Important: new systems receive exactly [numberOfTopics] AP values (no extra columns).
     *
     * @param expansionCoefficient Number of random systems to append.
     * @param trueNumberOfSystems Ground-truth number of systems; if
     * `current + expansionCoefficient < trueNumberOfSystems`, reverts the AP map to a prefix of the original systems.
     */
    fun expandSystems(expansionCoefficient: Int, trueNumberOfSystems: Int) {
        val seededRng: SplittableRandom? =
            if (RandomBridge.isInstalled()) SplittableRandom(RandomBridge.childSeed("EXPAND_SYSTEMS", expansionNonce++))
            else null
        val random = if (seededRng == null) Random() else null

        /* Generate stable, human-readable labels for synthetic systems. */
        val generatedSystemLabels = Array(expansionCoefficient) { index ->
            val suffix = (seededRng?.nextInt(800, 999) ?: (random!!.nextInt(999 - 800) + 800))
            "Sys$index$suffix (F)"
        }

        /* Current topic count is required to size each synthetic AP row.
           Guard against improper call order (must load before expanding). */
        val currentTopicCount =
            models.firstOrNull()?.numberOfTopics ?: throw IllegalStateException("expandSystems() called before load()")

        /* Produce one randomized AP row per synthetic system. */
        val randomizedAveragePrecisions = LinkedHashMap<String, DoubleArray>()
        generatedSystemLabels.forEach { systemLabel ->
            randomizedAveragePrecisions[systemLabel] =
                DoubleArray(currentTopicCount) { seededRng?.nextDouble() ?: Math.random() }
        }

        /* Expand all loaded models to keep state consistent across targets. */
        models.forEach { model ->
            model.expandSystems(expansionCoefficient, trueNumberOfSystems, randomizedAveragePrecisions, generatedSystemLabels)
        }
    }

    /**
     * Run the experiment(s) according to [parameters.targetToAchieve].
     *
     * Orchestration:
     * - Seals all loaded models (boxing → dense primitives) before solving.
     * - Configures thread/evaluator policy per target and workload.
     * - Starts the solve(s), streaming progress into a single back-pressured channel.
     * - A dedicated printer coroutine (IO dispatcher) consumes events and writes:
     *   FUN/VAR rows, batched TOP replacements, and RunCompleted closures (also triggering Parquet writes).
     * - After runs complete, writes Aggregated and Info tables (CSV and Parquet) and clears per-run caches.
     *
     * Determinism:
     * - When deterministic, runs execute sequentially (BEST → WORST → AVERAGE) with labeled child seeds.
     * - Randomness is routed through RandomBridge for reproducibility.
     */
    fun solve(parameters: Parameters) {

        this.parameters = parameters

        logger.info("Printing common execution parameters.")
        logger.info("Dataset name: ${parameters.datasetName}.")
        logger.info("Correlation: ${parameters.correlationMethod}.")
        logger.info("Target: ${parameters.targetToAchieve}.")
        logger.info("[Experiments: Best, Worst] Number of iterations: ${parameters.numberOfIterations}.")
        logger.info("[Experiments: Best, Worst] Population size: ${parameters.populationSize}. ")
        logger.info("[Experiment: Average] Number of repetitions: ${parameters.numberOfRepetitions}.")
        if (parameters.currentExecution > 0) logger.info("Current Execution: ${parameters.currentExecution}.")
        if (parameters.deterministic) logger.info("Deterministic mode is ON. Master seed: ${parameters.seed ?: "(derived)"}")
        if (parameters.targetToAchieve == Constants.TARGET_ALL || parameters.targetToAchieve == Constants.TARGET_AVERAGE) {
            val percentilesText = parameters.percentiles.joinToString(", ") { "$it%" }
            logger.info("Percentiles: $percentilesText. [Experiment: Average]")
        }

        /* Seal boxed AP rows into dense primitive bundles and free big maps before the run.
           This reduces GC pressure during solving and ensures hot paths use primitives. */
        models.forEach { it.sealData() }

        /* Evaluator thread tuner.
           Purpose:
           - Decide if a parallel evaluator is worth the overhead given (systems, population, correlation).
           - When parallel, pick an initial threads bound that keeps one core for the system and I/O. */
        fun tunedParamsFor(
            model: DatasetModel,
            base: Parameters,
            runningAlongside: Boolean
        ): Parameters {
            val systemsCount = model.numberOfSystems
            val populationSize = base.populationSize
            val cpuCores = Runtime.getRuntime().availableProcessors()
            val isKendall = base.correlationMethod == Constants.CORRELATION_KENDALL

            /* Kendall τ-b is heavier; lower the threshold to 128 systems. */
            val heavyWork = (systemsCount >= 256) || (isKendall && systemsCount >= 128)

            /* Population must be large enough to amortize synchronization and task stealing. */
            val bigPopulation = populationSize >= (cpuCores * 8)

            /* Deterministic runs force sequential evaluator to preserve exact reproducibility. */
            val useParallel = !base.deterministic && heavyWork && bigPopulation
            if (!useParallel) return base

            /* Reserve one core for OS, GC, and the printer. */
            val effectiveCores = (cpuCores - 1).coerceAtLeast(1)

            /* Heuristic: at least 2 threads when parallel; scale with population. The model may further cap this. */
            val initialThreads = min(effectiveCores, max(2, populationSize / 512))
            return base.copy(evaluatorThreads = initialThreads)
        }

        /* Progress channel sizing.
           - Deterministic runs prefer tight back-pressure (small or rendezvous) to keep producer/consumer in lockstep.
           - Non-deterministic runs use a bounded but roomy buffer to absorb bursts from parallel evaluators. */
        fun makeProgressChannel(targetCount: Int, topicCount: Int): Channel<ProgressEvent> {
            /* Rough estimate: per target we expect a fraction of topics worth of events plus some constant. */
            val estimatedEventsPerTarget = (topicCount / 3) + 64
            return if (parameters.deterministic) {
                val capacity = minOf(256, maxOf(0, estimatedEventsPerTarget))
                if (capacity == 0) Channel(Channel.RENDEZVOUS) else Channel(capacity)
            } else {
                val base = estimatedEventsPerTarget * targetCount * 4
                val capacity = base.coerceIn(2_048, 131_072)
                Channel(capacity)
            }
        }

        if (parameters.targetToAchieve == Constants.TARGET_ALL) {
            /* Prepare per-target parameters (cloned so the tuner can diverge them independently). */
            var bestParams = parameters.copy(targetToAchieve = Constants.TARGET_BEST)
            var worstParams = parameters.copy(targetToAchieve = Constants.TARGET_WORST)
            var averageParams = parameters.copy(targetToAchieve = Constants.TARGET_AVERAGE)

            val topicCount = models[0].numberOfTopics
            val progressEvents: Channel<ProgressEvent> = makeProgressChannel(targetCount = 3, topicCount = topicCount)

            if (parameters.deterministic) {
                /* Sequential solves; the single printer still handles the stream but there is no concurrency. */
                bestParams = tunedParamsFor(models.first { it.targetToAchieve == Constants.TARGET_BEST }, bestParams, runningAlongside = false)
                worstParams = tunedParamsFor(models.first { it.targetToAchieve == Constants.TARGET_WORST }, worstParams, runningAlongside = false)
                averageParams = averageParams.copy(evaluatorThreads = 1)

                runBlocking {
                    supervisorScope {
                        val printerJob = launchPrinter(progressEvents, models)

                        /* Child seeds labeled by target keep runs reproducible across ALL vs single-target modes. */
                        RandomBridge.withSeed(RandomBridge.childSeed("BEST")) {
                            models.first { it.targetToAchieve == Constants.TARGET_BEST }.solve(bestParams, progressEvents, this)
                        }
                        RandomBridge.withSeed(RandomBridge.childSeed("WORST")) {
                            models.first { it.targetToAchieve == Constants.TARGET_WORST }.solve(worstParams, progressEvents, this)
                        }
                        RandomBridge.withSeed(RandomBridge.childSeed("AVERAGE")) {
                            models.first { it.targetToAchieve == Constants.TARGET_AVERAGE }.solve(averageParams, progressEvents, this)
                        }

                        /* Always close the channel so the printer can finish its for-loop cleanly. */
                        progressEvents.close()
                        printerJob.join()
                    }
                }
            } else {
                /* Non-deterministic ALL: run BEST/WORST/AVERAGE concurrently with a global CPU budget. */

                val runBest = true
                val runWorst = true
                val runAverage = true

                /* Tune BEST/WORST if they benefit from parallel evaluators. */
                var bestParamsTuned = tunedParamsFor(
                    models.first { it.targetToAchieve == Constants.TARGET_BEST },
                    parameters.copy(targetToAchieve = Constants.TARGET_BEST),
                    runningAlongside = true
                )
                var worstParamsTuned = tunedParamsFor(
                    models.first { it.targetToAchieve == Constants.TARGET_WORST },
                    parameters.copy(targetToAchieve = Constants.TARGET_WORST),
                    runningAlongside = true
                )
                val averageParamsTuned = parameters.copy(
                    targetToAchieve = Constants.TARGET_AVERAGE,
                    evaluatorThreads = 1 /* Keep AVERAGE single-threaded for stability and limited gains. */
                )

                /* CPU budgeting:
                   - leave 1 core for system/printer,
                   - allocate the rest to BEST/WORST proportionally to their wishes,
                   - AVERAGE occupies one worker coroutine but no parallel evaluator threads. */
                val totalCores = Runtime.getRuntime().availableProcessors()
                val reserveForSystem = 1
                val coreBudget = (totalCores - reserveForSystem).coerceAtLeast(1)

                val averageCost = if (runAverage) 1 else 0
                val budgetForBestWorst = (coreBudget - averageCost).coerceAtLeast(1)

                var bestWish = max(1, bestParamsTuned.evaluatorThreads)
                var worstWish = max(1, worstParamsTuned.evaluatorThreads)
                val wishSum = bestWish + worstWish

                if (wishSum > budgetForBestWorst) {
                    val bestShare = max(1, (bestWish.toDouble() / wishSum * budgetForBestWorst).toInt())
                    val worstShare = max(1, budgetForBestWorst - bestShare)
                    bestWish = bestShare
                    worstWish = worstShare
                }

                bestParamsTuned = bestParamsTuned.copy(evaluatorThreads = bestWish)
                worstParamsTuned = worstParamsTuned.copy(evaluatorThreads = worstWish)

                logger.info(
                    "Thread plan (ALL): BEST={}, WORST={}, AVERAGE={}, totalCores={}, budget={}",
                    bestParamsTuned.evaluatorThreads,
                    worstParamsTuned.evaluatorThreads,
                    averageParamsTuned.evaluatorThreads,
                    totalCores,
                    coreBudget
                )

                /* Share a single progress channel among producers; capacity sized for parallel emitters. */
                val topicsForAll = models[0].numberOfTopics
                val progressEventsAll: Channel<ProgressEvent> = makeProgressChannel(targetCount = 3, topicCount = topicsForAll)

                runBlocking {
                    supervisorScope {
                        val printerJob = launchPrinter(progressEventsAll, models)
                        val producerJobs = mutableListOf<Job>()

                        /* Use limitedParallelism(1) so each NSGA job holds one coroutine; inner parallelism is managed by jMetal. */
                        if (runBest) producerJobs += launch(Dispatchers.Default.limitedParallelism(1)) {
                            models.first { it.targetToAchieve == Constants.TARGET_BEST }.solve(bestParamsTuned, progressEventsAll, this)
                        }
                        if (runWorst) producerJobs += launch(Dispatchers.Default.limitedParallelism(1)) {
                            models.first { it.targetToAchieve == Constants.TARGET_WORST }.solve(worstParamsTuned, progressEventsAll, this)
                        }
                        if (runAverage) producerJobs += launch(Dispatchers.Default.limitedParallelism(1)) {
                            models.first { it.targetToAchieve == Constants.TARGET_AVERAGE }.solve(averageParamsTuned, progressEventsAll, this)
                        }

                        /* Ensure the channel closes even if one producer fails, allowing the printer to terminate. */
                        try {
                            producerJobs.joinAll()
                        } finally {
                            progressEventsAll.close()
                        }
                        printerJob.join()
                    }
                }
            }

            /* After running producers and draining events, collect paths and write final summary tables. */
            aggregatedDataResultPaths.add(view.getAggregatedDataFilePath(models[0], isTargetAll = true))
            models.forEach { model ->
                functionValuesResultPaths.add(view.getFunctionValuesFilePath(model))
                variableValuesResultPaths.add(view.getVariableValuesFilePath(model))
                if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                    topSolutionsResultPaths.add(view.getTopSolutionsFilePath(model))
                }
            }
            infoResultPaths.add(view.getInfoFilePath(models[0], isTargetAll = true))

            logger.info("Data aggregation started.")
            val aggregatedRows = aggregate(models)
            view.writeCsv(aggregatedRows, view.getAggregatedDataFilePath(models[0], isTargetAll = true))
            view.writeParquet(aggregatedRows, view.getAggregatedDataParquetPath(models[0], isTargetAll = true))

            logger.info("Execution information gathering started.")
            val infoRows = info(models)
            view.writeCsv(infoRows, view.getInfoFilePath(models[0], isTargetAll = true))
            view.writeParquet(infoRows, view.getInfoParquetPath(models[0], isTargetAll = true))

            logger.info("Execution result paths:")
            models.forEach { model ->
                logger.info("\"${view.getFunctionValuesFilePath(model)}\" (Function values CSV)")
                logger.info("\"${view.getVariableValuesFilePath(model)}\" (Variable values CSV)")
                logger.info("\"${view.getFunctionValuesParquetPath(model)}\" (Function values Parquet)")
                logger.info("\"${view.getVariableValuesParquetPath(model)}\" (Variable values Parquet)")
                if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                    logger.info("\"${view.getTopSolutionsFilePath(model)}\" (Top Solutions CSV)")
                    logger.info("\"${view.getTopSolutionsParquetPath(model)}\" (Top Solutions Parquet)")
                }
            }
            logger.info("\"${view.getAggregatedDataFilePath(models[0], isTargetAll = true)}\" (Aggregated data CSV)")
            logger.info("\"${view.getInfoFilePath(models[0], isTargetAll = true)}\" (Info CSV)")

            models.forEach { model ->
                /* Drop transient caches that were needed only during solving/serialization. */
                model.clearPercentiles()
                model.clearAfterSerialization()
            }
        } else {
            /* Single-target run (simpler orchestration). */
            val topicCount = models[0].numberOfTopics
            val progressEvents: Channel<ProgressEvent> = makeProgressChannel(targetCount = 1, topicCount = topicCount)

            /* AVERAGE stays single-threaded; BEST/WORST may be upgraded by the tuner. */
            val tunedParameters =
                if (parameters.targetToAchieve == Constants.TARGET_AVERAGE) parameters.copy(evaluatorThreads = 1)
                else tunedParamsFor(models[0], parameters, runningAlongside = false)

            if (parameters.deterministic) {
                runBlocking {
                    supervisorScope {
                        val printerJob = launchPrinter(progressEvents, listOf(models[0]))
                        /* Use a labeled child seed even in single-target mode for consistency with ALL. */
                        val label = parameters.targetToAchieve.uppercase(Locale.ROOT)
                        RandomBridge.withSeed(RandomBridge.childSeed(label)) {
                            models[0].solve(tunedParameters, progressEvents, this)
                        }
                        progressEvents.close()
                        printerJob.join()
                    }
                }
            } else {
                runBlocking {
                    supervisorScope {
                        val printerJob = launchPrinter(progressEvents, listOf(models[0]))
                        val job = launch(Dispatchers.Default.limitedParallelism(1)) {
                            models[0].solve(tunedParameters, progressEvents, this)
                        }
                        job.join()
                        progressEvents.close()
                        printerJob.join()
                    }
                }
            }

            /* Collect paths and produce aggregated/info outputs for the single target. */
            aggregatedDataResultPaths.add(view.getAggregatedDataFilePath(models[0], isTargetAll = false))
            functionValuesResultPaths.add(view.getFunctionValuesFilePath(models[0]))
            variableValuesResultPaths.add(view.getVariableValuesFilePath(models[0]))
            if (models[0].targetToAchieve != Constants.TARGET_AVERAGE) {
                topSolutionsResultPaths.add(view.getTopSolutionsFilePath(models[0]))
            }
            infoResultPaths.add(view.getInfoFilePath(models[0], isTargetAll = false))

            logger.info("Data aggregation started.")
            val aggregatedRows = aggregate(models)
            view.writeCsv(aggregatedRows, view.getAggregatedDataFilePath(models[0], isTargetAll = false))
            view.writeParquet(aggregatedRows, view.getAggregatedDataParquetPath(models[0], isTargetAll = false))

            logger.info("Execution information gathering started.")
            val infoRows = info(models)
            view.writeCsv(infoRows, view.getInfoFilePath(models[0], isTargetAll = false))
            view.writeParquet(infoRows, view.getInfoParquetPath(models[0], isTargetAll = false))

            logger.info("Execution result paths:")
            logger.info("\"${view.getFunctionValuesFilePath(models[0])}\" (Function values CSV)")
            logger.info("\"${view.getVariableValuesFilePath(models[0])}\" (Variable values CSV)")
            logger.info("\"${view.getFunctionValuesParquetPath(models[0])}\" (Function values Parquet)")
            logger.info("\"${view.getVariableValuesParquetPath(models[0])}\" (Variable values Parquet)")
            if (models[0].targetToAchieve != Constants.TARGET_AVERAGE) {
                logger.info("\"${view.getTopSolutionsFilePath(models[0])}\" (Top Solutions CSV)")
                logger.info("\"${view.getTopSolutionsParquetPath(models[0])}\" (Top Solutions Parquet)")
            }
            logger.info("\"${view.getAggregatedDataFilePath(models[0], isTargetAll = false)}\" (Aggregated data CSV)")
            logger.info("\"${view.getInfoFilePath(models[0], isTargetAll = false)}\" (Info CSV)")

            models.forEach { model ->
                model.clearPercentiles()
                model.clearAfterSerialization()
            }
        }

        logger.info("Execution information gathering completed.")
        logger.info("Data aggregation completed.")
        logger.info("Problem resolution completed.")
    }

    /**
     * Printer: single consumer that batches FUN/VAR and preserves output order.
     *
     * Models are pre-tagged with their targets in load(), so event routing
     * by target is stable from the first message. A safe fallback is kept.
     */
    private fun CoroutineScope.launchPrinter(
        progressEvents: Channel<ProgressEvent>, models: List<DatasetModel>
    ): Job = launch(Dispatchers.IO) {
        /* Fast target→model map; immutable after construction of this coroutine. */
        val modelByTarget = HashMap<String, DatasetModel>(4).apply {
            models.forEach { put(it.targetToAchieve, it) }
        }

        /* Batch buffer reduces I/O syscalls and contention with producer threads. */
        val funBatch = ArrayList<CardinalityResult>(256)

        fun flushFunBatchIfAny() {
            var index = 0
            val batchSize = funBatch.size
            while (index < batchSize) {
                /* Resolve model by target for each event; fallback to models[0] if unexpectedly missing. */
                val event = funBatch[index]
                val model = modelByTarget[event.target] ?: models[0]
                view.appendCardinality(model, event)
                index++
            }
            funBatch.clear()
        }

        /* Drain the channel until closed by producers. The order of events is preserved per insertion. */
        for (event in progressEvents) {
            when (event) {
                is CardinalityResult -> {
                    funBatch.add(event)
                    /* Flush periodically to bound latency and memory use. */
                    if (funBatch.size >= 256) flushFunBatchIfAny()
                }

                is TopKReplaceBatch -> {
                    /* Keep FUN/VAR aligned: flush before writing TOP batches. */
                    if (funBatch.isNotEmpty()) flushFunBatchIfAny()
                    val model = modelByTarget[event.target] ?: models[0]
                    view.replaceTopBatch(model, event.blocks)
                }

                is RunCompleted -> {
                    /* Finalize writers for this target; if TOP live writing was disabled, it flushes here. */
                    if (funBatch.isNotEmpty()) flushFunBatchIfAny()
                    val model = modelByTarget[event.target] ?: models[0]
                    view.closeStreams(model)
                }
            }
        }
        /* Safety flush in case the producer closed after pushing a partial batch. */
        if (funBatch.isNotEmpty()) flushFunBatchIfAny()
    }

    /**
     * Build the Aggregated table for the given list of models.
     *
     * Header: "Cardinality" + one column per model target + one column per AVERAGE percentile + "BestTopicsB64","WorstTopicsB64".
     *
     * - Correlations per target are taken from each model’s per K caches.
     * - Percentiles come from the AVERAGE model, if present.
     * - The Best/Worst topic masks are pulled from the respective models as "B64:<...>".
     *
     * @param datasetModels One or more models (when TARGET_ALL, three models are expected).
     * @return Rows including header (first row) and per K data.
     * @throws IllegalArgumentException if [datasetModels] is empty.
     */
    private fun aggregate(datasetModels: List<DatasetModel>): List<Array<String>> {
        require(datasetModels.isNotEmpty()) { "aggregate(datasetModels): input list must not be empty" }

        val referenceModel = datasetModels.first()
        val maximumCardinality = referenceModel.numberOfTopics

        /* Extract percentiles from the AVERAGE model if present. Keys are percent values (e.g., 25, 50, 75). */
        val averageModelPercentiles: Map<Int, List<Double>> =
            datasetModels.firstOrNull { it.targetToAchieve == Constants.TARGET_AVERAGE }?.percentiles ?: emptyMap()
        val sortedPercentileKeys: List<Int> = averageModelPercentiles.keys.sorted()

        /* Header layout: [Cardinality] + one column per model target + one per percentile + two mask columns. */
        val numberOfTargets = datasetModels.size
        val totalHeaderColumns = 1 + numberOfTargets + sortedPercentileKeys.size + 2

        val decimalFormatter = java.text.DecimalFormat("0.000000", java.text.DecimalFormatSymbols(Locale.ROOT))
        fun formatSixDecimals(value: Double?): String =
            if (value == null) Constants.CARDINALITY_NOT_AVAILABLE else decimalFormatter.format(value)

        val aggregatedRows = ArrayList<Array<String>>(1 + maximumCardinality)

        /* Header: write targets in the same order as provided in datasetModels to keep consistency with run mode. */
        run {
            val headerRow = Array(totalHeaderColumns) { "" }
            var headerColumnIndex = 0
            headerRow[headerColumnIndex++] = "Cardinality"
            datasetModels.forEach { headerRow[headerColumnIndex++] = it.targetToAchieve }
            sortedPercentileKeys.forEach { percentile -> headerRow[headerColumnIndex++] = "${percentile}%" }
            headerRow[headerColumnIndex++] = "BestTopicsB64"
            headerRow[headerColumnIndex++] = "WorstTopicsB64"
            aggregatedRows += headerRow
        }

        /* Track how many cardinalities were actually filled for each target (useful for logs). */
        val computedCardinalityByTarget = mutableMapOf(
            Constants.TARGET_BEST to 0, Constants.TARGET_WORST to 0, Constants.TARGET_AVERAGE to 0
        )

        val bestModel = datasetModels.firstOrNull { it.targetToAchieve == Constants.TARGET_BEST }
        val worstModel = datasetModels.firstOrNull { it.targetToAchieve == Constants.TARGET_WORST }

        /* Main body: one row per cardinality from 1 to maximum topics. */
        for (cardinalityIndex in 0 until maximumCardinality) {
            val cardinality = cardinalityIndex + 1
            val cardinalityAsDouble = cardinality.toDouble()

            val row = Array(totalHeaderColumns) { "" }
            var columnIndex = 0
            row[columnIndex++] = cardinality.toString()

            /* Per-model correlations, aligned with datasetModels order. */
            datasetModels.forEach { model ->
                val correlation = model.findCorrelationForCardinality(cardinalityAsDouble)
                row[columnIndex] = formatSixDecimals(correlation)
                if (correlation != null) {
                    computedCardinalityByTarget[model.targetToAchieve] =
                        (computedCardinalityByTarget[model.targetToAchieve] ?: 0) + 1
                }
                columnIndex++
            }

            /* Percentiles (from AVERAGE model), by increasing percentile key. */
            sortedPercentileKeys.forEach { percentile ->
                val valuesForPercentile = averageModelPercentiles[percentile] ?: emptyList()
                val percentileValue = valuesForPercentile.getOrNull(cardinalityIndex)
                row[columnIndex++] = formatSixDecimals(percentileValue)
            }

            /* Compact masks: BEST and WORST. If absent, default to empty mask. */
            row[columnIndex++] = bestModel?.retrieveMaskB64ForCardinality(cardinalityAsDouble) ?: "B64:"
            row[columnIndex++] = worstModel?.retrieveMaskB64ForCardinality(cardinalityAsDouble) ?: "B64:"

            aggregatedRows += row
        }

        /* Logging summary: emphasize coverage per target for diagnostics. */
        if (parameters.targetToAchieve != Constants.TARGET_ALL) {
            logger.info(
                "Total cardinality computed for target \"${parameters.targetToAchieve}\": " +
                        "${computedCardinalityByTarget[parameters.targetToAchieve]}/${referenceModel.numberOfTopics}."
            )
        } else {
            logger.info(
                "Total cardinality computed — BEST: ${computedCardinalityByTarget[Constants.TARGET_BEST]}/${referenceModel.numberOfTopics}, " +
                        "WORST: ${computedCardinalityByTarget[Constants.TARGET_WORST]}/${referenceModel.numberOfTopics}, " +
                        "AVERAGE: ${computedCardinalityByTarget[Constants.TARGET_AVERAGE]}/${referenceModel.numberOfTopics}."
            )
        }

        return aggregatedRows
    }

    /**
     * Build the Info table listing execution parameters and timing for each model.
     *
     * Columns:
     * "Dataset Name","Number of Systems","Number of Topics","Correlation Method",
     * "Target to Achieve","Number of Iterations","Population Size","Number of Repetitions",
     * "Evaluator Threads (effective)","Parallel Evaluator","CPU Logical Cores",
     * "Computing Time (ms)"
     *
     * Notes:
     * - "Evaluator Threads (effective)" reflects the actual threads used per model (`model.evaluatorThreadsUsed`).
     * - "Parallel Evaluator" is "YES" iff `evaluatorThreadsUsed > 1`, else "NO".
     * - "Computing Time (ms)" is the wall-clock runtime captured by each model.
     */
    private fun info(models: List<DatasetModel>): List<Array<String>> {
        val cpuLogicalCores: Int = Runtime.getRuntime().availableProcessors()

        val header = arrayOf(
            "Dataset Name",
            "Number of Systems",
            "Number of Topics",
            "Correlation Method",
            "Target to Achieve",
            "Number of Iterations",
            "Population Size",
            "Number of Repetitions",
            "Evaluator Threads (effective)",
            "Parallel Evaluator",
            "CPU Logical Cores",
            "Computing Time (ms)"
        )

        val rows = ArrayList<Array<String>>(models.size + 1)
        rows += header

        /* Each model contributes one row reflecting its actual settings and measured runtime. */
        models.forEach { model ->
            val effectiveThreads = model.evaluatorThreadsUsed.coerceAtLeast(1)
            val isParallel = if (effectiveThreads > 1) "YES" else "NO"
            val row = arrayOf(
                model.datasetName,
                model.numberOfSystems.toString(),
                model.numberOfTopics.toString(),
                model.correlationMethod,
                model.targetToAchieve,
                model.numberOfIterations.toString(),
                model.populationSize.toString(),
                model.numberOfRepetitions.toString(),
                effectiveThreads.toString(),
                isParallel,
                cpuLogicalCores.toString(),
                model.computingTime.toString()
            )
            rows += row
        }
        return rows
    }

    /**
     * Merge results from multiple executions (ex1..exN) into merged CSV/Parquet artifacts:
     *
     * - Aggregated (pick best-of/best and worst-of/worst per K, same logic as before).
     * - FUN/VAR per target (take the execution that produced the chosen aggregated value).
     * - TOP per target (copy rows matching the chosen execution per K).
     * - Info (sum "Computing Time (ms)" per target; keep other fields from the first occurrence).
     *
     * The method is schema-aware for INFO: it locates the "Computing Time (ms)"
     * column by header name (falls back to the last column if not found) so older
     * runs still merge correctly. It also reads the "Target to Achieve" column to sum
     * per-target time across executions when merging `ALL`.
     *
     * Also cleans the original per-execution CSV/Parquet files after writing merged outputs.
     *
     * @param numberOfExecutions Number of executions to merge (prefix of collected path lists).
     */
    fun merge(numberOfExecutions: Int) {
        logger.info("Starting to merge results of $numberOfExecutions executions.")

        /* Small CSV helpers. Keep them local to avoid polluting class scope. */
        fun readAllCsvRows(path: String): List<Array<String>> = try {
            CSVReader(FileReader(path)).use { it.readAll() ?: emptyList() }
        } catch (e: Exception) {
            logger.warn("Failed to read CSV at '$path': ${e.message}")
            emptyList()
        }

        fun readAllLines(path: String): List<String> = try {
            Files.readAllLines(Paths.get(path)).filter { it.isNotBlank() }
        } catch (e: Exception) {
            logger.warn("Failed to read text at '$path': ${e.message}")
            emptyList()
        }

        fun headerOrEmpty(rows: List<Array<String>>): Array<String> = if (rows.isNotEmpty()) rows.first() else emptyArray()
        fun dataRows(rows: List<Array<String>>): List<Array<String>> = if (rows.size > 1) rows.drop(1) else emptyList()
        fun minDataLen(tables: List<List<Array<String>>>): Int = tables.minOfOrNull { (if (it.isNotEmpty()) it.size - 1 else 0) } ?: 0
        fun parseDoubleOrNull(s: String): Double? = runCatching { s.trim().toDouble() }.getOrNull()

        /* Step 1: load aggregated tables for all executions. */
        logger.info("Loading aggregated data for all executions.")
        logger.info("Aggregated data paths:")
        aggregatedDataResultPaths.take(numberOfExecutions).forEach { logger.info("\"$it\"") }

        val aggregatedTables = aggregatedDataResultPaths.take(numberOfExecutions).map { readAllCsvRows(it) }
        if (aggregatedTables.any { it.isEmpty() }) {
            /* Defensive exit to avoid producing partially merged outputs if any exec is missing. */
            logger.warn("One or more aggregated CSV files are empty; aborting merge to avoid partial output.")
            return
        }

        val aggregatedHeader = headerOrEmpty(aggregatedTables.first())
        val aggregatedDataRowsPerExec = aggregatedTables.map { dataRows(it) }
        val alignedCardinalityCount = minDataLen(aggregatedTables)

        /* Progress logging scaled to dataset size to keep logs readable. */
        val loggingFactor = maxOf(1, (alignedCardinalityCount * Constants.LOGGING_FACTOR) / 100)
        var progressCounter = 0

        /* Locate optional mask columns so we can copy masks from the chosen execution. */
        val idxBestTopics = aggregatedHeader.indexOf("BestTopicsB64").takeIf { it >= 0 }
        val idxWorstTopics = aggregatedHeader.indexOf("WorstTopicsB64").takeIf { it >= 0 }

        /* Step 2: collect per-target per-execution FUN/VAR/TOP inputs. */
        var bestFunctionValues: LinkedList<LinkedList<String>>? = null
        var bestVariableValues: LinkedList<LinkedList<String>>? = null
        var bestTopSolutions: LinkedList<LinkedList<String>>? = null
        var worstFunctionValues: LinkedList<LinkedList<String>>? = null
        var worstVariableValues: LinkedList<LinkedList<String>>? = null
        var worstTopSolutions: LinkedList<LinkedList<String>>? = null
        var averageFunctionValues: LinkedList<LinkedList<String>>? = null
        var averageVariableValues: LinkedList<LinkedList<String>>? = null

        fun prepareExecListsFor(model: DatasetModel) {
            /* Filter input paths by target and by CSV location. */
            fun execSlicePaths(all: MutableList<String>, expect: Int, label: String): List<String> {
                if (all.size < expect) {
                    logger.warn("Expected at least $expect $label paths, found ${all.size}. Some executions may be missing.")
                }
                return all.take(expect)
            }

            val funPaths = mutableListOf<String>()
            val varPaths = mutableListOf<String>()
            val topPaths = mutableListOf<String>()

            when (model.targetToAchieve) {
                Constants.TARGET_BEST -> {
                    funPaths += functionValuesResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_BEST) }
                    varPaths += variableValuesResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_BEST) }
                    topPaths += topSolutionsResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_BEST) }
                }

                Constants.TARGET_WORST -> {
                    funPaths += functionValuesResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_WORST) }
                    varPaths += variableValuesResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_WORST) }
                    topPaths += topSolutionsResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_WORST) }
                }

                Constants.TARGET_AVERAGE -> {
                    funPaths += functionValuesResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_AVERAGE) }
                    varPaths += variableValuesResultPaths.filter { it.contains("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}") && it.contains(Constants.TARGET_AVERAGE) }
                }
            }

            /* Transpose exec-major lists into cardinality-major lists:
               result[k] is the list of lines for cardinality k across executions. */
            fun <T> transposeToLinked(rowsPerExec: List<List<T>>): LinkedList<LinkedList<T>> {
                val h = LinkedList<LinkedList<T>>()
                if (rowsPerExec.isEmpty()) return h
                val minLen = rowsPerExec.minOf { it.size }
                repeat(minLen) { idx ->
                    val col = LinkedList<T>()
                    rowsPerExec.forEach { execRows -> col += execRows[idx] }
                    h += col
                }
                return h
            }

            /* Read lines and align to the minimum cardinality count. */
            val funTables: List<List<String>> = execSlicePaths(funPaths, numberOfExecutions, "FUN")
                .map { readAllLines(it) }
                .map { it.take(alignedCardinalityCount) }

            val varTables: List<List<String>> = execSlicePaths(varPaths, numberOfExecutions, "VAR")
                .map { readAllLines(it) }
                .map { it.take(alignedCardinalityCount) }

            val topTables: List<List<String>> =
                if (model.targetToAchieve != Constants.TARGET_AVERAGE)
                    execSlicePaths(topPaths, numberOfExecutions, "TOP").map { readAllLines(it).drop(1) } /* drop header */
                else
                    emptyList()

            when (model.targetToAchieve) {
                Constants.TARGET_BEST -> {
                    bestFunctionValues = transposeToLinked(funTables)
                    bestVariableValues = transposeToLinked(varTables)
                    bestTopSolutions = transposeToLinked(topTables)
                }

                Constants.TARGET_WORST -> {
                    worstFunctionValues = transposeToLinked(funTables)
                    worstVariableValues = transposeToLinked(varTables)
                    worstTopSolutions = transposeToLinked(topTables)
                }

                Constants.TARGET_AVERAGE -> {
                    averageFunctionValues = transposeToLinked(funTables)
                    averageVariableValues = transposeToLinked(varTables)
                }
            }
        }

        if (targetToAchieve == Constants.TARGET_ALL) {
            prepareExecListsFor(models[0]) /* BEST */
            prepareExecListsFor(models[1]) /* WORST */
            prepareExecListsFor(models[2]) /* AVERAGE */
        } else {
            prepareExecListsFor(models[0])
        }

        /* Step 3: build merged Aggregated and select rows per target. */
        val mergedAggregatedData = LinkedList<Array<String>>().apply { add(aggregatedHeader) }

        val mergedBestFunctionValues = LinkedList<String>()
        val mergedBestVariableValues = LinkedList<String>()
        val mergedWorstFunctionValues = LinkedList<String>()
        val mergedWorstVariableValues = LinkedList<String>()
        val mergedAverageFunctionValues = LinkedList<String>()
        val mergedAverageVariableValues = LinkedList<String>()
        val mergedBestTopSolutions = LinkedList<String>()
        val mergedWorstTopSolutions = LinkedList<String>()

        for (cardIdx in 0 until alignedCardinalityCount) {
            if ((cardIdx % loggingFactor) == 0) {
                logger.info("Results merged for cardinality: ${cardIdx + 1}/$alignedCardinalityCount ($progressCounter%) for $numberOfExecutions total executions.")
                progressCounter += Constants.LOGGING_FACTOR
            }

            val rowsAtI: List<Array<String>> = aggregatedDataRowsPerExec.mapNotNull { execRows -> execRows.getOrNull(cardIdx) }
            if (rowsAtI.isEmpty()) continue

            val cardinalityToken = rowsAtI.first()[0]

            /* Choose the execution index that produced the best BEST and worst WORST correlation for this K. */
            var bestCorr = Double.NEGATIVE_INFINITY
            var bestExecIdx = -1
            var worstCorr = Double.POSITIVE_INFINITY
            var worstExecIdx = -1

            rowsAtI.forEachIndexed { execIdx, row ->
                when (targetToAchieve) {
                    Constants.TARGET_ALL -> {
                        parseDoubleOrNull(row.getOrNull(1) ?: "")?.let {
                            if (it > bestCorr) {
                                bestCorr = it; bestExecIdx = execIdx
                            }
                        }
                        parseDoubleOrNull(row.getOrNull(2) ?: "")?.let {
                            if (it < worstCorr) {
                                worstCorr = it; worstExecIdx = execIdx
                            }
                        }
                    }

                    Constants.TARGET_BEST -> {
                        parseDoubleOrNull(row.getOrNull(1) ?: "")?.let {
                            if (it > bestCorr) {
                                bestCorr = it; bestExecIdx = execIdx
                            }
                        }
                    }

                    Constants.TARGET_WORST -> {
                        parseDoubleOrNull(row.getOrNull(1) ?: "")?.let {
                            if (it < worstCorr) {
                                worstCorr = it; worstExecIdx = execIdx
                            }
                        }
                    }

                    Constants.TARGET_AVERAGE -> {
                        /* AVERAGE is copied as-is from the first exec row (already aligned). */
                    }
                }
            }

            /* Template row is taken from the first exec to preserve column count/order. */
            val template = rowsAtI.first()
            val out = Array(template.size) { "" }
            out[0] = cardinalityToken

            when (targetToAchieve) {
                Constants.TARGET_ALL -> {
                    out[1] = if (bestExecIdx >= 0) bestCorr.toString() else Constants.CARDINALITY_NOT_AVAILABLE
                    out[2] = if (worstExecIdx >= 0) worstCorr.toString() else Constants.CARDINALITY_NOT_AVAILABLE
                    for (col in 3 until template.size) out[col] = template[col]
                    /* If mask columns exist, replace them with those from the chosen executions. */
                    if (idxBestTopics != null && bestExecIdx >= 0) {
                        out[idxBestTopics] = rowsAtI[bestExecIdx].getOrNull(idxBestTopics) ?: out[idxBestTopics]
                    }
                    if (idxWorstTopics != null && worstExecIdx >= 0) {
                        out[idxWorstTopics] = rowsAtI[worstExecIdx].getOrNull(idxWorstTopics) ?: out[idxWorstTopics]
                    }
                }

                Constants.TARGET_BEST -> {
                    out[1] = if (bestExecIdx >= 0) bestCorr.toString() else Constants.CARDINALITY_NOT_AVAILABLE
                    for (col in 2 until template.size) out[col] = template[col]
                    if (idxBestTopics != null && bestExecIdx >= 0) {
                        out[idxBestTopics] = rowsAtI[bestExecIdx].getOrNull(idxBestTopics) ?: out[idxBestTopics]
                    }
                }

                Constants.TARGET_WORST -> {
                    out[1] = if (worstExecIdx >= 0) worstCorr.toString() else Constants.CARDINALITY_NOT_AVAILABLE
                    for (col in 2 until template.size) out[col] = template[col]
                    if (idxWorstTopics != null && worstExecIdx >= 0) {
                        out[idxWorstTopics] = rowsAtI[worstExecIdx].getOrNull(idxWorstTopics) ?: out[idxWorstTopics]
                    }
                }

                Constants.TARGET_AVERAGE -> {
                    /* Copy the first exec row verbatim (AVERAGE has no exec selection). */
                    val src = rowsAtI.first()
                    for (col in src.indices) out[col] = src[col]
                }
            }
            mergedAggregatedData.add(out)

            /* Stash the winning FUN/VAR/TOP for this K, respecting nulls and uneven execs. */
            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_BEST) {
                bestFunctionValues?.let { table ->
                    if (cardIdx < table.size) {
                        val execCol = if (bestExecIdx >= 0) bestExecIdx.coerceAtMost(table[cardIdx].size - 1) else 0
                        table[cardIdx].getOrNull(execCol)?.let { mergedBestFunctionValues += it }
                    }
                }
                bestVariableValues?.let { table ->
                    if (cardIdx < table.size) {
                        val execCol = if (bestExecIdx >= 0) bestExecIdx.coerceAtMost(table[cardIdx].size - 1) else 0
                        table[cardIdx].getOrNull(execCol)?.let { mergedBestVariableValues += it }
                    }
                }
                /* TOP lines are per-K blocks; pick the line from the chosen exec that matches this K. */
                bestTopSolutions?.forEach { perExecLineSet ->
                    val line = perExecLineSet.getOrNull(bestExecIdx) ?: return@forEach
                    val kToken = line.substringBefore(',').trim()
                    if (kToken == (cardIdx + 1).toString()) mergedBestTopSolutions += line
                }
            }

            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_WORST) {
                worstFunctionValues?.let { table ->
                    if (cardIdx < table.size) {
                        val execCol = if (worstExecIdx >= 0) worstExecIdx.coerceAtMost(table[cardIdx].size - 1) else 0
                        table[cardIdx].getOrNull(execCol)?.let { mergedWorstFunctionValues += it }
                    }
                }
                worstVariableValues?.let { table ->
                    if (cardIdx < table.size) {
                        val execCol = if (worstExecIdx >= 0) worstExecIdx.coerceAtMost(table[cardIdx].size - 1) else 0
                        table[cardIdx].getOrNull(execCol)?.let { mergedWorstVariableValues += it }
                    }
                }
                worstTopSolutions?.forEach { perExecLineSet ->
                    val line = perExecLineSet.getOrNull(worstExecIdx) ?: return@forEach
                    val kToken = line.substringBefore(',').trim()
                    if (kToken == (cardIdx + 1).toString()) mergedWorstTopSolutions += line
                }
            }

            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {
                averageFunctionValues?.let { table ->
                    if (cardIdx < table.size) table[cardIdx].firstOrNull()?.let { mergedAverageFunctionValues += it }
                }
                averageVariableValues?.let { table ->
                    if (cardIdx < table.size) table[cardIdx].firstOrNull()?.let { mergedAverageVariableValues += it }
                }
            }
        }

        logger.info("Results merged for cardinality: $alignedCardinalityCount/$alignedCardinalityCount (100%) for $numberOfExecutions total executions.")

        /* Step 4: merge Info (schema-aware on time and target columns). */
        logger.info("Loading info for all executions.")
        logger.info("Info paths:")
        infoResultPaths.take(numberOfExecutions).forEach { logger.info("\"$it\"") }
        val infoTables: List<List<Array<String>>> = infoResultPaths.take(numberOfExecutions).map { readAllCsvRows(it) }

        val defaultInfoHeader = arrayOf(
            "Dataset Name",
            "Number of Systems",
            "Number of Topics",
            "Correlation Method",
            "Target to Achieve",
            "Number of Iterations",
            "Population Size",
            "Number of Repetitions",
            "Evaluator Threads (effective)",
            "Parallel Evaluator",
            "CPU Logical Cores",
            "Computing Time (ms)"
        )
        val infoHeader = infoTables.firstOrNull()?.firstOrNull() ?: defaultInfoHeader

        fun findTimeColumnIndex(header: Array<String>): Int {
            val exact = header.indexOfFirst { it.trim().equals("Computing Time (ms)", ignoreCase = true) }
            if (exact >= 0) return exact
            val contains = header.indexOfFirst { it.trim().lowercase(Locale.ROOT).contains("computing time") }
            return if (contains >= 0) contains else header.lastIndex.coerceAtLeast(0)
        }

        fun findTargetColumnIndex(header: Array<String>): Int {
            val exact = header.indexOfFirst { it.trim().equals("Target to Achieve", ignoreCase = true) }
            return if (exact >= 0) exact else 4.coerceIn(0, header.lastIndex)
        }

        val timeColIndexGlobal = findTimeColumnIndex(infoHeader)

        val mergedInfoLines = LinkedList<Array<String>>().apply { add(infoHeader) }

        if (targetToAchieve == Constants.TARGET_ALL) {
            /* Sum times per target across executions; keep a template row per target from the first occurrence. */
            val sumByTarget = mutableMapOf(
                Constants.TARGET_BEST to 0L, Constants.TARGET_WORST to 0L, Constants.TARGET_AVERAGE to 0L
            )
            val templateRowByTarget = mutableMapOf<String, Array<String>>()

            infoTables.forEach { table ->
                if (table.isEmpty()) return@forEach
                val hdr = table.first()
                val timeIdx = findTimeColumnIndex(hdr)
                val tgtIdx = findTargetColumnIndex(hdr)
                table.drop(1).forEach { row ->
                    val tgt = row.getOrNull(tgtIdx)?.trim() ?: return@forEach
                    val ms = row.getOrNull(timeIdx)?.replace("\"", "")?.trim()?.toLongOrNull() ?: 0L
                    if (tgt in sumByTarget) {
                        sumByTarget[tgt] = (sumByTarget[tgt] ?: 0L) + ms
                        templateRowByTarget.putIfAbsent(tgt, row.copyOf())
                    }
                }
            }

            fun rewrite(row: Array<String>?, total: Long): Array<String>? =
                row?.copyOf()?.also { if (it.isNotEmpty()) it[timeColIndexGlobal] = total.toString() }

            listOf(Constants.TARGET_BEST, Constants.TARGET_WORST, Constants.TARGET_AVERAGE).forEach { tgt ->
                val tpl = templateRowByTarget[tgt]
                val total = sumByTarget[tgt] ?: 0L
                rewrite(tpl, total)?.let { mergedInfoLines += it }
            }
        } else {
            /* Single-target merge: sum the time across executions and overwrite the time cell. */
            val totalTime = infoTables.sumOf { table ->
                if (table.size <= 1) 0L
                else {
                    val hdr = table.first()
                    val timeIdx = findTimeColumnIndex(hdr)
                    table.drop(1).sumOf { r -> r.getOrNull(timeIdx)?.replace("\"", "")?.trim()?.toLongOrNull() ?: 0L }
                }
            }
            /* Copy the first row (or an empty fallback), pad if needed, and write the merged time. */
            val firstRow = infoTables.firstOrNull()?.getOrNull(1) ?: emptyArray()
            val targetSize = maxOf(firstRow.size, infoHeader.size, timeColIndexGlobal + 1)
            val rewritten: Array<String> = Array(targetSize) { "" }
            System.arraycopy(firstRow, 0, rewritten, 0, firstRow.size)
            rewritten[timeColIndexGlobal] = totalTime.toString()
            mergedInfoLines.add(rewritten)
        }

        /* Step 5: write merged CSV and Parquet outputs. */
        val isAllTargets = (targetToAchieve == Constants.TARGET_ALL)

        /* Aggregated */
        val mergedAggCsv = view.getAggregatedDataMergedFilePath(models[0], isTargetAll = isAllTargets)
        view.writeCsv(mergedAggregatedData, mergedAggCsv)
        logger.info("Merged aggregated data available at:\n\"$mergedAggCsv\"")

        val mergedAggParquet = view.getAggregatedDataMergedParquetPath(models[0], isTargetAll = isAllTargets)
        view.writeParquet(mergedAggregatedData, mergedAggParquet)

        /* Info */
        val mergedInfoCsv = view.getInfoMergedFilePath(models[0], isTargetAll = isAllTargets)
        java.nio.file.Files.newBufferedWriter(java.nio.file.Paths.get(mergedInfoCsv)).use { w ->
            mergedInfoLines.forEach { ln -> w.appendLine(ln.joinToString(",")) }
        }
        logger.info("Merged info available at:\n\"$mergedInfoCsv\"")

        val mergedInfoParquet = view.getInfoMergedParquetPath(models[0], isTargetAll = isAllTargets)
        view.writeParquet(mergedInfoLines.map { it.toList().toTypedArray() }, mergedInfoParquet)

        /* Per target: FUN / VAR / TOP. */
        fun writeMergedPerTarget(model: DatasetModel) {
            /* FUN CSV */
            val funMergedCsv = view.getFunctionValuesMergedFilePath(model)
            java.nio.file.Files.newBufferedWriter(java.nio.file.Paths.get(funMergedCsv)).use { w ->
                val lines = when (model.targetToAchieve) {
                    Constants.TARGET_BEST -> mergedBestFunctionValues
                    Constants.TARGET_WORST -> mergedWorstFunctionValues
                    else -> mergedAverageFunctionValues
                }
                lines.forEach { ln -> w.appendLine(ln) }
            }

            /* VAR CSV */
            val varMergedCsv = view.getVariableValuesMergedFilePath(model)
            java.nio.file.Files.newBufferedWriter(java.nio.file.Paths.get(varMergedCsv)).use { w ->
                val lines = when (model.targetToAchieve) {
                    Constants.TARGET_BEST -> mergedBestVariableValues
                    Constants.TARGET_WORST -> mergedWorstVariableValues
                    else -> mergedAverageVariableValues
                }
                lines.forEach { ln -> w.appendLine(ln) }
            }

            /* TOP CSV (not present for AVERAGE). */
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                val topMergedCsv = view.getTopSolutionsMergedFilePath(model)
                java.nio.file.Files.newBufferedWriter(java.nio.file.Paths.get(topMergedCsv)).use { w ->
                    w.appendLine("Cardinality,Correlation,TopicsB64")
                    val lines = if (model.targetToAchieve == Constants.TARGET_BEST) mergedBestTopSolutions else mergedWorstTopSolutions
                    lines.forEach { ln -> w.appendLine(ln) }
                }
            }

            /* FUN Parquet: header "K,Correlation"; robust split on comma or whitespace. */
            run {
                val funLines = when (model.targetToAchieve) {
                    Constants.TARGET_BEST -> mergedBestFunctionValues
                    Constants.TARGET_WORST -> mergedWorstFunctionValues
                    else -> mergedAverageFunctionValues
                }
                val rows = ArrayList<Array<String>>(funLines.size + 1)
                rows += arrayOf("K", "Correlation")
                val split = Regex("[,\\s]+")
                funLines.forEach { ln ->
                    val parts = ln.trim().split(split)
                    val kTok = parts.getOrNull(0) ?: ""
                    val corrTok = parts.getOrNull(1) ?: ""
                    rows += arrayOf(kTok, corrTok)
                }
                val out = view.getFunctionValuesMergedParquetPath(model)
                view.writeParquet(rows, out)
            }

            /* VAR Parquet: header "K,Topics"; K inferred from row index (1..N). */
            run {
                val varLines = when (model.targetToAchieve) {
                    Constants.TARGET_BEST -> mergedBestVariableValues
                    Constants.TARGET_WORST -> mergedWorstVariableValues
                    else -> mergedAverageVariableValues
                }
                val rows = ArrayList<Array<String>>(varLines.size + 1)
                rows += arrayOf("K", "Topics")
                varLines.forEachIndexed { idx, ln ->
                    val kTok = (idx + 1).toString()
                    val topicsTok = ln.trim()
                    rows += arrayOf(kTok, topicsTok)
                }
                val out = view.getVariableValuesMergedParquetPath(model)
                view.writeParquet(rows, out)
            }

            /* TOP Parquet: header "K,Correlation,TopicsB64". */
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                val rows = ArrayList<Array<String>>()
                rows += arrayOf("K", "Correlation", "TopicsB64")
                val src = if (model.targetToAchieve == Constants.TARGET_BEST) mergedBestTopSolutions else mergedWorstTopSolutions
                src.forEach { ln ->
                    val parts = ln.split(',', limit = 3)
                    val kTok = parts.getOrNull(0)?.trim() ?: ""
                    val corrTok = parts.getOrNull(1)?.trim() ?: ""
                    val topicsTok = parts.getOrNull(2)?.trim() ?: ""
                    rows += arrayOf(kTok, corrTok, topicsTok)
                }
                val out = view.getTopSolutionsMergedParquetPath(model)
                view.writeParquet(rows, out)
            }
        }

        if (targetToAchieve == Constants.TARGET_ALL) {
            writeMergedPerTarget(models[0]) /* BEST */
            writeMergedPerTarget(models[1]) /* WORST */
            writeMergedPerTarget(models[2]) /* AVERAGE */
        } else {
            writeMergedPerTarget(models[0])
        }

        /* Step 6: clean up non-merged CSV and Parquet files. */

        /* Snapshot original CSV lists before we mutate them, so we can derive Parquet siblings reliably. */
        val snapAggCsv = aggregatedDataResultPaths.toList()
        val snapFunCsv = functionValuesResultPaths.toList()
        val snapVarCsv = variableValuesResultPaths.toList()
        val snapTopCsv = topSolutionsResultPaths.toList()
        val snapInfoCsv = infoResultPaths.toList()

        /* CSV cleanup. Only remove files that actually exist. */
        logger.info("Cleaning of not merged CSV results started.")
        clean(aggregatedDataResultPaths, "Cleaning aggregated CSV data at paths:")
        clean(functionValuesResultPaths, "Cleaning function values CSV at paths:")
        clean(variableValuesResultPaths, "Cleaning variable values CSV at paths:")
        clean(topSolutionsResultPaths, "Cleaning top solutions CSV at paths:")
        clean(infoResultPaths, "Cleaning info CSV at paths:")
        logger.info("Cleaning of not merged CSV results completed.")

        /* Build Parquet lists from the snapshots (ex1/ex2 remain visible in the snapshot). */
        fun mapCsvToParquet(path: String): String =
            path.replace("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}", "${Constants.PATH_SEPARATOR}Parquet${Constants.PATH_SEPARATOR}")
                .replace(".csv", ".parquet")

        val aggParquetToClean = snapAggCsv.map(::mapCsvToParquet).toMutableList()
        val funParquetToClean = snapFunCsv.map(::mapCsvToParquet).toMutableList()
        val varParquetToClean = snapVarCsv.map(::mapCsvToParquet).toMutableList()
        val topParquetToClean = snapTopCsv.map(::mapCsvToParquet).toMutableList()
        val infoParquetToClean = snapInfoCsv.map(::mapCsvToParquet).toMutableList()

        logger.info("Cleaning of not merged Parquet results started.")
        clean(aggParquetToClean, "Cleaning aggregated Parquet data at paths:")
        clean(funParquetToClean, "Cleaning function values Parquet at paths:")
        clean(varParquetToClean, "Cleaning variable values Parquet at paths:")
        clean(topParquetToClean, "Cleaning top solutions Parquet at paths:")
        clean(infoParquetToClean, "Cleaning info Parquet at paths:")
        logger.info("Cleaning of not merged Parquet results completed.")

        logger.info("Executions result merging completed.")
    }

    /**
     * Copy the per-execution and merged results produced by the last solve/merge
     * into the `experiments` destination tree (CSV + Parquet), preserving filenames.
     *
     * Structure mirrors writers: `<root>/CSV` and `<root>/Parquet`.
     * Missing files are skipped with a log line.
     */
    fun copy() {
        logger.info("Execution result copy to ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} started.")

        val inputRootPath = Constants.NEWBESTSUB_EXPERIMENTS_INPUT_PATH
        val inputRootDir = File(inputRootPath)

        logger.info("Checking if ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} input dir. exists.")
        if (!inputRootDir.exists()) {
            logger.info("Input dir. not exists.")
            if (inputRootDir.mkdirs()) {
                logger.info("Input dir. created.")
                logger.info("Path: \"$inputRootPath\".")
            }
        } else {
            logger.info("Input dir. already exists.")
            logger.info("Input dir. creation skipped.")
            logger.info("Path: \"$inputRootPath\".")
        }

        /* Destination subfolders mirroring our writers. Create if missing. */
        val csvDestDir = Paths.get("$inputRootPath${Constants.PATH_SEPARATOR}CSV")
        val parquetDestDir = Paths.get("$inputRootPath${Constants.PATH_SEPARATOR}Parquet")
        if (!Files.exists(csvDestDir)) Files.createDirectories(csvDestDir)
        if (!Files.exists(parquetDestDir)) Files.createDirectories(parquetDestDir)

        /* Copy helper: only copy existing files, keep names, overwrite destination. */
        val copyIfExists = { srcPathStr: String, destDir: java.nio.file.Path ->
            val src = Paths.get(srcPathStr)
            if (Files.exists(src)) {
                val dst = destDir.resolve(src.fileName)
                Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING)
                logger.info("\"$src\" -> \"$dst\"")
                true
            } else {
                false
            }
        }

        /* Per-execution CSV artifacts (using collected path lists). */
        val csvListCopier = { dataList: MutableList<String>, logMessage: String ->
            if (dataList.isNotEmpty()) logger.info(logMessage)
            dataList.forEach { src -> copyIfExists(src, csvDestDir) }
        }

        csvListCopier(aggregatedDataResultPaths, "Aggregated data (CSV) copy started from paths:")
        csvListCopier(functionValuesResultPaths, "Function values (CSV) copy started from paths:")
        csvListCopier(variableValuesResultPaths, "Variable values (CSV) copy started from paths:")
        csvListCopier(topSolutionsResultPaths, "Top solutions (CSV) copy started from paths:")
        csvListCopier(infoResultPaths, "Info (CSV) copy started from paths:")

        /* Per-execution Parquet artifacts (computed from models). */
        models.forEach { model ->
            /* Aggregated/Info: ALL vs single-target follow the same paths as in solve(). */
            copyIfExists(
                view.getAggregatedDataParquetPath(model, isTargetAll = (targetToAchieve == Constants.TARGET_ALL)), parquetDestDir
            )
            copyIfExists(view.getFunctionValuesParquetPath(model), parquetDestDir)
            copyIfExists(view.getVariableValuesParquetPath(model), parquetDestDir)
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                copyIfExists(view.getTopSolutionsParquetPath(model), parquetDestDir)
            }
            copyIfExists(
                view.getInfoParquetPath(model, isTargetAll = (targetToAchieve == Constants.TARGET_ALL)), parquetDestDir
            )
        }

        /* Merged CSV artifacts */
        val copyMergedCsv = { path: String, label: String ->
            if (copyIfExists(path, csvDestDir)) {
                logger.info("$label copied.")
            }
        }

        copyMergedCsv(view.getAggregatedDataMergedFilePath(models[0], isTargetAll = true), "Merged aggregated data (CSV, ALL)")
        copyMergedCsv(view.getAggregatedDataMergedFilePath(models[0], isTargetAll = false), "Merged aggregated data (CSV, single target)")

        models.forEach { model ->
            copyMergedCsv(view.getFunctionValuesMergedFilePath(model), "Merged function values (CSV) for ${model.targetToAchieve}")
            copyMergedCsv(view.getVariableValuesMergedFilePath(model), "Merged variable values (CSV) for ${model.targetToAchieve}")
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                copyMergedCsv(view.getTopSolutionsMergedFilePath(model), "Merged top solutions (CSV) for ${model.targetToAchieve}")
            }
            copyMergedCsv(view.getInfoMergedFilePath(model, isTargetAll = true), "Merged info (CSV, ALL) for ${model.targetToAchieve}")
            copyMergedCsv(view.getInfoMergedFilePath(model, isTargetAll = false), "Merged info (CSV, single target) for ${model.targetToAchieve}")
        }

        /* Merged Parquet artifacts */
        val copyMergedParquet = { path: String, label: String ->
            if (copyIfExists(path, parquetDestDir)) {
                logger.info("$label copied.")
            }
        }

        copyMergedParquet(view.getAggregatedDataMergedParquetPath(models[0], isTargetAll = true), "Merged aggregated data (Parquet, ALL)")
        copyMergedParquet(view.getAggregatedDataMergedParquetPath(models[0], isTargetAll = false), "Merged aggregated data (Parquet, single target)")

        models.forEach { model ->
            copyMergedParquet(view.getFunctionValuesMergedParquetPath(model), "Merged function values (Parquet) for ${model.targetToAchieve}")
            copyMergedParquet(view.getVariableValuesMergedParquetPath(model), "Merged variable values (Parquet) for ${model.targetToAchieve}")
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                copyMergedParquet(view.getTopSolutionsMergedParquetPath(model), "Merged top solutions (Parquet) for ${model.targetToAchieve}")
            }
            copyMergedParquet(view.getInfoMergedParquetPath(model, isTargetAll = true), "Merged info (Parquet, ALL) for ${model.targetToAchieve}")
            copyMergedParquet(view.getInfoMergedParquetPath(model, isTargetAll = false), "Merged info (Parquet, single target) for ${model.targetToAchieve}")
        }

        logger.info("Execution result copy to ${Constants.NEWBESTSUB_EXPERIMENTS_NAME} completed.")
    }

    /**
     * Delete a list of files on disk and remove their paths from the given list.
     *
     * Paths that do not exist are logged and skipped.
     *
     * @param dataList Mutable list of absolute file paths to delete and prune in-place.
     * @param logMessage Message to log before deletion.
     */
    fun clean(dataList: MutableList<String>, logMessage: String) {
        logger.info(logMessage)
        val toBeRemoved = mutableListOf<String>()
        dataList.forEach { aResultPath ->
            if (Files.exists(Paths.get(aResultPath))) {
                Files.delete(Paths.get(aResultPath))
                toBeRemoved.add(aResultPath)
            }
            logger.info("\"$aResultPath\"")
        }
        dataList.removeAll(toBeRemoved)
    }
}
