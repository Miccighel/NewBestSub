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

/**
 * DatasetController
 * =================
 *
 * Orchestrates:
 * - [DatasetModel] lifecycle (load → optional expand → seal → solve)
 * - Streaming consumption (printer coroutine reading [ProgressEvent]s)
 * - View writing for final snapshots and merged artifacts (CSV + Parquet)
 *
 * ## Determinism
 * When [Parameters.deterministic] is `true`:
 * - Runs execute **sequentially** (BEST → WORST → AVERAGE).
 * - All randomness is routed through jMetal's singleton via [RandomBridge.withSeed].
 * - Expansion helpers ([expandTopics], [expandSystems]) also use a [SplittableRandom]
 *   seeded via [RandomBridge.childSeed] for reproducible fake data.
 *
 * ## Efficiency
 * - A single printer coroutine consumes a back-pressured [Channel] of events.
 * - Uses **batched** [TopKReplaceBatch] to avoid rewriting the TOP file multiple times per generation.
 *
 * @property targetToAchieve `"BEST" | "WORST" | "AVERAGE" | "ALL"` top-level routing for solve/merge/copy flows.
 */
class DatasetController(
    private var targetToAchieve: String
) {

    /** Per-target models (when TARGET_ALL, indices: 0=BEST, 1=WORST, 2=AVERAGE; else a single entry). */
    var models = mutableListOf<DatasetModel>()

    /** Unified view wrapper writing CSV and Parquet artifacts. */
    private var view = DatasetView()

    /** Parameters of the last/ongoing run. */
    private lateinit var parameters: Parameters

    /** Path passed to [load]. */
    private lateinit var datasetPath: String

    /* Collected output paths for later merge/copy steps (CSV lists; Parquet derived when needed). */
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
     * Load a dataset from a CSV path into one or three [DatasetModel]s depending on [targetToAchieve].
     *
     * - Always loads one model at index 0.
     * - If [targetToAchieve] == `ALL`, also loads models at indices 1 (WORST) and 2 (AVERAGE).
     *
     * @param datasetPath Path to the CSV dataset file.
     */
    fun load(datasetPath: String) {

        this.datasetPath = datasetPath

        logger.info("Dataset loading started.")
        logger.info("Path: \"$datasetPath\".")

        try {
            models.plusAssign(DatasetModel())
            models[0].loadData(this.datasetPath)
            if (targetToAchieve == Constants.TARGET_ALL) {
                models.plusAssign(DatasetModel())
                models[1].loadData(this.datasetPath)
                models.plusAssign(DatasetModel())
                models[2].loadData(this.datasetPath)
            }
        } catch (_: FileNotFoundException) {
            logger.warn("Dataset not found. Is file inside a \"data\" dir.?")
        } catch (exception: IOException) {
            logger.warn(exception.message as String)
        }

        logger.info("Dataset loading for input file \"${models[0].datasetName}\" completed.")
    }

    /**
     * Expand the dataset by appending `expansionCoefficient` **fake topics** to all loaded models.
     *
     * - Topic labels are generated as `"NNN (F)"` where `NNN ∈ [800..998]`.
     * - APs for the new topics are generated randomly per system.
     * - In deterministic mode, all randomness is derived from [RandomBridge.childSeed].
     *
     * @param expansionCoefficient Number of random topics to append.
     */
    fun expandTopics(expansionCoefficient: Int) {
        val rng: SplittableRandom? =
            if (RandomBridge.isInstalled()) SplittableRandom(RandomBridge.childSeed("EXPAND_TOPICS", expansionNonce++))
            else null

        val systemLabels = models[0].systemLabels
        val topicLabels = Array(expansionCoefficient) {
            val suffix = (rng?.nextInt(800, 999) ?: (Random().nextInt(999 - 800) + 800))
            "$suffix (F)"
        }

        val randomizedAveragePrecisions = LinkedHashMap<String, DoubleArray>()
        systemLabels.forEach { systemLabel ->
            randomizedAveragePrecisions[systemLabel] =
                DoubleArray(expansionCoefficient) { rng?.nextDouble() ?: Math.random() }
        }

        models.forEach { model -> model.expandTopics(expansionCoefficient, randomizedAveragePrecisions, topicLabels) }
    }

    /**
     * Expand the dataset by appending `expansionCoefficient` **fake systems** (or revert to a prefix),
     * across all loaded models.
     *
     * - System labels are generated as `"Sys{index}{NNN} (F)"` with `NNN ∈ [800..998]`.
     * - AP rows for the new systems are random; in deterministic mode they come from
     *   a [SplittableRandom] seeded via [RandomBridge.childSeed].
     * - **Important fix**: new systems receive **exactly `numberOfTopics`** AP values
     *   (previously `+ expansionCoefficient`, which was incorrect).
     *
     * @param expansionCoefficient Number of random systems to append.
     * @param trueNumberOfSystems Ground-truth number of systems; if `current + expansionCoefficient < trueNumberOfSystems`,
     * reverts the AP map to a prefix of the original systems instead of appending fakes.
     */
    fun expandSystems(expansionCoefficient: Int, trueNumberOfSystems: Int) {
        val rng: SplittableRandom? =
            if (RandomBridge.isInstalled()) SplittableRandom(RandomBridge.childSeed("EXPAND_SYSTEMS", expansionNonce++))
            else null

        val systemLabels = Array(expansionCoefficient) { index ->
            val suffix = (rng?.nextInt(800, 999) ?: (Random().nextInt(999 - 800) + 800))
            "Sys$index$suffix (F)"
        }

        /* Ensure we build rows that match the current topic count (no extra columns). */
        val topicsNow = models.firstOrNull()?.numberOfTopics
            ?: throw IllegalStateException("expandSystems() called before load()")

        val randomizedAveragePrecisions = LinkedHashMap<String, DoubleArray>()
        systemLabels.forEach { systemLabel ->
            randomizedAveragePrecisions[systemLabel] =
                DoubleArray(topicsNow) { rng?.nextDouble() ?: Math.random() }
        }

        models.forEach { model ->
            model.expandSystems(expansionCoefficient, trueNumberOfSystems, randomizedAveragePrecisions, systemLabels)
        }
    }

    /**
     * Run the experiment(s) according to [parameters.targetToAchieve].
     *
     * ### What this does
     * - Seals all loaded [DatasetModel]s (boxing → dense primitives) before solving.
     * - Starts the solve(s), streaming progress via a single back-pressured [Channel] of [ProgressEvent]s.
     * - A dedicated **printer** coroutine (IO dispatcher) consumes events and appends:
     *   - [CardinalityResult] → FUN/VAR lines
     *   - [TopKReplaceBatch] → atomically replaces Top blocks
     *   - [RunCompleted] → closes writers (also triggers Parquet writes in the view)
     * - After runs complete, writes **Aggregated** and **Info** tables (CSV + Parquet),
     *   then clears per-run caches in each model.
     *
     * ### Determinism
     * - If [Parameters.deterministic] is `true`, runs execute **sequentially** (BEST → WORST → AVERAGE).
     * - Randomness is routed through `RandomBridge` with labeled child seeds
     *   (`"BEST"`, `"WORST"`, `"AVERAGE"`) for reproducibility.
     *
     * ### Parallelism and thread budgeting
     * - BEST/WORST use jMetal’s **internal** parallel evaluator (`MultiThreadedSolutionListEvaluator`)
     *   sized via [Parameters.evaluatorThreads].
     * - When `targetToAchieve == ALL` **and** `deterministic == false`:
     *   - We **split CPU cores** between BEST and WORST to avoid oversubscription:
     *     reserve ~1 core for I/O/GC, split the remainder in half, and set
     *     `evaluatorThreads` for BEST and WORST accordingly (min 2 each).
     *   - AVERAGE remains effectively **single-threaded** inside the model
     *     (its loop is not jMetal-parallelized), so we set `evaluatorThreads = 1`.
     *   - Each NSGA job is launched on **one coroutine worker**
     *     (`Dispatchers.Default.limitedParallelism(1)`), letting the jMetal pool
     *     do the heavy lifting inside the job.
     * - For **single-target**, non-deterministic runs: if the caller didn’t set
     *   [Parameters.evaluatorThreads], we give the run **all available cores**
     *   explicitly; otherwise we honor the provided value.
     *
     * ### Back-pressure & I/O
     * - The event channel capacity is sized conservatively and all writes happen
     *   on the printer coroutine to keep file I/O serialized and GC-friendly.
     * - `TopKReplaceBatch` minimizes churn by replacing whole blocks only when
     *   content changes (version-gated inside the model).
     *
     * ### Post-run hygiene
     * - After artifacts are written, the controller calls:
     *   - [DatasetModel.clearPercentiles] to drop percentile lists.
     *   - [DatasetModel.clearAfterSerialization] to release representative masks,
     *     per-K correlation maps, and Top pools.
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
        if (parameters.deterministic) {
            logger.info("Deterministic mode is ON. Master seed: ${parameters.seed ?: "(derived)"}")
        }
        if (parameters.targetToAchieve == Constants.TARGET_ALL || parameters.targetToAchieve == Constants.TARGET_AVERAGE) {
            val pct = parameters.percentiles.joinToString(", ") { "$it%" }
            logger.info("Percentiles: $pct. [Experiment: Average]")
        }

        /* Seal boxed AP rows into dense primitive bundle and free big maps BEFORE the run. */
        models.forEach { it.sealData() }

        if (parameters.targetToAchieve == Constants.TARGET_ALL) {

            /* Prepare per-target parameters (may be tweaked for thread budgets below). */
            var bestParams = parameters.copy(targetToAchieve = Constants.TARGET_BEST)
            var worstParams = parameters.copy(targetToAchieve = Constants.TARGET_WORST)
            var averageParams = parameters.copy(targetToAchieve = Constants.TARGET_AVERAGE)

            /* Channel capacity tuned for throttled onGen with headroom. */
            val estimatedEventsPerTarget = (models[0].numberOfTopics / 3) + 64
            val channelCapacity = (estimatedEventsPerTarget * 3).coerceAtLeast(64)
            val progressEvents: Channel<ProgressEvent> = Channel(channelCapacity)

            if (parameters.deterministic) {
                /* ---------------- Deterministic: sequential solves with labeled seeds ---------------- */
                runBlocking {
                    supervisorScope {
                        /* Single printer on IO dispatcher */
                        val printerJob = launch(Dispatchers.IO) {
                            for (event in progressEvents) {
                                val model = models.first { it.targetToAchieve == event.target }
                                when (event) {
                                    is CardinalityResult -> view.appendCardinality(model, event)
                                    is TopKReplaceBatch -> view.replaceTopBatch(model, event.blocks)
                                    is RunCompleted -> view.closeStreams(model)
                                }
                            }
                        }

                        /* BEST */
                        RandomBridge.withSeed(RandomBridge.childSeed("BEST")) {
                            models[0].solve(bestParams, progressEvents)
                        }
                        /* WORST */
                        RandomBridge.withSeed(RandomBridge.childSeed("WORST")) {
                            models[1].solve(worstParams, progressEvents)
                        }
                        /* AVERAGE */
                        RandomBridge.withSeed(RandomBridge.childSeed("AVERAGE")) {
                            models[2].solve(averageParams, progressEvents)
                        }

                        progressEvents.close()
                        printerJob.join()
                    }
                }

            } else {
                /* ---------------- Non-deterministic: parallel solves ----------------
                 * Avoid CPU over-subscription by splitting cores across runs and letting
                 * jMetal’s evaluator pool do the heavy lifting inside each run. */

                /* Determine which targets are enabled in this branch.
                 * If ALL → run all three; otherwise run only the requested one. */
                val runAll = parameters.targetToAchieve == Constants.TARGET_ALL
                val runBest = runAll || parameters.targetToAchieve == Constants.TARGET_BEST
                val runWorst = runAll || parameters.targetToAchieve == Constants.TARGET_WORST
                val runAverage = runAll || parameters.targetToAchieve == Constants.TARGET_AVERAGE

                /* Compute a simple thread budget:
                 * - Reserve one core for I/O/GC.
                 * - Keep AVERAGE single-threaded only if it runs (its loop is not jMetal-parallelized).
                 * - Split the remaining worker cores between BEST and WORST.
                 *   If only one of BEST/WORST runs, it receives the whole pool.
                 *   If AVERAGE is disabled, its core is returned to BEST/WORST. */
                val totalCores = Runtime.getRuntime().availableProcessors()
                val reserveForSystem = 1
                val coresForWork = (totalCores - reserveForSystem).coerceAtLeast(1)
                val threadsForAverage = if (runAverage) 1 else 0
                val bwPool = (coresForWork - threadsForAverage).coerceAtLeast(1)

                var threadsForBest = 0
                var threadsForWorst = 0
                if (runBest && runWorst) {
                    threadsForBest = bwPool / 2
                    threadsForWorst = bwPool - threadsForBest
                    /* Ensure both get at least 1 when both enabled. */
                    if (threadsForBest == 0) {
                        threadsForBest = 1; threadsForWorst = (bwPool - 1).coerceAtLeast(0)
                    }
                    if (threadsForWorst == 0) {
                        threadsForWorst = 1; threadsForBest = (bwPool - 1).coerceAtLeast(0)
                    }
                } else if (runBest) {
                    threadsForBest = bwPool
                } else if (runWorst) {
                    threadsForWorst = bwPool
                }

                /* Apply per-run evaluator threads. (AVERAGE stays single-threaded when enabled.) */
                bestParams = bestParams.copy(evaluatorThreads = threadsForBest)
                worstParams = worstParams.copy(evaluatorThreads = threadsForWorst)
                averageParams = averageParams.copy(evaluatorThreads = threadsForAverage)

                /* Log the computed budget for traceability. */
                logger.info(
                    "Thread budget → BEST={}, WORST={}, AVERAGE={}, totalCores={}",
                    threadsForBest, threadsForWorst, threadsForAverage, totalCores
                )

                runBlocking {
                    supervisorScope {
                        val counters = intArrayOf(0, 0, 0) /* fun/var, top, done */
                        val t0 = System.nanoTime()

                        /* Single printer on IO dispatcher */
                        val printerJob = launch(Dispatchers.IO) {
                            for (event in progressEvents) {
                                val model =
                                    if (parameters.targetToAchieve == Constants.TARGET_ALL)
                                        models.first { it.targetToAchieve == event.target }
                                    else models[0]
                                when (event) {
                                    is CardinalityResult -> {
                                        view.appendCardinality(model, event); counters[0]++
                                    }

                                    is TopKReplaceBatch -> {
                                        view.replaceTopBatch(model, event.blocks); counters[1]++
                                    }

                                    is RunCompleted -> {
                                        counters[2]++
                                        logger.info(
                                            "RunCompleted received for target {} → closing streams ({} fun/var appends, {} top batches, {} ms).",
                                            event.target, counters[0], counters[1], (System.nanoTime() - t0) / 1_000_000
                                        )
                                        view.closeStreams(model) /* Parquet write happens here */
                                    }
                                }
                                /* Light heartbeat each ~10k events */
                                val total = counters[0] + counters[1] + counters[2]
                                if (total % 10_000 == 0 && total > 0) {
                                    logger.info("heartbeat events: funVar={}, top={}, done={}", counters[0], counters[1], counters[2])
                                }
                            }
                        }

                        /* Launch jobs, each bound to ONE coroutine worker.
                         * Inside each job, jMetal uses its own thread pool (evaluatorThreads). */
                        val jobs = mutableListOf<Job>()
                        if (runBest) jobs += launch(Dispatchers.Default.limitedParallelism(1)) { models[0].solve(bestParams, progressEvents) }
                        if (runWorst) jobs += launch(Dispatchers.Default.limitedParallelism(1)) { models[1].solve(worstParams, progressEvents) }
                        if (runAverage) jobs += launch(Dispatchers.Default.limitedParallelism(1)) { models[2].solve(averageParams, progressEvents) }

                        jobs.joinAll()
                        progressEvents.close()
                        printerJob.join()
                    }
                }
            }


            /* ---- Collect result paths and aggregate/info for TARGET_ALL ---- */
            aggregatedDataResultPaths.add(view.getAggregatedDataFilePath(models[0], isTargetAll = true))
            models.forEach { model ->
                functionValuesResultPaths.add(view.getFunctionValuesFilePath(model))
                variableValuesResultPaths.add(view.getVariableValuesFilePath(model))
                if (model.targetToAchieve != Constants.TARGET_AVERAGE)
                    topSolutionsResultPaths.add(view.getTopSolutionsFilePath(model))
            }
            infoResultPaths.add(view.getInfoFilePath(models[0], isTargetAll = true))

            logger.info("Data aggregation started.")
            val aggregatedRows = aggregate(models)
            view.writeCsv(aggregatedRows, view.getAggregatedDataFilePath(models[0], isTargetAll = true))
            view.writeParquet(aggregatedRows, view.getAggregatedDataParquetPath(models[0], isTargetAll = true))
            logger.info("Aggregated data available at:")
            logger.info("\"${view.getAggregatedDataFilePath(models[0], isTargetAll = true)}\"")

            logger.info("Execution information gathering started.")
            val infoRows = info(models)
            view.writeCsv(infoRows, view.getInfoFilePath(models[0], isTargetAll = true))
            view.writeParquet(infoRows, view.getInfoParquetPath(models[0], isTargetAll = true))
            logger.info("Execution information available at:")
            logger.info("\"${view.getInfoFilePath(models[0], isTargetAll = true)}\"")

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

            /* Final cleanup — drop per-run caches now that tables exist on disk. */
            models.forEach { m ->
                m.clearPercentiles()
                m.clearAfterSerialization()
            }

        } else {
            /* ---------------- Single target ---------------- */
            val estimatedEvents = (models[0].numberOfTopics / 3) + 64
            val channelCapacity = estimatedEvents.coerceAtLeast(64)
            val progressEvents: Channel<ProgressEvent> = Channel(channelCapacity)

            if (parameters.deterministic) {
                /* Sequential + seeded solve */
                runBlocking {
                    supervisorScope {
                        val printerJob = launch(Dispatchers.IO) {
                            for (event in progressEvents) {
                                val model = models[0]
                                when (event) {
                                    is CardinalityResult -> view.appendCardinality(model, event)
                                    is TopKReplaceBatch -> view.replaceTopBatch(model, event.blocks)
                                    is RunCompleted -> view.closeStreams(model)
                                }
                            }
                        }

                        val label = parameters.targetToAchieve.uppercase(Locale.ROOT)
                        RandomBridge.withSeed(RandomBridge.childSeed(label)) {
                            models[0].solve(parameters, progressEvents)
                        }

                        progressEvents.close()
                        printerJob.join()
                    }
                }

            } else {
                /* Non-deterministic single target:
                 * If caller didn’t set evaluatorThreads, give the run all cores explicitly. */
                val tunedParams =
                    if (parameters.evaluatorThreads > 0) parameters
                    else parameters.copy(evaluatorThreads = Runtime.getRuntime().availableProcessors().coerceAtLeast(2))

                runBlocking {
                    supervisorScope {
                        val printerJob = launch(Dispatchers.IO) {
                            for (event in progressEvents) {
                                val model = models[0]
                                when (event) {
                                    is CardinalityResult -> view.appendCardinality(model, event)
                                    is TopKReplaceBatch -> view.replaceTopBatch(model, event.blocks)
                                    is RunCompleted -> view.closeStreams(model)
                                }
                            }
                        }

                        /* One coroutine worker; evaluator pool handles parallel work. */
                        val job = launch(Dispatchers.Default.limitedParallelism(1)) { models[0].solve(tunedParams, progressEvents) }
                        job.join()
                        progressEvents.close()
                        printerJob.join()
                    }
                }
            }

            /* Collect paths (files already appended by the printer) */
            aggregatedDataResultPaths.add(view.getAggregatedDataFilePath(models[0], isTargetAll = false))
            functionValuesResultPaths.add(view.getFunctionValuesFilePath(models[0]))
            variableValuesResultPaths.add(view.getVariableValuesFilePath(models[0]))
            if (models[0].targetToAchieve != Constants.TARGET_AVERAGE)
                topSolutionsResultPaths.add(view.getTopSolutionsFilePath(models[0]))
            infoResultPaths.add(view.getInfoFilePath(models[0], isTargetAll = false))

            logger.info("Data aggregation started.")
            val aggregatedRows = aggregate(models)
            view.writeCsv(aggregatedRows, view.getAggregatedDataFilePath(models[0], isTargetAll = false))
            view.writeParquet(aggregatedRows, view.getAggregatedDataParquetPath(models[0], isTargetAll = false))

            logger.info("Execution information gathering started.")
            val infoRows = info(models)
            view.writeCsv(infoRows, view.getInfoFilePath(models[0], isTargetAll = false))
            view.writeParquet(infoRows, view.getInfoParquetPath(models[0], isTargetAll = false))
            logger.info("Execution information available at:")
            logger.info("\"${view.getInfoFilePath(models[0], isTargetAll = false)}\"")

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

            /* Final cleanup for single target. */
            models.forEach { m ->
                m.clearPercentiles()
                m.clearAfterSerialization()
            }
        }

        logger.info("Execution information gathering completed.")
        logger.info("Data aggregation completed.")
        logger.info("Problem resolution completed.")
    }


    /* ------------------------ Aggregation and info ------------------------ */

    /**
     * Build the **Aggregated** table for the given list of models.
     *
     * Header: `"Cardinality" + one column per model target + one column per AVERAGE percentile + "BestTopicsB64","WorstTopicsB64"`
     *
     * - Correlations per target are taken from each model’s per-K caches.
     * - Percentiles come from the AVERAGE model, if present.
     * - The Best/Worst topic masks are pulled from the respective models as `"B64:<...>"`.
     *
     * @param datasetModels One or more models (when TARGET_ALL, three models are expected).
     * @return Rows including header (first row) and per-K data.
     * @throws IllegalArgumentException if [datasetModels] is empty.
     */
    private fun aggregate(datasetModels: List<DatasetModel>): List<Array<String>> {
        /* ---------- validate input ---------- */
        require(datasetModels.isNotEmpty()) { "aggregate(datasetModels): input list must not be empty" }

        /* ---------- locals & column layout ---------- */
        val referenceModel = datasetModels.first()
        val maximumCardinality = referenceModel.numberOfTopics

        /* Extract percentiles from the AVERAGE model if present */
        val averageModelPercentiles: Map<Int, List<Double>> =
            datasetModels.firstOrNull { it.targetToAchieve == Constants.TARGET_AVERAGE }?.percentiles ?: emptyMap()
        val sortedPercentileKeys: List<Int> = averageModelPercentiles.keys.sorted()

        /* Header layout: [Cardinality] + one col per model target + one col per percentile + compact presence strings */
        val numberOfTargets = datasetModels.size
        val totalHeaderColumns = 1 + numberOfTargets + sortedPercentileKeys.size + 2  /* +2: BestTopicsB64, WorstTopicsB64 */

        /* Stable decimal formatter with six digits after the decimal point */
        val decimalFormatter = java.text.DecimalFormat("0.000000", java.text.DecimalFormatSymbols(Locale.ROOT))
        fun formatSixDecimals(value: Double?): String =
            if (value == null) Constants.CARDINALITY_NOT_AVAILABLE else decimalFormatter.format(value)

        /* ---------- build result container ---------- */
        val aggregatedRows = ArrayList<Array<String>>(1 + maximumCardinality)

        /* ---------- header row ---------- */
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

        /* ---------- coverage counters ---------- */
        val computedCardinalityByTarget = mutableMapOf(
            Constants.TARGET_BEST to 0,
            Constants.TARGET_WORST to 0,
            Constants.TARGET_AVERAGE to 0
        )

        /* ---------- cached model refs for compact masks ---------- */
        val bestModel = datasetModels.firstOrNull { it.targetToAchieve == Constants.TARGET_BEST }
        val worstModel = datasetModels.firstOrNull { it.targetToAchieve == Constants.TARGET_WORST }

        /* ---------- main rows: one per cardinality (1..N) ---------- */
        for (cardinalityIndex in 0 until maximumCardinality) {
            val cardinality = cardinalityIndex + 1
            val cardinalityAsDouble = cardinality.toDouble()

            val row = Array(totalHeaderColumns) { "" }
            var columnIndex = 0
            row[columnIndex++] = cardinality.toString()

            /* per-model correlations */
            datasetModels.forEach { model ->
                val correlation = model.findCorrelationForCardinality(cardinalityAsDouble)
                row[columnIndex] = formatSixDecimals(correlation)
                if (correlation != null) {
                    computedCardinalityByTarget[model.targetToAchieve] =
                        (computedCardinalityByTarget[model.targetToAchieve] ?: 0) + 1
                }
                columnIndex++
            }

            /* percentiles (from AVERAGE model) */
            sortedPercentileKeys.forEach { percentile ->
                val valuesForPercentile = averageModelPercentiles[percentile] ?: emptyList()
                val percentileValue = valuesForPercentile.getOrNull(cardinalityIndex)
                row[columnIndex++] = formatSixDecimals(percentileValue)
            }

            /* compact masks: BEST and WORST, using DatasetModel helpers */
            row[columnIndex++] = bestModel?.retrieveMaskB64ForCardinality(cardinalityAsDouble) ?: "B64:"
            row[columnIndex++] = worstModel?.retrieveMaskB64ForCardinality(cardinalityAsDouble) ?: "B64:"

            aggregatedRows += row
        }

        /* ---------- logging summary ---------- */
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
     * Build the **Info** table listing execution parameters and timing for each model.
     *
     * Columns (multicore-aware):
     * "Dataset Name","Number of Systems","Number of Topics","Correlation Method",
     * "Target to Achieve","Number of Iterations","Population Size","Number of Repetitions",
     * "Evaluator Threads (effective)","Parallel Evaluator","CPU Logical Cores",
     * "Computing Time (ms)"
     *
     * Notes:
     * - "Evaluator Threads (effective)" reflects the **actual** threads used per model (`model.evaluatorThreadsUsed`).
     * - "Parallel Evaluator" is "YES" iff `evaluatorThreadsUsed > 1`, else "NO".
     * - "Computing Time (ms)" is the wall-clock runtime captured by each model.
     *
     * @param models The models to summarize (one row per model).
     * @return Rows including header (first row).
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
     * Merge results from multiple executions (ex1..exN) into **merged** CSV/Parquet artifacts:
     *
     * - Aggregated (pick best-of/best and worst-of/worst per K, same logic as before).
     * - FUN/VAR per target (take the execution that produced the chosen aggregated value).
     * - TOP per target (copy rows matching the chosen execution per K).
     * - Info (sum "Computing Time (ms)" per target; keep other fields from the first occurrence).
     *
     * The method is **schema-aware** for INFO: it locates the "Computing Time (ms)"
     * column by header name (falls back to the last column if not found) so older
     * runs still merge correctly. It also reads the "Target to Achieve" column to sum
     * per-target time across executions when merging `ALL`.
     *
     * Also **cleans** the original per-execution CSV/Parquet files after writing merged outputs.
     *
     * @param numberOfExecutions Number of executions to merge (prefix of collected path lists).
     */
    fun merge(numberOfExecutions: Int) {
        logger.info("Starting to merge results of $numberOfExecutions executions.")

        /* ----------------- tiny helpers ----------------- */

        fun readAllCsvRows(path: String): List<Array<String>> =
            try {
                CSVReader(FileReader(path)).use { it.readAll() ?: emptyList() }
            } catch (e: Exception) {
                logger.warn("Failed to read CSV at '$path': ${e.message}")
                emptyList()
            }

        fun readAllLines(path: String): List<String> =
            try {
                Files.readAllLines(Paths.get(path)).filter { it.isNotBlank() }
            } catch (e: Exception) {
                logger.warn("Failed to read text at '$path': ${e.message}")
                emptyList()
            }

        fun headerOrEmpty(rows: List<Array<String>>): Array<String> =
            if (rows.isNotEmpty()) rows.first() else emptyArray()

        fun dataRows(rows: List<Array<String>>): List<Array<String>> =
            if (rows.size > 1) rows.drop(1) else emptyList()

        fun minDataLen(tables: List<List<Array<String>>>): Int =
            tables.minOfOrNull { (if (it.isNotEmpty()) it.size - 1 else 0) } ?: 0

        fun parseDoubleOrNull(s: String): Double? = runCatching { s.trim().toDouble() }.getOrNull()

        /* ----------------- 1) aggregated tables ----------------- */

        logger.info("Loading aggregated data for all executions.")
        logger.info("Aggregated data paths:")
        aggregatedDataResultPaths.take(numberOfExecutions).forEach { logger.info("\"$it\"") }

        val aggregatedTables = aggregatedDataResultPaths
            .take(numberOfExecutions)
            .map { readAllCsvRows(it) }

        if (aggregatedTables.any { it.isEmpty() }) {
            logger.warn("One or more aggregated CSV files are empty; aborting merge to avoid partial output.")
            return
        }

        val aggregatedHeader = headerOrEmpty(aggregatedTables.first())
        val aggregatedDataRowsPerExec = aggregatedTables.map { dataRows(it) }
        val alignedCardinalityCount = minDataLen(aggregatedTables)

        val loggingFactor = maxOf(1, (alignedCardinalityCount * Constants.LOGGING_FACTOR) / 100)
        var progressCounter = 0

        /* Identify mask column indices (if present). */
        val idxBestTopics = aggregatedHeader.indexOf("BestTopicsB64").takeIf { it >= 0 }
        val idxWorstTopics = aggregatedHeader.indexOf("WorstTopicsB64").takeIf { it >= 0 }

        /* ----------------- 2) per-target per-exec (FUN/VAR/TOP) ----------------- */

        var bestFunctionValues: LinkedList<LinkedList<String>>? = null
        var bestVariableValues: LinkedList<LinkedList<String>>? = null
        var bestTopSolutions: LinkedList<LinkedList<String>>? = null
        var worstFunctionValues: LinkedList<LinkedList<String>>? = null
        var worstVariableValues: LinkedList<LinkedList<String>>? = null
        var worstTopSolutions: LinkedList<LinkedList<String>>? = null
        var averageFunctionValues: LinkedList<LinkedList<String>>? = null
        var averageVariableValues: LinkedList<LinkedList<String>>? = null

        fun prepareExecListsFor(model: DatasetModel) {

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

            val funTables: List<List<String>> = execSlicePaths(funPaths, numberOfExecutions, "FUN")
                .map { readAllLines(it) }
                .map { it.take(alignedCardinalityCount) }

            val varTables: List<List<String>> = execSlicePaths(varPaths, numberOfExecutions, "VAR")
                .map { readAllLines(it) }
                .map { it.take(alignedCardinalityCount) }

            val topTables: List<List<String>> =
                if (model.targetToAchieve != Constants.TARGET_AVERAGE)
                    execSlicePaths(topPaths, numberOfExecutions, "TOP")
                        .map { readAllLines(it).drop(1) } /* drop header */
                else emptyList()

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

        /* ----------------- 3) merge Aggregated + pick per-target rows ----------------- */

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

            val rowsAtI: List<Array<String>> =
                aggregatedDataRowsPerExec.mapNotNull { execRows -> execRows.getOrNull(cardIdx) }
            if (rowsAtI.isEmpty()) continue

            val cardinalityToken = rowsAtI.first()[0]

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

                    Constants.TARGET_AVERAGE -> { /* copy as is */
                    }
                }
            }

            val template = rowsAtI.first()
            val out = Array(template.size) { "" }
            out[0] = cardinalityToken
            when (targetToAchieve) {
                Constants.TARGET_ALL -> {
                    out[1] = if (bestExecIdx >= 0) bestCorr.toString() else Constants.CARDINALITY_NOT_AVAILABLE
                    out[2] = if (worstExecIdx >= 0) worstCorr.toString() else Constants.CARDINALITY_NOT_AVAILABLE
                    for (col in 3 until template.size) out[col] = template[col]
                    /* Masks come from the chosen exec row(s) */
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
                    val src = rowsAtI.first()
                    for (col in src.indices) out[col] = src[col]
                }
            }
            mergedAggregatedData.add(out)

            /* Stash per-target selections (FUN/VAR/TOP) with null/size guards */
            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_BEST) {
                bestFunctionValues?.let { m ->
                    if (cardIdx < m.size) {
                        val execCol = if (bestExecIdx >= 0) bestExecIdx.coerceAtMost(m[cardIdx].size - 1) else 0
                        m[cardIdx].getOrNull(execCol)?.let { mergedBestFunctionValues += it }
                    }
                }
                bestVariableValues?.let { m ->
                    if (cardIdx < m.size) {
                        val execCol = if (bestExecIdx >= 0) bestExecIdx.coerceAtMost(m[cardIdx].size - 1) else 0
                        m[cardIdx].getOrNull(execCol)?.let { mergedBestVariableValues += it }
                    }
                }
                bestTopSolutions?.forEach { perExecLineSet ->
                    val line = perExecLineSet.getOrNull(bestExecIdx) ?: return@forEach
                    val kToken = line.substringBefore(',').trim()
                    if (kToken == (cardIdx + 1).toString()) mergedBestTopSolutions += line
                }
            }

            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_WORST) {
                worstFunctionValues?.let { m ->
                    if (cardIdx < m.size) {
                        val execCol = if (worstExecIdx >= 0) worstExecIdx.coerceAtMost(m[cardIdx].size - 1) else 0
                        m[cardIdx].getOrNull(execCol)?.let { mergedWorstFunctionValues += it }
                    }
                }
                worstVariableValues?.let { m ->
                    if (cardIdx < m.size) {
                        val execCol = if (worstExecIdx >= 0) worstExecIdx.coerceAtMost(m[cardIdx].size - 1) else 0
                        m[cardIdx].getOrNull(execCol)?.let { mergedWorstVariableValues += it }
                    }
                }
                worstTopSolutions?.forEach { perExecLineSet ->
                    val line = perExecLineSet.getOrNull(worstExecIdx) ?: return@forEach
                    val kToken = line.substringBefore(',').trim()
                    if (kToken == (cardIdx + 1).toString()) mergedWorstTopSolutions += line
                }
            }

            if (targetToAchieve == Constants.TARGET_ALL || targetToAchieve == Constants.TARGET_AVERAGE) {
                averageFunctionValues?.let { m ->
                    if (cardIdx < m.size) m[cardIdx].firstOrNull()?.let { mergedAverageFunctionValues += it }
                }
                averageVariableValues?.let { m ->
                    if (cardIdx < m.size) m[cardIdx].firstOrNull()?.let { mergedAverageVariableValues += it }
                }
            }
        }

        logger.info("Results merged for cardinality: $alignedCardinalityCount/$alignedCardinalityCount (100%) for $numberOfExecutions total executions.")

        /* ----------------- 4) merge Info (schema-aware on the time/target columns) ----------------- */

        logger.info("Loading info for all executions.")
        logger.info("Info paths:")
        infoResultPaths.take(numberOfExecutions).forEach { logger.info("\"$it\"") }
        val infoTables: List<List<Array<String>>> =
            infoResultPaths.take(numberOfExecutions).map { readAllCsvRows(it) }

        /* Find header; provide a fallback header if missing. */
        val defaultInfoHeader = arrayOf(
            "Dataset Name", "Number of Systems", "Number of Topics", "Correlation Method", "Target to Achieve",
            "Number of Iterations", "Population Size", "Number of Repetitions",
            "Evaluator Threads (effective)", "Parallel Evaluator", "CPU Logical Cores",
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
            return if (exact >= 0) exact else 4.coerceIn(0, header.lastIndex) /* best-effort */
        }

        val timeColIndexGlobal = findTimeColumnIndex(infoHeader)
        val targetColIndexGlobal = findTargetColumnIndex(infoHeader)

        val mergedInfoLines = LinkedList<Array<String>>().apply { add(infoHeader) }

        if (targetToAchieve == Constants.TARGET_ALL) {
            /* Sum times per target across executions; keep a template row per target from the first occurrence. */
            val sumByTarget = mutableMapOf(
                Constants.TARGET_BEST to 0L,
                Constants.TARGET_WORST to 0L,
                Constants.TARGET_AVERAGE to 0L
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
            /* Single-target merge: sum the time across executions */
            val totalTime = infoTables.sumOf { table ->
                if (table.size <= 1) 0L
                else {
                    val hdr = table.first()
                    val timeIdx = findTimeColumnIndex(hdr)
                    table.drop(1).sumOf { r -> r.getOrNull(timeIdx)?.replace("\"", "")?.trim()?.toLongOrNull() ?: 0L }
                }
            }
            val firstRow = infoTables.firstOrNull()?.getOrNull(1) ?: emptyArray()
            /* Build a NON-NULL copy padded to fit the time column; avoid copyOf(newSize) which yields Array<String?> */
            val targetSize = maxOf(
                firstRow.size,
                infoHeader.size,
                timeColIndexGlobal + 1  /* ensure we can write the time cell */
            )
            val rewritten: Array<String> = Array(targetSize) { "" }
            /* fast copy of existing cells (all non-null) */
            System.arraycopy(firstRow, 0, rewritten, 0, firstRow.size)
            /* write merged time */
            rewritten[timeColIndexGlobal] = totalTime.toString()

            mergedInfoLines.add(rewritten)

        }

        /* ----------------- 5) write merged CSV + Parquet ----------------- */

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

        /* Per target: FUN / VAR / TOP -> write CSV as before + Parquet siblings. */

        fun writeMergedPerTarget(model: DatasetModel) {
            /* ---- CSV ---- */

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

            /* TOP CSV (no AVERAGE) */
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                val topMergedCsv = view.getTopSolutionsMergedFilePath(model)
                java.nio.file.Files.newBufferedWriter(java.nio.file.Paths.get(topMergedCsv)).use { w ->
                    w.appendLine("Cardinality,Correlation,TopicsB64")
                    val lines = if (model.targetToAchieve == Constants.TARGET_BEST) mergedBestTopSolutions else mergedWorstTopSolutions
                    lines.forEach { ln -> w.appendLine(ln) }
                }
            }

            /* ---- Parquet ---- */

            /* FUN Parquet (header: K,Correlation) — robust split on comma **or** whitespace */
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

            /* VAR Parquet (header: K,Topics) – K inferred from row index (1..N) */
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

            /* TOP Parquet (header: K,Correlation,TopicsB64) */
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

        /* ----------------- 6) cleanup non-merged CSV + Parquet ----------------- */

        /* Snapshot the original CSV path lists BEFORE mutating them */
        val snapAggCsv = aggregatedDataResultPaths.toList()
        val snapFunCsv = functionValuesResultPaths.toList()
        val snapVarCsv = variableValuesResultPaths.toList()
        val snapTopCsv = topSolutionsResultPaths.toList()
        val snapInfoCsv = infoResultPaths.toList()

        /* CSV cleanup */
        logger.info("Cleaning of not merged CSV results started.")
        clean(aggregatedDataResultPaths, "Cleaning aggregated CSV data at paths:")
        clean(functionValuesResultPaths, "Cleaning function values CSV at paths:")
        clean(variableValuesResultPaths, "Cleaning variable values CSV at paths:")
        clean(topSolutionsResultPaths, "Cleaning top solutions CSV at paths:")
        clean(infoResultPaths, "Cleaning info CSV at paths:")
        logger.info("Cleaning of not merged CSV results completed.")

        /* Build Parquet lists from the SNAPSHOTS (so we still have ex1/ex2) */
        fun mapCsvToParquet(path: String): String =
            path
                .replace("${Constants.PATH_SEPARATOR}CSV${Constants.PATH_SEPARATOR}", "${Constants.PATH_SEPARATOR}Parquet${Constants.PATH_SEPARATOR}")
                .replace(".csv", ".parquet")

        val aggParquetToClean = snapAggCsv.map(::mapCsvToParquet).toMutableList()
        val funParquetToClean = snapFunCsv.map(::mapCsvToParquet).toMutableList()
        val varParquetToClean = snapVarCsv.map(::mapCsvToParquet).toMutableList()
        val topParquetToClean = snapTopCsv.map(::mapCsvToParquet).toMutableList()
        val infoParquetToClean = snapInfoCsv.map(::mapCsvToParquet).toMutableList()

        /* Parquet cleanup */
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

        /* Destination subfolders mirroring our writers */
        val csvDestDir = Paths.get("$inputRootPath${Constants.PATH_SEPARATOR}CSV")
        val parquetDestDir = Paths.get("$inputRootPath${Constants.PATH_SEPARATOR}Parquet")
        if (!Files.exists(csvDestDir)) Files.createDirectories(csvDestDir)
        if (!Files.exists(parquetDestDir)) Files.createDirectories(parquetDestDir)

        /* Small helper: copy if exists, preserve filename, overwrite */
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

        /* ---------- Per-execution CSV artifacts (from collected path lists) ---------- */
        val csvListCopier = { dataList: MutableList<String>, logMessage: String ->
            if (dataList.isNotEmpty()) logger.info(logMessage)
            dataList.forEach { src -> copyIfExists(src, csvDestDir) }
        }

        csvListCopier(aggregatedDataResultPaths, "Aggregated data (CSV) copy started from paths:")
        csvListCopier(functionValuesResultPaths, "Function values (CSV) copy started from paths:")
        csvListCopier(variableValuesResultPaths, "Variable values (CSV) copy started from paths:")
        csvListCopier(topSolutionsResultPaths, "Top solutions (CSV) copy started from paths:")
        csvListCopier(infoResultPaths, "Info (CSV) copy started from paths:")

        /* ---------- Per-execution Parquet artifacts (computed from models) ---------- */
        models.forEach { model ->
            /* Aggregated/Info: ALL vs single-target follow your solve() logic */
            copyIfExists(
                view.getAggregatedDataParquetPath(model, isTargetAll = (targetToAchieve == Constants.TARGET_ALL)),
                parquetDestDir
            )
            copyIfExists(view.getFunctionValuesParquetPath(model), parquetDestDir)
            copyIfExists(view.getVariableValuesParquetPath(model), parquetDestDir)
            if (model.targetToAchieve != Constants.TARGET_AVERAGE) {
                copyIfExists(view.getTopSolutionsParquetPath(model), parquetDestDir)
            }
            copyIfExists(
                view.getInfoParquetPath(model, isTargetAll = (targetToAchieve == Constants.TARGET_ALL)),
                parquetDestDir
            )
        }

        /* ---------- Merged CSV artifacts ---------- */

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

        /* ---------- Merged Parquet artifacts ---------- */
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
