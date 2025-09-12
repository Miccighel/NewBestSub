package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.dataset.model.CardinalityResult
import it.uniud.newbestsub.dataset.view.CSVView
import it.uniud.newbestsub.dataset.view.ParquetView
import it.uniud.newbestsub.utils.Constants
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Locale
import kotlin.io.path.exists

/**
 * Focused tests for NATURAL vs INTERNAL correlation as observed by the views.
 *
 * We verify:
 * • Views treat incoming correlations as NATURAL (no sign flips).
 * • CSVView emits FUN/VAR/TOP exactly as will be on disk (with final rewrite ordering).
 * • ParquetView code paths execute (smoke) without reflecting internals.
 *
 * Extra: we print an explanation for which line to pick from FUN/TOP and why:
 * • FUN (after final rewrite):
 *     BEST  → file is (K asc, corr asc)     → pick LAST line per K (max).
 *     WORST → file is (K asc, corr desc)    → pick LAST line per K (min).
 * • TOP (as provided to the view, already target-ordered):
 *     BEST  → descending per K              → pick FIRST line per K (max).
 *     WORST → ascending per K               → pick FIRST line per K (min).
 */
@DisplayName("NaturalCorrelation – CSV golden + Parquet buffer (with selection explanations)")
class NaturalCorrelationViewsTest {

    /*
     * CSV – FUN/VAR: out-of-order appends -> final rewrite; print selection rule (BEST)
     */
    @Test
    @DisplayName("CSVView FUN/VAR: global sort + selection explanation (BEST)")
    fun testCsvFunVarGlobalSortWithExplanation_Best() {
        /** Exercise the final rewrite logic. */
        System.setProperty("nbs.csv.finalRewrite", "true")

        println("[NaturalCorrelation/CSV] - Test begins (FUN/VAR, BEST).")

        val model = stubModel(
            datasetName = "DS-CSV-FUNVAR-BEST",
            targetToAchieve = Constants.TARGET_BEST,
            currentExecution = 111
        )
        val view = CSVView()

        val funPath = Paths.get(view.getFunctionValuesFilePath(model))
        val varPath = Paths.get(view.getVariableValuesFilePath(model))
        deleteIfExists(funPath); deleteIfExists(varPath)

        /**
         * Emit a single (K, corr, VAR) row into the CSV view.
         * Correlation is on the NATURAL scale and will be globally sorted on close.
         */
        fun emit(cardinality: Int, correlation: Double, variableCsvRaw: String) {
            val corrStr = String.format(Locale.ROOT, "%.6f", correlation)
            val ev = CardinalityResult(
                target = model.targetToAchieve,
                threadName = "t",
                cardinality = cardinality,
                correlation = correlation,
                functionValuesCsvLine = "$cardinality,$corrStr",
                variableValuesCsvLine = variableCsvRaw
            )
            view.onAppendCardinality(model, ev)
            println("[NaturalCorrelation/CSV] - append FUN/VAR -> K=$cardinality corr=$corrStr")
        }

        /** Deliberately out of order; final rewrite will sort (K asc, corr asc) for BEST. */
        emit(2, 0.70, "1 1 0 0")
        emit(1, 0.90, "1 0 0 0")
        emit(2, 0.60, "0 1 0 0")
        emit(1, 0.80, "1 1 1 0")

        view.closeStreams(model)
        println("[NaturalCorrelation/CSV] - closeStreams -> global FUN/VAR sort (K asc, corr asc) [BEST]")

        assertTrue(funPath.exists() && varPath.exists(), "FUN/VAR files should exist")

        val funLines = Files.readAllLines(funPath).filter { it.isNotBlank() }
        println("[NaturalCorrelation/CSV] - FUN lines (BEST):")
        funLines.forEach { println("  $it") }
        explainFunSelection(model.targetToAchieve, funLines, tag = "Explain/FUN-BEST")

        val varLines = Files.readAllLines(varPath).filter { it.isNotBlank() }
        println("[NaturalCorrelation/CSV] - VAR lines (BEST, first 4 shown):")
        varLines.take(4).forEach { println("  $it") }

        /** Minimal sanity checks for line counts and B64 prefix. */
        assertEquals(4, funLines.size)
        assertEquals(4, varLines.size)
        assertTrue(varLines.all { it.startsWith("B64:") })

        println("[NaturalCorrelation/CSV] - Test ends (FUN/VAR, BEST).")
    }

    /*
     * CSV – FUN/VAR: out-of-order appends -> final rewrite; print selection rule (WORST)
     */
    @Test
    @DisplayName("CSVView FUN/VAR: global sort + selection explanation (WORST)")
    fun testCsvFunVarGlobalSortWithExplanation_Worst() {
        /** Exercise the final rewrite logic. */
        System.setProperty("nbs.csv.finalRewrite", "true")

        println("[NaturalCorrelation/CSV] - Test begins (FUN/VAR, WORST).")

        val model = stubModel(
            datasetName = "DS-CSV-FUNVAR-WORST",
            targetToAchieve = Constants.TARGET_WORST,
            currentExecution = 112
        )
        val view = CSVView()

        val funPath = Paths.get(view.getFunctionValuesFilePath(model))
        val varPath = Paths.get(view.getVariableValuesFilePath(model))
        deleteIfExists(funPath); deleteIfExists(varPath)

        /**
         * Emit a single (K, corr, VAR) row into the CSV view.
         * Correlation is on the NATURAL scale and will be globally sorted on close.
         */
        fun emit(cardinality: Int, correlation: Double, variableCsvRaw: String) {
            val corrStr = String.format(Locale.ROOT, "%.6f", correlation)
            val ev = CardinalityResult(
                target = model.targetToAchieve,
                threadName = "t",
                cardinality = cardinality,
                correlation = correlation,
                functionValuesCsvLine = "$cardinality,$corrStr",
                variableValuesCsvLine = variableCsvRaw
            )
            view.onAppendCardinality(model, ev)
            println("[NaturalCorrelation/CSV] - append FUN/VAR -> K=$cardinality corr=$corrStr")
        }

        /** Deliberately out of order; final rewrite will sort (K asc, corr desc) for WORST. */
        emit(2, 0.70, "1 1 0 0")
        emit(1, 0.90, "1 0 0 0")
        emit(2, 0.60, "0 1 0 0")
        emit(1, 0.80, "1 1 1 0")

        view.closeStreams(model)
        println("[NaturalCorrelation/CSV] - closeStreams -> global FUN/VAR sort (K asc, corr desc) [WORST]")

        assertTrue(funPath.exists() && varPath.exists(), "FUN/VAR files should exist")

        val funLines = Files.readAllLines(funPath).filter { it.isNotBlank() }
        println("[NaturalCorrelation/CSV] - FUN lines (WORST):")
        funLines.forEach { println("  $it") }
        explainFunSelection(model.targetToAchieve, funLines, tag = "Explain/FUN-WORST")

        val varLines = Files.readAllLines(varPath).filter { it.isNotBlank() }
        println("[NaturalCorrelation/CSV] - VAR lines (WORST, first 4 shown):")
        varLines.take(4).forEach { println("  $it") }

        /** Minimal sanity checks for line counts and B64 prefix. */
        assertEquals(4, funLines.size)
        assertEquals(4, varLines.size)
        assertTrue(varLines.all { it.startsWith("B64:") })

        println("[NaturalCorrelation/CSV] - Test ends (FUN/VAR, WORST).")
    }

    /*
     * CSV – FUN/VAR: WORST with NEGATIVE correlations (ordering + selection)
     */
    @Test
    @DisplayName("CSVView FUN/VAR: WORST with negative values → file is desc, pick LAST = min (possibly negative)")
    fun testCsvFunVarGlobalSortWithExplanation_Worst_Negatives() {
        System.setProperty("nbs.csv.finalRewrite", "true")

        println("[NaturalCorrelation/CSV] - Test begins (FUN/VAR, WORST with negatives).")

        val model = stubModel(
            datasetName = "DS-CSV-FUNVAR-WORST-NEG",
            targetToAchieve = Constants.TARGET_WORST,
            currentExecution = 113
        )
        val view = CSVView()

        val funPath = Paths.get(view.getFunctionValuesFilePath(model))
        val varPath = Paths.get(view.getVariableValuesFilePath(model))
        deleteIfExists(funPath); deleteIfExists(varPath)

        /**
         * Emit a single (K, corr, VAR) row into the CSV view.
         * Correlation is on the NATURAL scale and will be globally sorted on close.
         */
        fun emit(cardinality: Int, correlation: Double, variableCsvRaw: String) {
            val corrStr = String.format(Locale.ROOT, "%.6f", correlation)
            val ev = CardinalityResult(
                target = model.targetToAchieve,
                threadName = "t",
                cardinality = cardinality,
                correlation = correlation,
                functionValuesCsvLine = "$cardinality,$corrStr",
                variableValuesCsvLine = variableCsvRaw
            )
            view.onAppendCardinality(model, ev)
            println("[NaturalCorrelation/CSV] - append FUN/VAR -> K=$cardinality corr=$corrStr")
        }

        /**
         * Mix positives and negatives; WORST final file sorts desc per K, and we pick LAST (min) per K.
         * K=1 group: 0.10, -0.30  → file order: 0.10, -0.30  → pick LAST = -0.30
         * K=2 group: 0.05, -0.20  → file order: 0.05, -0.20  → pick LAST = -0.20
         */
        emit(1, 0.10, "1 0 0 0")
        emit(2, 0.05, "0 1 0 0")
        emit(1, -0.30, "1 1 0 0")
        emit(2, -0.20, "0 0 1 0")

        view.closeStreams(model)
        println("[NaturalCorrelation/CSV] - closeStreams -> global FUN/VAR sort (K asc, corr desc) [WORST-NEG]")

        assertTrue(funPath.exists() && varPath.exists(), "FUN/VAR files should exist")

        val funLines = Files.readAllLines(funPath).filter { it.isNotBlank() }
        println("[NaturalCorrelation/CSV] - FUN lines (WORST-NEG):")
        funLines.forEach { println("  $it") }
        explainFunSelection(model.targetToAchieve, funLines, tag = "Explain/FUN-WORST-NEG")

        /** Quick check on picked minima per K. */
        val byK = parseFunPairs(funLines)
        assertEquals(-0.30, byK[1]!!.last(), 1e-12)
        assertEquals(-0.20, byK[2]!!.last(), 1e-12)

        val varLines = Files.readAllLines(varPath).filter { it.isNotBlank() }
        assertEquals(4, funLines.size)
        assertEquals(4, varLines.size)
        assertTrue(varLines.all { it.startsWith("B64:") })

        println("[NaturalCorrelation/CSV] - Test ends (FUN/VAR, WORST with negatives).")
    }

    /*
     * CSV – TOP: batch replace semantics; print selection rule (WORST)
     */
    @Test
    @DisplayName("CSVView TOP: replace-batch and selection explanation (WORST)")
    fun testCsvTopWriteWithExplanation_Worst() {
        /** Disable live TOP writes; flush once on close. */
        System.setProperty("nbs.csv.top.live", "false")

        println("[NaturalCorrelation/CSV] - Test begins (TOP, WORST).")

        val model = stubModel(
            datasetName = "DS-CSV-TOP-WORST",
            targetToAchieve = Constants.TARGET_WORST,
            currentExecution = 222
        )
        val view = CSVView()

        /**
         * For WORST, provide ascending correlation within each K.
         */
        fun topBlockAsc(cardinality: Int, base: Double): List<String> =
            (0 until 10).map { i -> "$cardinality,${String.format(Locale.ROOT, "%.6f", base + 0.01 * i)},B64:" }

        /** Initial blocks for K=1 and K=2. */
        val batch1 = mapOf(
            1 to topBlockAsc(1, 0.10),
            2 to topBlockAsc(2, 0.20)
        )
        view.onReplaceTopBatch(model, batch1)
        println("[NaturalCorrelation/CSV] - replaceTopBatch -> K=1,2 (initial)")

        /** Replace only K=1 with a new ascending block starting at 0.30. */
        val batch2 = mapOf(1 to topBlockAsc(1, 0.30))
        view.onReplaceTopBatch(model, batch2)
        println("[NaturalCorrelation/CSV] - replaceTopBatch -> K=1 (new content)")

        view.closeStreams(model)
        println("[NaturalCorrelation/CSV] - closeStreams -> write TOP once [WORST, asc]")

        assertTrue(topPathExists(view, model), "TOP file should exist")

        val topLines = readTopLines(view, model)
        println("[NaturalCorrelation/CSV] - TOP lines (WORST, first 6):")
        topLines.take(6).forEach { println("  $it") }

        /** Clarify selection rule: for WORST, pick the FIRST line per K (ascending). */
        val dataRows = topLines.drop(1)
        explainTopSelection(model.targetToAchieve, dataRows, tag = "Explain/TOP-WORST")

        println("[NaturalCorrelation/CSV] - Test ends (TOP, WORST).")
    }

    /*
     * CSV – TOP: WORST with NEGATIVE correlations (ordering + selection)
     */
    @Test
    @DisplayName("CSVView TOP: WORST with negative values → ascending per K, pick FIRST = min (negative)")
    fun testCsvTopWriteWithExplanation_Worst_Negatives() {
        System.setProperty("nbs.csv.top.live", "false")

        println("[NaturalCorrelation/CSV] - Test begins (TOP, WORST with negatives).")

        val model = stubModel(
            datasetName = "DS-CSV-TOP-WORST-NEG",
            targetToAchieve = Constants.TARGET_WORST,
            currentExecution = 224
        )
        val view = CSVView()

        /**
         * Ascending sequences that include negatives:
         * K=1: -0.50, -0.40, …, -0.10 → pick FIRST = -0.50
         * K=2: -0.20, -0.10,  0.00, …, 0.20 → pick FIRST = -0.20
         */
        fun topBlockAscSeries(cardinality: Int, start: Double, step: Double, count: Int): List<String> =
            (0 until count).map { i -> "$cardinality,${String.format(Locale.ROOT, "%.6f", start + step * i)},B64:" }

        val batch = mapOf(
            1 to topBlockAscSeries(1, -0.50, 0.10, 10),
            2 to topBlockAscSeries(2, -0.20, 0.05, 10)
        )
        view.onReplaceTopBatch(model, batch)
        println("[NaturalCorrelation/CSV] - replaceTopBatch -> K=1,2 (negatives, ascending)")

        view.closeStreams(model)
        println("[NaturalCorrelation/CSV] - closeStreams -> write TOP once [WORST-NEG, asc]")

        assertTrue(topPathExists(view, model), "TOP file should exist")

        val topLines = readTopLines(view, model)
        println("[NaturalCorrelation/CSV] - TOP lines (WORST-NEG, first 6):")
        topLines.take(6).forEach { println("  $it") }

        val dataRows = topLines.drop(1)
        explainTopSelection(model.targetToAchieve, dataRows, tag = "Explain/TOP-WORST-NEG")

        /** Sanity: first of each K is the minimum (negative). */
        val byK = parseTopTriples(dataRows)
        assertEquals(-0.50, byK[1]!!.first(), 1e-12)
        assertEquals(-0.20, byK[2]!!.first(), 1e-12)

        println("[NaturalCorrelation/CSV] - Test ends (TOP, WORST with negatives).")
    }

    /*
     * CSV – TOP: batch replace semantics; print selection rule (BEST)
     */
    @Test
    @DisplayName("CSVView TOP: replace-batch and selection explanation (BEST)")
    fun testCsvTopWriteWithExplanation_Best() {
        /** Disable live TOP writes; flush once on close. */
        System.setProperty("nbs.csv.top.live", "false")

        println("[NaturalCorrelation/CSV] - Test begins (TOP, BEST).")

        val model = stubModel(
            datasetName = "DS-CSV-TOP-BEST",
            targetToAchieve = Constants.TARGET_BEST,
            currentExecution = 223
        )
        val view = CSVView()

        /**
         * For BEST, provide descending correlation within each K.
         */
        fun topBlockDesc(cardinality: Int, start: Double): List<String> =
            (0 until 10).map { i -> "$cardinality,${String.format(Locale.ROOT, "%.6f", start - 0.01 * i)},B64:" }

        /** Initial blocks for K=1 and K=2 (descending). */
        val batch1 = mapOf(
            1 to topBlockDesc(1, 0.30),
            2 to topBlockDesc(2, 0.20)
        )
        view.onReplaceTopBatch(model, batch1)
        println("[NaturalCorrelation/CSV] - replaceTopBatch -> K=1,2 (initial, desc)")

        /** Replace only K=1 with a new descending block starting at 0.40. */
        val batch2 = mapOf(1 to topBlockDesc(1, 0.40))
        view.onReplaceTopBatch(model, batch2)
        println("[NaturalCorrelation/CSV] - replaceTopBatch -> K=1 (new content, desc)")

        view.closeStreams(model)
        println("[NaturalCorrelation/CSV] - closeStreams -> write TOP once [BEST, desc]")

        assertTrue(topPathExists(view, model), "TOP file should exist")

        val topLines = readTopLines(view, model)
        println("[NaturalCorrelation/CSV] - TOP lines (BEST, first 6):")
        topLines.take(6).forEach { println("  $it") }

        /** Clarify selection rule: for BEST, pick the FIRST line per K (descending). */
        val dataRows = topLines.drop(1)
        explainTopSelection(model.targetToAchieve, dataRows, tag = "Explain/TOP-BEST")

        println("[NaturalCorrelation/CSV] - Test ends (TOP, BEST).")
    }

    /*
     * Parquet – buffer NATURAL preservation; simple smoke for file paths
     */
    @Test
    @DisplayName("ParquetView.onAppendCardinality keeps NATURAL correlation in buffer (smoke)")
    fun testParquetViewBufferKeepsNaturalCorrelation() {
        println("[NaturalCorrelation/Parquet] - Test begins (buffer).")

        val model = stubModel(
            datasetName = "DS-PARQ",
            targetToAchieve = Constants.TARGET_BEST,
            currentExecution = 1
        )
        val view = ParquetView()

        val ev = CardinalityResult(
            target = model.targetToAchieve,
            threadName = "t",
            cardinality = 5,
            correlation = 0.42,
            functionValuesCsvLine = "5,0.42",
            variableValuesCsvLine = "B64:"
        )

        /** Append one row; ensure code path executes. */
        view.onAppendCardinality(model, ev)
        println("[NaturalCorrelation/Parquet] - onAppendCardinality -> K=5 corr=0.420000")
        println("[NaturalCorrelation/Parquet] - Test ends (buffer).")
    }

    @Test
    @DisplayName("ParquetView smoke: FUN/VAR/TOP calls run without exception")
    fun testParquetFilesSmoke() {
        println("[NaturalCorrelation/Parquet] - Test begins (files).")

        val model = stubModel(
            datasetName = "DS-PARQ-FILES",
            targetToAchieve = Constants.TARGET_BEST,
            currentExecution = 2
        )
        val view = ParquetView()

        fun emit(cardinality: Int, correlation: Double) {
            val corrStr = String.format(Locale.ROOT, "%.6f", correlation)
            val ev = CardinalityResult(
                target = model.targetToAchieve,
                threadName = "t",
                cardinality = cardinality,
                correlation = correlation,
                functionValuesCsvLine = "$cardinality,$corrStr",
                variableValuesCsvLine = "B64:"
            )
            view.onAppendCardinality(model, ev)
            println("[NaturalCorrelation/Parquet] - append FUN/VAR -> K=$cardinality corr=$corrStr")
        }

        emit(2, 0.70); emit(1, 0.90); emit(2, 0.60); emit(1, 0.80)

        fun topBlock(cardinality: Int, base: Double): List<String> =
            (0 until 10).map { i -> "$cardinality,${String.format(Locale.ROOT, "%.6f", base + 0.01 * i)},B64:" }

        view.onReplaceTopBatch(model, mapOf(1 to topBlock(1, 0.30), 2 to topBlock(2, 0.20)))
        println("[NaturalCorrelation/Parquet] - replaceTopBatch -> K=1,2 (initial)")
        view.onReplaceTopBatch(model, mapOf(1 to topBlock(1, 0.40)))
        println("[NaturalCorrelation/Parquet] - replaceTopBatch -> K=1 (new content)")

        /**
         * We do not inspect Parquet output here; the goal is to ensure the code path runs without exceptions.
         */
        view.closeStreams(model)
        println("[NaturalCorrelation/Parquet] - closeStreams -> write FUN/VAR/TOP")
        println("[NaturalCorrelation/Parquet] - Test ends (files).")
    }

    /*
     * Explain/parse helpers
     */

    private fun parseFunPairs(lines: List<String>): Map<Int, List<Double>> {
        val pairs = lines.filter { it.isNotBlank() }.map { line ->
            val parts = line.trim().split(Regex("[,;\\s]+"), limit = 2)
            require(parts.size == 2) { "Bad FUN line: '$line'" }
            parts[0].toInt() to parts[1].toDouble()
        }
        return pairs.groupBy({ it.first }, { it.second })
    }

    private fun parseTopTriples(dataRows: List<String>): Map<Int, List<Double>> {
        val triples = dataRows.filter { it.isNotBlank() }.map { line ->
            val parts = line.split(',', limit = 3)
            require(parts.size >= 2) { "Bad TOP line: '$line'" }
            parts[0].trim().toInt() to parts[1].trim().toDouble()
        }
        return triples.groupBy({ it.first }, { it.second })
    }

    private fun explainFunSelection(target: String, functionLines: List<String>, tag: String = "Explain/FUN") {
        val groups = parseFunPairs(functionLines)
        val isWorst = target == Constants.TARGET_WORST
        if (isWorst) {
            println("[$tag] - Target=WORST -> FUN sorted (K asc, corr desc). Pick LAST line per K (min = best-of-worst).")
        } else {
            println("[$tag] - Target=BEST  -> FUN sorted (K asc, corr asc). Pick LAST line per K (max = best-of-best).")
        }
        val sorted = groups.toSortedMap()
        for ((k, vals) in sorted) {
            val pick = vals.last()
            println("[$tag] - K=$k group=${vals.joinToString(prefix = "[", postfix = "]")} -> pick(last)=$pick")
        }
    }

    private fun explainTopSelection(target: String, topDataRows: List<String>, tag: String = "Explain/TOP") {
        val groups = parseTopTriples(topDataRows)
        val isWorst = target == Constants.TARGET_WORST
        if (isWorst) {
            println("[$tag] - Target=WORST -> TOP sorted ascending. Pick FIRST line per K (min = best-of-worst).")
        } else {
            println("[$tag] - Target=BEST  -> TOP sorted descending. Pick FIRST line per K (max = best-of-best).")
        }
        val sorted = groups.toSortedMap()
        for ((k, vals) in sorted) {
            val pick = vals.first()
            println("[$tag] - K=$k head=${vals.first()} tail=${vals.last()} -> pick(first)=$pick")
        }
    }

    /*
     * CSV – FUN/VAR: BEST with MIXED positive/negative (ordering + selection)
     */
    @Test
    @DisplayName("CSVView FUN/VAR: BEST with mixed ± values → file asc, pick LAST = max")
    fun testCsvFunVarGlobalSortWithExplanation_Best_MixedSigns() {
        System.setProperty("nbs.csv.finalRewrite", "true")

        println("[NaturalCorrelation/CSV] - Test begins (FUN/VAR, BEST with mixed ±).")

        val model = stubModel(
            datasetName = "DS-CSV-FUNVAR-BEST-MIX",
            targetToAchieve = Constants.TARGET_BEST,
            currentExecution = 114
        )
        val view = CSVView()

        val funPath = Paths.get(view.getFunctionValuesFilePath(model))
        val varPath = Paths.get(view.getVariableValuesFilePath(model))
        deleteIfExists(funPath); deleteIfExists(varPath)

        /**
         * Emit a single (K, corr, VAR) row into the CSV view.
         * Correlation is on the NATURAL scale and will be globally sorted on close.
         */
        fun emit(k: Int, corr: Double, rawVar: String) {
            val corrStr = String.format(Locale.ROOT, "%.6f", corr)
            view.onAppendCardinality(
                model,
                CardinalityResult(
                    target = model.targetToAchieve,
                    threadName = "t",
                    cardinality = k,
                    correlation = corr,
                    functionValuesCsvLine = "$k,$corrStr",
                    variableValuesCsvLine = rawVar
                )
            )
            println("[NaturalCorrelation/CSV] - append FUN/VAR -> K=$k corr=$corrStr")
        }

        /**
         * Mixed per K; BEST final file is ascending per K; we pick LAST (max).
         * K=1: -0.30, +0.10 → asc: -0.30, 0.10 → pick LAST = 0.10
         * K=2: -0.05, +0.20 → asc: -0.05, 0.20 → pick LAST = 0.20
         */
        emit(1, -0.30, "1 0 0 0")
        emit(2, 0.20, "0 1 0 0")
        emit(1, 0.10, "1 1 0 0")
        emit(2, -0.05, "0 0 1 0")

        view.closeStreams(model)
        println("[NaturalCorrelation/CSV] - closeStreams -> global FUN/VAR sort (K asc, corr asc) [BEST-MIX]")

        assertTrue(funPath.exists() && varPath.exists(), "FUN/VAR files should exist")

        val funLines = Files.readAllLines(funPath).filter { it.isNotBlank() }
        println("[NaturalCorrelation/CSV] - FUN lines (BEST-MIX):")
        funLines.forEach { println("  $it") }
        explainFunSelection(model.targetToAchieve, funLines, tag = "Explain/FUN-BEST-MIX")

        val byK = parseFunPairs(funLines)
        assertEquals(0.10, byK[1]!!.last(), 1e-12)
        assertEquals(0.20, byK[2]!!.last(), 1e-12)

        val varLines = Files.readAllLines(varPath).filter { it.isNotBlank() }
        assertEquals(4, funLines.size)
        assertEquals(4, varLines.size)
        assertTrue(varLines.all { it.startsWith("B64:") })

        println("[NaturalCorrelation/CSV] - Test ends (FUN/VAR, BEST with mixed ±).")
    }

    /*
     * CSV – TOP: BEST with MIXED positive/negative (ordering + selection)
     */
    @Test
    @DisplayName("CSVView TOP: BEST with mixed ± values → descending per K, pick FIRST = max")
    fun testCsvTopWriteWithExplanation_Best_MixedSigns() {
        System.setProperty("nbs.csv.top.live", "false")

        println("[NaturalCorrelation/CSV] - Test begins (TOP, BEST with mixed ±).")

        val model = stubModel(
            datasetName = "DS-CSV-TOP-BEST-MIX",
            targetToAchieve = Constants.TARGET_BEST,
            currentExecution = 225
        )
        val view = CSVView()

        val topPath = Paths.get(view.getTopSolutionsFilePath(model))
        deleteIfExists(topPath)

        /**
         * For BEST provide descending sequences that cross zero (positive down to negative).
         * K=1 initial: +0.15, +0.10, +0.05, 0.00, -0.05, -0.10, -0.15 → pick FIRST = +0.15
         * Replacement for K=1: +0.25, +0.20, …, -0.25 → pick FIRST = +0.25 (overwrites older K=1)
         * K=2: +0.12, +0.08, +0.04, 0.00, -0.04, -0.08, -0.12 → pick FIRST = +0.12
         */
        fun descCrossZero(cardinality: Int, start: Double, step: Double, count: Int): List<String> =
            (0 until count).map { i ->
                val v = start - step * i
                "$cardinality,${String.format(Locale.ROOT, "%.6f", v)},B64:"
            }

        val initial = mapOf(
            1 to descCrossZero(1, start = 0.15, step = 0.05, count = 7),
            2 to descCrossZero(2, start = 0.12, step = 0.04, count = 7)
        )
        view.onReplaceTopBatch(model, initial)
        println("[NaturalCorrelation/CSV] - replaceTopBatch -> K=1,2 (initial, desc across zero)")

        val replaceK1 = mapOf(
            1 to descCrossZero(1, start = 0.25, step = 0.05, count = 11)
        )
        view.onReplaceTopBatch(model, replaceK1)
        println("[NaturalCorrelation/CSV] - replaceTopBatch -> K=1 (replacement, stronger desc)")

        view.closeStreams(model)
        println("[NaturalCorrelation/CSV] - closeStreams -> write TOP once [BEST-MIX, desc]")

        assertTrue(topPath.exists(), "TOP file should exist")

        val topLines = Files.readAllLines(topPath).filter { it.isNotBlank() }
        println("[NaturalCorrelation/CSV] - TOP lines (BEST-MIX, first 8):")
        topLines.take(8).forEach { println("  $it") }

        val dataRows = topLines.drop(1)
        explainTopSelection(model.targetToAchieve, dataRows, tag = "Explain/TOP-BEST-MIX")

        /** Sanity: K=1 first should be 0.25; K=2 first should be 0.12. */
        val byK = parseTopTriples(dataRows)
        assertEquals(0.25, byK[1]!!.first(), 1e-12)
        assertEquals(0.12, byK[2]!!.first(), 1e-12)

        println("[NaturalCorrelation/CSV] - Test ends (TOP, BEST with mixed ±).")
    }

    /*
     * CSV – FUN/VAR: WORST with MIXED positive/negative (ordering + selection)
     */
    @Test
    @DisplayName("CSVView FUN/VAR: WORST with mixed ± values → file desc, pick LAST = min")
    fun testCsvFunVarGlobalSortWithExplanation_Worst_MixedSigns() {
        System.setProperty("nbs.csv.finalRewrite", "true")

        println("[NaturalCorrelation/CSV] - Test begins (FUN/VAR, WORST with mixed ±).")

        val model = stubModel(
            datasetName = "DS-CSV-FUNVAR-WORST-MIX",
            targetToAchieve = Constants.TARGET_WORST,
            currentExecution = 115
        )
        val view = CSVView()

        val funPath = Paths.get(view.getFunctionValuesFilePath(model))
        val varPath = Paths.get(view.getVariableValuesFilePath(model))
        deleteIfExists(funPath); deleteIfExists(varPath)

        /**
         * Emit a single (K, corr, VAR) row into the CSV view.
         * Correlation is on the NATURAL scale and will be globally sorted on close.
         */
        fun emit(k: Int, corr: Double, rawVar: String) {
            val corrStr = String.format(Locale.ROOT, "%.6f", corr)
            val ev = CardinalityResult(
                target = model.targetToAchieve,
                threadName = "t",
                cardinality = k,
                correlation = corr,
                functionValuesCsvLine = "$k,$corrStr",
                variableValuesCsvLine = rawVar
            )
            view.onAppendCardinality(model, ev)
            println("[NaturalCorrelation/CSV] - append FUN/VAR -> K=$k corr=$corrStr")
        }

        /**
         * Mixed per K; WORST final file is (K asc, corr desc); we pick LAST (min).
         * K=1:  0.30, -0.10 → desc: 0.30, -0.10 → pick LAST = -0.10
         * K=2:  0.20, -0.05 → desc: 0.20, -0.05 → pick LAST = -0.05
         */
        emit(1, 0.30, "1 0 0 0")
        emit(2, 0.20, "0 1 0 0")
        emit(1, -0.10, "1 1 0 0")
        emit(2, -0.05, "0 0 1 0")

        view.closeStreams(model)
        println("[NaturalCorrelation/CSV] - closeStreams -> global FUN/VAR sort (K asc, corr desc) [WORST-MIX]")

        assertTrue(funPath.exists() && varPath.exists(), "FUN/VAR files should exist")

        val funLines = Files.readAllLines(funPath).filter { it.isNotBlank() }
        println("[NaturalCorrelation/CSV] - FUN lines (WORST-MIX):")
        funLines.forEach { println("  $it") }
        explainFunSelection(model.targetToAchieve, funLines, tag = "Explain/FUN-WORST-MIX")

        val byK = parseFunPairs(funLines)
        assertEquals(-0.10, byK[1]!!.last(), 1e-12)
        assertEquals(-0.05, byK[2]!!.last(), 1e-12)

        val varLines = Files.readAllLines(varPath).filter { it.isNotBlank() }
        assertEquals(4, funLines.size)
        assertEquals(4, varLines.size)
        assertTrue(varLines.all { it.startsWith("B64:") })

        println("[NaturalCorrelation/CSV] - Test ends (FUN/VAR, WORST with mixed ±).")
    }

    /*
     * CSV – TOP: WORST with MIXED positive/negative (ordering + selection)
     */
    @Test
    @DisplayName("CSVView TOP: WORST with mixed ± values → ascending per K, pick FIRST = min")
    fun testCsvTopWriteWithExplanation_Worst_MixedSigns() {
        System.setProperty("nbs.csv.top.live", "false")

        println("[NaturalCorrelation/CSV] - Test begins (TOP, WORST with mixed ±).")

        val model = stubModel(
            datasetName = "DS-CSV-TOP-WORST-MIX",
            targetToAchieve = Constants.TARGET_WORST,
            currentExecution = 226
        )
        val view = CSVView()

        val topPath = Paths.get(view.getTopSolutionsFilePath(model))
        deleteIfExists(topPath)

        /**
         * For WORST provide ascending sequences that cross zero (negative up to positive).
         * K=1 initial: -0.15, -0.10, -0.05, 0.00, 0.05, 0.10, 0.15 → pick FIRST = -0.15
         * Replacement for K=1: -0.40, -0.35, …, 0.10 → pick FIRST = -0.40 (overwrites older K=1)
         * K=2: -0.12, -0.08, -0.04, 0.00, 0.04, 0.08, 0.12 → pick FIRST = -0.12
         */
        fun ascCrossZero(cardinality: Int, start: Double, step: Double, count: Int): List<String> =
            (0 until count).map { i ->
                val v = start + step * i
                "$cardinality,${String.format(Locale.ROOT, "%.6f", v)},B64:"
            }

        val initial = mapOf(
            1 to ascCrossZero(1, start = -0.15, step = 0.05, count = 7),
            2 to ascCrossZero(2, start = -0.12, step = 0.04, count = 7)
        )
        view.onReplaceTopBatch(model, initial)
        println("[NaturalCorrelation/CSV] - replaceTopBatch -> K=1,2 (initial, asc across zero)")

        val replaceK1 = mapOf(
            1 to ascCrossZero(1, start = -0.40, step = 0.05, count = 11)
        )
        view.onReplaceTopBatch(model, replaceK1)
        println("[NaturalCorrelation/CSV] - replaceTopBatch -> K=1 (replacement, stronger asc)")

        view.closeStreams(model)
        println("[NaturalCorrelation/CSV] - closeStreams -> write TOP once [WORST-MIX, asc]")

        assertTrue(topPath.exists(), "TOP file should exist")

        val topLines = Files.readAllLines(topPath).filter { it.isNotBlank() }
        println("[NaturalCorrelation/CSV] - TOP lines (WORST-MIX, first 8):")
        topLines.take(8).forEach { println("  $it") }

        val dataRows = topLines.drop(1)
        explainTopSelection(model.targetToAchieve, dataRows, tag = "Explain/TOP-WORST-MIX")

        val byK = parseTopTriples(dataRows)
        assertEquals(-0.40, byK[1]!!.first(), 1e-12)
        assertEquals(-0.12, byK[2]!!.first(), 1e-12)

        println("[NaturalCorrelation/CSV] - Test ends (TOP, WORST with mixed ±).")
    }

    /*
     * Test scaffolding
     */

    private fun stubModel(
        datasetName: String,
        targetToAchieve: String,
        currentExecution: Int
    ): DatasetModel =
        DatasetModel().apply {
            this.datasetName = datasetName
            this.targetToAchieve = targetToAchieve
            this.correlationMethod = Constants.CORRELATION_PEARSON
            this.numberOfTopics = 4
            this.numberOfSystems = 2
            this.numberOfIterations = 10
            this.numberOfRepetitions = 1
            this.populationSize = 8
            this.currentExecution = currentExecution
            this.expansionCoefficient = 0
            this.percentiles = linkedMapOf()
            this.topicLabels = arrayOf("t0", "t1", "t2", "t3")
        }

    private fun deleteIfExists(p: Path) {
        runCatching { if (Files.exists(p)) Files.delete(p) }
    }

    private fun topPathExists(view: CSVView, model: DatasetModel): Boolean =
        Paths.get(view.getTopSolutionsFilePath(model)).exists()

    private fun readTopLines(view: CSVView, model: DatasetModel): List<String> =
        Files.readAllLines(Paths.get(view.getTopSolutionsFilePath(model))).filter { it.isNotBlank() }
}
