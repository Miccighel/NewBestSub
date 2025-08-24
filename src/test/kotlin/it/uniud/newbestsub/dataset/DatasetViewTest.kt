package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.dataset.DatasetView
import it.uniud.newbestsub.dataset.model.CardinalityResult
import it.uniud.newbestsub.utils.Constants
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import java.nio.file.Files
import java.nio.file.Paths
import java.io.File
import java.util.Locale

/**
 * Tests for [DatasetView] streaming helpers.
 *
 * Covered scenarios:
 * - **FUN/VAR**: verify that out-of-order rows are globally rewritten and sorted
 *   by `(K asc, corr asc)` on [DatasetView.closeStreams], keeping FUN and VAR aligned.
 * - **TOP**: verify that `replaceTopBatch` replaces blocks as expected
 *   (newer batch replaces older for the same K; multiple K’s are preserved).
 *
 * Notes:
 * - A minimal [DatasetModel] stub is used (only fields needed for path resolution).
 * - Topic labels are provided for conversions.
 * - Progress messages are printed to mimic the existing test style.
 */
@DisplayName("DatasetView – streaming helpers (FUN/VAR sort + TOP batch replace)")
class DatasetViewTest {

    /*
     * Build a minimal model that satisfies DatasetView path computation.
     * Provides topic labels so conversions have a label universe.
     */
    private fun stubModel(
        datasetName: String = "TEST_DS",
        targetToAchieve: String = Constants.TARGET_BEST,
        currentExecution: Int = 1
    ): DatasetModel =
        DatasetModel().apply {
            this.datasetName = datasetName
            this.targetToAchieve = targetToAchieve
            this.correlationMethod = Constants.CORRELATION_PEARSON
            this.numberOfTopics = 50
            this.numberOfSystems = 5
            this.numberOfIterations = 100
            this.numberOfRepetitions = 1
            this.populationSize = 50
            this.currentExecution = currentExecution
            this.expansionCoefficient = 0
            this.percentiles = linkedMapOf()
            /* Provide labels like 401..450 */
            this.topicLabels = Array(50) { (401 + it).toString() }
        }

    /**
     * Verifies [DatasetView] streaming behavior:
     * - FUN/VAR: appends rows out-of-order, then checks that [DatasetView.closeStreams]
     *   rewrites them globally sorted by `(K asc, corr asc)`.
     * - VAR: lines are normalized into `"B64:<payload>"` and decodable.
     * - TOP: `replaceTopBatch` updates blocks, newer K=1 batch replaces the previous one,
     *   and K=2 remains from the earlier batch.
     *
     * Also checks:
     * - FUN/VAR alignment (same row count).
     * - TOP header correctness and Base64-encoding of topics.
     * - Test cleans up generated CSV files at the end.
     */
    @Test
    @DisplayName("Streaming: FUN/VAR global sort + TOP replace-batch")
    fun testViewStreaming() {

        println("[DatasetViewTest] - Test begins.")

        /* Arrange */
        val view = DatasetView()
        val model = stubModel(targetToAchieve = Constants.TARGET_BEST, currentExecution = 1)

        /* Resolve output paths exactly as the View will write them */
        val functionValuesPath = Paths.get(view.getFunctionValuesFilePath(model))
        val variableValuesPath = Paths.get(view.getVariableValuesFilePath(model))
        val topSolutionsPath = Paths.get(view.getTopSolutionsFilePath(model))
        Files.createDirectories(functionValuesPath.parent)

        // NOTE: do NOT call view.openStreams(model) — streaming opens lazily on first append.

        /**
         * Helper to emit a single improved K row (intentionally out-of-order).
         *
         * For FUN we use `"K,corr"`, formatted with `Locale.ROOT` to ensure dot-decimal.
         * For VAR we pass raw forms; CSV view will normalize to Base64 on close.
         */
        fun emit(cardinality: Int, correlation: Double, variableCsvRaw: String) {
            val corrStr = String.format(Locale.ROOT, "%.6f", correlation)
            val event = CardinalityResult(
                target = model.targetToAchieve,
                threadName = Thread.currentThread().name,
                cardinality = cardinality,
                correlation = correlation,
                functionValuesCsvLine = "$cardinality,$corrStr",
                variableValuesCsvLine = variableCsvRaw
            )
            view.appendCardinality(model, event)
            println("[DatasetViewTest] - append FUN/VAR -> K=$cardinality corr=$corrStr")
        }

        /* Intentionally emit out-of-order FUN/VAR rows */
        emit(2, 0.70, "1 1 0 0")
        emit(1, 0.90, "1 0 0 0")
        emit(2, 0.60, "0 1 0 0")
        emit(1, 0.80, "1 1 1 0")

        /**
         * Helper to create a 10-row TOP block for a given K with ascending correlations.
         */
        fun topBlock(cardinality: Int, baseCorrelation: Double): List<String> =
            (0 until 10).map { i -> "$cardinality,${baseCorrelation + i * 0.01},topicA|topicB|topicC" }

        /* First TOP batch: K=1 and K=2 */
        val firstBatch = mapOf(
            1 to topBlock(1, 0.10),
            2 to topBlock(2, 0.20)
        )
        view.replaceTopBatch(model, firstBatch)
        println("[DatasetViewTest] - replaceTopBatch -> K=1,2 (initial)")

        /* Second TOP batch: replace only K=1 */
        val secondBatch = mapOf(1 to topBlock(1, 0.30))
        view.replaceTopBatch(model, secondBatch)
        println("[DatasetViewTest] - replaceTopBatch -> K=1 (new content)")

        /* Closing triggers global FUN/VAR sort and canonical write of all streams */
        view.closeStreams(model)
        println("[DatasetViewTest] - closeStreams -> FUN/VAR globally sorted & rewritten")

        /* ===== Assertions: FUN ===== */
        val functionLines = Files.readAllLines(functionValuesPath).filter { it.isNotBlank() }
        val expectedFunctionPairs = listOf(
            1 to 0.80, 1 to 0.90, 2 to 0.60, 2 to 0.70
        ).sortedWith(compareBy<Pair<Int, Double>> { it.first }.thenBy { it.second })

        val computedFunctionPairs = functionLines.map { line ->
            val parts = line.trim().split(Regex("[,;\\s]+"), limit = 2)
            require(parts.size == 2) { "Bad FUN/VAR line: '$line'" }
            val k = parts[0].toInt()
            val corr = parts[1].toDouble()
            k to corr
        }

        println("[DatasetViewTest] - FUN expected vs computed:")
        expectedFunctionPairs.zip(computedFunctionPairs).forEach { (expected, computed) ->
            println("  expected=$expected, computed=$computed")
        }
        assertEquals(expectedFunctionPairs.size, computedFunctionPairs.size)
        for (index in expectedFunctionPairs.indices) {
            assertEquals(expectedFunctionPairs[index].first, computedFunctionPairs[index].first)
            assertEquals(expectedFunctionPairs[index].second, computedFunctionPairs[index].second, 1e-12)
        }

        /* ===== Assertions: VAR ===== */
        val variableLines = Files.readAllLines(variableValuesPath).filter { it.isNotBlank() }
        println("[DatasetViewTest] - VAR line count -> expected=${functionLines.size}, computed=${variableLines.size}")
        assertEquals(functionLines.size, variableLines.size)

        variableLines.forEachIndexed { idx, line ->
            assertTrue(line.startsWith("B64:"), "VAR line #$idx must start with B64:")
            val payload = line.removePrefix("B64:")
            java.util.Base64.getDecoder().decode(payload) // throws if invalid
        }

        /* ===== Assertions: TOP ===== */
        val topLines = Files.readAllLines(topSolutionsPath).filter { it.isNotBlank() }
        println("[DatasetViewTest] - TOP total lines (including header): ${topLines.size}")
        assertEquals(1 + 20, topLines.size)

        val header = topLines.first()
        println("[DatasetViewTest] - TOP header: $header")
        assertEquals("Cardinality,Correlation,TopicsB64", header)

        val dataRows = topLines.drop(1)
        val kColumn = dataRows.map { it.substringBefore(',').toInt() }

        println("[DatasetViewTest] - TOP first 10 K values: ${kColumn.take(10)}")
        println("[DatasetViewTest] - TOP last  10 K values: ${kColumn.drop(10)}")
        for (i in 0 until 10) assertEquals(1, kColumn[i])
        for (i in 10 until 20) assertEquals(2, kColumn[i])

        val firstK1 = dataRows.first().split(',', limit = 3)
        val firstK1Correlation = firstK1[1].toDouble()
        val firstK1Topics = firstK1[2]
        println("[DatasetViewTest] - TOP K=1 first corr (expected 0.30): $firstK1Correlation")
        assertEquals(0.30, firstK1Correlation, 1e-12)

        assertTrue(firstK1Topics.startsWith("B64:"), "TOP topics must start with B64:")
        val topicsPayload = firstK1Topics.removePrefix("B64:")
        java.util.Base64.getDecoder().decode(topicsPayload) // throws if invalid

        /* Cleanup so the test is repeatable without manual deletes */
        cleanup(functionValuesPath.toFile(), variableValuesPath.toFile(), topSolutionsPath.toFile())

        println("[DatasetViewTest] - Test ends.")
    }

    /**
     * Best-effort cleanup helper to delete generated files.
     * Failures are ignored to keep test repeatable across runs.
     */
    private fun cleanup(vararg files: File) {
        files.forEach { file ->
            try { file.delete() } catch (_: Exception) { /* ignored */ }
        }
    }
}
