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

/*
 * DatasetViewTest
 * ===============
 *
 * This test exercises the streaming helpers in DatasetView:
 *  • Append out-of-order FUN/VAR rows and verify that closeStreams() performs a
 *    global rewrite sorted by (K asc, corr asc), keeping FUN and VAR aligned.
 *  • Send two TopKReplaceBatch updates and verify the TOP file contains the
 *    latest block for K=1 and both K=1 and K=2 blocks in K-ascending order.
 *
 * Notes:
 *  • We use a minimal DatasetModel stub (only fields the View needs for paths).
 *  • We print progress messages, mimicking your existing test style.
 */
class DatasetViewTest {

    /*
     * Build a minimal model that satisfies DatasetView path computation.
     * We also provide topic labels so conversions have a label universe.
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

        /*
         * Helper to emit a single improved K row (intentionally out-of-order).
         * We write to FUN/VAR and buffer internally for the final global sort.
         */
        fun emit(cardinality: Int, correlation: Double, varLine: String) {
            val event = CardinalityResult(
                target = model.targetToAchieve,
                threadName = Thread.currentThread().name,
                cardinality = cardinality,
                correlation = correlation,
                functionValuesCsvLine = "$cardinality $correlation",
                variableValuesCsvLine = varLine
            )
            view.appendCardinality(model, event)
            println("[DatasetViewTest] - append FUN/VAR -> K=$cardinality corr=$correlation")
        }

        /*
         * Intentionally emit out-of-order:
         *  Expected final order by (K asc, corr asc):
         *    (1, 0.80), (1, 0.90), (2, 0.60), (2, 0.70)
         *
         * For VAR we pass simple bit-like strings; the CSV view will normalize them
         * into compact B64 on close.
         */
        emit(2, 0.70, "1 1 0 0")
        emit(1, 0.90, "1 0 0 0")
        emit(2, 0.60, "0 1 0 0")
        emit(1, 0.80, "1 1 1 0")

        /*
         * Helper to create a 10-row TOP block for a given K with ascending correlations.
         * We deliberately give label-like payloads; the view will convert to Base64.
         */
        fun topBlock(cardinality: Int, baseCorrelation: Double): List<String> =
            (0 until 10).map { i -> "$cardinality,${baseCorrelation + i * 0.01},topicA|topicB|topicC" }

        /*
         * First TOP batch: K=1 and K=2.
         */
        val firstBatch = mapOf(
            1 to topBlock(1, 0.10),
            2 to topBlock(2, 0.20)
        )
        view.replaceTopBatch(model, firstBatch)
        println("[DatasetViewTest] - replaceTopBatch -> K=1,2 (initial)")

        /*
         * Second TOP batch: replace only K=1 with newer content (starts at 0.30).
         */
        val secondBatch = mapOf(1 to topBlock(1, 0.30))
        view.replaceTopBatch(model, secondBatch)
        println("[DatasetViewTest] - replaceTopBatch -> K=1 (new content)")

        /*
         * Closing the streams triggers the global sort & rewrite for FUN/VAR,
         * and writes Parquet from the canonical snapshot (handled inside DatasetView).
         */
        view.closeStreams(model)
        println("[DatasetViewTest] - closeStreams -> FUN/VAR globally sorted & rewritten")

        /* ===== Assertions: FUN ===== */
        val functionLines = Files.readAllLines(functionValuesPath).filter { it.isNotBlank() }
        val expectedFunctionPairs = listOf(
            1 to 0.80, 1 to 0.90, 2 to 0.60, 2 to 0.70
        ).sortedWith(compareBy<Pair<Int, Double>> { it.first }.thenBy { it.second })

        val computedFunctionPairs = functionLines.map { line ->
            val parts = line.trim().split(Regex("\\s+"))
            parts[0].toDouble().toInt() to parts[1].toDouble()
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

        // Sanity: each VAR line should be "B64:<payload>" and decodable
        variableLines.forEachIndexed { idx, line ->
            assertTrue(line.startsWith("B64:"), "VAR line #$idx must start with B64:")
            val payload = line.removePrefix("B64:")
            java.util.Base64.getDecoder().decode(payload)  // throws if invalid
        }

        /* ===== Assertions: TOP =====
         * Header + 20 data rows (10 for K=1, 10 for K=2).
         * K=1 block must reflect the SECOND batch (first corr = 0.30).
         */
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
        val firstK1Topics = firstK1[2]  /* "B64:<payload>" */
        println("[DatasetViewTest] - TOP K=1 first corr (expected 0.30): $firstK1Correlation")
        assertEquals(0.30, firstK1Correlation, 1e-12)

        // Sanity: topics are Base64 (CSV uses "B64:" prefix)
        assertTrue(firstK1Topics.startsWith("B64:"), "TOP topics must start with B64:")
        val topicsPayload = firstK1Topics.removePrefix("B64:")
        java.util.Base64.getDecoder().decode(topicsPayload)  // throws if invalid

        /* Optional cleanup so the test is repeatable without manual deletes */
        cleanup(functionValuesPath.toFile(), variableValuesPath.toFile(), topSolutionsPath.toFile())

        println("[DatasetViewTest] - Test ends.")
    }

    /*
     * Best-effort cleanup helper (ignores failures).
     */
    private fun cleanup(vararg files: File) {
        files.forEach { file ->
            try { file.delete() } catch (_: Exception) { /* ignored */ }
        }
    }
}
