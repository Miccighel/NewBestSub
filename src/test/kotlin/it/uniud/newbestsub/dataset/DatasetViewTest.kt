package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.utils.Constants
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import java.nio.file.Files
import java.nio.file.Paths
import java.io.File

/*
 * DatasetViewTest
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
     */
    private fun stubModel(
        dataset: String = "TEST_DS",
        target: String = Constants.TARGET_BEST,
        exec: Int = 1
    ): DatasetModel =
        DatasetModel().apply {
            datasetName = dataset
            targetToAchieve = target
            correlationMethod = Constants.CORRELATION_PEARSON
            numberOfTopics = 50
            numberOfSystems = 5
            numberOfIterations = 100
            numberOfRepetitions = 1
            populationSize = 50
            currentExecution = exec
            expansionCoefficient = 0
            percentiles = linkedMapOf()
        }

    @Test
    @DisplayName("Streaming: FUN/VAR global sort + TOP replace-batch")
    fun testViewStreaming() {

        println("[DatasetViewTest] - Test begins.")

        /* Arrange */
        val view = DatasetView()
        val model = stubModel(target = Constants.TARGET_BEST, exec = 1)

        /* Resolve output paths exactly as the View will write them */
        val funPath = Paths.get(view.getFunctionValuesFilePath(model))
        val varPath = Paths.get(view.getVariableValuesFilePath(model))
        val topPath = Paths.get(view.getTopSolutionsFilePath(model))
        Files.createDirectories(funPath.parent)

        /* Start streaming session (opens and truncates FUN/VAR, then append mode) */
        view.openStreams(model)

        /*
         * Helper to emit a single improved K row (intentionally out-of-order).
         * We write to FUN/VAR and buffer internally for the final global sort.
         */
        fun emit(k: Int, corr: Double, varLine: String) {
            val ev = CardinalityResult(
                target = model.targetToAchieve,
                threadName = Thread.currentThread().name,
                cardinality = k,
                correlation = corr,
                functionValuesCsvLine = "$k $corr",
                variableValuesCsvLine = varLine
            )
            view.appendCardinality(model, ev)
            println("[DatasetViewTest] - append FUN/VAR -> K=$k corr=$corr")
        }

        /*
         * Intentionally emit out-of-order:
         *  Expected final order by (K asc, corr asc):
         *    (1, 0.80), (1, 0.90), (2, 0.60), (2, 0.70)
         */
        emit(2, 0.70, "1 1 0 0")
        emit(1, 0.90, "1 0 0 0")
        emit(2, 0.60, "0 1 0 0")
        emit(1, 0.80, "1 1 1 0")

        /*
         * Helper to create a 10-row TOP block for a given K with ascending correlations.
         */
        fun topBlock(k: Int, base: Double): List<String> =
            (0 until 10).map { i -> "$k,${base + i * 0.01},topicA|topicB|topicC" }

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
        val secondBatch = mapOf(
            1 to topBlock(1, 0.30)
        )
        view.replaceTopBatch(model, secondBatch)
        println("[DatasetViewTest] - replaceTopBatch -> K=1 (new content)")

        /*
         * Closing the streams triggers the global sort & rewrite for FUN/VAR.
         */
        view.closeStreams(model)
        println("[DatasetViewTest] - closeStreams -> FUN/VAR globally sorted & rewritten")

        /* ===== Assertions: FUN ===== */
        val funLines = Files.readAllLines(funPath).filter { it.isNotBlank() }
        val expectedFunPairs = listOf(
            1 to 0.80, 1 to 0.90, 2 to 0.60, 2 to 0.70
        ).sortedWith(compareBy<Pair<Int, Double>> { it.first }.thenBy { it.second })

        val computedFunPairs = funLines.map { line ->
            val p = line.trim().split(Regex("\\s+"))
            p[0].toDouble().toInt() to p[1].toDouble()
        }

        println("[DatasetViewTest] - FUN expected vs computed:")
        expectedFunPairs.zip(computedFunPairs).forEach { (exp, got) ->
            println("  expected=$exp, computed=$got")
        }
        assertEquals(expectedFunPairs.size, computedFunPairs.size)
        for (i in expectedFunPairs.indices) {
            assertEquals(expectedFunPairs[i].first, computedFunPairs[i].first)
            assertEquals(expectedFunPairs[i].second, computedFunPairs[i].second, 1e-12)
        }

        /* ===== Assertions: VAR ===== */
        val varLines = Files.readAllLines(varPath).filter { it.isNotBlank() }
        println("[DatasetViewTest] - VAR line count -> expected=${funLines.size}, computed=${varLines.size}")
        assertEquals(funLines.size, varLines.size)

        /* ===== Assertions: TOP =====
         * Header + 20 data rows (10 for K=1, 10 for K=2).
         * K=1 block must reflect the SECOND batch (first corr = 0.30).
         */
        val topLines = Files.readAllLines(topPath).filter { it.isNotBlank() }
        println("[DatasetViewTest] - TOP total lines (including header): ${topLines.size}")
        assertEquals(1 + 20, topLines.size)

        val header = topLines.first()
        println("[DatasetViewTest] - TOP header: $header")
        assertEquals("Cardinality,Correlation,Topics", header)

        val data = topLines.drop(1)
        val kColumn = data.map { it.substringBefore(',').toInt() }

        println("[DatasetViewTest] - TOP first 10 K values: ${kColumn.take(10)}")
        println("[DatasetViewTest] - TOP last  10 K values: ${kColumn.drop(10)}")
        for (i in 0 until 10) assertEquals(1, kColumn[i])
        for (i in 10 until 20) assertEquals(2, kColumn[i])

        val firstK1Corr = data.first().split(',')[1].toDouble()
        println("[DatasetViewTest] - TOP K=1 first corr (expected 0.30): $firstK1Corr")
        assertEquals(0.30, firstK1Corr, 1e-12)

        /* Optional cleanup so the test is repeatable without manual deletes */
        cleanup(funPath.toFile(), varPath.toFile(), topPath.toFile())

        println("[DatasetViewTest] - Test ends.")
    }

    /*
     * Best-effort cleanup helper (ignores failures).
     */
    private fun cleanup(vararg files: File) {
        files.forEach { f ->
            try { f.delete() } catch (_: Exception) {}
        }
    }
}
