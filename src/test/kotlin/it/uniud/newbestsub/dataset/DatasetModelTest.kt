package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.dataset.view.ViewPaths
import it.uniud.newbestsub.problem.getCardinality
import it.uniud.newbestsub.problem.getCorrelation
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.random.Random

@DisplayName("DatasetModel – core behaviors and encoding round‑trips")
class DatasetModelTest {

    /* ----------------------------------------------------------------------------------------------------------------
     * Helpers: Base64 mask pack/unpack (must mirror production layout)
     * ----------------------------------------------------------------------------------------------------------------
     */

    /**
     * Decode a Base64-packed bitmask (as produced by the model/CSV/Parquet) back to a BooleanArray.
     *
     * Accepted input formats:
     *  - "B64:<base64>"  (CSV/stream format)
     *  - "<base64>"      (Parquet format: bare payload, no prefix)
     *
     * numberOfTopics: original mask length (N). Extra bits in the last word are ignored.
     *
     * Layout assumed (must match the model packer):
     *  - The mask is packed into 64-bit words.
     *  - Bits are LSB-first within each 64-bit word (bit 0 -> topic 0, bit 63 -> topic 63).
     *  - Words are serialized little-endian to bytes (least significant byte first).
     *  - Bytes are then Base64-encoded without padding.
     */
    private fun decodeBase64MaskToBooleanArray(b64OrPrefixed: String, numberOfTopics: Int): BooleanArray {
        val base64Payload = if (b64OrPrefixed.startsWith("B64:")) b64OrPrefixed.substring(4) else b64OrPrefixed
        val byteBuffer = java.util.Base64.getDecoder().decode(base64Payload)

        val booleanMask = BooleanArray(numberOfTopics)
        var topicIndex = 0
        var byteOffset = 0

        /* Read little-endian 64-bit words from the byte array */
        while (byteOffset < byteBuffer.size && topicIndex < numberOfTopics) {
            var word = 0L
            var bitShift = 0
            /* Assemble one 64-bit word (little-endian) */
            for (i in 0 until java.lang.Long.BYTES) {
                if (byteOffset + i >= byteBuffer.size) break
                val b = (byteBuffer[byteOffset + i].toInt() and 0xFF)
                word = word or (b.toLong() shl bitShift)
                bitShift += 8
            }
            byteOffset += java.lang.Long.BYTES

            /* Expand word bits into booleans (LSB-first) */
            var bitIndexWithinWord = 0
            while (bitIndexWithinWord < 64 && topicIndex < numberOfTopics) {
                booleanMask[topicIndex] = ((word ushr bitIndexWithinWord) and 1L) != 0L
                bitIndexWithinWord++
                topicIndex++
            }
        }
        return booleanMask
    }

    /**
     * Encode a BooleanArray mask into the CSV VAR/TOP line format used by the model: "B64:<base64>".
     * Mirrors the packing layout implemented in the model:
     *  - 64-bit words, LSB-first bits, little-endian bytes, Base64 without padding.
     */
    private fun encodeMaskToVarLineBase64(mask: BooleanArray): String =
        "B64:" + encodeMaskToBareBase64(mask)

    /**
     * Encode a BooleanArray mask into the Parquet VAR/TOP payload format: bare "<base64>" (no prefix).
     * Same packing as above.
     */
    private fun encodeMaskToBareBase64(mask: BooleanArray): String {
        val numberOfTopics = mask.size
        val numberOfWords = (numberOfTopics + 63) ushr 6
        val packedWords = LongArray(numberOfWords)

        var currentWordIndex = 0
        var bitIndexWithinWord = 0
        var accumulator = 0L

        var i = 0
        while (i < numberOfTopics) {
            if (mask[i]) accumulator = accumulator or (1L shl bitIndexWithinWord)
            bitIndexWithinWord++
            if (bitIndexWithinWord == 64) {
                packedWords[currentWordIndex++] = accumulator
                accumulator = 0L
                bitIndexWithinWord = 0
            }
            i++
        }
        if (bitIndexWithinWord != 0) {
            packedWords[currentWordIndex] = accumulator
        }

        /* Serialize as little-endian bytes */
        val byteBuffer = ByteArray(numberOfWords * java.lang.Long.BYTES)
        var writeOffset = 0
        for (word in packedWords) {
            var w = word
            for (b in 0 until java.lang.Long.BYTES) {
                byteBuffer[writeOffset + b] = (w and 0xFF).toByte()
                w = w ushr 8
            }
            writeOffset += java.lang.Long.BYTES
        }

        return java.util.Base64.getEncoder().withoutPadding().encodeToString(byteBuffer)
    }

    /* ----------------------------------------------------------------------------------------------------------------
     * Tests: model behavior (AVERAGE branch) and encoding round-trips
     * ----------------------------------------------------------------------------------------------------------------
     */

    @Test
    @DisplayName("AVERAGE path caches exactly one representative per K = 1..N (mask size=N, |mask|=K)")
    fun testSolveAverageEmitsOnePerCardinality() {
        /* AVERAGE path now streams rows and caches one representative per K (no big in-memory list). */
        val controller = DatasetController(Constants.TARGET_AVERAGE)
        controller.load("src/test/resources/AP96.csv")

        val parameters = Parameters(
            datasetName = "AH99",
            correlationMethod = Constants.CORRELATION_KENDALL,
            targetToAchieve = Constants.TARGET_AVERAGE,
            numberOfIterations = 100_000,   // ignored in AVERAGE
            numberOfRepetitions = 1_000,
            populationSize = 1_000,         // ignored in AVERAGE
            currentExecution = 0,
            percentiles = listOf(1, 5, 25, 99)
        )

        val model = controller.models[0]

        /* Run once: fills per-K caches (corrByK + rep mask) and percentiles. */
        model.solve(parameters)

        val numberOfTopics = model.numberOfTopics
        var seen = 0
        for (k in 1..numberOfTopics) {
            /* Correlation must exist and be finite for each K. */
            val corr = model.findCorrelationForCardinality(k.toDouble())
            assertTrue(corr != null && corr!!.isFinite(), "Expected a finite correlation for K=$k.")

            /* Representative mask must exist, be size N, and have exactly K bits set. */
            val mask = model.retrieveMaskForCardinality(k.toDouble())
            assertTrue(mask != null, "Expected a representative mask for K=$k.")
            assertEquals(numberOfTopics, mask!!.size, "Mask size mismatch for K=$k.")
            val ones = mask.count { it }
            assertEquals(k, ones, "Mask cardinality must equal K for K=$k.")

            seen++
        }

        /* One and only one representative per K = 1..N. */
        assertEquals(numberOfTopics, seen, "Expected exactly one representative per cardinality K = 1..N.")

        /* Percentiles collected from the same samples: one value per K for each requested percentile. */
        val expectedPercentiles = setOf(1, 5, 25, 99)
        assertTrue(model.percentiles.keys.containsAll(expectedPercentiles), "Missing requested percentiles.")
        expectedPercentiles.forEach { p ->
            val values = model.percentiles[p] ?: emptyList()
            assertEquals(numberOfTopics, values.size, "Percentile $p should have one value per K (1..$numberOfTopics).")
            assertTrue(values.all { it.isFinite() }, "Percentile $p contains non-finite values.")
        }
    }


    @Test
    @DisplayName("Packed Base64 VAR round‑trip equals original mask (CSV B64‑prefixed)")
    fun testPackedBase64CsvPrefixedRoundTrip() {
        val sizesToTest = listOf(1, 7, 8, 9, 63, 64, 65, 127, 128, 257, 511)
        val randomGenerator = Random(1234)

        for (numberOfTopics in sizesToTest) {
            val originalMask = BooleanArray(numberOfTopics) { randomGenerator.nextBoolean() }
            val varLine = encodeMaskToVarLineBase64(originalMask)  /* "B64:<payload>" */
            val decodedMask = decodeBase64MaskToBooleanArray(varLine, numberOfTopics)
            assertArrayEquals(originalMask, decodedMask, "Round‑trip failed for N=$numberOfTopics (CSV B64‑prefixed).")
        }
    }

    @Test
    @DisplayName("Packed Base64 VAR round‑trip equals original mask (Parquet bare payload)")
    fun testPackedBase64ParquetBareRoundTrip() {
        val sizesToTest = listOf(1, 7, 8, 9, 63, 64, 65, 127, 128, 257, 511)
        val randomGenerator = Random(42)

        for (numberOfTopics in sizesToTest) {
            val originalMask = BooleanArray(numberOfTopics) { randomGenerator.nextBoolean() }
            val barePayload = encodeMaskToBareBase64(originalMask)  /* "<payload>" */
            val decodedMask = decodeBase64MaskToBooleanArray(barePayload, numberOfTopics)
            assertArrayEquals(originalMask, decodedMask, "Round‑trip failed for N=$numberOfTopics (Parquet bare).")
        }
    }

    @Test
    @DisplayName("Edge patterns decode correctly across 64‑bit word boundaries")
    fun testEdgePatternsAcrossWordBoundaries() {
        val sizesToTest = listOf(1, 63, 64, 65, 127, 128, 129)

        for (numberOfTopics in sizesToTest) {
            /* All zeros */
            run {
                val mask = BooleanArray(numberOfTopics) { false }
                val prefixed = encodeMaskToVarLineBase64(mask)
                val bare = encodeMaskToBareBase64(mask)
                assertArrayEquals(mask, decodeBase64MaskToBooleanArray(prefixed, numberOfTopics), "Zeros failed N=$numberOfTopics (prefixed).")
                assertArrayEquals(mask, decodeBase64MaskToBooleanArray(bare, numberOfTopics), "Zeros failed N=$numberOfTopics (bare).")
            }
            /* All ones */
            run {
                val mask = BooleanArray(numberOfTopics) { true }
                val prefixed = encodeMaskToVarLineBase64(mask)
                val bare = encodeMaskToBareBase64(mask)
                assertArrayEquals(mask, decodeBase64MaskToBooleanArray(prefixed, numberOfTopics), "Ones failed N=$numberOfTopics (prefixed).")
                assertArrayEquals(mask, decodeBase64MaskToBooleanArray(bare, numberOfTopics), "Ones failed N=$numberOfTopics (bare).")
            }
            /* Alternating 1010... */
            run {
                val mask = BooleanArray(numberOfTopics) { index -> (index and 1) == 0 }
                val prefixed = encodeMaskToVarLineBase64(mask)
                val bare = encodeMaskToBareBase64(mask)
                assertArrayEquals(mask, decodeBase64MaskToBooleanArray(prefixed, numberOfTopics), "Alternating failed N=$numberOfTopics (prefixed).")
                assertArrayEquals(mask, decodeBase64MaskToBooleanArray(bare, numberOfTopics), "Alternating failed N=$numberOfTopics (bare).")
            }
            /* Single bits near boundaries and ends */
            run {
                val interestingPositions = listOf(0, 1, 62, 63, 64, 65, numberOfTopics - 2, numberOfTopics - 1)
                    .filter { it in 0 until numberOfTopics }
                for (bitPosition in interestingPositions) {
                    val mask = BooleanArray(numberOfTopics) { false }.also { it[bitPosition] = true }
                    val prefixed = encodeMaskToVarLineBase64(mask)
                    val bare = encodeMaskToBareBase64(mask)
                    assertArrayEquals(mask, decodeBase64MaskToBooleanArray(prefixed, numberOfTopics), "Single bit at $bitPosition failed N=$numberOfTopics (prefixed).")
                    assertArrayEquals(mask, decodeBase64MaskToBooleanArray(bare, numberOfTopics), "Single bit at $bitPosition failed N=$numberOfTopics (bare).")
                }
            }
        }
    }

    /* ----------------------------------------------------------------------------------------------------------------
     * Light path/token checks: correlation must appear in names (no heavy I/O)
     * ----------------------------------------------------------------------------------------------------------------
     */

    @Test
    @DisplayName("Params token includes correlation method (prevents cross‑run collisions)")
    fun testParamsTokenIncludesCorrelation() {
        val token = Tools.buildParamsToken(
            datasetName = "DS",
            correlationMethod = Constants.CORRELATION_KENDALL,
            numberOfTopics = 256,
            numberOfSystems = 15,
            populationSize = 512,
            numberOfIterations = 100_000,
            numberOfRepetitions = 1_000,
            expansionCoefficient = 0,
            includePercentiles = true,
            percentiles = listOf(1, 99)
        )
        assertTrue(token.contains(Constants.CORRELATION_KENDALL), "Params token should include the correlation method.")
        assertTrue(token.contains("top256") && token.contains("sys15") && token.contains("po512"), "Token should include core dimensions.")
        assertTrue(token.contains("pe1_99"), "Token should encode percentiles as 'pe<first>_<last>'.")
    }

    @Test
    @DisplayName("Log filename is parameterized with token and run timestamp")
    fun testFinalLogFilenameUsesTokenAndTimestamp() {
        val paramsToken = "DS-${Constants.CORRELATION_PEARSON}-top10-sys5-po50-i1000"
        val finalLogPath = it.uniud.newbestsub.utils.LogManager.buildFinalLogFilePathFromParams(paramsToken)
        assertTrue(finalLogPath.endsWith(".log"), "Final log path must end with .log")
        assertTrue(finalLogPath.contains(paramsToken), "Final log path must contain the params token.")
        assertTrue(finalLogPath.contains(Constants.RUN_TIMESTAMP), "Final log path must include the run timestamp.")
    }
}
