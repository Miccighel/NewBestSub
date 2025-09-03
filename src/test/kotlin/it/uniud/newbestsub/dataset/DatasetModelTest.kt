package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.random.Random

/**
 * Test suite for core `DatasetModel` behaviors and bitmask encoding/decoding round-trips.
 *
 * Covered areas:
 * - AVERAGE path: guarantees exactly one representative per cardinality K = 1..N
 * - Base64 mask pack/unpack round-trips (CSV `"B64:<base64>"` and Parquet bare payload)
 * - Edge patterns around 64-bit word boundaries
 * - Lightweight helpers: presence/size checks and parameterized tokens/paths
 * - BEST/WORST NATURAL correlation semantics:
 *   - Streamed correlations are on the NATURAL scale (positive-is-better for BEST, lower-is-better for WORST).
 *   - Per-K improvements are strictly monotone in NATURAL scale (↑ BEST, ↓ WORST).
 *   - TOP pools are ordered on NATURAL correlation; degenerate `NaN` correlations are allowed but must be
 *     ranked after all finite values (suffix) to keep deterministic ordering.
 */
@DisplayName("DatasetModel – core behaviors and encoding round-trips")
class DatasetModelTest {

    /* ----------------------------------------------------------------------------------------------------------------
     * Helpers: Base64 mask pack/unpack (must mirror production layout)
     * ----------------------------------------------------------------------------------------------------------------
     */

    /**
     * Decodes a Base64-packed bitmask into a [BooleanArray], mirroring production layout.
     *
     * Accepted input formats:
     * - `"B64:<base64>"` (CSV/stream format)
     * - `"<base64>"` (Parquet format: bare payload, no prefix)
     *
     * Layout (must match production):
     * - The mask is split into 64-bit words.
     * - Bits are **LSB-first** inside each word (bit 0 → topic 0, bit 63 → topic 63).
     * - Words are serialized as **little-endian** bytes.
     * - Bytes are Base64-encoded **without padding**.
     *
     * Extra bits in the last word (beyond [numberOfTopics]) are ignored on decode.
     *
     * @param b64OrPrefixed Either a CSV-style `"B64:<base64>"` string or a bare Base64 payload.
     * @param numberOfTopics Expected length N of the returned boolean mask.
     * @return A [BooleanArray] of size N with `true` for selected topics and `false` otherwise.
     */
    private fun decodeBase64MaskToBooleanArray(b64OrPrefixed: String, numberOfTopics: Int): BooleanArray {
        val base64Payload = if (b64OrPrefixed.startsWith("B64:")) b64OrPrefixed.substring(4) else b64OrPrefixed
        val byteBuffer = java.util.Base64.getDecoder().decode(base64Payload)

        val booleanMask = BooleanArray(numberOfTopics)
        var topicIndex = 0
        var byteOffset = 0

        while (byteOffset < byteBuffer.size && topicIndex < numberOfTopics) {
            /* assemble one 64-bit little-endian word */
            var word = 0L
            var shift = 0
            for (i in 0 until java.lang.Long.BYTES) {
                if (byteOffset + i >= byteBuffer.size) break
                val b = (byteBuffer[byteOffset + i].toInt() and 0xFF)
                word = word or (b.toLong() shl shift)
                shift += 8
            }
            byteOffset += java.lang.Long.BYTES

            /* expand LSB-first bits */
            var bit = 0
            while (bit < 64 && topicIndex < numberOfTopics) {
                booleanMask[topicIndex] = ((word ushr bit) and 1L) != 0L
                bit++
                topicIndex++
            }
        }
        return booleanMask
    }

    /**
     * Encodes a boolean mask into the CSV VAR/TOP form: `"B64:<base64>"`.
     *
     * The payload is packed as 64-bit words (LSB-first), serialized as little-endian bytes,
     * and Base64-encoded without padding, then prefixed with `"B64:"`.
     *
     * @param mask Boolean selection mask of length N.
     * @return CSV-style `"B64:<base64>"` string.
     * @see encodeMaskToBareBase64
     */
    private fun encodeMaskToVarLineBase64(mask: BooleanArray): String =
        "B64:" + encodeMaskToBareBase64(mask)

    /**
     * Encodes a boolean mask into a **bare** Base64 payload suitable for Parquet.
     *
     * Packing rules:
     * - Accumulate 64 bits LSB-first into a 64-bit word.
     * - Serialize each word as 8 bytes in little-endian order.
     * - Base64-encode the concatenated bytes **without padding**.
     *
     * @param mask Boolean selection mask of length N.
     * @return Base64 string without any prefix.
     */
    private fun encodeMaskToBareBase64(mask: BooleanArray): String {
        val n = mask.size
        val words = (n + 63) ushr 6
        val packed = LongArray(words)
        var acc = 0L
        var bitInWord = 0
        var w = 0
        var i = 0
        while (i < n) {
            if (mask[i]) acc = acc or (1L shl bitInWord)
            bitInWord++
            if (bitInWord == 64) {
                packed[w++] = acc
                acc = 0L
                bitInWord = 0
            }
            i++
        }
        if (bitInWord != 0) packed[w] = acc

        val bytes = ByteArray(words * java.lang.Long.BYTES)
        var off = 0
        for (word in packed) {
            var x = word
            repeat(java.lang.Long.BYTES) {
                bytes[off++] = (x and 0xFF).toByte()
                x = x ushr 8
            }
        }
        return java.util.Base64.getEncoder().withoutPadding().encodeToString(bytes)
    }

    /* ----------------------------------------------------------------------------------------------------------------
     * Tests: model behavior (AVERAGE branch) and encoding round-trips
     * ----------------------------------------------------------------------------------------------------------------
     */

    /**
     * Verifies that the AVERAGE path streams rows and caches **exactly one representative** per
     * cardinality K = 1..N, each with a finite correlation and a mask of size N and weight K.
     *
     * Also verifies that requested percentiles are present and provide one value per K.
     */
    @Test
    @DisplayName("AVERAGE path caches exactly one representative per K = 1..N (mask size=N, |mask|=K)")
    fun testSolveAverageEmitsOnePerCardinality() {
        // AVERAGE path now streams rows and caches one representative per K (no big in-memory list).
        val controller = DatasetController(Constants.TARGET_AVERAGE)
        controller.load("src/test/resources/AP96.csv")
        val model = controller.models[0]
        assertTrue(
            model.numberOfTopics > 0,
            "Fixture AP96.csv not found or empty; numberOfTopics should be > 0."
        )

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

        // Run once: fills per-K caches (corrByK + rep mask) and percentiles.
        model.solve(parameters)

        val n = model.numberOfTopics
        var seen = 0
        for (k in 1..n) {
            // correlation must exist and be finite for each K
            val corr = model.findCorrelationForCardinality(k.toDouble())
            assertTrue(corr != null && corr.isFinite(), "Expected a finite correlation for K=$k.")

            // representative mask must exist, be size N, and have exactly K bits set
            val mask = model.retrieveMaskForCardinality(k.toDouble())
            assertTrue(mask != null, "Expected a representative mask for K=$k.")
            assertEquals(n, mask!!.size, "Mask size mismatch for K=$k.")
            assertEquals(k, mask.count { it }, "Mask cardinality must equal K for K=$k.")

            seen++
        }

        assertEquals(n, seen, "Expected exactly one representative per cardinality K = 1..N.")

        // Percentiles: one value per K for each requested percentile
        val expected = setOf(1, 5, 25, 99)
        assertTrue(model.percentiles.keys.containsAll(expected), "Missing requested percentiles.")
        expected.forEach { p ->
            val values = model.percentiles[p] ?: emptyList()
            assertEquals(n, values.size, "Percentile $p should have one value per K (1..$n).")
            assertTrue(values.all { it.isFinite() }, "Percentile $p contains non-finite values.")
        }
    }

    /**
     * Ensures that CSV-style `"B64:<payload>"` packed masks round-trip to the original boolean mask
     * for a variety of sizes, including non-multiples of 64.
     */
    @Test
    @DisplayName("Packed Base64 VAR round-trip equals original mask (CSV B64-prefixed)")
    fun testPackedBase64CsvPrefixedRoundTrip() {
        val sizes = listOf(1, 7, 8, 9, 63, 64, 65, 127, 128, 257, 511)
        val rnd = Random(1234)

        for (n in sizes) {
            val original = BooleanArray(n) { rnd.nextBoolean() }
            val varLine = encodeMaskToVarLineBase64(original)     // "B64:<payload>"
            val decoded = decodeBase64MaskToBooleanArray(varLine, n)
            assertArrayEquals(original, decoded, "Round-trip failed for N=$n (CSV B64-prefixed).")
        }
    }

    /**
     * Ensures that Parquet-style **bare** Base64 payloads round-trip to the original boolean mask
     * for a variety of sizes, including non-multiples of 64.
     */
    @Test
    @DisplayName("Packed Base64 VAR round-trip equals original mask (Parquet bare payload)")
    fun testPackedBase64ParquetBareRoundTrip() {
        val sizes = listOf(1, 7, 8, 9, 63, 64, 65, 127, 128, 257, 511)
        val rnd = Random(42)

        for (n in sizes) {
            val original = BooleanArray(n) { rnd.nextBoolean() }
            val bare = encodeMaskToBareBase64(original)           // "<payload>"
            val decoded = decodeBase64MaskToBooleanArray(bare, n)
            assertArrayEquals(original, decoded, "Round-trip failed for N=$n (Parquet bare).")
        }
    }

    /**
     * Validates tricky patterns around 64-bit word boundaries for multiple lengths.
     * Patterns include: all zeros, all ones, alternating `1010…`, and single-bit masks near
     * boundaries and ends.
     */
    @Test
    @DisplayName("Edge patterns decode correctly across 64-bit word boundaries")
    fun testEdgePatternsAcrossWordBoundaries() {
        val sizes = listOf(1, 63, 64, 65, 127, 128, 129)

        for (n in sizes) {
            // all zeros
            run {
                val m = BooleanArray(n) { false }
                assertArrayEquals(m, decodeBase64MaskToBooleanArray(encodeMaskToVarLineBase64(m), n), "Zeros failed N=$n (prefixed).")
                assertArrayEquals(m, decodeBase64MaskToBooleanArray(encodeMaskToBareBase64(m), n), "Zeros failed N=$n (bare).")
            }
            // all ones
            run {
                val m = BooleanArray(n) { true }
                assertArrayEquals(m, decodeBase64MaskToBooleanArray(encodeMaskToVarLineBase64(m), n), "Ones failed N=$n (prefixed).")
                assertArrayEquals(m, decodeBase64MaskToBooleanArray(encodeMaskToBareBase64(m), n), "Ones failed N=$n (bare).")
            }
            // alternating 1010...
            run {
                val m = BooleanArray(n) { idx -> (idx and 1) == 0 }
                assertArrayEquals(m, decodeBase64MaskToBooleanArray(encodeMaskToVarLineBase64(m), n), "Alternating failed N=$n (prefixed).")
                assertArrayEquals(m, decodeBase64MaskToBooleanArray(encodeMaskToBareBase64(m), n), "Alternating failed N=$n (bare).")
            }
            // single bits near boundaries + ends
            run {
                val interesting = listOf(0, 1, 62, 63, 64, 65, n - 2, n - 1).filter { it in 0 until n }
                for (pos in interesting) {
                    val m = BooleanArray(n) { false }.also { it[pos] = true }
                    assertArrayEquals(m, decodeBase64MaskToBooleanArray(encodeMaskToVarLineBase64(m), n), "Single bit at $pos failed N=$n (prefixed).")
                    assertArrayEquals(m, decodeBase64MaskToBooleanArray(encodeMaskToBareBase64(m), n), "Single bit at $pos failed N=$n (bare).")
                }
            }
        }
    }

    /* ----------------------------------------------------------------------------------------------------------------
     * Extra lightweight checks: helpers around presence/mask sizing and tokens/paths
     * ----------------------------------------------------------------------------------------------------------------
     */

    /**
     * Checks that `retrieveMaskB64ForCardinality` returns a `"B64:"`-prefixed string and
     * that decoding it yields exactly `numberOfTopics` bits.
     */
    @Test
    @DisplayName("Presence helpers: B64 string has prefix and correct length semantics")
    fun testRetrieveMaskB64Helpers() {
        val controller = DatasetController(Constants.TARGET_AVERAGE)
        controller.load("src/test/resources/AP96.csv")
        val model = controller.models[0]
        assertTrue(model.numberOfTopics > 0, "Fixture AP96.csv not found or empty.")

        val parameters = Parameters(
            datasetName = "AH99",
            correlationMethod = Constants.CORRELATION_PEARSON,
            targetToAchieve = Constants.TARGET_AVERAGE,
            numberOfIterations = 0,
            numberOfRepetitions = 10,
            populationSize = 0,
            currentExecution = 0,
            percentiles = emptyList()
        )
        model.solve(parameters)

        val n = model.numberOfTopics
        val k = (n / 2).coerceAtLeast(1).toDouble()
        val b64 = model.retrieveMaskB64ForCardinality(k)
        assertTrue(b64.startsWith("B64:"), "retrieveMaskB64ForCardinality must return a 'B64:'-prefixed string.")

        // decode back and ensure size is exactly N
        val decoded = decodeBase64MaskToBooleanArray(b64, n)
        assertEquals(n, decoded.size, "Decoded mask must be sized to numberOfTopics.")
    }

    /**
     * Verifies that `retrieveMaskForCardinalitySized` returns a mask that is padded or truncated
     * to the requested size while preserving the original prefix bits.
     */
    @Test
    @DisplayName("retrieveMaskForCardinalitySized pads/truncates to expected size")
    fun testRetrieveMaskForCardinalitySized() {
        val controller = DatasetController(Constants.TARGET_AVERAGE)
        controller.load("src/test/resources/AP96.csv")
        val model = controller.models[0]
        assertTrue(model.numberOfTopics > 0, "Fixture AP96.csv not found or empty.")

        val parameters = Parameters(
            datasetName = "AH99",
            correlationMethod = Constants.CORRELATION_PEARSON,
            targetToAchieve = Constants.TARGET_AVERAGE,
            numberOfIterations = 0,
            numberOfRepetitions = 10,
            populationSize = 0,
            currentExecution = 0,
            percentiles = emptyList()
        )
        model.solve(parameters)

        val n = model.numberOfTopics
        val k = (n / 3).coerceAtLeast(1).toDouble()

        // smaller than N
        val smaller = model.retrieveMaskForCardinalitySized(k, expectedSize = (n / 2).coerceAtLeast(1))
        assertEquals((n / 2).coerceAtLeast(1), smaller.size)

        // larger than N
        val larger = model.retrieveMaskForCardinalitySized(k, expectedSize = n + 10)
        assertEquals(n + 10, larger.size)
        // the prefix should match the real mask; suffix should be padded with false
        val real = model.retrieveMaskForCardinality(k)!!
        assertTrue(real.indices.all { real[it] == larger[it] }, "Prefix of padded mask must match the real mask.")
        assertTrue((n until larger.size).all { !larger[it] }, "Padded tail must be all false.")
    }

    /* ----------------------------------------------------------------------------------------------------------------
     * Light path/token checks: correlation must appear in names (no heavy I/O)
     * ----------------------------------------------------------------------------------------------------------------
     */

    /**
     * Confirms that the params token produced by [Tools.buildParamsToken] includes:
     * - the correlation method
     * - core dimensions (topics/systems/population)
     * - percentiles encoded as `pe<first>_<last>`
     */
    @Test
    @DisplayName("Params token includes correlation method and core dimensions")
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

    /**
     * Checks that the final log filename is parameterized with the provided token and the
     * current run timestamp marker.
     */
    @Test
    @DisplayName("Final log filename is parameterized with token and run timestamp")
    fun testFinalLogFilenameUsesTokenAndTimestamp() {
        val paramsToken = "DS-${Constants.CORRELATION_PEARSON}-top10-sys5-po50-i1000"
        val finalLogPath = it.uniud.newbestsub.utils.LogManager.buildFinalLogFilePathFromParams(paramsToken)
        assertTrue(finalLogPath.endsWith(".log"), "Final log path must end with .log")
        assertTrue(finalLogPath.contains(paramsToken), "Final log path must contain the params token.")
        assertTrue(finalLogPath.contains(Constants.RUN_TIMESTAMP), "Final log path must include the run timestamp.")
    }

    /* ----------------------------------------------------------------------------------------------------------------
     * Natural vs. internal correlation semantics (BEST & WORST)
     * ----------------------------------------------------------------------------------------------------------------
     */

    /**
     * We run a tiny synthetic dataset (3 systems × 4 topics), stream events, and assert:
     * - Emitted correlations are on the NATURAL scale.
     * - BEST emits strictly increasing corr per K; WORST emits strictly decreasing corr per K.
     * - The model cache (findCorrelationForCardinality) equals the last emitted improvement per K.
     * - TOP blocks are ordered by NATURAL corr (BEST: desc, WORST: asc) with NaNs ranked last (suffix).
     */
    @Test
    @DisplayName("BEST/WORST: NATURAL corr streaming monotonicity + TOP ordering with NaN suffix")
    fun testBestWorstNaturalCorrelationMonotoneAndTop() {
        println("[Natural/Internal] - Test begins.")

        // --- Build a tiny dataset on the fly (3 systems × 4 topics) ---
        val tmpCsv = java.nio.file.Files.createTempFile("nbs-mini", ".csv")
        val csv = buildString {
            appendLine("sys,t1,t2,t3,t4")
            appendLine("S1,0.20,0.40,0.60,0.80")
            appendLine("S2,0.10,0.50,0.70,0.90")
            appendLine("S3,0.30,0.30,0.80,0.70")
        }
        java.nio.file.Files.writeString(tmpCsv, csv)

        data class RunOut(
            val improvementsByK: Map<Int, List<Double>>,
            val lastTopByK: Map<Int, List<String>>,
            val model: DatasetModel
        )

        fun runOnce(target: String): RunOut {
            val model = DatasetModel().apply { loadData(tmpCsv.toString()) }

            val params = Parameters(
                datasetName = "MINI",
                correlationMethod = Constants.CORRELATION_PEARSON,
                targetToAchieve = target,
                numberOfIterations = 2_000,    // small but enough to see improvements
                numberOfRepetitions = 0,       // not used in BEST/WORST
                populationSize = 8,            // must be >= numberOfTopics (4)
                currentExecution = 0,
                percentiles = emptyList()
            )

            val chan = Channel<it.uniud.newbestsub.dataset.model.ProgressEvent>(Channel.UNLIMITED)
            val improvementsByK = mutableMapOf<Int, MutableList<Double>>()
            val lastTopBlockByK = mutableMapOf<Int, List<String>>()

            // Run and then drain events (UNLIMITED channel avoids back-pressure)
            model.solve(params, chan)
            chan.close()

            runBlocking {
                for (ev in chan) {
                    when (ev) {
                        is it.uniud.newbestsub.dataset.model.CardinalityResult -> {
                            improvementsByK.getOrPut(ev.cardinality) { mutableListOf() }.add(ev.correlation)
                        }
                        is it.uniud.newbestsub.dataset.model.TopKReplaceBatch -> {
                            ev.blocks.forEach { (k, lines) -> lastTopBlockByK[k] = lines }
                        }
                        else -> { /* ignore */ }
                    }
                }
            }

            return RunOut(
                improvementsByK = improvementsByK.mapValues { it.value.toList() },
                lastTopByK = lastTopBlockByK.toMap(),
                model = model
            )
        }

        // --- Run BEST and WORST
        val best = runOnce(Constants.TARGET_BEST)
        val worst = runOnce(Constants.TARGET_WORST)

        // --- Assert monotone improvements per K (strict)
        fun assertStrictMonotone(name: String, byK: Map<Int, List<Double>>, increasing: Boolean) {
            println("[Natural/Internal] - Monotonicity: $name")
            for ((k, seq) in byK.toSortedMap()) {
                if (seq.size <= 1) {
                    println("  K=$k: only ${seq.size} improvement(s) observed — OK")
                    continue
                }
                for (i in 1 until seq.size) {
                    val a = seq[i - 1]; val b = seq[i]
                    if (increasing) {
                        assertTrue(b > a + 1e-12, "BEST K=$k not strictly increasing at i=$i: $a -> $b")
                    } else {
                        assertTrue(b < a - 1e-12, "WORST K=$k not strictly decreasing at i=$i: $a -> $b")
                    }
                }
                println("  K=$k: seq=${seq.joinToString(prefix = "[", postfix = "]")}")
            }
        }
        assertStrictMonotone("BEST (↑)", best.improvementsByK, increasing = true)
        assertStrictMonotone("WORST (↓)", worst.improvementsByK, increasing = false)

        // --- Cached NATURAL reps must equal last emitted improvement per K
        fun assertCacheMatchesLast(name: String, out: RunOut) {
            println("[Natural/Internal] - Cache check: $name")
            for ((k, seq) in out.improvementsByK) {
                val cached = out.model.findCorrelationForCardinality(k.toDouble())
                val last = seq.last()
                assertNotNull(cached, "Expected cached correlation for K=$k")
                assertEquals(last, cached!!, 1e-12, "Cached NATURAL corr must equal last improvement for K=$k")
            }
        }
        assertCacheMatchesLast("BEST", best)
        assertCacheMatchesLast("WORST", worst)

        // --- TOP ordering by NATURAL correlation with NaN suffix rule
        fun parseCorr(line: String): Double = line.split(',', limit = 3)[1].trim().toDouble()

        fun assertTopOrdered(
            label: String,
            blocks: Map<Int, List<String>>,
            ascending: Boolean
        ) {
            println("[Natural/Internal] - TOP ordering: $label (${if (ascending) "asc" else "desc"})")
            blocks.toSortedMap().forEach { (k, lines) ->
                val corrs = lines.map(::parseCorr)
                // NaNs must be a suffix (ranked last by production sorter)
                val firstNaN = corrs.indexOfFirst { it.isNaN() }
                if (firstNaN >= 0) {
                    val suffixAllNaN = corrs.drop(firstNaN).all { it.isNaN() }
                    assertTrue(suffixAllNaN, "TOP K=$k: NaNs must form a contiguous suffix")
                }
                // Finite prefix must be monotone in NATURAL order
                val finite = if (firstNaN >= 0) corrs.subList(0, firstNaN) else corrs
                for (i in 1 until finite.size) {
                    val a = finite[i - 1]; val b = finite[i]
                    if (ascending) {
                        assertTrue(a <= b + 1e-12, "TOP K=$k not asc at i=$i: $a -> $b")
                    } else {
                        assertTrue(a >= b - 1e-12, "TOP K=$k not desc at i=$i: $a -> $b")
                    }
                }
                if (finite.isNotEmpty()) {
                    println("  K=$k: first=${finite.first()}, last=${finite.last()}, NaN_suffix=${corrs.size - finite.size}")
                } else {
                    println("  K=$k: all NaN (${corrs.size} entries)")
                }
            }
        }

        assertTopOrdered(label = "BEST",  blocks = best.lastTopByK,  ascending = false)
        assertTopOrdered(label = "WORST", blocks = worst.lastTopByK, ascending = true)

        // Cleanup
        runCatching { java.nio.file.Files.deleteIfExists(tmpCsv) }
        println("[Natural/Internal] - Test ends.")
    }
}
