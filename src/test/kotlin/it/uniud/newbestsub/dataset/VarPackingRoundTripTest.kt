package it.uniud.newbestsub.dataset

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Test
import kotlin.random.Random

class VarPackingRoundTripTest {

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
        val base64 = if (b64OrPrefixed.startsWith("B64:")) b64OrPrefixed.substring(4) else b64OrPrefixed
        val bytes = java.util.Base64.getDecoder().decode(base64)

        val out = BooleanArray(numberOfTopics)
        var topicIndex = 0
        var byteOffset = 0

        // Read little-endian 64-bit words from the byte array
        while (byteOffset < bytes.size && topicIndex < numberOfTopics) {
            var word = 0L
            var shift = 0
            // assemble one 64-bit word (little-endian)
            for (i in 0 until java.lang.Long.BYTES) {
                if (byteOffset + i >= bytes.size) break
                val b = (bytes[byteOffset + i].toInt() and 0xFF)
                word = word or (b.toLong() shl shift)
                shift += 8
            }
            byteOffset += java.lang.Long.BYTES

            // expand word bits into booleans (LSB-first)
            var bit = 0
            while (bit < 64 && topicIndex < numberOfTopics) {
                out[topicIndex] = ((word ushr bit) and 1L) != 0L
                bit++
                topicIndex++
            }
        }
        return out
    }

    /**
     * Encode a BooleanArray mask into the CSV VAR/TOP line format used by the model: "B64:<base64>".
     * Mirrors the packing layout implemented in the model:
     *  - 64-bit words, LSB-first bits, little-endian bytes, Base64 without padding.
     */
    private fun encodeMaskToVarLineBase64(mask: BooleanArray): String {
        return "B64:" + encodeMaskToBareBase64(mask)
    }

    /**
     * Encode a BooleanArray mask into the Parquet VAR/TOP payload format: bare "<base64>" (no prefix).
     * Same packing as above.
     */
    private fun encodeMaskToBareBase64(mask: BooleanArray): String {
        val n = mask.size
        val words = (n + 63) ushr 6
        val packed = LongArray(words)
        var w = 0
        var bitInWord = 0
        var acc = 0L
        for (i in 0 until n) {
            if (mask[i]) acc = acc or (1L shl bitInWord)
            bitInWord++
            if (bitInWord == 64) {
                packed[w++] = acc
                acc = 0L
                bitInWord = 0
            }
        }
        if (bitInWord != 0) packed[w] = acc

        // to little-endian bytes
        val bytes = ByteArray(words * java.lang.Long.BYTES)
        var off = 0
        for (word in packed) {
            var x = word
            for (i in 0 until java.lang.Long.BYTES) {
                bytes[off + i] = (x and 0xFF).toByte()
                x = x ushr 8
            }
            off += java.lang.Long.BYTES
        }
        return java.util.Base64.getEncoder().withoutPadding().encodeToString(bytes)
    }

    @Test
    fun `packed Base64 VAR round-trip matches original mask (CSV B64-prefixed)`() {
        val sizes = listOf(1, 7, 8, 9, 63, 64, 65, 127, 128, 257, 511)
        val rng = Random(1234)

        for (n in sizes) {
            val mask = BooleanArray(n) { rng.nextBoolean() }
            val varLine = encodeMaskToVarLineBase64(mask)  // "B64:<payload>"
            val decoded = decodeBase64MaskToBooleanArray(varLine, n)
            assertArrayEquals(mask, decoded, "round-trip failed for N=$n (CSV B64-prefixed)")
        }
    }

    @Test
    fun `packed Base64 VAR round-trip matches original mask (Parquet bare)`() {
        val sizes = listOf(1, 7, 8, 9, 63, 64, 65, 127, 128, 257, 511)
        val rng = Random(42)

        for (n in sizes) {
            val mask = BooleanArray(n) { rng.nextBoolean() }
            val bare = encodeMaskToBareBase64(mask)        // "<payload>"
            val decoded = decodeBase64MaskToBooleanArray(bare, n)
            assertArrayEquals(mask, decoded, "round-trip failed for N=$n (Parquet bare)")
        }
    }

    @Test
    fun `edge patterns decode correctly across word boundaries`() {
        val sizes = listOf(1, 63, 64, 65, 127, 128, 129)

        for (n in sizes) {
            // all zeros
            run {
                val mask = BooleanArray(n) { false }
                val pref = encodeMaskToVarLineBase64(mask)
                val bare = encodeMaskToBareBase64(mask)
                assertArrayEquals(mask, decodeBase64MaskToBooleanArray(pref, n), "zeros failed N=$n (pref)")
                assertArrayEquals(mask, decodeBase64MaskToBooleanArray(bare, n), "zeros failed N=$n (bare)")
            }
            // all ones
            run {
                val mask = BooleanArray(n) { true }
                val pref = encodeMaskToVarLineBase64(mask)
                val bare = encodeMaskToBareBase64(mask)
                assertArrayEquals(mask, decodeBase64MaskToBooleanArray(pref, n), "ones failed N=$n (pref)")
                assertArrayEquals(mask, decodeBase64MaskToBooleanArray(bare, n), "ones failed N=$n (bare)")
            }
            // alternating 1010...
            run {
                val mask = BooleanArray(n) { i -> (i and 1) == 0 }
                val pref = encodeMaskToVarLineBase64(mask)
                val bare = encodeMaskToBareBase64(mask)
                assertArrayEquals(mask, decodeBase64MaskToBooleanArray(pref, n), "alt failed N=$n (pref)")
                assertArrayEquals(mask, decodeBase64MaskToBooleanArray(bare, n), "alt failed N=$n (bare)")
            }
            // single bits at interesting positions
            run {
                val positions = listOf(0, 1, 62, 63, 64, 65, n - 2, n - 1).filter { it in 0 until n }
                for (pos in positions) {
                    val mask = BooleanArray(n) { false }.also { it[pos] = true }
                    val pref = encodeMaskToVarLineBase64(mask)
                    val bare = encodeMaskToBareBase64(mask)
                    assertArrayEquals(mask, decodeBase64MaskToBooleanArray(pref, n), "single bit at $pos failed N=$n (pref)")
                    assertArrayEquals(mask, decodeBase64MaskToBooleanArray(bare, n), "single bit at $pos failed N=$n (bare)")
                }
            }
        }
    }
}
