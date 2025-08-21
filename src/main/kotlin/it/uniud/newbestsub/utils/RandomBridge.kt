package it.uniud.newbestsub.utils

import org.uma.jmetal.util.pseudorandom.JMetalRandom
import org.uma.jmetal.util.pseudorandom.PseudoRandomGenerator
import java.util.SplittableRandom

/*
 * RandomBridge
 * ---------
 * Deterministic RNG plumbing for jMetal.
 *
 * API expectations (your jMetal variant):
 *  - PseudoRandomGenerator requires:
 *      nextDouble(): Double
 *      nextDouble(lower: Double, upper: Double): Double
 *      nextInt(lower: Int, upper: Int): Int
 *      setSeed(seed: Long): Unit
 *      getSeed(): Long
 *      getName(): String
 *
 * We therefore DO NOT override nextInt() or nextLong() (they are not in your interface).
 * Program.kt ensures sequential execution when deterministic mode is enabled.
 */
object RandomBridge {

    /* Master seed installed when deterministic mode is enabled */
    private var masterSeed: Long = 0L
    private var installed: Boolean = false

    /* Golden ratio increment (signed literal to avoid overflow in Kotlin) */
    private const val GOLDEN_GAMMA: Long = -0x61C8_8646_80B5_83EBL  /* 0x9E3779B97F4A7C15 */

    /*
     * Adapter: SplittableRandom → PseudoRandomGenerator
     * Implements exactly the methods required by your jMetal interface.
     */
    private class SplittableAdapter(seed: Long) : PseudoRandomGenerator {
        private var currentSeed: Long = seed
        private var rng: SplittableRandom = SplittableRandom(currentSeed)

        override fun nextDouble(): Double = rng.nextDouble()

        override fun nextDouble(lowerBound: Double, upperBound: Double): Double {
            if (upperBound <= lowerBound) return lowerBound
            return rng.nextDouble(lowerBound, upperBound)
        }

        override fun nextInt(lowerBound: Int, upperBound: Int): Int {
            if (upperBound <= lowerBound) return lowerBound
            val width = upperBound - lowerBound
            return lowerBound + rng.nextInt(width)
        }

        override fun setSeed(seed: Long) {
            currentSeed = seed
            rng = SplittableRandom(currentSeed)
        }

        override fun getSeed(): Long = currentSeed

        override fun getName(): String = "SplittableRandomAdapter(seed=$currentSeed)"
    }

    /*
     * Install a deterministic master generator into jMetal.
     */
    @Synchronized
    fun installDeterministic(seed: Long) {
        masterSeed = seed
        val adapter = SplittableAdapter(masterSeed)
        JMetalRandom.getInstance().randomGenerator = adapter
        installed = true
    }

    /*
     * Derive a stable child seed for a labeled sub-run (e.g., "BEST", "WORST", "AVERAGE").
     */
    fun childSeed(label: String, index: Int = 0): Long {
        val h = label.hashCode().toLong()
        val base = masterSeed xor (h shl 32) xor index.toLong()
        return mix64(base + GOLDEN_GAMMA)
    }

    /*
     * Temporarily replace jMetal's generator for the duration of 'block', then restore it.
     * Use only in sequential sections.
     */
    fun <T> withSeed(seed: Long, block: () -> T): T {
        val jmr = JMetalRandom.getInstance()
        val prev: PseudoRandomGenerator = jmr.randomGenerator
        val temp = SplittableAdapter(seed)
        jmr.randomGenerator = temp
        try {
            return block()
        } finally {
            jmr.randomGenerator = prev
        }
    }

    fun isInstalled(): Boolean = installed

    /*
     * SplitMix64 finalizer (signed literals to avoid "Value out of range").
     * Constants: -0x40A7_B892_E31B_1A47L and -0x6B2F_B644_ECCE_EE15L
     */
    private fun mix64(z0: Long): Long {
        var z = z0
        z = (z xor (z ushr 30)) * -0x40A7_B892_E31B_1A47L
        z = (z xor (z ushr 27)) * -0x6B2F_B644_ECCE_EE15L
        z = z xor (z ushr 31)
        return z
    }
}
