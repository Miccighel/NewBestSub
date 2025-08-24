package it.uniud.newbestsub.utils

import org.uma.jmetal.util.pseudorandom.JMetalRandom
import org.uma.jmetal.util.pseudorandom.PseudoRandomGenerator
import java.util.SplittableRandom

/**
 * Bridges a deterministic RNG to jMetal's [PseudoRandomGenerator] interface.
 *
 * This utility centralizes random generation so that experiments are reproducible under a
 * single master seed. It installs an adapter over [SplittableRandom] that implements exactly
 * the methods required by your jMetal variant.
 *
 * Design notes:
 * - Installation is explicit: call [installDeterministic] once at startup to set the master seed.
 * - Use [childSeed] to derive stable seeds for labeled sub‑runs (for example "BEST", "WORST", "AVERAGE").
 * - Use [withSeed] to temporarily swap the jMetal RNG inside a sequential block and then restore it.
 * - The adapter intentionally does not override methods that your interface does not require.
 *
 * Determinism:
 * - Determinism is guaranteed for the same JVM, jMetal version, adapter logic, and identical call order.
 * - Program-level sequential execution must be ensured by the caller when determinism is requested.
 */
object RandomBridge {

    /** Master seed installed when deterministic mode is enabled. */
    private var masterSeed: Long = 0L

    /** Tracks whether a deterministic generator has been installed in jMetal. */
    private var installed: Boolean = false

    /**
     * Golden ratio increment used for decorrelating derived seeds.
     * Signed literal avoids overflow in Kotlin.
     *
     * Hex reference: `0x9E3779B97F4A7C15`
     */
    private const val GOLDEN_GAMMA: Long = -0x61C8_8646_80B5_83EBL

    /**
     * Adapter from [SplittableRandom] to jMetal's [PseudoRandomGenerator].
     *
     * Implements exactly the methods required by the target interface:
     * - [nextDouble]
     * - [nextDouble] with bounds
     * - [nextInt] with bounds
     * - [setSeed]
     * - [getSeed]
     * - [getName]
     *
     * The adapter does not implement other methods such as `nextInt()` or `nextLong()` without bounds,
     * since they are not used by your jMetal interface.
     */
    private class SplittableAdapter(seed: Long) : PseudoRandomGenerator {
        private var currentSeed: Long = seed
        private var rng: SplittableRandom = SplittableRandom(currentSeed)

        /** Returns a uniform double in [0.0, 1.0). */
        override fun nextDouble(): Double = rng.nextDouble()

        /**
         * Returns a uniform double in [lowerBound, upperBound).
         * If `upperBound <= lowerBound`, returns `lowerBound`.
         *
         * @param lowerBound inclusive lower bound
         * @param upperBound exclusive upper bound
         */
        override fun nextDouble(lowerBound: Double, upperBound: Double): Double {
            if (upperBound <= lowerBound) return lowerBound
            return rng.nextDouble(lowerBound, upperBound)
        }

        /**
         * Returns a uniform int in [lowerBound, upperBound).
         * If `upperBound <= lowerBound`, returns `lowerBound`.
         *
         * @param lowerBound inclusive lower bound
         * @param upperBound exclusive upper bound
         */
        override fun nextInt(lowerBound: Int, upperBound: Int): Int {
            if (upperBound <= lowerBound) return lowerBound
            val width = upperBound - lowerBound
            return lowerBound + rng.nextInt(width)
        }

        /** Re-seeds the adapter with the given seed. */
        override fun setSeed(seed: Long) {
            currentSeed = seed
            rng = SplittableRandom(currentSeed)
        }

        /** Returns the current seed. */
        override fun getSeed(): Long = currentSeed

        /** Returns a short diagnostic name including the current seed. */
        override fun getName(): String = "SplittableRandomAdapter(seed=$currentSeed)"
    }

    /**
     * Installs a deterministic RNG into jMetal under the provided master [seed].
     *
     * This replaces [JMetalRandom.getInstance().randomGenerator] with a [SplittableAdapter]
     * and marks this bridge as installed. Call once during program initialization when
     * deterministic mode is required.
     *
     * Thread-safety: synchronized. The replacement itself is atomic, but determinism also
     * requires sequential use of the RNG by the caller.
     *
     * @param seed master seed for the whole experiment run
     */
    @Synchronized
    fun installDeterministic(seed: Long) {
        masterSeed = seed
        val adapter = SplittableAdapter(masterSeed)
        JMetalRandom.getInstance().randomGenerator = adapter
        installed = true
    }

    /**
     * Derives a stable child seed from the installed master seed for a labeled sub‑run.
     *
     * The derivation uses the label hash, the optional index, and a SplitMix64 finalizer
     * with a golden‑ratio increment to decorrelate streams.
     *
     * Determinism requires that [installDeterministic] has been called before.
     *
     * @param label semantic label for the child stream (for example "BEST")
     * @param index optional index to obtain additional distinct seeds under the same label
     * @return a 64‑bit seed suitable for initializing a new RNG
     */
    fun childSeed(label: String, index: Int = 0): Long {
        val h = label.hashCode().toLong()
        val base = masterSeed xor (h shl 32) xor index.toLong()
        return mix64(base + GOLDEN_GAMMA)
    }

    /**
     * Temporarily replaces jMetal's RNG with one seeded by [seed] for the duration of [block],
     * then restores the previous generator.
     *
     * Intended for short, sequential sections that require a specific seed without affecting
     * the global stream. The previous generator is restored even if [block] throws.
     *
     * @param seed temporary seed to use inside [block]
     * @param block code to execute under the temporary RNG
     * @return the result of [block]
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

    /**
     * Indicates whether a deterministic RNG has been installed into jMetal via [installDeterministic].
     *
     * @return true if installed, false otherwise
     */
    fun isInstalled(): Boolean = installed

    /**
     * SplitMix64 finalizer for 64‑bit mixing.
     *
     * Uses signed literals to avoid value out of range issues in Kotlin.
     * Constants:
     * - `-0x40A7_B892_E31B_1A47L`
     * - `-0x6B2F_B644_ECCE_EE15L`
     *
     * @param z0 input value to mix
     * @return mixed 64‑bit output
     */
    private fun mix64(z0: Long): Long {
        var z = z0
        z = (z xor (z ushr 30)) * -0x40A7_B892_E31B_1A47L
        z = (z xor (z ushr 27)) * -0x6B2F_B644_ECCE_EE15L
        z = z xor (z ushr 31)
        return z
    }
}
