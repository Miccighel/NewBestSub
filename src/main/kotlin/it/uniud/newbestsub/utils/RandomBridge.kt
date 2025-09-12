package it.uniud.newbestsub.utils

import org.uma.jmetal.util.pseudorandom.JMetalRandom
import org.uma.jmetal.util.pseudorandom.PseudoRandomGenerator
import java.util.SplittableRandom

/**
 * Bridges a deterministic RNG to jMetal's [PseudoRandomGenerator] interface.
 *
 * Centralizes random generation so experiments are reproducible under a single master seed.
 * Installs a thin adapter over [SplittableRandom] that implements only the methods required
 * by the jMetal interface in this project.
 *
 * Design notes:
 * • Installation is explicit: call [installDeterministic] once at startup to set the master seed.
 * • Use [childSeed] to derive stable seeds for labeled sub-runs (for example "BEST", "WORST", "AVERAGE").
 * • Use [withSeed] to temporarily swap the jMetal RNG inside a sequential block and then restore it.
 *
 * Determinism:
 * • Guaranteed for the same JVM, jMetal version, adapter logic, and identical call order.
 * • Program-level sequential execution must be ensured by the caller when determinism is requested.
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
     * • [nextDouble]
     * • [nextDouble] with bounds
     * • [nextInt] with bounds
     * • [setSeed]
     * • [getSeed]
     * • [getName]
     *
     * The adapter intentionally avoids implementing unused methods such as `nextInt()` or
     * `nextLong()` without bounds to keep behavior surface minimal and stable.
     */
    private class SplittableAdapter(seed: Long) : PseudoRandomGenerator {
        private var currentSeed: Long = seed
        private var rng: SplittableRandom = SplittableRandom(currentSeed)

        /** Uniform double in [0.0, 1.0). */
        override fun nextDouble(): Double = rng.nextDouble()

        /**
         * Uniform double in [lowerBound, upperBound).
         *
         * Edge cases:
         * • If `upperBound <= lowerBound`, returns `lowerBound` to avoid exceptions and undefined widths.
         */
        override fun nextDouble(lowerBound: Double, upperBound: Double): Double {
            if (upperBound <= lowerBound) return lowerBound
            return rng.nextDouble(lowerBound, upperBound)
        }

        /**
         * Uniform int in [lowerBound, upperBound).
         *
         * Edge cases:
         * • If `upperBound <= lowerBound`, returns `lowerBound`.
         * Implementation detail:
         * • Uses `rng.nextInt(width)` with `width = upperBound - lowerBound` which is guaranteed ≥ 1 here.
         */
        override fun nextInt(lowerBound: Int, upperBound: Int): Int {
            if (upperBound <= lowerBound) return lowerBound
            val width = upperBound - lowerBound
            return lowerBound + rng.nextInt(width)
        }

        /**
         * Re-seeds the adapter.
         *
         * Contract:
         * • Replacing the seed replaces the underlying [SplittableRandom] instance to ensure
         *   the next draw begins from the new seed.
         */
        override fun setSeed(seed: Long) {
            currentSeed = seed
            rng = SplittableRandom(currentSeed)
        }

        /** Returns the current seed used by this adapter. */
        override fun getSeed(): Long = currentSeed

        /** Short diagnostic name useful in logs. */
        override fun getName(): String = "SplittableRandomAdapter(seed=$currentSeed)"
    }

    /**
     * Installs a deterministic RNG into jMetal under the provided master [seed].
     *
     * Mechanics:
     * • Replaces [JMetalRandom.getInstance().randomGenerator] with a [SplittableAdapter].
     * • Marks this bridge as installed so callers can check [isInstalled].
     *
     * Thread-safety:
     * • Synchronized to avoid races when multiple components initialize concurrently.
     * • Determinism still requires sequential use of the RNG by the caller.
     */
    @Synchronized
    fun installDeterministic(seed: Long) {
        masterSeed = seed
        val adapter = SplittableAdapter(masterSeed)
        JMetalRandom.getInstance().randomGenerator = adapter
        installed = true
    }

    /**
     * Derives a stable child seed from the installed master seed for a labeled sub-run.
     *
     * Recipe:
     * • Combine `masterSeed`, `label.hashCode()` and `index`.
     * • Add a golden-ratio increment to spread seeds across the 64-bit space.
     * • Finalize with SplitMix64 mixer for high-quality diffusion.
     *
     * Preconditions:
     * • Deterministic mode should have been installed via [installDeterministic].
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
     * Use cases:
     * • Short, deterministic sections that need an isolated stream without affecting the global RNG.
     *
     * Guarantees:
     * • The previous generator is restored even if [block] throws.
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

    /** True if a deterministic RNG has been installed into jMetal via [installDeterministic]. */
    fun isInstalled(): Boolean = installed

    /**
     * SplitMix64 finalizer for 64-bit mixing.
     *
     * Uses signed literals to avoid value out-of-range issues in Kotlin.
     * Constants:
     * • `-0x40A7_B892_E31B_1A47L`
     * • `-0x6B2F_B644_ECCE_EE15L`
     */
    private fun mix64(z0: Long): Long {
        var z = z0
        z = (z xor (z ushr 30)) * -0x40A7_B892_E31B_1A47L
        z = (z xor (z ushr 27)) * -0x6B2F_B644_ECCE_EE15L
        z = z xor (z ushr 31)
        return z
    }
}
