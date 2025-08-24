package it.uniud.newbestsub.utils

import it.uniud.newbestsub.dataset.Parameters
import java.nio.ByteBuffer
import java.security.MessageDigest

/**
 * Utility helpers for filename sanitization, run-token construction, and deterministic seeding.
 *
 * The functions here are pure and side-effect free. They are used by the CLI and dataset layers
 * to build stable, human-readable folder names and to derive reproducible seeds from a parameter set.
 */
object Tools {

    // -------------------------------------------------------------------------
    // Sanitize & Name Builders
    // -------------------------------------------------------------------------

    /**
     * Sanitizes an arbitrary string so that it is safe to use as part of a filename or path segment.
     *
     * Transformations:
     * - Replaces characters invalid on common filesystems (`\ / : * ? " < > |`) with `-`.
     * - Collapses runs of whitespace into a single `-`.
     * - Collapses runs of `-` into a single `-`.
     * - Trims leading and trailing `-`.
     *
     * @param part Arbitrary input text to sanitize.
     * @return A filesystem-friendly token.
     *
     * @sample
     * val s = Tools.sanitizeForFile("ACME: Results  2025/08/24")
     * // s == "ACME-Results-2025-08-24"
     */
    fun sanitizeForFile(part: String): String = part
        .replace("[\\\\/:*?\"<>|]".toRegex(), "-")
        .replace("\\s+".toRegex(), "-")
        .replace("-{2,}".toRegex(), "-")
        .trim('-')

    /**
     * Builds a base name by joining sanitized, non-blank parts with the global file name separator.
     *
     * Empty or `null` parts are ignored. If no valid parts remain, the fallback `"Run"` is returned.
     *
     * @param parts Arbitrary parts to concatenate (any type; converted via `toString()`).
     * @return A sanitized base name, or `"Run"` if nothing usable is provided.
     *
     * @see sanitizeForFile
     */
    fun folderBaseName(vararg parts: Any?): String =
        parts.asSequence()
            .filterNotNull()
            .map { it.toString() }
            .map { sanitizeForFile(it) }
            .filter { it.isNotBlank() }
            .joinToString(Constants.FILE_NAME_SEPARATOR)
            .ifBlank { "Run" }

    /**
     * Builds the parameter **token** used in filenames and containers (without timestamp and target).
     *
     * Shape:
     * ```
     * <dataset>-<correlation>-top<T>-sys<S>-po<P>-i<I>-r<R>-mx<M>[ -peA_B ]
     * ```
     * where:
     * - `top<T>`: number of topics (optional, included only if `> 0`)
     * - `sys<S>`: number of systems (optional)
     * - `po<P>`: population size (optional)
     * - `i<I>`: number of iterations (optional)
     * - `r<R>`: number of repetitions (optional)
     * - `mx<M>`: expansion coefficient (optional)
     * - `peA_B`: percentile range (first–last), included only if `includePercentiles` is true and the list is non-empty
     *
     * @param datasetName Dataset identifier (already human-readable).
     * @param correlationMethod Correlation key (e.g., `Pearson`, `Kendall`).
     * @param numberOfTopics Number of topics (`top<T>`), included if `> 0`.
     * @param numberOfSystems Number of systems (`sys<S>`), included if `> 0`.
     * @param populationSize Population size (`po<P>`), included if `> 0`.
     * @param numberOfIterations Iterations (`i<I>`), included if `> 0`.
     * @param numberOfRepetitions Repetitions (`r<R>`), included if `> 0`.
     * @param expansionCoefficient Expansion coefficient (`mx<M>`), included if `> 0`.
     * @param includePercentiles If true and `percentiles` is non-empty, appends `peA_B` using the first and last values.
     * @param percentiles Ordered list of percentiles; only the first and last are used in the token.
     * @return A deterministic token without timestamp and target.
     *
     * @sample
     * val token = Tools.buildParamsToken(
     *   datasetName = "AH99",
     *   correlationMethod = "Pearson",
     *   numberOfTopics = 50,
     *   numberOfSystems = 129,
     *   populationSize = 1000,
     *   numberOfIterations = 10000,
     *   numberOfRepetitions = 2000,
     *   expansionCoefficient = 3,
     *   includePercentiles = true,
     *   percentiles = listOf(5, 10, 25, 50)
     * )
     * // "AH99-Pearson-top50-sys129-po1000-i10000-r2000-mx3-pe5_50"
     */
    fun buildParamsToken(
        datasetName: String,
        correlationMethod: String,
        numberOfTopics: Int,
        numberOfSystems: Int,
        populationSize: Int,
        numberOfIterations: Int,
        numberOfRepetitions: Int,
        expansionCoefficient: Int,
        includePercentiles: Boolean,
        percentiles: List<Int>
    ): String {
        val parts = mutableListOf<String>()
        parts += datasetName
        parts += correlationMethod
        if (numberOfTopics > 0) parts += "top$numberOfTopics"
        if (numberOfSystems > 0) parts += "sys$numberOfSystems"
        if (populationSize > 0) parts += "po$populationSize"
        if (numberOfIterations > 0) parts += "i$numberOfIterations"
        if (numberOfRepetitions > 0) parts += "r$numberOfRepetitions"
        if (expansionCoefficient > 0) parts += "mx$expansionCoefficient"

        if (includePercentiles && percentiles.isNotEmpty()) {
            val first = percentiles.first()
            val last = percentiles.last()
            parts += "pe${first}_${last}"
        }

        return folderBaseName(*parts.toTypedArray())
    }

    /**
     * Builds a **container folder name** (target-agnostic) by appending a run timestamp
     * to the parameter token produced by [buildParamsToken].
     *
     * Shape:
     * ```
     * <dataset>-<correlation>-top<T>-sys<S>-po<P>-i<I>-r<R>-mx<M>[ -peA_B ]-time<RUN_TIMESTAMP>
     * ```
     *
     * @param datasetName Dataset identifier (already human-readable).
     * @param correlationMethod Correlation key (e.g., `Pearson`, `Kendall`).
     * @param numberOfTopics Number of topics (`top<T>`), included if `> 0`.
     * @param numberOfSystems Number of systems (`sys<S>`), included if `> 0`.
     * @param populationSize Population size (`po<P>`), included if `> 0`.
     * @param numberOfIterations Iterations (`i<I>`), included if `> 0`.
     * @param numberOfRepetitions Repetitions (`r<R>`), included if `> 0`.
     * @param expansionCoefficient Expansion coefficient (`mx<M>`), included if `> 0`.
     * @param includePercentiles If true and `percentiles` is non-empty, appends `peA_B` using the first and last values.
     * @param percentiles Ordered list of percentiles; only the first and last are used in the token.
     * @return A target-agnostic container folder name with `-time<RUN_TIMESTAMP>` appended.
     *
     * @see buildParamsToken
     * @see Constants.RUN_TIMESTAMP
     */
    fun buildContainerFolderName(
        datasetName: String,
        correlationMethod: String,
        numberOfTopics: Int,
        numberOfSystems: Int,
        populationSize: Int,
        numberOfIterations: Int,
        numberOfRepetitions: Int,
        expansionCoefficient: Int,
        includePercentiles: Boolean,
        percentiles: List<Int>
    ): String {
        val parts = mutableListOf<String>()
        parts += datasetName
        parts += correlationMethod
        if (numberOfTopics > 0) parts += "top$numberOfTopics"
        if (numberOfSystems > 0) parts += "sys$numberOfSystems"
        if (populationSize > 0) parts += "po$populationSize"
        if (numberOfIterations > 0) parts += "i$numberOfIterations"
        if (numberOfRepetitions > 0) parts += "r$numberOfRepetitions"
        if (expansionCoefficient > 0) parts += "mx$expansionCoefficient"

        if (includePercentiles && percentiles.isNotEmpty()) {
            val first = percentiles.first()
            val last = percentiles.last()
            parts += "pe${first}_${last}"
        }

        val token = folderBaseName(*parts.toTypedArray())
        return token + Constants.FILE_NAME_SEPARATOR + "time" + Constants.RUN_TIMESTAMP
    }

    // -------------------------------------------------------------------------
    // Deterministic Seed Helper
    // -------------------------------------------------------------------------

    /**
     * Builds a deterministic 64-bit seed from a [Parameters] snapshot.
     *
     * The seed is derived by hashing a canonical string signature (UTF‑8) with SHA‑256
     * and interpreting the first 8 bytes as a signed big‑endian `Long`.
     *
     * **Signature layout** (fields separated by `'|'`):
     * ```
     * datasetName | correlationMethod | targetToAchieve | numberOfIterations |
     * numberOfRepetitions | populationSize | percentilesCSV
     * ```
     *
     * Notes:
     * - This seed is stable for the same [Parameters] content.
     * - Changing the layout or ordering will change all derived seeds (by design).
     *
     * @param p Parameter set to encode.
     * @return A reproducible `Long` seed suitable for RNG initialization.
     */
    fun stableSeedFrom(p: Parameters): Long {
        val signature = buildString {
            append(p.datasetName).append('|')
            append(p.correlationMethod).append('|')
            append(p.targetToAchieve).append('|')
            append(p.numberOfIterations).append('|')
            append(p.numberOfRepetitions).append('|')
            append(p.populationSize).append('|')
            append(p.percentiles.joinToString(","))
        }
        val sha = MessageDigest.getInstance("SHA-256").digest(signature.toByteArray())
        return ByteBuffer.wrap(sha, 0, 8).long
    }
}
