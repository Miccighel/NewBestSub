package it.uniud.newbestsub.utils

import it.uniud.newbestsub.dataset.Parameters
import java.nio.ByteBuffer
import java.security.MessageDigest

/**
 * Utility helpers for filename sanitization, run-token construction, and deterministic seeding.
 *
 * Pure, side-effect-free functions used by the CLI and dataset layers to build stable,
 * human-readable folder names and to derive reproducible seeds from a parameter set.
 *
 * Conventions:
 * • Outputs are safe for filesystem use and reuse the project-wide separators from [Constants].
 * • Builders ignore empty or null parts rather than emitting placeholders.
 * • Seeding is stable for identical [Parameters] snapshots.
 */
object Tools {

    /*
     * Sanitize & name builders
     */

    /**
     * Sanitizes an arbitrary string so it is safe to use as part of a filename or path segment.
     *
     * Transformations:
     * • Replace characters invalid on common filesystems (`\ / : * ? " < > |`) with `-`.
     * • Collapse runs of whitespace into a single `-`.
     * • Collapse runs of `-` into a single `-`.
     * • Trim leading and trailing `-`.
     */
    fun sanitizeForFile(part: String): String = part
        .replace("[\\\\/:*?\"<>|]".toRegex(), "-")
        .replace("\\s+".toRegex(), "-")
        .replace("-{2,}".toRegex(), "-")
        .trim('-')

    /**
     * Builds a base name by joining sanitized, non-blank parts with the global file name separator.
     *
     * Behavior:
     * • `null` or blank parts are ignored.
     * • When no usable parts remain, the fallback `"Run"` is returned.
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
     * Builds the parameter token used in filenames and containers (without timestamp and target).
     *
     * Shape:
     *    <dataset>-<correlation>-top<T>-sys<S>-po<P>-i<I>-r<R>-mx<M>[ -peA_B ]
     *
     * Inclusion rules:
     * • Each numeric component is appended only if strictly greater than zero.
     * • Percentiles are appended as `peA_B` when `includePercentiles == true` and the list is non-empty
     *   (only the first and last values are used to represent the range compactly).
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

        /* Required tokens first to keep a stable, human-readable prefix. */
        parts += datasetName
        parts += correlationMethod

        /* Optional numeric tokens; omit zeros to avoid noise. */
        if (numberOfTopics > 0) parts += "top$numberOfTopics"
        if (numberOfSystems > 0) parts += "sys$numberOfSystems"
        if (populationSize > 0) parts += "po$populationSize"
        if (numberOfIterations > 0) parts += "i$numberOfIterations"
        if (numberOfRepetitions > 0) parts += "r$numberOfRepetitions"
        if (expansionCoefficient > 0) parts += "mx$expansionCoefficient"

        /* Percentile range is summarized as first..last when requested. */
        if (includePercentiles && percentiles.isNotEmpty()) {
            val first = percentiles.first()
            val last = percentiles.last()
            parts += "pe${first}_${last}"
        }

        return folderBaseName(*parts.toTypedArray())
    }

    /**
     * Builds a container folder name (target-agnostic) by appending the run timestamp
     * to the parameter token produced by [buildParamsToken].
     *
     * Shape:
     *    <dataset>-<correlation>-top<T>-sys<S>-po<P>-i<I>-r<R>-mx<M>[ -peA_B ]-time<RUN_TIMESTAMP>
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
        /* Delegate token assembly to keep naming consistent across builders. */
        val token = buildParamsToken(
            datasetName = datasetName,
            correlationMethod = correlationMethod,
            numberOfTopics = numberOfTopics,
            numberOfSystems = numberOfSystems,
            populationSize = populationSize,
            numberOfIterations = numberOfIterations,
            numberOfRepetitions = numberOfRepetitions,
            expansionCoefficient = expansionCoefficient,
            includePercentiles = includePercentiles,
            percentiles = percentiles
        )

        /* Append the timestamp with the global file name separator. */
        return token + Constants.FILE_NAME_SEPARATOR + "time" + Constants.RUN_TIMESTAMP
    }

    /*
     * Deterministic seed helper
     */

    /**
     * Builds a deterministic 64-bit seed from a [Parameters] snapshot.
     *
     * Method:
     * • Serialize a canonical signature string with fields separated by `'|'`.
     * • Compute SHA-256 over the UTF-8 bytes.
     * • Interpret the first 8 bytes as a signed big-endian `Long`.
     *
     * Signature layout:
     *    datasetName | correlationMethod | targetToAchieve | numberOfIterations |
     *    numberOfRepetitions | populationSize | percentilesCSV
     *
     * Notes:
     * • The seed is stable for the same [Parameters] content.
     * • Changing the layout or field order intentionally changes all derived seeds.
     */
    fun stableSeedFrom(p: Parameters): Long {
        /* Canonical, order-preserving signature to feed into SHA-256. */
        val signature = buildString {
            append(p.datasetName).append('|')
            append(p.correlationMethod).append('|')
            append(p.targetToAchieve).append('|')
            append(p.numberOfIterations).append('|')
            append(p.numberOfRepetitions).append('|')
            append(p.populationSize).append('|')
            append(p.percentiles.joinToString(","))
        }

        /* Digest → first 8 bytes → signed big-endian long (ByteBuffer default). */
        val sha = MessageDigest.getInstance("SHA-256").digest(signature.toByteArray())
        return ByteBuffer.wrap(sha, 0, 8).long
    }
}
