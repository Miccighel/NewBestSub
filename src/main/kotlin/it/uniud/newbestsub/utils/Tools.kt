package it.uniud.newbestsub.utils

import it.uniud.newbestsub.dataset.Parameters
import java.nio.ByteBuffer
import java.security.MessageDigest

object Tools {

    /* ---------------- Sanitize & Name Builders ---------------- */

    fun sanitizeForFile(part: String): String = part
        .replace("[\\\\/:*?\"<>|]".toRegex(), "-")
        .replace("\\s+".toRegex(), "-")
        .replace("-{2,}".toRegex(), "-")
        .trim('-')

    fun folderBaseName(vararg parts: Any?): String =
        parts.asSequence()
            .filterNotNull()
            .map { it.toString() }
            .map { sanitizeForFile(it) }
            .filter { it.isNotBlank() }
            .joinToString(Constants.FILE_NAME_SEPARATOR)
            .ifBlank { "Run" }

    /*
     * buildParamsToken
     * ----------------
     * Filenames base token (NO timestamp, NO target).
     * Shape:
     *   <dataset>-<correlation>-top<T>-sys<S>-po<P>-i<I>-r<R>-mx<M>[ -peA_B ]
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

    /*
     * buildContainerFolderName
     * ------------------------
     * Container folder is TARGET-agnostic:
     *   <dataset>-<correlation>-top<T>-sys<S>-po<P>-i<I>-r<R>-mx<M>[ -peA_B ]-time<RUN_TIMESTAMP>
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


    /* ---------------- Deterministic Seed Helper ---------------- */

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
