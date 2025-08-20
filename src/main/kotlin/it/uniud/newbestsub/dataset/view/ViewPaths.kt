package it.uniud.newbestsub.dataset.view

import it.uniud.newbestsub.dataset.DatasetModel
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools
import it.uniud.newbestsub.utils.Tools.folderBaseName
import java.io.File

/**
 * ViewPaths
 * =========
 *
 * Centralized path helpers shared by CSVView and ParquetView:
 *  - Container folder (per run) is created once.
 *  - Two subfolders are used for outputs:
 *      • CSV     -> "<container>/CSV/"
 *      • Parquet -> "<container>/Parquet/"
 *  - Filenames have NO timestamps, but include the executed target token.
 *  - If a CLI seed (--seed) is explicitly provided, filenames carry "sd<seed>".
 */
object ViewPaths {

    /** Cached container directory path so all targets share the same folder. */
    @Volatile
    private var cachedOutDirPath: String? = null

    /** If Program set a CLI seed, expose it as a token like "sd12345". */
    private fun explicitSeedToken(): String? =
        System.getProperty("nbs.seed.cli")  // set only when --seed is used
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?.let { "sd$it" }

    /** Build the shared container folder and cache it. */
    fun ensureRunDir(model: DatasetModel): String {
        cachedOutDirPath?.let { return it }

        val containerFolder = Tools.buildContainerFolderName(
            datasetName = model.datasetName,
            correlationMethod = model.correlationMethod,
            numberOfTopics = model.numberOfTopics,
            numberOfSystems = model.numberOfSystems,
            populationSize = model.populationSize,
            numberOfIterations = model.numberOfIterations,
            numberOfRepetitions = model.numberOfRepetitions,
            expansionCoefficient = model.expansionCoefficient,
            includePercentiles = (model.targetToAchieve == Constants.TARGET_AVERAGE),
            percentiles = model.percentiles.keys.sorted()
        )

        val out = Constants.NEWBESTSUB_PATH +
            "res" + Constants.PATH_SEPARATOR +
            containerFolder + Constants.PATH_SEPARATOR

        File(out).mkdirs()
        cachedOutDirPath = out
        return out
    }

    /** Ensure the CSV subdirectory exists and return its path (with trailing separator). */
    fun ensureCsvDir(model: DatasetModel): String {
        val dir = ensureRunDir(model) + "CSV" + Constants.PATH_SEPARATOR
        File(dir).mkdirs()
        return dir
    }

    /** Ensure the Parquet subdirectory exists and return its path (with trailing separator). */
    fun ensureParquetDir(model: DatasetModel): String {
        val dir = ensureRunDir(model) + "Parquet" + Constants.PATH_SEPARATOR
        File(dir).mkdirs()
        return dir
    }

    /** Assemble the core name parts used by all files for a given run/target. */
    fun fileBaseParts(model: DatasetModel, target: String): Array<String> {
        val includePct = (target == Constants.TARGET_AVERAGE)
        val pctKeys = model.percentiles.keys.sorted()
        val pctPart: String? = if (includePct && pctKeys.isNotEmpty())
            "pe${pctKeys.first()}_${pctKeys.last()}"
        else null

        val parts = mutableListOf<String>()
        parts += model.datasetName
        parts += model.correlationMethod
        parts += target
        if (model.numberOfTopics > 0) parts += "top${model.numberOfTopics}"
        if (model.numberOfSystems > 0) parts += "sys${model.numberOfSystems}"
        if (model.populationSize > 0) parts += "po${model.populationSize}"
        if (model.numberOfIterations > 0) parts += "i${model.numberOfIterations}"
        if (model.numberOfRepetitions > 0) parts += "r${model.numberOfRepetitions}"
        if (model.expansionCoefficient > 0) parts += "mx${model.expansionCoefficient}"
        if (model.currentExecution > 0) parts += "ex${model.currentExecution}"
        if (pctPart != null) parts += pctPart

        // Append sd<seed> ONLY when --seed was explicitly provided
        explicitSeedToken()?.let { parts += it }

        return parts.toTypedArray()
    }

    /* -------- Filenames (no path prefix) -------- */

    fun csvNameNoTs(baseParts: Array<String>, suffix: String): String =
        folderBaseName(*baseParts) + Constants.FILE_NAME_SEPARATOR + suffix + Constants.CSV_FILE_EXTENSION

    fun csvNameNoTsMerged(baseParts: Array<String>, suffix: String): String =
        folderBaseName(*baseParts) + Constants.FILE_NAME_SEPARATOR + suffix +
            Constants.FILE_NAME_SEPARATOR + Constants.MERGED_RESULT_FILE_SUFFIX + Constants.CSV_FILE_EXTENSION

    fun parquetNameNoTs(baseParts: Array<String>, suffix: String): String =
        folderBaseName(*baseParts) + Constants.FILE_NAME_SEPARATOR + suffix + ".parquet"
}
