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
 * Centralized path helpers shared by [CSVView] and [ParquetView].
 *
 * ## Responsibilities
 * - Create and cache the container folder per run.
 * - Provide subfolders:
 *   - CSV → `<container>/CSV/`
 *   - Parquet → `<container>/Parquet/`
 * - Generate deterministic, timestamp-free filenames that include the executed target token.
 * - Append `"sd<seed>"` to filenames if an explicit CLI seed (`--seed`) was provided.
 *
 * ## Safety
 * - All directory creation is idempotent: existing folders are reused.
 * - Only invoked at run startup or when writing output.
 */
object ViewPaths {

    /** Cached container directory path so all targets share the same folder. */
    @Volatile
    private var cachedOutDirPath: String? = null

    /**
     * If Program set a CLI seed, expose it as a token like `"sd12345"`.
     *
     * @return A `"sd<seed>"` token if `nbs.seed.cli` system property is set, else `null`.
     */
    private fun explicitSeedToken(): String? =
        System.getProperty("nbs.seed.cli")  // set only when --seed is used
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?.let { "sd$it" }

    /**
     * Ensure the shared container folder for this run exists and cache its path.
     *
     * @param model The dataset model containing run configuration parameters.
     * @return The absolute path to the container folder, with trailing separator.
     */
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

    /**
     * Ensure the CSV subdirectory exists and return its path.
     *
     * @param model The dataset model.
     * @return Absolute path to the CSV output folder (with trailing separator).
     */
    fun ensureCsvDir(model: DatasetModel): String {
        val dir = ensureRunDir(model) + "CSV" + Constants.PATH_SEPARATOR
        File(dir).mkdirs()
        return dir
    }

    /**
     * Ensure the Parquet subdirectory exists and return its path.
     *
     * @param model The dataset model.
     * @return Absolute path to the Parquet output folder (with trailing separator).
     */
    fun ensureParquetDir(model: DatasetModel): String {
        val dir = ensureRunDir(model) + "Parquet" + Constants.PATH_SEPARATOR
        File(dir).mkdirs()
        return dir
    }

    /**
     * Assemble the core parts of a base filename for a given run and target.
     *
     * @param model The dataset model.
     * @param target Target identifier (`BEST`, `WORST`, or `AVERAGE`).
     * @return Array of string tokens that form the deterministic filename base.
     */
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

    /**
     * Build a CSV filename (without timestamp).
     *
     * @param baseParts The core filename tokens from [fileBaseParts].
     * @param suffix Suffix describing the file content (e.g., `"Fun"`, `"Var"`).
     * @return Deterministic CSV filename.
     */
    fun csvNameNoTs(baseParts: Array<String>, suffix: String): String =
        folderBaseName(*baseParts) + Constants.FILE_NAME_SEPARATOR + suffix + Constants.CSV_FILE_EXTENSION

    /**
     * Build a merged CSV filename (without timestamp).
     *
     * @param baseParts The core filename tokens from [fileBaseParts].
     * @param suffix Suffix describing the file content.
     * @return Deterministic merged CSV filename.
     */
    fun csvNameNoTsMerged(baseParts: Array<String>, suffix: String): String =
        folderBaseName(*baseParts) + Constants.FILE_NAME_SEPARATOR + suffix +
            Constants.FILE_NAME_SEPARATOR + Constants.MERGED_RESULT_FILE_SUFFIX + Constants.CSV_FILE_EXTENSION

    /**
     * Build a Parquet filename (without timestamp).
     *
     * @param baseParts The core filename tokens from [fileBaseParts].
     * @param suffix Suffix describing the file content.
     * @return Deterministic Parquet filename.
     */
    fun parquetNameNoTs(baseParts: Array<String>, suffix: String): String =
        folderBaseName(*baseParts) + Constants.FILE_NAME_SEPARATOR + suffix + ".parquet"
}
