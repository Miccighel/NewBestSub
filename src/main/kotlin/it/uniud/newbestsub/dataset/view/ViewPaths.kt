package it.uniud.newbestsub.dataset.view

import it.uniud.newbestsub.dataset.DatasetModel
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools
import it.uniud.newbestsub.utils.Tools.folderBaseName
import java.io.File

/**
 * ViewPaths.
 *
 * Centralized path utilities shared by [CSVView] and [ParquetView].
 *
 * Responsibilities
 * - Create and cache the per run container directory.
 * - Provide the CSV and Parquet subdirectories:
 *   - CSV → "<container>/CSV/"
 *   - Parquet → "<container>/Parquet/"
 * - Generate deterministic, timestamp free filenames that include the executed target token.
 *   - In AVERAGE mode, filenames intentionally omit the iteration token ("i…"), even if provided.
 * - Append "sd<seed>" to filenames when an explicit CLI seed (--seed) is present.
 *
 * Safety
 * - Directory creation is idempotent; existing directories are reused.
 * - Invoked at run startup and on write paths only.
 */
object ViewPaths {

    /** Cached container directory path so all targets share the same folder. */
    @Volatile
    private var cachedOutDirPath: String? = null

    /**
     * Build the seed token if a CLI seed was provided by Program.
     *
     * @return "sd<seed>" if the "nbs.seed.cli" system property is set, otherwise null.
     */
    private fun explicitSeedToken(): String? =
        System.getProperty("nbs.seed.cli")  /* set only when --seed is used */
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?.let { "sd$it" }

    /**
     * Ensure the shared container directory for this run exists and cache its absolute path.
     *
     * @param model Dataset model carrying the run configuration parameters.
     * @return Absolute path to the container directory with a trailing separator.
     */
    fun ensureRunDir(model: DatasetModel): String {
        cachedOutDirPath?.let { return it }

        val containerFolderName = Tools.buildContainerFolderName(
            datasetName = model.datasetName,
            correlationMethod = model.correlationMethod,
            numberOfTopics = model.numberOfTopics,
            numberOfSystems = model.numberOfSystems,
            populationSize = model.populationSize,
            numberOfIterations = model.numberOfIterations, /* container keeps -i… if > 0 */
            numberOfRepetitions = model.numberOfRepetitions,
            expansionCoefficient = model.expansionCoefficient,
            includePercentiles = (model.targetToAchieve == Constants.TARGET_AVERAGE),
            percentiles = model.percentiles.keys.sorted()
        )

        val outDirPath = Constants.NEWBESTSUB_PATH +
            "res" + Constants.PATH_SEPARATOR +
            containerFolderName + Constants.PATH_SEPARATOR

        File(outDirPath).mkdirs()
        cachedOutDirPath = outDirPath
        return outDirPath
    }

    /**
     * Ensure the CSV subdirectory exists and return its path.
     *
     * @param model Dataset model.
     * @return Absolute path to the CSV output directory with a trailing separator.
     */
    fun ensureCsvDir(model: DatasetModel): String {
        val csvDirPath = ensureRunDir(model) + "CSV" + Constants.PATH_SEPARATOR
        File(csvDirPath).mkdirs()
        return csvDirPath
    }

    /**
     * Ensure the Parquet subdirectory exists and return its path.
     *
     * @param model Dataset model.
     * @return Absolute path to the Parquet output directory with a trailing separator.
     */
    fun ensureParquetDir(model: DatasetModel): String {
        val parquetDirPath = ensureRunDir(model) + "Parquet" + Constants.PATH_SEPARATOR
        File(parquetDirPath).mkdirs()
        return parquetDirPath
    }

    /**
     * Assemble the core parts of a base filename for a given run and target.
     *
     * Rules
     * - Always include the target token (BEST, WORST, or AVERAGE).
     * - Include numeric tokens only when their value is greater than zero.
     * - In AVERAGE mode, filenames omit the iteration token ("i…").
     *
     * @param model Dataset model.
     * @param target Target identifier ("BEST", "WORST", or "AVERAGE").
     * @return Array of string tokens that form the deterministic filename base.
     */
    fun fileBaseParts(model: DatasetModel, target: String): Array<String> {
        val includePercentiles = (target == Constants.TARGET_AVERAGE)
        val percentileKeys = model.percentiles.keys.sorted()
        val percentilesPart: String? =
            if (includePercentiles && percentileKeys.isNotEmpty())
                "pe${percentileKeys.first()}_${percentileKeys.last()}"
            else
                null

        val baseParts = mutableListOf<String>()
        baseParts += model.datasetName
        baseParts += model.correlationMethod
        baseParts += target
        if (model.numberOfTopics > 0) baseParts += "top${model.numberOfTopics}"
        if (model.numberOfSystems > 0) baseParts += "sys${model.numberOfSystems}"
        if (model.populationSize > 0) baseParts += "po${model.populationSize}"

        /* Omit the iteration token in AVERAGE filenames by design. */
        if (target != Constants.TARGET_AVERAGE && model.numberOfIterations > 0) {
            baseParts += "i${model.numberOfIterations}"
        }

        if (model.numberOfRepetitions > 0) baseParts += "r${model.numberOfRepetitions}"
        if (model.expansionCoefficient > 0) baseParts += "mx${model.expansionCoefficient}"
        if (model.currentExecution > 0) baseParts += "ex${model.currentExecution}"
        if (percentilesPart != null) baseParts += percentilesPart

        /* Append sd<seed> only when an explicit seed was provided. */
        explicitSeedToken()?.let { baseParts += it }

        return baseParts.toTypedArray()
    }

    /* Filenames (no path prefix). */

    /**
     * Build a CSV filename (without timestamp).
     *
     * @param baseParts Core filename tokens from [fileBaseParts].
     * @param suffix Suffix describing the file content, for example "Fun" or "Var".
     * @return Deterministic CSV filename.
     */
    fun csvNameNoTs(baseParts: Array<String>, suffix: String): String =
        folderBaseName(*baseParts) + Constants.FILE_NAME_SEPARATOR + suffix + Constants.CSV_FILE_EXTENSION

    /**
     * Build a merged CSV filename (without timestamp).
     *
     * @param baseParts Core filename tokens from [fileBaseParts].
     * @param suffix Suffix describing the file content.
     * @return Deterministic merged CSV filename.
     */
    fun csvNameNoTsMerged(baseParts: Array<String>, suffix: String): String =
        folderBaseName(*baseParts) + Constants.FILE_NAME_SEPARATOR + suffix +
            Constants.FILE_NAME_SEPARATOR + Constants.MERGED_RESULT_FILE_SUFFIX + Constants.CSV_FILE_EXTENSION

    /**
     * Build a Parquet filename (without timestamp).
     *
     * @param baseParts Core filename tokens from [fileBaseParts].
     * @param suffix Suffix describing the file content.
     * @return Deterministic Parquet filename.
     */
    fun parquetNameNoTs(baseParts: Array<String>, suffix: String): String =
        folderBaseName(*baseParts) + Constants.FILE_NAME_SEPARATOR + suffix + ".parquet"
}
