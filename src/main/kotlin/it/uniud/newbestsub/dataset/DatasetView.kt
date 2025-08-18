package it.uniud.newbestsub.dataset

import com.opencsv.CSVWriter
import it.uniud.newbestsub.problem.BestSubsetSolution
import it.uniud.newbestsub.problem.getCardinality
import it.uniud.newbestsub.problem.getCorrelation
import it.uniud.newbestsub.utils.Constants
import it.uniud.newbestsub.utils.Tools
import it.uniud.newbestsub.utils.Tools.folderBaseName
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.fileoutput.SolutionListOutput
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext
import java.io.File
import java.io.FileWriter

class DatasetView {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    companion object {
        /* Single run folder cached so Best/Worst/Average land together */
        @Volatile
        private var cachedOutDirPath: String? = null
    }

    /* ---------------- Public path helpers (use these in the controller) ---------------- */

    fun getFunctionValuesFilePath(model: DatasetModel): String =
        ensureRunDir(model) + csvNameNoTs(fileBaseParts(model, model.targetToAchieve), Constants.FUNCTION_VALUES_FILE_SUFFIX)

    fun getVariableValuesFilePath(model: DatasetModel): String =
        ensureRunDir(model) + csvNameNoTs(fileBaseParts(model, model.targetToAchieve), Constants.VARIABLE_VALUES_FILE_SUFFIX)

    fun getTopSolutionsFilePath(model: DatasetModel): String =
        ensureRunDir(model) + csvNameNoTs(fileBaseParts(model, model.targetToAchieve), Constants.TOP_SOLUTIONS_FILE_SUFFIX)

    fun getInfoFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val target = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ensureRunDir(model) + csvNameNoTs(fileBaseParts(model, target), Constants.INFO_FILE_SUFFIX)
    }

    fun getAggregatedDataFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val target = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ensureRunDir(model) + csvNameNoTs(fileBaseParts(model, target), Constants.AGGREGATED_DATA_FILE_SUFFIX)
    }

    /* -------- Merged helpers -------- */

    fun getFunctionValuesMergedFilePath(model: DatasetModel): String =
        ensureRunDir(model) + csvNameNoTsMerged(fileBaseParts(model, model.targetToAchieve), Constants.FUNCTION_VALUES_FILE_SUFFIX)

    fun getVariableValuesMergedFilePath(model: DatasetModel): String =
        ensureRunDir(model) + csvNameNoTsMerged(fileBaseParts(model, model.targetToAchieve), Constants.VARIABLE_VALUES_FILE_SUFFIX)

    fun getTopSolutionsMergedFilePath(model: DatasetModel): String =
        ensureRunDir(model) + csvNameNoTsMerged(fileBaseParts(model, model.targetToAchieve), Constants.TOP_SOLUTIONS_FILE_SUFFIX)

    fun getAggregatedDataMergedFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val target = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ensureRunDir(model) + csvNameNoTsMerged(fileBaseParts(model, target), Constants.AGGREGATED_DATA_FILE_SUFFIX)
    }

    fun getInfoMergedFilePath(model: DatasetModel, isTargetAll: Boolean = false): String {
        val target = if (isTargetAll) Constants.TARGET_ALL else model.targetToAchieve
        return ensureRunDir(model) + csvNameNoTsMerged(fileBaseParts(model, target), Constants.INFO_FILE_SUFFIX)
    }

    /* ---------------- Main printing for FUN / VAR / TOP ---------------- */

    fun print(
        runResult: Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>>,
        model: DatasetModel
    ) {
        val (allSolutions, topSolutions, executionInfo) = runResult
        val (actualTarget, threadName, computingTime) = executionInfo
        val populationHelper = SolutionListOutput(allSolutions)

        logger.info("Starting to print result for execution on \"$threadName\" with target \"$actualTarget\" completed in ${computingTime}ms.")

        /* Single run folder shared by Best/Worst/Average */
        val outDirPath = ensureRunDir(model)

        /* Filenames have NO timestamp, but do include the executed target */
        val base = fileBaseParts(model, actualTarget)
        val funPath = outDirPath + csvNameNoTs(base, Constants.FUNCTION_VALUES_FILE_SUFFIX)
        val varPath = outDirPath + csvNameNoTs(base, Constants.VARIABLE_VALUES_FILE_SUFFIX)
        val topPath = outDirPath + csvNameNoTs(base, Constants.TOP_SOLUTIONS_FILE_SUFFIX)

        /* FUN / VAR */
        populationHelper
            .setVarFileOutputContext(DefaultFileOutputContext(varPath))
            .setFunFileOutputContext(DefaultFileOutputContext(funPath))
            .print()

        /* TOP only for Best/Worst */
        if (actualTarget != Constants.TARGET_AVERAGE) {
            val rows = mutableListOf(arrayOf("Cardinality", "Correlation", "Topics"))
            topSolutions.forEach { sol ->
                sol as BestSubsetSolution
                rows.add(
                    arrayOf(
                        sol.getCardinality().toString(),
                        sol.getCorrelation().toString(),
                        sol.getTopicLabelsFromTopicStatus()
                    )
                )
            }
            writeCsv(rows, topPath)
        }

        logger.info("Result for execution on \"$threadName\" with target \"$actualTarget\" available at:")
        logger.info("\"$varPath\"")
        logger.info("\"$funPath\"")
        if (actualTarget != Constants.TARGET_AVERAGE) logger.info("\"$topPath\"")
        logger.info("Print completed.")
    }

    fun writeCsv(data: List<Array<String>>, resultPath: String) {
        val writer = CSVWriter(FileWriter(resultPath))
        writer.writeAll(data)
        writer.close()
    }

    /* ---------------- Internals ---------------- */

    private fun ensureRunDir(model: DatasetModel): String {
        cachedOutDirPath?.let { return it }

        /* Build the shared container folder via Tools (canonical logic). */
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

        /* Cache and return */
        cachedOutDirPath = out
        return out
    }

    private fun fileBaseParts(model: DatasetModel, target: String): Array<String> {
        /* Percentiles token ONLY for Average in file names as well */
        val includePct = (target == Constants.TARGET_AVERAGE)
        val pctKeys = model.percentiles.keys.sorted()
        val pctPart: String? = if (includePct && pctKeys.isNotEmpty())
            "pe${pctKeys.first()}_${pctKeys.last()}"
        else null

        /* Filenames: include target; include params only if > 0; keep order stable */
        val parts = mutableListOf<String>()
        parts += model.datasetName
        parts += model.correlationMethod
        parts += target /* Best | Worst | Average | All */
        if (model.numberOfTopics > 0)        parts += "top${model.numberOfTopics}"
        if (model.numberOfSystems > 0)       parts += "sys${model.numberOfSystems}"
        if (model.populationSize > 0)        parts += "po${model.populationSize}"
        if (model.numberOfIterations > 0)    parts += "i${model.numberOfIterations}"
        if (model.numberOfRepetitions > 0)   parts += "r${model.numberOfRepetitions}"
        if (model.expansionCoefficient > 0)  parts += "mx${model.expansionCoefficient}"
        if (model.currentExecution > 0)      parts += "ex${model.currentExecution}"
        if (pctPart != null)                 parts += pctPart

        return parts.toTypedArray()
    }

    /* Build "<base>-<suffix>.csv" WITHOUT timestamp in filenames */
    private fun csvNameNoTs(baseParts: Array<String>, suffix: String): String =
        folderBaseName(*baseParts) + Constants.FILE_NAME_SEPARATOR + suffix + Constants.CSV_FILE_EXTENSION

    /* Build "<base>-<suffix>-Merged.csv" WITHOUT timestamp in filenames */
    private fun csvNameNoTsMerged(baseParts: Array<String>, suffix: String): String =
        folderBaseName(*baseParts) + Constants.FILE_NAME_SEPARATOR + suffix +
                Constants.FILE_NAME_SEPARATOR + Constants.MERGED_RESULT_FILE_SUFFIX + Constants.CSV_FILE_EXTENSION
}
