package it.uniud.newbestsub.dataset

import com.opencsv.CSVWriter
import it.uniud.newbestsub.problem.BestSubsetSolution
import it.uniud.newbestsub.problem.getCardinality
import it.uniud.newbestsub.problem.getCorrelation
import it.uniud.newbestsub.utils.Constants
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.util.fileoutput.SolutionListOutput
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext
import java.io.FileWriter

class DatasetView {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    fun print(runResult: Triple<List<BinarySolution>, List<BinarySolution>, Triple<String, String, Long>>, datasetModel: DatasetModel) {

        val (allSolutions, topSolutions, executionInfo) = runResult
        val (targetToAchieve, threadName, computingTime) = executionInfo
        val populationHelper: SolutionListOutput = SolutionListOutput(allSolutions)

        logger.info("Starting to print result for execution on \"$threadName\" with target \"$targetToAchieve\" completed in ${computingTime}ms.")

        val variabileValuesFilePath = datasetModel.getVariableValuesFilePath()
        val functionValuesFilePath = datasetModel.getFunctionValuesFilePath()
        val topSolutionsFilePath = datasetModel.getTopSolutionsFilePath()

        populationHelper
                .setVarFileOutputContext(DefaultFileOutputContext(variabileValuesFilePath))
                .setFunFileOutputContext(DefaultFileOutputContext(functionValuesFilePath))
                .print()

        if (datasetModel.targetToAchieve != Constants.TARGET_AVERAGE) {
            val topSolutionsToPrint = mutableListOf<Array<String>>()
            val header = mutableListOf<String>()
            header.add("Cardinality")
            header.add("Correlation")
            header.add("Topics")
            topSolutionsToPrint.add(header.toTypedArray())
            topSolutions.forEach { aTopSolution ->
                aTopSolution as BestSubsetSolution
                val aTopSolutionToPrint = mutableListOf<String>()
                aTopSolutionToPrint.add(aTopSolution.getCardinality().toString())
                aTopSolutionToPrint.add(aTopSolution.getCorrelation().toString())
                aTopSolutionToPrint.add(aTopSolution.getTopicLabelsFromTopicStatus())
                topSolutionsToPrint.add(aTopSolutionToPrint.toTypedArray())
            }
            print(topSolutionsToPrint, datasetModel.getTopSolutionsFilePath())
        }

        logger.info("Result for execution on \"$threadName\" with target \"$targetToAchieve\" available at:")
        logger.info("\"$variabileValuesFilePath\"")
        logger.info("\"$functionValuesFilePath\"")
        if (datasetModel.targetToAchieve != Constants.TARGET_AVERAGE) logger.info("\"$topSolutionsFilePath\"")

        logger.info("Print completed.")

    }

    fun print(data: List<Array<String>>, resultPath: String) {

        val writer = CSVWriter(FileWriter(resultPath))
        writer.writeAll(data)
        writer.close()

    }
}
