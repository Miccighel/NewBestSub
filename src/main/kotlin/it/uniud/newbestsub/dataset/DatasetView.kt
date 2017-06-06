package it.uniud.newbestsub.dataset

import com.opencsv.CSVWriter
import it.uniud.newbestsub.utils.Constants
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.runner.AbstractAlgorithmRunner
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.util.fileoutput.SolutionListOutput
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext
import java.io.FileWriter

class DatasetView : AbstractAlgorithmRunner() {

    private val logger = LogManager.getLogger()

    fun print(runResult: Pair<List<BinarySolution>, Triple<String, String, Long>>, resultPath: String) {

        val (population, info) = runResult
        val (targetToAchieve, threadName, computingTime) = info
        val populationHelper: SolutionListOutput = SolutionListOutput(population)

        logger.info("Starting to print result for execution on \"$threadName\" with target \"$targetToAchieve\" completed in ${computingTime}ms.")

        populationHelper
                .setVarFileOutputContext(DefaultFileOutputContext("${Constants.OUTPUT_PATH}$resultPath-Var.csv"))
                .setFunFileOutputContext(DefaultFileOutputContext("${Constants.OUTPUT_PATH}$resultPath-Fun.csv"))
                .print()

        logger.info("Result for execution on \"$threadName\" with target \"$targetToAchieve\" available at:")
        logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Var.csv\"")
        logger.info("\"${Constants.OUTPUT_PATH}$resultPath-Fun.csv\"")

        logger.info("Print completed.")

    }

    fun print(aggregatedData: List<Array<String>>, resultPath: String) {

        val writer = CSVWriter(FileWriter(resultPath))
        writer.writeAll(aggregatedData)
        writer.close()

    }
}
