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

        val (_, info) = runResult
        val (targetToAchieve, threadName, computingTime) = info
        logger.info("Execution on \"$threadName\" with target \"$targetToAchieve\" completed in ${computingTime}ms.")

    }

    fun print(aggregatedData: List<Array<String>>, resultPath: String) {

        val writer = CSVWriter(FileWriter(resultPath))
        writer.writeAll(aggregatedData)
        writer.close()

    }
}
