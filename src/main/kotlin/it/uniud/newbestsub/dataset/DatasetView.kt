package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.utils.BestSubsetLogger
import it.uniud.newbestsub.utils.Constants

import org.uma.jmetal.runner.AbstractAlgorithmRunner
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.solution.Solution
import org.uma.jmetal.util.fileoutput.SolutionListOutput
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext

class DatasetView : AbstractAlgorithmRunner() {

    fun print(runResult: Pair<List<Solution<BinarySolution>>, Long>, outputPath: String) {

        BestSubsetLogger.log("VIEW - Starting to print the result")

        val (population, computingTime) = runResult; val populationHelper = SolutionListOutput(population)

        populationHelper
                .setSeparator(",")
                .setVarFileOutputContext(DefaultFileOutputContext(Constants.OUTPUT_PATH + outputPath + "-Var.csv"))
                .setFunFileOutputContext(DefaultFileOutputContext(Constants.OUTPUT_PATH + outputPath + "-Fun.csv"))
                .print()

        BestSubsetLogger.log("VIEW - Algorithm computing time: ${computingTime / 1000} seconds.")
        BestSubsetLogger.log("VIEW - Finished to print the result")

    }

}
