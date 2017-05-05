package it.uniud.newbestsub.dataset

import it.uniud.newbestsub.problem.BestSubsetSolution
import it.uniud.newbestsub.utils.BestSubsetLogger
import it.uniud.newbestsub.utils.Constants

import org.apache.commons.lang3.tuple.ImmutablePair

import org.uma.jmetal.runner.AbstractAlgorithmRunner
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.solution.Solution
import org.uma.jmetal.util.SolutionListUtils
import org.uma.jmetal.util.fileoutput.SolutionListOutput
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext

import java.io.FileNotFoundException
import java.util.Comparator
import java.util.LinkedList

class DatasetView : AbstractAlgorithmRunner() {

    fun print(runResult: ImmutablePair<List<Solution<BinarySolution>>, Long>, outputPath: String) {

        BestSubsetLogger.Companion.log("VIEW - Starting to print the result")

        val population = runResult.left
        val computingTime = runResult.right

        val populationHelper = SolutionListOutput(population)

        populationHelper.setSeparator(",")
                .setVarFileOutputContext(DefaultFileOutputContext(Constants.OUTPUT_PATH + outputPath + "-Var.csv"))
                .setFunFileOutputContext(DefaultFileOutputContext(Constants.OUTPUT_PATH + outputPath + "-Fun.csv"))
                .print()

        BestSubsetLogger.Companion.log("VIEW - Algorithm computing time: " + computingTime / 1000 + " seconds.")
        BestSubsetLogger.Companion.log("VIEW - Finished to print the result")

    }

}
