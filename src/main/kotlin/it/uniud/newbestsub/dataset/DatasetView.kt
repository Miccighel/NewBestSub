package it.uniud.newbestsub.dataset

import com.opencsv.CSVWriter
import it.uniud.newbestsub.utils.Constants
import org.apache.logging.log4j.LogManager
import org.uma.jmetal.runner.AbstractAlgorithmRunner
import org.uma.jmetal.solution.BinarySolution
import org.uma.jmetal.util.fileoutput.SolutionListOutput
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext
import java.io.FileWriter
import java.util.*

class DatasetView : AbstractAlgorithmRunner() {

    private val logger = LogManager.getLogger()

    fun print(runResult: Pair<List<BinarySolution>, Triple<String, String, Long>>, resultPath: String) {

        val (population, info) = runResult
        val (targetToAchieve, threadName, computingTime) = info
        val populationHelper: SolutionListOutput = SolutionListOutput(population)

        logger.info("Printing result for execution on \"$threadName\" with target \"$targetToAchieve\" completed in ${computingTime}ms.")

        populationHelper
                .setSeparator(",")
                .setVarFileOutputContext(DefaultFileOutputContext(Constants.OUTPUT_PATH + resultPath + "-Var.csv"))
                .setFunFileOutputContext(DefaultFileOutputContext(Constants.OUTPUT_PATH + resultPath + "-Fun.csv"))
                .print()

        logger.info("Result for execution on \"$threadName\" with target \"$targetToAchieve\" available.")

    }

    fun aggregate(models: List<DatasetModel>, resultPath: String) {

        val writer = CSVWriter(FileWriter(resultPath))
        val lineHeader = LinkedList<String>()
        val allLines = LinkedList<Array<String>>()

        logger.info("Printing common data between models.")
        logger.info("Topics number: ${models[0].numberOfTopics}")
        logger.info("Systems number: ${models[0].numberOfSystems}")

        if (models.size > 1) {
            lineHeader.add("Cardinality")
            lineHeader.add("Best")
            lineHeader.add("Worst")
            lineHeader.add("Average")

            val modelBest = models[0]
            val modelWorst = models[1]
            val modelAverage = models[2]
            val topicLabels = modelBest.topicLabels

            modelBest.topicLabels.forEach { topicLabel -> lineHeader.add(topicLabel) }

            allLines.add(lineHeader.toTypedArray())

            topicLabels.forEachIndexed {
                index, _ ->
                val currentLine = LinkedList<String>()

                val currentCardinality = (index + 1).toDouble()
                val correlationValueForCurrentCardinalityForBest = modelBest.findCorrelationForCardinality(currentCardinality)
                val correlationValueForCurrentCardinalityForWorst = modelWorst.findCorrelationForCardinality(currentCardinality)
                val correlationValueForCurrentCardinalityForAverage = modelAverage.findCorrelationForCardinality(currentCardinality)

                currentLine.add(currentCardinality.toString())
                if (correlationValueForCurrentCardinalityForBest != null) currentLine.add(correlationValueForCurrentCardinalityForBest.toString()) else currentLine.add("UNAVAILABLE")
                if (correlationValueForCurrentCardinalityForWorst != null) currentLine.add(correlationValueForCurrentCardinalityForWorst.toString()) else currentLine.add("UNAVAILABLE")
                if (correlationValueForCurrentCardinalityForAverage != null) currentLine.add(correlationValueForCurrentCardinalityForAverage.toString()) else currentLine.add("UNAVAILABLE")

                topicLabels.forEach {
                    currentLabel ->
                    val isTopicInASolutionOfCurrentCardForBest = modelBest.isTopicInASolutionOfCardinality(currentLabel, currentCardinality)
                    val isTopicInASolutionOfCurrentCardForWorst = modelWorst.isTopicInASolutionOfCardinality(currentLabel, currentCardinality)

                    var topicPresence = ""

                    if (isTopicInASolutionOfCurrentCardForBest) topicPresence += "b"
                    if (isTopicInASolutionOfCurrentCardForWorst) topicPresence += "w"

                    if (!isTopicInASolutionOfCurrentCardForBest && !isTopicInASolutionOfCurrentCardForWorst) topicPresence += "n"
                    currentLine.add(topicPresence)

                }
                allLines.add(currentLine.toTypedArray())
            }

        } else {

            lineHeader.add("Cardinality")

            val model = models[0]

            lineHeader.add(model.targetToAchieve)

            val topicLabels = model.topicLabels

            model.topicLabels.forEach { topicLabel -> lineHeader.add(topicLabel) }

            allLines.add(lineHeader.toTypedArray())

            topicLabels.forEachIndexed {
                index, _ ->
                val currentLine = LinkedList<String>()

                val currentCardinality = (index + 1).toDouble()
                val correlationValueForCurrentCardinality = model.findCorrelationForCardinality(currentCardinality)

                currentLine.add(currentCardinality.toString())
                if (correlationValueForCurrentCardinality != null) currentLine.add(correlationValueForCurrentCardinality.toString()) else currentLine.add("UNAVAILABLE")

                topicLabels.forEach {
                    currentLabel ->
                    val isTopicInASolutionOfCurrentCard = model.isTopicInASolutionOfCardinality(currentLabel, currentCardinality)
                    if (isTopicInASolutionOfCurrentCard) currentLine.add(model.targetToAchieve.substring(0, 1).toLowerCase())
                    if (!isTopicInASolutionOfCurrentCard) currentLine.add("n")
                }
                allLines.add(currentLine.toTypedArray())
            }

        }
        writer.writeAll(allLines)
        writer.close()
    }
}
