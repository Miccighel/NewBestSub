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
                .setVarFileOutputContext(DefaultFileOutputContext(Constants.OUTPUT_PATH + resultPath + "-Var.csv"))
                .setFunFileOutputContext(DefaultFileOutputContext(Constants.OUTPUT_PATH + resultPath + "-Fun.csv"))
                .print()

        logger.info("Result for execution on \"$threadName\" with target \"$targetToAchieve\" available.")

    }

    fun aggregate(models: List<DatasetModel>, resultPath: String) {

        val writer = CSVWriter(FileWriter(resultPath))
        val firstLine = LinkedList<String>()
        val incompleteLines = LinkedList<Array<String>>()
        val completeLines = LinkedList<Array<String>>()

        logger.info("Printing common data between models.")
        logger.info("Topics number: ${models[0].numberOfTopics}")
        logger.info("Systems number: ${models[0].numberOfSystems}")

        val topicLabels = models[0].topicLabels
        val percentiles = models[2].percentiles

        firstLine.add("Cardinality")
        models.forEach { model -> firstLine.add(model.targetToAchieve) }
        topicLabels.forEach { topicLabel -> firstLine.add(topicLabel) }
        percentiles.keys.forEach { percentile -> firstLine.add(" Percentile: $percentile") }

        (0..models[0].numberOfTopics - 1).forEach {
            index ->
            val currentCardinality = (index + 1).toDouble()
            val currentLine = LinkedList<String>()
            currentLine.add(currentCardinality.toString())
            models.forEach {
                model ->
                val correlationValueForCurrentCardinality = model.findCorrelationForCardinality(currentCardinality)
                if (correlationValueForCurrentCardinality != null) currentLine.add(correlationValueForCurrentCardinality.toString()) else currentLine.add("UNAVAILABLE")
            }
            incompleteLines.add(currentLine.toTypedArray())
        }

        incompleteLines.forEach {
            aLine ->
            val currentCardinality = aLine[0].toDouble()
            var newLine = aLine
            topicLabels.forEach {
                currentLabel ->
                var topicPresence = ""
                models.forEach {
                    model ->
                    val isTopicInASolutionOfCurrentCard = model.isTopicInASolutionOfCardinality(currentLabel, currentCardinality)
                    when (model.targetToAchieve) {
                        "Best" -> if (isTopicInASolutionOfCurrentCard) topicPresence += "b"
                        "Worst" -> if (isTopicInASolutionOfCurrentCard) topicPresence += "w"
                    }
                }
                if (topicPresence == "") topicPresence += "n"
                newLine = newLine.plus(topicPresence)
            }

            percentiles.entries.forEach {
                (_, percentileValues) ->
                newLine = newLine.plus(percentileValues[currentCardinality.toInt() - 1].toString())
            }
            completeLines.add(newLine)
        }

        incompleteLines.clear()
        completeLines.addFirst(firstLine.toTypedArray())

        writer.writeAll(completeLines)
        writer.close()
    }
}
