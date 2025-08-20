package it.uniud.newbestsub.problem

import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.crossover.CrossoverOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.pseudorandom.JMetalRandom

class BinaryPruningCrossover(var probability: Double) : CrossoverOperator<BinarySolution> {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    /* jMetal 6.x: operator API uses property-style names (no get*). */
    override fun crossoverProbability(): Double = probability
    override fun numberOfRequiredParents(): Int = 2
    override fun numberOfGeneratedChildren(): Int = 2

    override fun execute(solutionList: MutableList<BinarySolution>): MutableList<BinarySolution> {
        /* Defensive: NSGA-II should always pass exactly 2 parents. */
        require(solutionList.size >= 2) { "BinaryPruningCrossover requires 2 parents (got ${solutionList.size})" }

        val parent1 = solutionList[0] as BestSubsetSolution
        val parent2 = solutionList[1] as BestSubsetSolution

        val p1Mask = parent1.retrieveTopicStatus()
        val p2Mask = parent2.retrieveTopicStatus()

        /* Children are deep copies of parents; we then edit bits in-place */
        val child1 = parent1.copy()
        val child2 = parent2.copy()

        /* Determinism: all RNG calls go through jMetal's singleton. */
        if (JMetalRandom.getInstance().nextDouble() < probability) {
            for (i in p1Mask.indices) {
                /* Pruning crossover: AND for child1 (more selective), OR for child2 (more inclusive) */
                child1.setBitValue(i, p1Mask[i] && p2Mask[i])
                child2.setBitValue(i, p1Mask[i] || p2Mask[i])
            }

            /* Ensure child1 has at least one topic selected */
            if (child1.numberOfSelectedTopics == 0) {
                val nBits = child1.getNumberOfBits(0)
                val flipIndex = if (nBits <= 1) 0
                else JMetalRandom.getInstance().nextInt(0, nBits - 1)  /* inclusive upper */
                child1.setBitValue(flipIndex, true)
            }
        }

        /* Debug logs (legacy string helper kept via your shim) */
        logger.debug("<Num. Sel. Topics: ${parent1.numberOfSelectedTopics}, Parent 1: ${parent1.getVariableValueString(0)}>")
        logger.debug("<Num. Sel. Topics: ${parent2.numberOfSelectedTopics}, Parent 2: ${parent2.getVariableValueString(0)}>")
        logger.debug("<Num. Sel. Topics: ${child1.numberOfSelectedTopics}, Child 1: ${child1.getVariableValueString(0)}>")
        logger.debug("<Num. Sel. Topics: ${child2.numberOfSelectedTopics}, Child 2: ${child2.getVariableValueString(0)}>")
        return mutableListOf(child1, child2)
    }
}
