package it.uniud.newbestsub.problem

import org.apache.logging.log4j.LogManager
import org.uma.jmetal.operator.crossover.CrossoverOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/**
 * Binary "pruning" crossover:
 *  - childA selects topics present in BOTH parents (AND) → more selective
 *  - childB selects topics present in EITHER parent (OR) → more inclusive
 *  - Guarantees childA has at least 1 bit set (flip one random bit if needed)
 *
 * Notes:
 *  - All randomness goes through jMetal’s singleton RNG for determinism.
 *  - Indices use exclusive-upper-bound ranges (0 until nBits) to avoid OOB.
 */
class BinaryPruningCrossover(private var probability: Double) : CrossoverOperator<BinarySolution> {

    private val logger = LogManager.getLogger(LogManager.ROOT_LOGGER_NAME)

    // jMetal 6.x operator API
    override fun crossoverProbability(): Double = probability
    override fun numberOfRequiredParents(): Int = 2
    override fun numberOfGeneratedChildren(): Int = 2

    override fun execute(solutionList: MutableList<BinarySolution>): MutableList<BinarySolution> {
        /* Defensive: NSGA-II should always pass exactly 2 parents. */
        require(solutionList.size >= 2) { "BinaryPruningCrossover requires 2 parents (got ${solutionList.size})" }

        val parent1 = solutionList[0] as BestSubsetSolution
        val parent2 = solutionList[1] as BestSubsetSolution

        val maskP1: BooleanArray = parent1.retrieveTopicStatus()
        val maskP2: BooleanArray = parent2.retrieveTopicStatus()

        /* Children are deep copies; we’ll edit bits in-place. */
        val child1 = parent1.copy()
        val child2 = parent2.copy()

        if (JMetalRandom.getInstance().nextDouble() < probability) {
            /* Compute a safe working length across parents and children. */
            val bitsChild1 = child1.numberOfBitsPerVariable()[0]
            val bitsChild2 = child2.numberOfBitsPerVariable()[0]
            val safeLen = minOf(maskP1.size, maskP2.size, bitsChild1, bitsChild2)

            /* Log if anything is mismatched (shouldn’t happen, but better visible than crashing). */
            if (!(maskP1.size == maskP2.size && maskP1.size == bitsChild1 && bitsChild1 == bitsChild2)) {
                logger.warn(
                    "BinaryPruningCrossover: mismatched lengths -> " +
                            "p1Mask=${maskP1.size}, p2Mask=${maskP2.size}, c1=$bitsChild1, c2=$bitsChild2; using safeLen=$safeLen"
                )
            }

            // Pruning crossover over the common, safe range.
            for (bitIndex in 0 until safeLen) {
                val p1 = maskP1[bitIndex]
                val p2 = maskP2[bitIndex]
                child1.setBitValue(bitIndex, p1 && p2)  // more selective
                child2.setBitValue(bitIndex, p1 || p2)  // more inclusive
            }

            // If children are longer than safeLen, we leave tail bits untouched (from parent copy).

            // Ensure feasibility: at least one topic selected in each child.
            fun ensureAtLeastOneSelected(sol: BestSubsetSolution) {
                if (sol.numberOfSelectedTopics == 0) {
                    val n = sol.numberOfBitsPerVariable()[0]
                    val flipIdx = if (n <= 1) 0 else JMetalRandom.getInstance().nextInt(0, n - 1)
                    sol.setBitValue(flipIdx, true)
                }
            }
            ensureAtLeastOneSelected(child1)
            ensureAtLeastOneSelected(child2)
        }

        /* Debug (kept for traceability) */
        logger.debug("<Num. Sel. Topics: ${parent1.numberOfSelectedTopics}, Parent 1: ${parent1.getVariableValueString(0)}>")
        logger.debug("<Num. Sel. Topics: ${parent2.numberOfSelectedTopics}, Parent 2: ${parent2.getVariableValueString(0)}>")
        logger.debug("<Num. Sel. Topics: ${child1.numberOfSelectedTopics}, Child 1: ${child1.getVariableValueString(0)}>")
        logger.debug("<Num. Sel. Topics: ${child2.numberOfSelectedTopics}, Child 2: ${child2.getVariableValueString(0)}>")
        return mutableListOf(child1, child2)
    }

}
