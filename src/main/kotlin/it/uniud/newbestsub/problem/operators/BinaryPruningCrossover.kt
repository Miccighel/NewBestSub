package it.uniud.newbestsub.problem.operators

import it.uniud.newbestsub.problem.BestSubsetSolution
import org.uma.jmetal.operator.crossover.CrossoverOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/**
 * Uniform crossover for binary masks with a light "pruning"/repair step so children are valid.
 *
 * Semantics (intended to match your previous behavior):
 * - **Uniform gene mixing**: for each bit, children receive either (A,B) or (B,A) with p=0.5.
 * - **K can drift**: crossover is free to change the number of selected topics.
 * - **Repair**: if a child ends up with K==0 (all zeros), flip one random bit to true
 *   so the solution is usable by the evaluator (and avoids NaNs).
 *
 * Determinism: relies on the jMetal [JMetalRandom] singleton. Seed it upstream.
 */
class BinaryPruningCrossover(
    private val crossoverProbability: Double
) : CrossoverOperator<BinarySolution> {

    override fun execute(parents: List<BinarySolution>): List<BinarySolution> {
        require(parents.size == 2) { "BinaryPruningCrossover requires exactly two parents" }

        val parentA = parents[0] as BestSubsetSolution
        val parentB = parents[1] as BestSubsetSolution

        val topicCount = parentA.retrieveTopicStatus().size
        val rng = JMetalRandom.getInstance().randomGenerator

        // No crossover? Return copies as children.
        if (rng.nextDouble() > crossoverProbability) {
            return listOf(parentA.copy(), parentB.copy())
        }

        // Prepare empty children (variables and mirrors will be set below)
        val child1 = BestSubsetSolution(1, 2, topicCount, parentA.topicLabels, null)
        val child2 = BestSubsetSolution(1, 2, topicCount, parentA.topicLabels, null)

        // Parent bitsets
        val bitsA = parentA.variables()[0] as BinarySet
        val bitsB = parentB.variables()[0] as BinarySet

        // New child bitsets
        val childBits1 = BinarySet(topicCount)
        val childBits2 = BinarySet(topicCount)

        var bitIndex = 0
        while (bitIndex < topicCount) {
            val takeFromAFirst = rng.nextDouble() < 0.5
            if (takeFromAFirst) {
                childBits1.set(bitIndex, bitsA.get(bitIndex))
                childBits2.set(bitIndex, bitsB.get(bitIndex))
            } else {
                childBits1.set(bitIndex, bitsB.get(bitIndex))
                childBits2.set(bitIndex, bitsA.get(bitIndex))
            }
            bitIndex++
        }

        // Repair: ensure non-empty masks
        if (childBits1.cardinality() == 0) {
            val flip = if (topicCount == 1) 0 else (rng.nextDouble() * topicCount).toInt()
            childBits1.set(flip, true)
        }
        if (childBits2.cardinality() == 0) {
            val flip = if (topicCount == 1) 0 else (rng.nextDouble() * topicCount).toInt()
            childBits2.set(flip, true)
        }

        // Install bitsets and refresh mirrors
        child1.variables()[0] = childBits1
        child2.variables()[0] = childBits2
        child1.topicStatus = child1.retrieveTopicStatus().toTypedArray()
        child2.topicStatus = child2.retrieveTopicStatus().toTypedArray()

        return listOf(child1, child2)
    }

    override fun crossoverProbability(): Double {
        return crossoverProbability
    }

    override fun numberOfRequiredParents(): Int = 2
    override fun numberOfGeneratedChildren(): Int = 2
}
