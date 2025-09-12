package it.uniud.newbestsub.problem.operators

import it.uniud.newbestsub.problem.BestSubsetSolution
import org.uma.jmetal.operator.crossover.CrossoverOperator
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.pseudorandom.JMetalRandom

/**
 * BinaryPruningCrossover — AND/OR crossover with pruning/expansion.
 *
 * Behavior (applied with probability [crossoverProbability]):
 * • child1 = A AND B  (intersection / pruning)
 * • child2 = A OR  B  (union / expansion)
 * • If child1 ends up empty (K == 0), flip one random bit to true (repair).
 * • If crossover does not occur, children are value-copies of the parents.
 *
 * Determinism:
 * All randomness uses jMetal's [JMetalRandom] singleton; set the seed upstream.
 *
 * @param crossoverProbability Per-mating crossover probability in (0.0, 1.0].
 */
class BinaryPruningCrossover(
    private val crossoverProbability: Double
) : CrossoverOperator<BinarySolution> {

    /** Two parents are required by this operator. */
    override fun numberOfRequiredParents(): Int = 2

    /** Two children are produced by this operator. */
    override fun numberOfGeneratedChildren(): Int = 2

    /** Returns the configured crossover probability. */
    override fun crossoverProbability(): Double = crossoverProbability

    /**
     * Execute AND/OR crossover with repair on the pruned child.
     *
     * Parents must be [BestSubsetSolution]. Children preserve objective vectors
     * from their respective parents.
     *
     * @param parents Exactly two [BestSubsetSolution] parents.
     * @return Two children (list size = 2).
     * @throws IllegalArgumentException if [parents].size != 2.
     * @throws ClassCastException if parents are not [BestSubsetSolution].
     */
    override fun execute(parents: List<BinarySolution>): List<BinarySolution> {
        require(parents.size == 2) { "BinaryPruningCrossover requires exactly two parents" }

        val pA = parents[0] as BestSubsetSolution
        val pB = parents[1] as BestSubsetSolution

        /* Sizes derived from parent A; parent B is assumed shape-compatible. */
        val n = pA.retrieveTopicStatus().size
        val m = pA.objectives().size

        val rng = JMetalRandom.getInstance()

        /* No crossover path: return value-copies of parents. */
        if (rng.nextDouble() >= crossoverProbability) {
            val c1 = cloneFromParent(pA, n, m)
            val c2 = cloneFromParent(pB, n, m)
            return listOf(c1, c2)
        }

        /* Prepare empty children with the same shape as parents. */
        val c1 = BestSubsetSolution(1, m, n, pA.topicLabels, null)
        val c2 = BestSubsetSolution(1, m, n, pA.topicLabels, null)

        val a = pA.variables()[0] as BinarySet
        val b = pB.variables()[0] as BinarySet

        /* Build AND / OR masks in fresh BinarySets to avoid mutating parents. */
        val andBits = BinarySet(n).apply { this.or(a); this.and(b) }   /* A ∧ B (pruning) */
        val orBits  = BinarySet(n).apply { this.or(a); this.or(b) }    /* A ∨ B (expansion) */

        c1.variables()[0] = andBits
        c2.variables()[0] = orBits

        /* Repair: ensure child1 is non-empty to keep K ≥ 1. */
        if (andBits.cardinality() == 0) {
            val idx = if (n <= 1) 0 else rng.nextInt(0, n - 1)
            andBits.set(idx, true)
        }

        /* Copy objective values from parents. */
        copyObjectives(from = pA, to = c1, m = m)
        copyObjectives(from = pB, to = c2, m = m)

        /* Refresh mirrors / derived status if BestSubsetSolution expects it. */
        c1.topicStatus = c1.retrieveTopicStatus().toTypedArray()
        c2.topicStatus = c2.retrieveTopicStatus().toTypedArray()

        return listOf(c1, c2)
    }

    /**
     * Create a value-clone child from a parent (variables + objectives).
     * Bitset is copied; objectives are copied; metadata replicated where available.
     */
    private fun cloneFromParent(parent: BestSubsetSolution, n: Int, m: Int): BestSubsetSolution {
        val child = BestSubsetSolution(1, m, n, parent.topicLabels, null)
        /* Copy bitset */
        val bits = BinarySet(n).apply { this.or(parent.variables()[0] as BinarySet) }
        child.variables()[0] = bits
        /* Copy objectives */
        copyObjectives(from = parent, to = child, m = m)
        /* Refresh mirrors */
        child.topicStatus = child.retrieveTopicStatus().toTypedArray()
        return child
    }

    /** Copy objective vector values (size [m]) from one solution to another. */
    private fun copyObjectives(from: BestSubsetSolution, to: BestSubsetSolution, m: Int) {
        var i = 0
        while (i < m) {
            to.objectives()[i] = from.objectives()[i]
            i++
        }
    }
}
