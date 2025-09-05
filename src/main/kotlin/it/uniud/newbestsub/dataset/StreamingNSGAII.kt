package it.uniud.newbestsub.dataset

import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.apache.logging.log4j.Logger
import org.uma.jmetal.operator.crossover.CrossoverOperator
import org.uma.jmetal.operator.mutation.MutationOperator
import org.uma.jmetal.operator.selection.SelectionOperator
import org.uma.jmetal.problem.Problem
import org.uma.jmetal.solution.binarysolution.BinarySolution
import org.uma.jmetal.util.binarySet.BinarySet
import org.uma.jmetal.util.evaluator.SolutionListEvaluator
import org.uma.jmetal.util.ranking.Ranking
import org.uma.jmetal.util.ranking.impl.MergeNonDominatedSortRanking
import kotlin.math.min

/**
 * Streaming NSGA-II variant with:
 * - Per-generation callback [onGeneration] (init, every update, before result()).
 * - MNDS (MergeNonDomininatedSortRanking).
 * - Optimized replacement:
 *   - Offspring de-dup via sparse 64-bit hash over set bit indices (FastUtil LongOpenHashSet, no boxing).
 *   - Array-based crowding distance (no boxing/maps) with reusable scratch buffers.
 *   - Partial selection on the split front via quickselect (avg O(n)).
 *   - Deterministic tie-breaking when distances are equal.
 *   - Early exits for “no new offspring” and “single front ≤ capacity”.
 */
class StreamingNSGAII(
    problem: Problem<BinarySolution>,
    maxEvaluations: Int,
    populationSize: Int,
    crossover: CrossoverOperator<BinarySolution>,
    mutation: MutationOperator<BinarySolution>,
    selection: SelectionOperator<List<BinarySolution>, BinarySolution>,
    evaluator: SolutionListEvaluator<BinarySolution>,
    private val onGeneration: (Int, List<BinarySolution>) -> Unit,
    private val logger: Logger
) : org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAII<BinarySolution>(
    problem,
    maxEvaluations,
    populationSize,
    populationSize, // matingPoolSize
    populationSize, // offspringPopulationSize
    crossover,
    mutation,
    selection,
    evaluator
) {

    /* ---------------- Progress / lifecycle ---------------- */

    private var generationIndex: Int = 0

    override fun initProgress() {
        super.initProgress()
        generationIndex = 0
        logger.debug("gen={} initProgress -> initial population ready (size={})", generationIndex, population.size)
        onGeneration.invoke(generationIndex, population)
    }

    override fun updateProgress() {
        super.updateProgress()
        generationIndex += 1
        logger.debug("gen={} updateProgress -> population updated (size={})", generationIndex, population.size)
        onGeneration.invoke(generationIndex, population)
    }

    override fun result(): List<BinarySolution> {
        logger.debug("gen={} final onGeneration before result() (size={})", generationIndex, population.size)
        onGeneration.invoke(generationIndex, population)
        return super.result()
    }

    /* ---------------- Replacement (MNDS + optimized crowding) ---------------- */

    // Reusable scratch buffers to avoid per-generation allocations
    private var crowdingDistanceScratch: DoubleArray = DoubleArray(0)
    private var indexOrderScratch: IntArray = IntArray(0)
    private var wasPickedScratch: BooleanArray = BooleanArray(0)

    override fun replacement(
        parentPopulation: List<BinarySolution>,
        offspringPopulation: List<BinarySolution>
    ): List<BinarySolution> {
        val targetPopulationSize = parentPopulation.size

        // --- Offspring de-duplication (sparse hash over set bits) ---
        val seenHashes = LongOpenHashSet(offspringPopulation.size * 2)
        val uniqueOffspring = ArrayList<BinarySolution>(offspringPopulation.size)
        for (child in offspringPopulation) {
            val mask = child.variables()[0] as BinarySet
            var h = 0x9E3779B97F4A7C15uL.toLong()              // seed
            var card = 0
            var i = mask.nextSetBit(0)
            while (i >= 0) {
                // Mix index with splitmix64-style mixer, then fold
                h = (h xor mix64(i.toLong())) * 0xC2B2AE3D27D4EB4FuL.toLong()
                card++
                i = mask.nextSetBit(i + 1)
            }
            // Fold in cardinality and universe size to further decorrelate
            h = h xor (card.toLong() * 0x9E3779B97F4A7C15uL.toLong())
            if (seenHashes.add(h)) uniqueOffspring.add(child)
        }

        // Early exit #1: no new offspring after de-dup → keep parents as-is
        if (uniqueOffspring.isEmpty()) {
            return parentPopulation
        }

        // --- MNDS ranking on merged population ---
        val mergedPopulation = ArrayList<BinarySolution>(targetPopulationSize + uniqueOffspring.size).apply {
            addAll(parentPopulation)
            addAll(uniqueOffspring)
        }
        val mnds: Ranking<BinarySolution> = MergeNonDominatedSortRanking()
        mnds.compute(mergedPopulation)

        // Early exit #2: single non-dominated front that fits entirely → no crowding/selection needed
        if (mnds.numberOfSubFronts == 1) {
            val onlyFront = mnds.getSubFront(0)
            if (onlyFront.size <= targetPopulationSize) {
                return ArrayList(onlyFront)
            }
        }

        // --- Survivor selection with partial selection on split front ---
        val nextGeneration = ArrayList<BinarySolution>(targetPopulationSize)
        var frontIndex = 0
        while (nextGeneration.size < targetPopulationSize && frontIndex < mnds.numberOfSubFronts) {
            val currentFront = mnds.getSubFront(frontIndex)
            if (nextGeneration.size + currentFront.size <= targetPopulationSize) {
                nextGeneration.addAll(currentFront)
            } else {
                val slotsRemaining = targetPopulationSize - nextGeneration.size
                ensureScratchCapacity(currentFront.size)

                // Crowding distances (array-based)
                computeCrowdingDistancesInto(currentFront, crowdingDistanceScratch, indexOrderScratch)

                // Determine threshold via quickselect on a copy so we don't mutate the scratch
                val distancesForSelect = crowdingDistanceScratch.copyOf(currentFront.size)
                val kthToDrop = currentFront.size - slotsRemaining  // 0-based rank of the smallest we keep threshold against
                val distanceThreshold = quickselect(distancesForSelect, kthToDrop)

                // Pick all with distance > threshold first (strictly better)
                var chosenCount = 0
                var i = 0
                while (i < currentFront.size && chosenCount < slotsRemaining) {
                    if (crowdingDistanceScratch[i] > distanceThreshold) {
                        nextGeneration.add(currentFront[i])
                        wasPickedScratch[i] = true
                        chosenCount++
                    }
                    i++
                }

                // Then pick those with distance == threshold in deterministic index order
                i = 0
                while (i < currentFront.size && chosenCount < slotsRemaining) {
                    if (!wasPickedScratch[i] && crowdingDistanceScratch[i] == distanceThreshold) {
                        nextGeneration.add(currentFront[i])
                        wasPickedScratch[i] = true
                        chosenCount++
                    }
                    i++
                }
            }
            frontIndex++
        }
        return nextGeneration
    }

    /**
     * Ensure reusable buffers are large enough for the current front size.
     */
    private fun ensureScratchCapacity(requiredSize: Int) {
        if (crowdingDistanceScratch.size < requiredSize) crowdingDistanceScratch = DoubleArray(requiredSize)
        if (indexOrderScratch.size < requiredSize) indexOrderScratch = IntArray(requiredSize)
        if (wasPickedScratch.size < requiredSize) wasPickedScratch = BooleanArray(requiredSize)
        // Reset only the slice we’ll use
        java.util.Arrays.fill(wasPickedScratch, 0, requiredSize, false)
    }

    /**
     * Compute crowding distances into [distanceOut] for the given [front].
     * Uses [indexOrderOut] as an index scratch for per-objective sorting.
     * Distances are accumulated; boundary points get +∞.
     */
    private fun computeCrowdingDistancesInto(
        front: List<BinarySolution>,
        distanceOut: DoubleArray,
        indexOrderOut: IntArray
    ) {
        val n = front.size
        java.util.Arrays.fill(distanceOut, 0, n, 0.0)
        if (n <= 2) {
            java.util.Arrays.fill(distanceOut, 0, n, Double.POSITIVE_INFINITY)
            return
        }

        // initialize 0..n-1
        var i = 0
        while (i < n) {
            indexOrderOut[i] = i
            i++
        }

        val objectiveCount = front[0].objectives().size
        var obj = 0
        while (obj < objectiveCount) {
            // sort indices by this objective (ascending, deterministic on ties)
            quicksortIndicesByObjective(front, indexOrderOut, 0, n - 1, obj)

            val minValue = front[indexOrderOut[0]].objectives()[obj]
            val maxValue = front[indexOrderOut[n - 1]].objectives()[obj]
            val range = maxValue - minValue

            distanceOut[indexOrderOut[0]] = Double.POSITIVE_INFINITY
            distanceOut[indexOrderOut[n - 1]] = Double.POSITIVE_INFINITY

            if (range != 0.0) {
                var pos = 1
                while (pos < n - 1) {
                    val left = front[indexOrderOut[pos - 1]].objectives()[obj]
                    val right = front[indexOrderOut[pos + 1]].objectives()[obj]
                    val increment = (right - left) / range
                    if (distanceOut[indexOrderOut[pos]].isFinite()) {
                        distanceOut[indexOrderOut[pos]] += increment
                    }
                    pos++
                }
            }
            obj++
        }
    }

    /* ---------------- In-place quicksort of index array for a specific objective ---------------- */

    private fun quicksortIndicesByObjective(
        front: List<BinarySolution>,
        indices: IntArray,
        leftBound: Int,
        rightBound: Int,
        objectiveIndex: Int
    ) {
        var left = leftBound
        var right = rightBound
        // small manual stack to avoid recursion
        val stackLeft = IntArray(64)
        val stackRight = IntArray(64)
        var top = 0
        stackLeft[top] = left
        stackRight[top] = right
        top++

        while (top > 0) {
            top--
            left = stackLeft[top]
            right = stackRight[top]
            if (left >= right) continue

            val pivotIndex = (left + right) ushr 1
            val pivotValIndex = indices[pivotIndex]

            var i = left
            var j = right
            while (i <= j) {
                while (compareObjective(front, indices[i], pivotValIndex, objectiveIndex) < 0) i++
                while (compareObjective(front, indices[j], pivotValIndex, objectiveIndex) > 0) j--
                if (i <= j) {
                    val tmp = indices[i]; indices[i] = indices[j]; indices[j] = tmp
                    i++; j--
                }
            }
            // Push larger partition first to keep stack shallow
            val leftLen = j - left
            val rightLen = right - i
            if (leftLen > rightLen) {
                if (left < j) {
                    stackLeft[top] = left; stackRight[top] = j; top++
                }
                if (i < right) {
                    stackLeft[top] = i; stackRight[top] = right; top++
                }
            } else {
                if (i < right) {
                    stackLeft[top] = i; stackRight[top] = right; top++
                }
                if (left < j) {
                    stackLeft[top] = left; stackRight[top] = j; top++
                }
            }
        }
    }

    private fun compareObjective(
        front: List<BinarySolution>,
        leftIndex: Int,
        rightIndex: Int,
        objectiveIndex: Int
    ): Int {
        val a = front[leftIndex].objectives()[objectiveIndex]
        val b = front[rightIndex].objectives()[objectiveIndex]
        return when {
            a < b -> -1
            a > b -> 1
            else -> leftIndex - rightIndex // deterministic tie-breaker
        }
    }

    /* ---------------- Quickselect (kth order statistic, 0-based) ---------------- */

    private fun quickselect(values: DoubleArray, k: Int): Double {
        var left = 0
        var right = values.lastIndex
        val targetRank = min(k, values.size - 1).coerceAtLeast(0)
        while (left <= right) {
            val pivotIdx = partition(values, left, right)
            when {
                targetRank < pivotIdx -> right = pivotIdx - 1
                targetRank > pivotIdx -> left = pivotIdx + 1
                else -> return values[pivotIdx]
            }
        }
        return values[targetRank]
    }

    private fun partition(arr: DoubleArray, left: Int, right: Int): Int {
        val pivot = arr[right]
        var store = left
        var scan = left
        while (scan < right) {
            if (arr[scan] <= pivot) {
                val tmp = arr[store]; arr[store] = arr[scan]; arr[scan] = tmp
                store++
            }
            scan++
        }
        val tmp = arr[store]; arr[store] = arr[right]; arr[right] = tmp
        return store
    }

    /* ---------------- 64-bit mixing (splitmix64-style) for robust hashing ---------------- */

    private fun mix64(z0: Long): Long {
        var z = z0 + 0x9E3779B97F4A7C15uL.toLong()
        z = (z xor (z ushr 30)) * 0xBF58476D1CE4E5B9uL.toLong()
        z = (z xor (z ushr 27)) * 0x94D049BB133111EBuL.toLong()
        return z xor (z ushr 31)
    }
}
