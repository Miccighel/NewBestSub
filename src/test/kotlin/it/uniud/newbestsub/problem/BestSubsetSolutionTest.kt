package it.uniud.newbestsub.problem

import it.uniud.newbestsub.dataset.Parameters
import it.uniud.newbestsub.utils.Constants
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.uma.jmetal.solution.binarysolution.BinarySolution
import java.util.Random
import kotlin.test.assertEquals

/**
 * Tests for core operations of [BestSubsetSolution].
 *
 * Covered behavior:
 * - Bit flip semantics via [BestSubsetSolution.setBitValue] and selection count consistency.
 * - Correct computation of total bit capacity ([BestSubsetSolution.totalNumberOfBits]) as the sum
 *   of per‑variable capacities ([BestSubsetSolution.numberOfBitsPerVariable]).
 * - Value‑semantics cloning via [BestSubsetSolution.copy].
 *
 * Test data:
 * - A deterministic fake AP table is created for `numberOfTopics + 1` systems to satisfy
 *   [BestSubsetProblem] construction.
 */
@DisplayName("BestSubsetSolution – core operations")
class BestSubsetSolutionTest {

    /* ----------------------------------------------------------------------------------------------------------------
     * Test fixtures
     * ---------------------------------------------------------------------------------------------------------------- */

    /** Problem instance used only for construction needs in the fixtures. */
    private lateinit var testProblem: BestSubsetProblem

    /** Solution under test; independent from [testProblem] once constructed. */
    private lateinit var testSolution: BestSubsetSolution

    /** Dummy correlation strategy injected into the problem (returns a constant). */
    private lateinit var dummyCorrelationFunc: (Array<Double>, Array<Double>) -> Double

    /** Dummy target strategy injected into the problem (identity passthrough). */
    private lateinit var dummyTargetEncoder: (BinarySolution, Double) -> BinarySolution

    /** Deterministic fake AP rows keyed by system name. */
    private var fakeAveragePrecisions: MutableMap<String, Array<Double>> = LinkedHashMap()

    /**
     * Initializes deterministic fixtures:
     *
     * - Builds `numberOfTopics = 10`, `numberOfSystems = 11`.
     * - Populates [fakeAveragePrecisions] with values in `(0.01 .. 1.00]`.
     * - Constructs a minimal [BestSubsetProblem] with dummy strategies.
     * - Allocates a [BestSubsetSolution] with a single binary variable and two objectives.
     */
    @BeforeEach
    @DisplayName("BestSubsetSolution – initialize fixtures")
    fun initTest() {
        val numberOfTopics = 10
        val numberOfSystems = numberOfTopics + 1

        /* Build deterministic fake AP rows for systems */
        val rng = Random(12345)
        for (systemIndex in 0 until numberOfSystems) {
            val apRow = Array(numberOfTopics) { 0.0 }
            for (topicIdx in 0 until numberOfTopics) {
                // values in (0.01 .. 1.00], deterministic
                apRow[topicIdx] = (1 + (100 - 1) * rng.nextDouble()) / 100.0
            }
            fakeAveragePrecisions["System $systemIndex"] = apRow
        }

        dummyCorrelationFunc = { _, _ -> 0.0 }
        dummyTargetEncoder = { sol, _ -> sol }

        val parameters = Parameters(
            datasetName = "AH99",
            correlationMethod = Constants.CORRELATION_PEARSON,
            targetToAchieve = Constants.TARGET_BEST,
            numberOfIterations = 10_000,
            numberOfRepetitions = 1_000,
            populationSize = 1_000,
            currentExecution = 0,
            percentiles = listOf(50)
        )

        val dummyMeanAP = Array(numberOfSystems) { 0.0 }
        val topicLabels = Array(numberOfTopics) { i -> "Topic-$i" }

        testProblem = BestSubsetProblem(
            parameters = parameters,
            numberOfTopics = numberOfTopics,
            averagePrecisions = fakeAveragePrecisions,
            meanAveragePrecisions = dummyMeanAP,
            topicLabels = topicLabels,
            correlationStrategy = dummyCorrelationFunc,
            targetStrategy = dummyTargetEncoder
        )

        /* BestSubsetSolution instances are independent of the problem instance */
        testSolution = BestSubsetSolution(
            numberOfVariables = 1,
            numberOfObjectives = 2,
            numberOfTopics = numberOfTopics,
            topicLabels = topicLabels,
            forcedCardinality = null
        )
    }

    /* ----------------------------------------------------------------------------------------------------------------
     * Tests
     * ---------------------------------------------------------------------------------------------------------------- */

    /**
     * Verifies that toggling a bit with [BestSubsetSolution.setBitValue] both:
     * 1) changes the corresponding position in the internal mask, and
     * 2) updates [BestSubsetSolution.numberOfSelectedTopics] by exactly ±1 accordingly.
     */
    @Test
    @DisplayName("setBitValue toggles selection and updates counts consistently")
    fun setBitValueTest() {
        println("[BestSubsetSolutionTest setBitValue] - Test begins.")

        val oldSelectedCount = testSolution.numberOfSelectedTopics
        val oldMask = testSolution.retrieveTopicStatus()
        val wasSelected = oldMask[0]

        // flip bit 0
        testSolution.setBitValue(0, !wasSelected)

        val newSelectedCount = testSolution.numberOfSelectedTopics
        val newMask = testSolution.retrieveTopicStatus()
        val isSelected = newMask[0]

        println(
            "[BestSubsetSolutionTest setBitValue] - Before bit0=$wasSelected, After bit0=$isSelected, " +
                "SelectedCount: $oldSelectedCount -> $newSelectedCount"
        )

        // The bit must reflect the set value
        assertEquals(!wasSelected, isSelected, "bit 0 should be toggled")

        // Count must increase by 1 on select, decrease by 1 on deselect
        val expectedDelta = if (!wasSelected) +1 else -1
        assertEquals(oldSelectedCount + expectedDelta, newSelectedCount, "selected topics count should be updated")

        println("[BestSubsetSolutionTest setBitValue] - Test ends.")
    }

    /**
     * Ensures that [BestSubsetSolution.totalNumberOfBits] equals the sum of
     * [BestSubsetSolution.numberOfBitsPerVariable] for jMetal 6.x binary solutions.
     */
    @Test
    @DisplayName("totalNumberOfBits equals sum(numberOfBitsPerVariable)")
    fun getTotalNumberOfBitsTest() {
        println("[BestSubsetSolutionTest getTotalNumberOfBits] - Test begins.")

        // jMetal 6.x: capacity is reported via numberOfBitsPerVariable(); total is the sum
        val capacitySum = testSolution.numberOfBitsPerVariable().sum()
        val totalBits = testSolution.totalNumberOfBits()

        println("[BestSubsetSolutionTest getTotalNumberOfBits] - capacitySum=$capacitySum, totalBits=$totalBits")

        assertEquals(totalBits, capacitySum, "total bits must equal sum of per-variable capacities")

        println("[BestSubsetSolutionTest getTotalNumberOfBits] - Test ends.")
    }

    /**
     * Checks that [BestSubsetSolution.copy] produces a value‑equal clone (reflexive and equal‑to‑original).
     * jMetal’s [BinarySolution] equality is value‑based, so the cloned instance should compare equal.
     */
    @Test
    @DisplayName("copy produces an equal solution (value semantics)")
    fun copyTest() {
        println("[BestSubsetSolutionTest copy] - Test begins.")

        val cloned = testSolution.copy()
        // jMetal's BinarySolution implements equals with value semantics; ensure reflexivity and copy equality
        assertEquals(cloned, testSolution, "copy must be value-equal to the original")
        assertEquals(testSolution, testSolution, "solution must be equal to itself (sanity)")

        println("[BestSubsetSolutionTest copy] - Test ends.")
    }
}
