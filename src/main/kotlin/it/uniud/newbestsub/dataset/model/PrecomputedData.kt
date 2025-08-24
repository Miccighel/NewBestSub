package it.uniud.newbestsub.dataset.model

import kotlin.collections.iterator

/**
 * Immutable, shared, hot-path numeric bundle.
 *
 * Built once at dataset load time and injected wherever evaluations happen
 * (Best, Worst, and Average problems, as well as seeders).
 *
 * ## Layout
 * - Row-major matrix of AP scores per system and topic.
 * - Column views to support delta evaluations.
 * - Precomputed full-set means per system.
 *
 * ## Indices & IDs
 * - Systems are referenced by `0..numberOfSystems-1` in the order of [systemIdsInOrder].
 * - [systemIndexById] maps a system identifier to its compact row index.
 *
 * ## Safety
 * - All arrays are owned by this object.
 * - Do not mutate them after construction.
 *
 * @property averagePrecisionBySystem Row-major AP matrix: `averagePrecisionBySystem[s][t]`
 *  gives the AP of system *s* on topic *t*.
 * @property topicColumnViewByTopic Column-major AP matrix: `topicColumnViewByTopic[t][s]`
 *  gives the AP of system *s* on topic *t*. Used for fast ± updates.
 * @property fullSetMeanAPBySystem Mean AP of each system over all topics.
 * @property numberOfSystems Total number of systems in the dataset.
 * @property numberOfTopics Total number of topics in the dataset.
 * @property systemIdsInOrder Deterministic list of system IDs, defining row order.
 * @property systemIndexById Map from system ID to its row index.
 */
data class PrecomputedData(
    val averagePrecisionBySystem: Array<DoubleArray>,  // [S][T]
    val topicColumnViewByTopic: Array<DoubleArray>,    // [T][S]
    val fullSetMeanAPBySystem: DoubleArray,            // [S]
    val numberOfSystems: Int,
    val numberOfTopics: Int,
    val systemIdsInOrder: List<String>,
    val systemIndexById: Map<String, Int>
) {
    init {
        require(numberOfSystems == averagePrecisionBySystem.size) {
            "Row count mismatch: got ${averagePrecisionBySystem.size}, expected $numberOfSystems"
        }
        averagePrecisionBySystem.forEachIndexed { s, row ->
            require(row.size == numberOfTopics) {
                "Column count mismatch at system row $s: got ${row.size}, expected $numberOfTopics"
            }
        }
        require(topicColumnViewByTopic.size == numberOfTopics) {
            "Column view count mismatch: got ${topicColumnViewByTopic.size}, expected $numberOfTopics"
        }
        topicColumnViewByTopic.forEachIndexed { t, col ->
            require(col.size == numberOfSystems) {
                "Column view height mismatch at topic $t: got ${col.size}, expected $numberOfSystems"
            }
        }
        require(fullSetMeanAPBySystem.size == numberOfSystems) {
            "fullSetMeanAPBySystem length mismatch: got ${fullSetMeanAPBySystem.size}, expected $numberOfSystems"
        }
        require(systemIdsInOrder.size == numberOfSystems) {
            "systemIdsInOrder length mismatch: got ${systemIdsInOrder.size}, expected $numberOfSystems"
        }
        require(systemIndexById.size == numberOfSystems) {
            "systemIndexById length mismatch: got ${systemIndexById.size}, expected $numberOfSystems"
        }
    }
}

/**
 * Build [PrecomputedData] from primitive AP rows.
 *
 * Input is a mapping `systemId -> DoubleArray(AP per topic)`.
 *
 * ## Steps
 * - Copy rows into a dense row-major matrix `[S][T]`
 * - Compute full-set means per system
 * - Build column views `[T][S]` for fast ± updates
 *
 * @param averagePrecisionsBySystem Mapping from system ID to per-topic AP scores.
 *  Each entry is a `DoubleArray` where `array[t]` is the AP of the system on topic *t*.
 * @return a [PrecomputedData] structure containing row-major, column-major, and mean views.
 * @throws IllegalArgumentException if input is empty or inconsistent.
 */
fun buildPrecomputedData(
    averagePrecisionsBySystem: Map<String, DoubleArray>
): PrecomputedData {
    require(averagePrecisionsBySystem.isNotEmpty()) { "averagePrecisionsBySystem must not be empty" }

    // Deterministic system order (stable across runs)
    val systemIdsInOrder = averagePrecisionsBySystem.keys.sorted()
    val numberOfSystems = systemIdsInOrder.size

    val firstRow = averagePrecisionsBySystem.getValue(systemIdsInOrder.first())
    val numberOfTopics = firstRow.size
    require(numberOfTopics > 0) { "AP rows must contain at least one topic" }

    // Dense row-major: [system][topic]
    val averagePrecisionBySystem = Array(numberOfSystems) { DoubleArray(numberOfTopics) }
    for ((sysIdx, sysId) in systemIdsInOrder.withIndex()) {
        val src = averagePrecisionsBySystem.getValue(sysId)
        require(src.size == numberOfTopics) {
            "Row length mismatch for system '$sysId': got ${src.size}, expected $numberOfTopics"
        }
        System.arraycopy(src, 0, averagePrecisionBySystem[sysIdx], 0, numberOfTopics)
    }

    // Full-set mean AP per system
    val fullSetMeanAPBySystem = DoubleArray(numberOfSystems)
    var s = 0
    while (s < numberOfSystems) {
        val row = averagePrecisionBySystem[s]
        var sum = 0.0
        var t = 0
        while (t < numberOfTopics) {
            sum += row[t]; t++
        }
        fullSetMeanAPBySystem[s] = sum / numberOfTopics
        s++
    }

    // Column views: [topic][system] → used by delta-evaluation later
    val topicColumnViewByTopic = Array(numberOfTopics) { DoubleArray(numberOfSystems) }
    var topicIdx = 0
    while (topicIdx < numberOfTopics) {
        var sysIdx = 0
        while (sysIdx < numberOfSystems) {
            topicColumnViewByTopic[topicIdx][sysIdx] = averagePrecisionBySystem[sysIdx][topicIdx]
            sysIdx++
        }
        topicIdx++
    }

    val systemIndexById = systemIdsInOrder.withIndex().associate { it.value to it.index }

    return PrecomputedData(
        averagePrecisionBySystem = averagePrecisionBySystem,
        topicColumnViewByTopic = topicColumnViewByTopic,
        fullSetMeanAPBySystem = fullSetMeanAPBySystem,
        numberOfSystems = numberOfSystems,
        numberOfTopics = numberOfTopics,
        systemIdsInOrder = systemIdsInOrder,
        systemIndexById = systemIndexById
    )
}

/**
 * Convert legacy boxed rows to primitive arrays.
 *
 * Intended to be used **once at load time**.
 * Do **not** call this inside evaluation loops.
 *
 * @param legacy Mapping from system ID to boxed `Array<Double>`.
 * @return A mapping with equivalent primitive `DoubleArray` rows.
 */
fun toPrimitiveAPRows(
    legacy: Map<String, Array<Double>>
): Map<String, DoubleArray> {
    val out = HashMap<String, DoubleArray>(legacy.size)
    for ((k, boxed) in legacy) {
        val row = DoubleArray(boxed.size)
        var i = 0
        while (i < boxed.size) {
            row[i] = boxed[i]; i++
        }
        out[k] = row
    }
    return out
}
