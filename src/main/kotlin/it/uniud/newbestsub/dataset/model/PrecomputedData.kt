package it.uniud.newbestsub.dataset.model

import kotlin.collections.iterator

/**
 * PrecomputedData
 * ---------------
 * Immutable, shared, hot-path numeric bundle. Built once at dataset load and
 * injected wherever evaluations happen (Best/Worst/Average problems, seeders).
 *
 * Layout
 *  - averagePrecisionBySystem[s][t] : AP of system s on topic t  (row-major)
 *  - topicColumnViewByTopic[t][s]   : AP of system s on topic t  (column view)
 *  - fullSetMeanAPBySystem[s]       : mean AP of system s over ALL topics
 *
 * Indices & IDs
 *  - Systems are referenced by 0..numberOfSystems-1 in the order systemIdsInOrder
 *  - systemIndexById["sysId"] -> compact row index
 *
 * Safety
 *  - All arrays are owned by this object. Do not mutate them after construction.
 */
data class PrecomputedData(
    val averagePrecisionBySystem: Array<DoubleArray>,  // [S][T]
    val topicColumnViewByTopic: Array<DoubleArray>,  // [T][S]
    val fullSetMeanAPBySystem: DoubleArray,         // [S]
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
 * Build PrecomputedData from primitive AP rows:
 *    systemId -> DoubleArray(AP per topic)
 *
 * Cost:
 *  - 1 pass to copy rows into a dense row-major matrix [S][T]
 *  - 1 pass to compute full-set means per system
 *  - 1 pass to build column views [T][S] for fast ± updates
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
 * Convert legacy boxed rows to primitives. Use this ONCE at load time.
 * Do NOT call this inside evaluation loops.
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
