package it.uniud.newbestsub.dataset.model

/**
 * ProgressEvent family.
 *
 * These messages flow from the solver or model to the presentation layer.
 * The payloads are compact and transport neutral so that the model publishes
 * progress without any knowledge of CSV or filesystem details. The view layer
 * is responsible for formatting and persistence.
 *
 * Conventions
 * - Every event carries:
 *   - target: "BEST" | "WORST" | "AVERAGE" to route to the proper sinks.
 *   - threadName: informational field for readable logs.
 *
 * Typical routing example
 * ```
 * when (event) {
 *   is CardinalityResult -> view.appendCardinality(model, event)
 *   is TopKReplaceBatch  -> view.replaceTopBatch(model, event.blocks)
 *   is RunCompleted      -> view.closeStreams(model)
 * }
 * ```
 *
 * Event semantics
 * - CardinalityResult
 *   - AVERAGE: exactly once per K in 1..#topics.
 *   - BEST and WORST: emitted only when the per K representative improves
 *     within a generation.
 *   - functionValuesCsvLine and variableValuesCsvLine are normalized in the
 *     jMetal style expected by the sinks. The view appends them and keeps a
 *     buffer to globally sort and rewrite on close.
 * - TopKReplaceBatch
 *   - Emitted once per generation and once at termination.
 *   - Contains the set of cardinalities K whose top lists changed.
 *   - Each K maps to exactly 10 pre sorted CSV lines, ordered by correlation
 *     ascending. The view rewrites the entire -Top file with all known blocks
 *     ordered by K.
 * - RunCompleted
 *   - Signals end of stream so the view can close writers and perform the final
 *     global sort and rewrite of -Fun and -Var.
 *
 * Threading
 * - The threadName field is intended for log clarity only. No ordering is implied.
 */
sealed interface ProgressEvent {
    /** Logical target bucket: "BEST", "WORST", or "AVERAGE". */
    val target: String

    /** Originating thread name used for log readability. */
    val threadName: String
}

/**
 * Streams one improved checkpoint row (and matching bitstring) for a specific cardinality K.
 *
 * Notes
 * - The view parses K and correlation from functionValuesCsvLine to maintain an aligned buffer.
 * - The correlation property is included for convenience and logging. The sinks treat the CSV
 *   line as the authoritative payload.
 *
 * @property target Logical target bucket: "BEST", "WORST", or "AVERAGE".
 * @property threadName Originating thread name (informational).
 * @property cardinality Cardinality K (number of selected topics) referred to by this row.
 * @property correlation Correlation value for this checkpoint (informational).
 * @property functionValuesCsvLine jMetal style FUN line (for example, "K corr") already normalized
 * for the external view.
 * @property variableValuesCsvLine Matching VAR line: space separated bitstring
 * (for example, "0 1 0 1 ...") or a compact format if configured.
 */
data class CardinalityResult(
    override val target: String,
    override val threadName: String,
    val cardinality: Int,
    val correlation: Double,
    /** jMetal style FUN line, for example "K corr". */
    val functionValuesCsvLine: String,
    /** Matching VAR line: space separated bitstring, for example "0 1 0 1 ...". */
    val variableValuesCsvLine: String
) : ProgressEvent

/**
 * Batch of replacements for the top lists per cardinality K.
 *
 * Each entry associates a cardinality K with a list of exactly 10 CSV rows, already
 * sorted by correlation ascending. The view merges these blocks with previously known
 * content and rewrites the entire -Top file in increasing K order.
 *
 * Row format example
 * ```
 * "Cardinality,Correlation,Topics"
 * e.g. "12,0.1375,topicA|topicB|..."
 * ```
 *
 * @property target Logical target bucket: "BEST", "WORST", or "AVERAGE".
 * @property threadName Originating thread name (informational).
 * @property blocks Map cardinality K -> list of 10 pre sorted CSV rows for that K.
 */
data class TopKReplaceBatch(
    override val target: String,
    override val threadName: String,
    /** Map cardinality K -> list of 10 pre sorted CSV rows for that K. */
    val blocks: Map<Int, List<String>>
) : ProgressEvent

/**
 * Terminal signal for a target run.
 *
 * The view should perform the following:
 * - Close open writers.
 * - Globally sort buffered -Fun and -Var rows by (K ascending, correlation ascending).
 * - Rewrite both files in aligned order.
 *
 * @property target Logical target bucket: "BEST", "WORST", or "AVERAGE".
 * @property threadName Originating thread name (informational).
 * @property computingTime Wall clock computing time in milliseconds (informational).
 */
data class RunCompleted(
    override val target: String,
    override val threadName: String,
    /** Wall clock computing time in milliseconds (informational). */
    val computingTime: Long
) : ProgressEvent
