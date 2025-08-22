package it.uniud.newbestsub.dataset.model

/**
 * ProgressEvent family
 * ====================
 *
 * These are the *only* messages that flow from the solver/model to the outside world.
 * They are intentionally small and transport-format-neutral so the model can publish
 * progress without knowing anything about CSV or filesystem details (that’s the View’s job).
 *
 * Conventions
 * -----------
 * - Every event carries:
 *     • `target`: BEST | WORST | AVERAGE  (so the consumer can route to the right files)
 *     • `threadName`: purely informational; useful to keep logs readable.
 *
 * - The controller generally does:
 *       when (ev) {
 *         is CardinalityResult -> view.appendCardinality(model, ev)
 *         is TopKReplaceBatch  -> view.replaceTopBatch(model, ev.blocks)
 *         is RunCompleted      -> view.closeStreams(model)
 *       }
 *
 * - `CardinalityResult`:
 *     Generated for:
 *       • AVERAGE: exactly once per K (1..#topics).
 *       • BEST/WORST: only when the per-K representative improves in that generation.
 *     The strings `functionValuesCsvLine` and `variableValuesCsvLine` are already formatted
 *     in the jMetal style expected by the sink. The View will append them and also keep a
 *     buffer to globally sort & rewrite on close.
 *
 * - `TopKReplaceBatch`:
 *     Generated once per generation (and once at the very end), containing the set of K
 *     blocks whose content changed. Each list must contain **exactly 10 pre-sorted CSV lines**
 *     (by correlation ASC). The View overwrites the whole -Top file with all known blocks
 *     ordered by K.
 *
 * - `RunCompleted`:
 *     Signals end-of-stream for the target so the View can close writers and perform the final
 *     global sort & rewrite of -Fun/-Var.
 */
sealed interface ProgressEvent {
    val target: String
    val threadName: String
}

/**
 * Stream one improved checkpoint row (and paired bitstring) for a specific cardinality K.
 *
 * Notes
 * -----
 * - The View parses K and corr from `functionValuesCsvLine` to keep an aligned buffer.
 * - `correlation` is included here for convenience/logging but the sink uses the CSV line
 *   as the authoritative payload.
 */
data class CardinalityResult(
    override val target: String,
    override val threadName: String,
    val cardinality: Int,
    val correlation: Double,
    /** jMetal-style FUN line: `"K corr"` (external-view normalized already). */
    val functionValuesCsvLine: String,
    /** Matching VAR line: space-separated bitstring `"0 1 0 1 ..."` or compact if you prefer. */
    val variableValuesCsvLine: String
) : ProgressEvent

/**
 * Batch of K → 10 CSV lines, where each list is already sorted by correlation ASC and
 * contains **exactly 10** rows for that K:
 *
 *   "Cardinality,Correlation,Topics"
 *   e.g. "12,0.1375,topicA|topicB|..."
 *
 * The View merges these with previously known blocks and rewrites the entire -Top file
 * in K ascending order (header + concatenated blocks).
 */
data class TopKReplaceBatch(
    override val target: String,
    override val threadName: String,
    /** Map of cardinality K -> list of 10 pre-sorted CSV rows for that K. */
    val blocks: Map<Int, List<String>>
) : ProgressEvent

/**
 * Terminal signal for a target run. The View should:
 *   - close open writers
 *   - globally sort buffered -Fun/-Var rows by (K asc, corr asc)
 *   - rewrite both files aligned
 */
data class RunCompleted(
    override val target: String,
    override val threadName: String,
    /** Wall-clock computing time in milliseconds (informational). */
    val computingTime: Long
) : ProgressEvent
