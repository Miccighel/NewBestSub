package it.uniud.newbestsub.dataset.model

/**
 * ProgressEvent family
 * ====================
 *
 * These are the *only* messages that flow from the solver/model to the outside world.
 * They are intentionally small and transport‑format‑neutral so the model can publish
 * progress without knowing anything about CSV or filesystem details (that’s the View’s job).
 *
 * ## Conventions
 * - Every event carries:
 *   - `target`: `BEST` | `WORST` | `AVERAGE` (so the consumer can route to the right files)
 *   - `threadName`: purely informational; useful to keep logs readable
 *
 * - Typical controller routing:
 *   ```
 *   when (ev) {
 *     is CardinalityResult -> view.appendCardinality(model, ev)
 *     is TopKReplaceBatch  -> view.replaceTopBatch(model, ev.blocks)
 *     is RunCompleted      -> view.closeStreams(model)
 *   }
 *   ```
 *
 * - `CardinalityResult`:
 *   - AVERAGE: exactly once per K (1..#topics)
 *   - BEST/WORST: only when the per‑K representative improves in that generation
 *   - `functionValuesCsvLine` and `variableValuesCsvLine` are already in the jMetal style
 *     expected by the sink; the View appends them and keeps a buffer to globally sort & rewrite on close
 *
 * - `TopKReplaceBatch`:
 *   - Emitted once per generation (and once at the very end)
 *   - Contains the set of K blocks whose content changed
 *   - Each K maps to **exactly 10 pre‑sorted CSV lines** (by correlation ASC)
 *   - The View overwrites the entire `-Top` file with all known blocks ordered by K
 *
 * - `RunCompleted`:
 *   - Signals end‑of‑stream so the View can close writers and perform the final
 *     global sort & rewrite of `-Fun`/`-Var`
 *
 * @property target Logical target bucket: `BEST`, `WORST`, or `AVERAGE`.
 * @property threadName Originating thread name (informational for logs).
 */
sealed interface ProgressEvent {
    val target: String
    val threadName: String
}

/**
 * Stream one improved checkpoint row (and paired bitstring) for a specific cardinality K.
 *
 * ## Notes
 * - The View parses K and correlation from [functionValuesCsvLine] to keep an aligned buffer.
 * - [correlation] is included for convenience/logging; the sink uses the CSV line
 *   as the authoritative payload.
 *
 * @property target Logical target bucket: `BEST`, `WORST`, or `AVERAGE`.
 * @property threadName Originating thread name (informational for logs).
 * @property cardinality The K (number of selected topics) this row refers to.
 * @property correlation Correlation value for this checkpoint (informational).
 * @property functionValuesCsvLine jMetal‑style FUN line, e.g. `"K corr"` (already normalized for external view).
 * @property variableValuesCsvLine Matching VAR line: space‑separated bitstring (`"0 1 0 1 ..."` or compact if configured).
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
 * Batch of **K → 10 CSV lines**, where each list is already sorted by correlation ASC and
 * contains **exactly 10** rows for that K:
 *
 * ```
 * "Cardinality,Correlation,Topics"
 * e.g. "12,0.1375,topicA|topicB|..."
 * ```
 *
 * The View merges these with previously known blocks and rewrites the entire `-Top` file
 * in K ascending order (header + concatenated blocks).
 *
 * @property target Logical target bucket: `BEST`, `WORST`, or `AVERAGE`.
 * @property threadName Originating thread name (informational for logs).
 * @property blocks Map of cardinality K → list of 10 pre‑sorted CSV rows for that K.
 */
data class TopKReplaceBatch(
    override val target: String,
    override val threadName: String,
    /** Map of cardinality K -> list of 10 pre-sorted CSV rows for that K. */
    val blocks: Map<Int, List<String>>
) : ProgressEvent

/**
 * Terminal signal for a target run.
 *
 * The View should:
 * - close open writers
 * - globally sort buffered `-Fun`/`-Var` rows by `(K asc, corr asc)`
 * - rewrite both files aligned
 *
 * @property target Logical target bucket: `BEST`, `WORST`, or `AVERAGE`.
 * @property threadName Originating thread name (informational for logs).
 * @property computingTime Wall‑clock computing time in milliseconds (informational).
 */
data class RunCompleted(
    override val target: String,
    override val threadName: String,
    /** Wall-clock computing time in milliseconds (informational). */
    val computingTime: Long
) : ProgressEvent
