package it.uniud.newbestsub.problem

import org.uma.jmetal.solution.binarysolution.BinarySolution

/**
 * Convenience extension forwarders for call sites that type solutions as [BinarySolution].
 *
 * These functions downcast to [BestSubsetSolution] to expose domain-specific metrics
 * while preserving compatibility with code that uses generic jMetal types.
 *
 * Note: A runtime `ClassCastException` will be thrown if the receiver is not a [BestSubsetSolution].
 * Ensure that solutions passed through jMetal are instances of the project-specific implementation.
 */

/**
 * Returns the subset cardinality stored in the underlying [BestSubsetSolution].
 *
 * @receiver a [BinarySolution] that is actually a [BestSubsetSolution]
 * @return the subset size as a Double
 * @throws ClassCastException if the receiver is not a [BestSubsetSolution]
 */
fun BinarySolution.getCardinality(): Double =
    (this as BestSubsetSolution).getCardinality()

/**
 * Returns the correlation objective stored in the underlying [BestSubsetSolution].
 *
 * @receiver a [BinarySolution] that is actually a [BestSubsetSolution]
 * @return the correlation value as a Double
 * @throws ClassCastException if the receiver is not a [BestSubsetSolution]
 */
fun BinarySolution.getCorrelation(): Double =
    (this as BestSubsetSolution).getCorrelation()
