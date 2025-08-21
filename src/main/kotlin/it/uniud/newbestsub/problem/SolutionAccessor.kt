package it.uniud.newbestsub.problem

import org.uma.jmetal.solution.binarysolution.BinarySolution

/* Forwarders so existing code that types solutions as BinarySolution keeps compiling. */
fun BinarySolution.getCardinality(): Double =
    (this as BestSubsetSolution).getCardinality()

fun BinarySolution.getCorrelation(): Double =
    (this as BestSubsetSolution).getCorrelation()
