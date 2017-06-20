package it.uniud.newbestsub.dataset

data class Parameters(val correlationMethod: String, val targetToAchieve: String, val numberOfIterations: Int, val numberOfRepetitions: Int, val populationSize: Int, val percentiles: List<Int>)
