package it.uniud.newbestsub.problem;

public interface CorrelationStrategy<A,B,C> {

    C computeCorrelation(A firstArray, B secondArray);

}
