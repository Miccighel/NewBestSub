package it.uniud.newbestsub.utils;

public class Formula {

    public static int stringComparison(String firstString, String secondString){
        int distance = 0;
        int i = 0;
        while (i < firstString.length()) {
            if (secondString.toCharArray()[i] != firstString.toCharArray()[i] ) {
                distance++;
            }
            i++;
        }
        return distance;
    }

    public static double getMean(double[] run, boolean[] useColumns){

        double mean = 0.0;

        int numberOfUsedCols = 0;
        for(int i=0; i<useColumns.length; i++){
            if(useColumns[i]){
                numberOfUsedCols++;
            }
        }

        for(int i=0; i<run.length;i++){
            if(useColumns[i]){
                mean = mean + run[i];
            }
        }
        mean = mean / ((double) numberOfUsedCols);

        return mean;
    }

    public static double getMean(double[] run, int[] useColumns){

        double mean = 0.0;

        int numberOfUsedCols = 0;
        for(int i=0; i<useColumns.length; i++){
            if(useColumns[i]==1){
                numberOfUsedCols++;
            }
        }

        for(int i=0; i<run.length;i++){
            if(useColumns[i]==1){
                mean = mean + run[i];
            }
        }
        mean = mean / ((double) numberOfUsedCols);

        return mean;
    }

}
