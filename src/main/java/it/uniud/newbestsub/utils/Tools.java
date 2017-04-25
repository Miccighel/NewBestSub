package it.uniud.newbestsub.utils;

public class Tools {

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

}
