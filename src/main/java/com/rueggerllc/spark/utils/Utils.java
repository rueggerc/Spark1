package com.rueggerllc.spark.utils;

public class Utils {

    private Utils(){
    };

    // a regular expression which matches commas but not commas within double quotations
    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
}
