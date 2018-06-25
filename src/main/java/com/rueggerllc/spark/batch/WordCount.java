package com.rueggerllc.spark.batch;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {
	
	private static final Logger logger = Logger.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
    	
    	
        Logger.getLogger("org").setLevel(Level.ERROR);
        
        logger.info("WordCount BEGIN");
        
        if (args.length < 2) {
        	System.out.println("Usage WordCount inputPath outputPath");
        }
        String inputPath = args[0];
        String outputPath = args[1];
        logger.info("inputPath=" + inputPath);
        logger.info("outputPath=" + outputPath);
        
        // SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        SparkConf conf = new SparkConf().setAppName("WordCount");
        
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    logger.info("spark.master=" + conf.get("spark.master"));
	    logger.info("Context Created");
        
	    
	    // JavaRDD<String> lines = sc.textFile("input/hamlet.txt");
	    JavaRDD<String> lines = sc.textFile(inputPath);
	    JavaPairRDD<String, Integer> counts = lines
	    		.flatMap(new MyFlatMapFunction())
	    		.mapToPair(new MyPairFunction())
	    		.reduceByKey(new MyReduceFunction());
        
	    counts.saveAsTextFile(outputPath);
		
		// Done
        if (sc != null) {
        	sc.close();
        }
        
        logger.info("WordCount END");
    }
    
    private static class MyFlatMapFunction implements FlatMapFunction<String,String> {
		@Override
		public Iterator<String> call(String s) throws Exception {
			return Arrays.asList(s.split(" ")).iterator();
		}
    }
    
    private static class MyPairFunction implements PairFunction<String,String,Integer> {
		@Override
		public Tuple2<String, Integer> call(String s) throws Exception {
			return new Tuple2<String,Integer>(s,1);
		}

     }
    
    private static class MyReduceFunction implements Function2<Integer,Integer,Integer> {
		@Override
		public Integer call(Integer v1, Integer v2) throws Exception {
			return v1 + v2;
		}
    }
    
    
    
    
}