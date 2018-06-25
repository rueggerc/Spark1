package com.rueggerllc.spark.batch.tests;

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import scala.Tuple2;

public class BatchTests {

	private static Logger logger = Logger.getLogger(BatchTests.class);
	private static final Pattern SPACE = Pattern.compile(" ");

	
	@BeforeClass
	public static void setupClass() throws Exception {
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
	}

	@Before
	public void setupTest() throws Exception {
	}

	@After
	public void tearDownTest() throws Exception {
	}
	
	@Test
	@Ignore
	public void testDummy() {
		logger.info("Dummy Test Begin");
	}
	
	@Test
	// @Ignore
	public void testSocketWordCount() {
		try {
			logger.info("SocketWordCount BEGIN");
			
//			SparkConf conf = new SparkConf().setAppName("StreamingWordCount");
//			conf.setMaster("localhost[*]");
//			JavaSparkContext sc = new JavaSparkContext(conf);
			  
			// Create the context with a 1 second batch size
			SparkConf sparkConf = new SparkConf().setAppName("StreamingWorCount");
			// sparkConf.set("spark.driver.allowMultipleContexts","true");
			sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true");
			sparkConf.setMaster("local[*]");
			JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(5000));
			
			String host = "localhost";
			String port = "9999";
			logger.info("host=" + host);
			logger.info("port=" + port);			

			// DStream: 
			// Create a JavaReceiverInputDStream on target ip:port and count the
			// words in input stream of \n delimited text (eg. generated by 'nc')
			// Note that no duplication in storage level only for running locally.
			// Replication necessary in distributed scenario for fault tolerance.
			JavaReceiverInputDStream<String> lines = 
				ssc.socketTextStream(host, Integer.parseInt(port), StorageLevels.MEMORY_AND_DISK_SER);
			JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
			JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);

			// wordCounts.foreachRDD(foreachFunc);
			    
			// Display Output
			wordCounts.print();
			    
			// Start Program
			ssc.start();
			ssc.awaitTermination();
			    
			if (ssc != null) {
				ssc.close();
			}
		        
			logger.info("==== StreamingWordCount END ====");
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	
		
	
	
}