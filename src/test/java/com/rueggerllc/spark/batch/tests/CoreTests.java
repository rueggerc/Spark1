package com.rueggerllc.spark.batch.tests;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.rueggerllc.spark.beans.MyFilter;

import scala.Tuple2;

public class CoreTests {

	private static Logger logger = Logger.getLogger(CoreTests.class);

	
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
	// @Ignore
	public void testDummy() {
		logger.info("Dummy Test Begin");
	}
	
	@Test
	// @Ignore
	public void testGetTimeStamp() {
		logger.info("NOW=" + getNow());
	}
	
	private String getNow() {
		SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		Date now = Calendar.getInstance().getTime();
		return format.format(now);
	}
	
	@Test
	@Ignore
	public void testWordCount() {
		try {
	        Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);

	        JavaRDD<String> lines = sc.textFile("input/word_count.text");
	        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

	        Map<String, Long> wordCounts = words.countByValue();
	        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
	            System.out.println(entry.getKey() + " : " + entry.getValue());
	        }
	        
	        
	        logger.info("====== Map Reduce ====");
		    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
		    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
		    
		    counts.foreach(data -> {
		        logger.info("word="+data._1() + " count=" + data._2());
		    }); 
	        
	        
		    
		} catch (Exception e) {
			logger.error("Error", e);
		}
	}

	
	@Test
	@Ignore
	public void testLamda1() {
		logger.info("Test Lambda1");
		Predicate<Integer> isOdd = n -> n % 2 != 0;
	}
	
	@Test
	@Ignore
	public void testSplitLogEntries() {
		try {
			logger.info("=== SPLIT LOG ENTRIES BEGIN");
			String line = "199.72.81.55	-	804571201	GET	/history/apollo/	200	6245";
			String[] splits = line.split("\\s+");
			logger.info("Host=" + splits[0]);
		} catch (Exception e) {
			
		}
	}
	
	@Test
	@Ignore
	public void testFlatMap() {
		
		try {
			Logger.getLogger("org").setLevel(Level.ERROR);
			
			List<String> thePets = Arrays.asList("Captain and the Foos", "later Much", "the Dakota", "Foos Oscar");
			
		    SparkConf conf = new SparkConf().setAppName("myFlatMap").setMaster("local[2]");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		   
		    JavaRDD<String> lines = sc.parallelize(thePets);
		    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		    
//		    logger.info("========= Test FlatMap BEGIN ===========");
//		    // Collect RDD for printing
//	        for(String word : words.collect()){
//	            logger.info(word);
//	        }		    
//	        logger.info("========= Test FlatMap END ===========");
	        
	        Map<String, Long> wordCounts = words.countByValue();
	        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
	            logger.info(entry.getKey() + " : " + entry.getValue());
	        }
	        
		    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
		    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
		    
		    counts.foreach(data -> {
		        logger.info("word="+data._1() + " count=" + data._2());
		    }); 
		    
		    
		} catch (Exception e) {
			logger.error("Error", e);
		}
		

	}
	
	@Test
	@Ignore
	public void testSparkCentOS1() {
		
		try {
			Logger.getLogger("org").setLevel(Level.ERROR);
			
			SparkConf conf = new SparkConf().setAppName("myFlatMap").setMaster("local[2]");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		    
		    JavaRDD<String> lines = sc.textFile("hdfs://192.168.243.128:9000/my_storage/word_count.text");
		    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		    
	        Map<String, Long> wordCounts = words.countByValue();
	        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
	            logger.info(entry.getKey() + " : " + entry.getValue());
	        }
		    
		} catch (Exception e) {
			logger.error("Error", e);
		}
		

	}
	
	@Test
	@Ignore
	public void testSpark() {
		
		try {
			Logger.getLogger("org").setLevel(Level.ERROR);
			
	
		    SparkConf conf = new SparkConf().setAppName("mySparkTest")
		    		         .setMaster("spark://192.168.243.123:7077")
		    		         .set("spark.akka.heartbeat.interval", "100")
		    		         .set("spark.local.ip", "127.0.0.1");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		    
		    JavaRDD<String> lines = sc.textFile("hdfs://192.168.243.123:9000/my_storage/word_count.text");
		    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		    
		    logger.info("========= Test FlatMap BEGIN ===========");
		    
		    // Collect RDD for printing
	        for(String word : words.collect()){
	            logger.info(word);
	        }		    
	        logger.info("========= Test FlatMap END ===========");
	        
	        
	        Map<String, Long> wordCounts = words.countByValue();
	        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
	            logger.info(entry.getKey() + " : " + entry.getValue());
	        }
		    
		} catch (Exception e) {
			logger.error("Error", e);
		}
		

	}
	
	
	
	
}
