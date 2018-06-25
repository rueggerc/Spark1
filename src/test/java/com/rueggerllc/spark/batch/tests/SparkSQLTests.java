package com.rueggerllc.spark.batch.tests;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SparkSQLTests {

	private static Logger logger = Logger.getLogger(SparkSQLTests.class);

	
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
	
	
	// @Test
	// @Ignore
//	public void testReadJSON() {
//		try {
//			
//			Logger.getLogger("org").setLevel(Level.ERROR);
//			System.out.println("================ SPARK SQL BEGIN ===============");
//			SparkSession session = SparkSession.builder().appName("SparkSQLTests").master("local[*]").getOrCreate();
//			DataFrameReader dataFrameReader = session.read();
//
//			// Create Data Frame
//			Dataset<Row> pets = dataFrameReader.schema(buildSchema()).json("input/pets.json");
//			
//			// Schema
//			pets.printSchema();
//			pets.show(10);
//		    
//	        // SELECT * 
//	        // FROM pets
//	        // WHERE species='canine'
//	        System.out.println("=== Display Canines ===");
//	        pets.filter(col("species").equalTo("canine")).show();
//
//			// SELECT avg(weight)
//	        // FROM pets
//	        pets.agg(sum("weight")).show();
//	        
//	        pets.agg(avg("weight")).show();
//	        
//	        
//			session.stop();
//		        
//		} catch (Exception e) {
//			logger.error("ERROR", e);
//		}
//	}
	
//	private static StructType buildSchema() {
//	    StructType schema = new StructType(
//	        new StructField[] {
//	        	DataTypes.createStructField("id", DataTypes.IntegerType, false),
//	            DataTypes.createStructField("species", DataTypes.StringType, false),
//	            DataTypes.createStructField("color", DataTypes.StringType, false),
//	            DataTypes.createStructField("weight", DataTypes.DoubleType, false),
//	            DataTypes.createStructField("name", DataTypes.StringType, false)});
//	    return (schema);
//	}

	
}
