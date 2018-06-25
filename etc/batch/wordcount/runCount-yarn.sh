#!/bin/bash
MASTER=yarn
JAR_FILE=/home/hadoop/code/Spark1/build/libs/Spark1-all-1.0.jar

INPUT_PATH=hdfs://captain:9000/inputs/hamlet.txt
OUTPUT_PATH=hdfs://captain:9000/outputs/WordCount/

# Remove output
rm -f out
hdfs dfs -rm -r /outputs/WordCount

# Submit
spark-submit --class com.rueggerllc.spark.batch.WordCount --master $MASTER $JAR_FILE $INPUT_PATH $OUTPUT_PATH

# Output Results
hdfs dfs -cat /outputs/WordCount/part* > out

