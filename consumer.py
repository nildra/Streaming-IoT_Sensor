import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from pyspark.sql.window import Window

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("""
        Usage: consumer.py <bootstrap-servers> <subscribe-type> <topic_temp> <topic_uv>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    #subscribeType should be "subscribe" in this exercise.
    subscribeType = sys.argv[2]
    topic_temp = sys.argv[3]
    topic_uv = sys.argv[4]

    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    schema_topic_temp = StructType([
        StructField("datestamp", TimestampType(), True), 
        StructField("temperature", IntegerType(), True),
        StructField("humidity", IntegerType(), True)
    ])

    schema_topic_uv = StructType([
        StructField("datestamp", TimestampType(), True),  
        StructField("uv", IntegerType(), True),
        StructField("wind", IntegerType(), True)
    ])

    # Create DataSet representing the stream of input lines from kafka
    lines_topic_temp = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option("kafka.sasl.mechanism", "PLAIN")\
        .option("kafka.security.protocol", "SASL_SSL")\
        .option("kafka.sasl.jaas.config", """org.apache.kafka.common.security.scram.ScramLoginModule required username="USERNAME" password="PASSWORD";""")\
        .option("startingOffsets", "earliest")\
        .option(subscribeType, topic_temp)\
        .option("auto.offset.reset", "earliest")\
        .load()\
        .selectExpr("CAST(value AS STRING)")
    
    lines_topic_uv = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option("kafka.sasl.mechanism", "PLAIN")\
        .option("kafka.security.protocol", "SASL_SSL")\
        .option("kafka.sasl.jaas.config", """org.apache.kafka.common.security.scram.ScramLoginModule required username="USERNAME" password="PASSWORD";""")\
        .option("startingOffsets", "earliest")\
        .option(subscribeType, topic_uv)\
        .option("auto.offset.reset", "earliest")\
        .load()\
        .selectExpr("CAST(value AS STRING)")
    
    windowed_temp = lines_topic_temp\
            .select(F.from_json(lines_topic_temp.value, schema_topic_temp).alias("data")).select("data.*")\
            .withWatermark("datestamp", "30 minutes") \
             
    windowed_uv = lines_topic_uv\
            .select(F.from_json(lines_topic_uv.value, schema_topic_uv).alias("data")).select("data.*")\
            .withWatermark("datestamp", "30 minutes") \
        
    json_df = windowed_temp.join(windowed_uv, on="datestamp", how="inner")
    
    def process_batch(batch_df, batch_id):
        try:
            print(f"### Batch ID {batch_id} ###")
            # Conditions that indicate thermal stress risks for crops
            # Query 1) Identify hours where temperature is under 0°C
            temp_sensor_uzero = batch_df.filter(batch_df.temperature < 0)
            # Query 2) Identify hours where temperature is above 5°C
            temp_sensor_afive = batch_df.filter(batch_df.temperature > 5)
            # Query 3) Identify hours where temperature is above 8°C and UV index is above 3
            stress_conditions = batch_df.filter((batch_df.temperature > 8) & (batch_df.uv > 3))
    
            if temp_sensor_uzero.count() > 0:
                print(f"/!\ Temperature is below 0°C !")
                temp_sensor_uzero.write.format("console").save()
            if temp_sensor_afive.count() > 0:
                print(f"/!\ Temperature is above 5°C !")
                temp_sensor_afive.write.format("console").save()
            if stress_conditions.count() > 0:
                print(f"/!\ Temperature is above 8°C and UV index is above 3 !")
                stress_conditions.write.format("console").save()
        except Exception as e:
            print(f"Error while processing batch {batch_id} : {e}")
    
    query_1 = json_df.writeStream\
        .outputMode("append")\
        .foreachBatch(process_batch)\
        .start()

    query_1.awaitTermination()
