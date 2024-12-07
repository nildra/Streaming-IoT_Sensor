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
        Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topic_temp> <topic_uv>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    #subscribeType should be "subscribe" in this exercise.
    subscribeType = sys.argv[2]
    topic_1 = sys.argv[3]
    topic_2 = sys.argv[4]

    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    schema_topic_1 = StructType([
        StructField("datestamp", TimestampType(), True), 
        StructField("temperature", IntegerType(), True),
        StructField("humidity", IntegerType(), True)
    ])

    schema_topic_2 = StructType([
        StructField("datestamp", TimestampType(), True),  
        StructField("uv", IntegerType(), True),
        StructField("wind", IntegerType(), True)
    ])

    # Create DataSet representing the stream of input lines from kafka
    
    lines_topic_1 = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option("kafka.sasl.mechanism", "PLAIN")\
        .option("kafka.security.protocol", "SASL_SSL")\
        .option("kafka.sasl.jaas.config", """org.apache.kafka.common.security.scram.ScramLoginModule required username="ZBJ7RMDHM5E2YXBE" password="ZgcrCbBCsUWYBT6x4YmkV8j+Rq7/xojOiWQ9H7Bd27ScgZIxVOtNxIPjoZMweuHw";""")\
        .option("startingOffsets", "earliest")\
        .option(subscribeType, topic_1)\
        .option("auto.offset.reset", "earliest")\
        .load()\
        .selectExpr("CAST(value AS STRING)")
    
    lines_topic_2 = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option("kafka.sasl.mechanism", "PLAIN")\
        .option("kafka.security.protocol", "SASL_SSL")\
        .option("kafka.sasl.jaas.config", """org.apache.kafka.common.security.scram.ScramLoginModule required username="ZBJ7RMDHM5E2YXBE" password="ZgcrCbBCsUWYBT6x4YmkV8j+Rq7/xojOiWQ9H7Bd27ScgZIxVOtNxIPjoZMweuHw";""")\
        .option("startingOffsets", "earliest")\
        .option(subscribeType, topic_2)\
        .option("auto.offset.reset", "earliest")\
        .load()\
        .selectExpr("CAST(value AS STRING)")
    
    
    json_df_topic_1 = lines_topic_1\
            .select(F.from_json(lines_topic_1.value, schema_topic_2).alias("data")).select("data.*")                
    json_df_topic_2 = lines_topic_2\
            .select(F.from_json(lines_topic_2.value, schema_topic_1).alias("data")).select("data.*")
        
    json_df = json_df_topic_2.join(json_df_topic_1, on="datestamp", how="inner")
    
    def process_batch(batch_df, batch_id):
        print(f"### Batch ID {batch_id} ###")
        # Conditions that indicate thermal stress risks for crops
        # Query 1) Identify days where temperature is under 0°C
        positive_cold_room_uzero = batch_df.filter(batch_df.temperature < 0)
        # Query 2) Identify days where temperature is above 5°C
        positive_cold_room_afour = batch_df.filter(batch_df.temperature > 5)
        # Query 3) Identify days where temperature is above 8° and UV index is above 3
        stress_conditions = batch_df.filter((batch_df.temperature > 8) & (batch_df.uv > 3))

        if positive_cold_room_uzero.count() > 0:
            print(f"/!\ Temperature is below 0°C !")
            positive_cold_room_uzero.write.format("console").save()
        if positive_cold_room_afour.count() > 0:
            print(f"/!\ Temperature is above 5°C !")
            positive_cold_room_afour.write.format("console").save()
        if stress_conditions.count() > 0:
            print(f"/!\ Temperature is above 8°C and UV index is above 3 !")
            stress_conditions.write.format("console").save()
    
    query_1 = json_df.writeStream\
        .outputMode("append")\
        .foreachBatch(process_batch)\
        .start()

    query_1.awaitTermination()

