import json

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, desc, from_json, window, to_json, struct
from pyspark.sql.types import (FloatType, StringType, StructField, StructType,
                               TimestampType)
from pyspark.streaming import StreamingContext

from kafka import KafkaProducer
import time


producer = KafkaProducer(bootstrap_servers=["localhost:9092"])

# Set Spark configuration properties
conf = SparkConf().setAppName("KafkaStreamReader") \
                  .set("spark.sql.shuffle.partitions", "2") \
                  .set("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
                  .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
                  .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")

# Create a SparkSession with the specified configuration properties

spark = SparkSession.builder.config(conf=conf).getOrCreate()


# Create a SparkSession
# spark = SparkSession.builder.appName("KafkaStreamReader").getOrCreate()


def sample(data):
    data = json.loads(data.decode('utf-8'))
    with open("sample.txt", "w+") as f:
        f.write(json.dumps(data))


# Define the Kafka source and configuration
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "adaniports_input",
    "value_deserializer": sample
}


schema = StructType([
    StructField("Date", TimestampType(), True),
    StructField("Symbol", StringType(), True),
    StructField("Series", StringType(), True),
    StructField("Open", FloatType(), True),
    StructField("High", FloatType(), True),
    StructField("Low", FloatType(), True),
    StructField("Last", FloatType(), True),
    StructField("Close", FloatType(), True),
    StructField("VWAP", FloatType(), True),
    StructField("Volume", FloatType(), True),
    StructField("Turnover", FloatType(), True),
    StructField("Deliverable Volume", FloatType(), True),
    StructField("%Deliverble", FloatType(), True),
])

# Read data from Kafka as a DataFrame
df = spark.readStream.format("kafka").options(**kafka_params).load()

kafka_df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").withWatermark(
    "timestamp", "20 seconds")
# watermark_df = kafka_df.withWatermark("timestamp", "10 seconds")


parsed_df = kafka_df.select(from_json("value", schema).alias("data"))

windowed_stream = parsed_df \
    .groupBy(window(col("data.Date"), "10 seconds", "10 seconds")) \
    .agg(
        avg(col("data.Open")),
        avg(col("data.Open")),
        avg(col("data.Open")),
    )

# write_stream = ( windowed_stream.writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", False) \
#     .trigger(processingTime="10 seconds") \
#     .start()
# )

#   .selectExpr("CAST(value AS STRING)") \
result = windowed_stream \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("update") \
    .trigger(processingTime="10 seconds") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "adaniports_results") \
    .start()


result.awaitTermination()

# stream_input = parsed_df.writeStream.format("console").start()

# Write the data to a new Kafka topic

# avg_values = (
#     windowed_stream.writeStream.outputMode("complete")
# .format("console")
# .option("truncate", False)
# .trigger(processingTime="10 seconds")
#     .start()
# )

# avg_values.awaitTermination()


# stream_input.awaitTermination()
# Start streaming query and display results
# query = parsed_df.writeStream.format("console").start()
# query.awaitTermination()


# ssc.start()
# write_stream.awaitTermination()
