import json
import logging
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, window, sum as _sum
from pyspark.sql.types import (FloatType, StringType, StructField, StructType,
                               TimestampType)

from kafka import KafkaProducer

# logging.basicConfig(filename='example.log', level=logging.WARN,
#                     format='%(asctime)s:%(levelname)s:%(message)s')


producer = KafkaProducer(bootstrap_servers=["localhost:9092"])

# Set Spark configuration properties
conf = SparkConf().setAppName("KafkaStreamReader") \
                  .set("spark.sql.shuffle.partitions", "2") \
                  .set("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
                  .set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
                  .set("spark.executor.memory", "1g") \
                  .set("spark.driver.memory", "1g") \
                  .set("spark.memory.offHeap.enabled", "true") \
                  .set("spark.memory.offHeap.size", "1g")
                  .set("driver","/opt/bitnami/spark/drivers/postgresql-42.6.0.jar")

# Create a SparkSession with the specified configuration properties
spark = SparkSession.builder \
    .config(conf=conf) \
    .config("spark.driver.log", "spark_streaming.log") \
    .getOrCreate()


# Create a SparkSession
# spark = SparkSession.builder.appName("KafkaStreamReader").getOrCreate()


def sample(data):
    data = json.loads(data.decode('utf-8'))
    with open("sample.txt", "w+") as f:
        f.write(json.dumps(data))


# Define the Kafka source and configuration
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "SPARK_ANALYSIS",
    "value_deserializer": sample
}


# schema = StructType([
#     StructField("Date", TimestampType(), True),
#     StructField("Symbol", StringType(), True),
#     StructField("Series", StringType(), True),
#     StructField("Open", FloatType(), True),
#     StructField("High", FloatType(), True),
#     StructField("Low", FloatType(), True),
#     StructField("Last", FloatType(), True),
#     StructField("Close", FloatType(), True),
#     StructField("VWAP", FloatType(), True),
#     StructField("Volume", FloatType(), True),
#     StructField("Turnover", FloatType(), True),
#     StructField("Deliverable Volume", FloatType(), True),
#     StructField("%Deliverble", FloatType(), True),
# ])

schema = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("s", StringType(), True),
    StructField("p", FloatType(), True),
    StructField("v", FloatType(), True),
    StructField("c", StringType(), True),
])

# Read data from Kafka as a DataFrame
df = spark.readStream.format("kafka").options(**kafka_params).load()

kafka_df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").withWatermark(
    "timestamp", "20 seconds")


parsed_df = kafka_df.select(from_json("value", schema).alias("data"))

windowed_stream = parsed_df \
    .groupBy(window(col("data.datetime"), "10 seconds", "10 seconds")) \
    .agg(
        avg(col("data.p")),
        _sum(col("data.v"))
    )

# Write to console
console_write = ( windowed_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()
)

# Write to kafka topic
kafka_write = windowed_stream \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("update") \
    .trigger(processingTime="10 seconds") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "SPARK_RESULTS") \
    .start()

kafka_write.awaitTermination()
console_write.awaitTermination()

# stream_input = parsed_df.writeStream.format("console").start()
# stream_input.awaitTermination()


# Start streaming query and display results
# query = parsed_df.selectExpr("data.*").writeStream.format("console").start()
# query.awaitTermination()
